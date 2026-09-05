//! The periodic reconciliation (ALG-624).
//!
//! Reconciling only on `StreamStatus::Connected` means a process that stays
//! connected for a week never reconciles at all, and this transport has no
//! replay to fall back on. So the same sweep runs on a schedule, plus a
//! weekly deep pass over supply, burned assets and attributes.
//!
//! Three deliberate choices:
//!
//! - **Spawned, not another arm of the consumer's `select!`.** The `Connected`
//!   reconcile is `await`ed inline, which stalls event handling for the length
//!   of a full sweep; doing that every hour would stall it on a timer.
//!   `.railway/railway.ts` already budgets the connection for it — "one for the
//!   live writer, one for the concurrent reconciler, one spare".
//! - **Due-ness is durable.** The supervisor restarts the consumer with a
//!   backoff, so an in-memory `Instant` would be reset by every crash and a
//!   flapping ingester would either never reconcile or reconcile constantly.
//!   The schedule reads `backfill_state.finished_at` instead.
//! - **Throttled where the live path is not.** The sweep shares a Helius rate
//!   budget with live ingestion, and it is the half that can afford to wait.
//!
//! The deep pass is [`indexer_das::backfill::run`] rather than a
//! reimplementation: it already diffs supply, lands new burns, re-fetches only
//! the documents whose URI changed, and reports through the same
//! `BatchCounts` this crate uses for corrections.

use std::time::Duration;

use indexer_config::ReconcileConfig;
use indexer_das::backfill::{self, BackfillOptions};
use indexer_das::DasClient;
use indexer_data_model::{ingest_state, PgPool};
use serde_json::json;
use tokio::sync::watch;

use crate::pipeline::Pipeline;
use crate::reconcile;

/// How often due-ness is checked. Well under the shortest useful interval, so
/// a job starts close to when it falls due without polling the database hard.
const TICK: Duration = Duration::from_secs(60);

/// Runs the schedule until the shutdown signal fires.
///
/// Errors are logged and the loop continues: a reconciliation that cannot
/// reach DAS must not take the live pipeline down with it, and the next tick
/// is a minute away.
pub async fn run(
    pool: PgPool,
    das: DasClient,
    config: ReconcileConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    if !config.enabled() {
        log::info!("periodic reconciliation disabled (RECONCILE_INTERVAL_SECS=0)");
        return;
    }
    let sweep_every = Duration::from_secs(config.interval_secs);
    let deep_every = Duration::from_secs(config.deep_interval_secs);
    log::info!(
        "reconciling every {}s, deep pass every {}s, at {} rpc/s",
        sweep_every.as_secs(),
        deep_every.as_secs(),
        config.rps
    );

    // Throttled: the live writer is not, and this is the half that can wait.
    let das = das.with_rate_limit(config.rps);
    let mut tick = tokio::time::interval(TICK);
    // `interval` fires immediately; the boot-time reconcile is the consumer's
    // job on `Connected`, so the first scheduled run is one interval away.
    tick.tick().await;

    loop {
        tokio::select! {
            biased;

            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    log::info!("reconciliation schedule stopping");
                    return;
                }
            }

            _ = tick.tick() => {
                if due(&pool, reconcile::KIND, sweep_every).await
                    && !run_job("scheduled reconcile", sweep(&pool, &das), &mut shutdown).await
                {
                    return;
                }
                if due(&pool, reconcile::DEEP_KIND, deep_every).await
                    && !run_job("deep reconcile", deep(&pool, &das), &mut shutdown).await
                {
                    return;
                }
            }
        }
    }
}

/// Runs one job, abandoning it if the shutdown signal fires first.
///
/// Returns `false` when it was abandoned, which is the caller's cue to stop.
/// A sweep takes tens of seconds, and this binary is PID 1 under the
/// Dockerfile: without this, a redeploy's SIGTERM would wait out the sweep and
/// risk being SIGKILLed mid-run. Abandoning is safe because every step commits
/// its own transaction — the next run picks up from the same state, and
/// `backfill_state.finished_at` is only written when a run completes, so an
/// abandoned job stays due.
async fn run_job(
    label: &str,
    job: impl std::future::Future<Output = anyhow::Result<()>>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        biased;

        _ = shutdown.changed() => {
            if *shutdown.borrow() {
                log::info!("shutdown during {label}; abandoning it");
                return false;
            }
            true
        }

        result = job => {
            if let Err(error) = result {
                log::error!("{label} failed: {error:#}");
            }
            true
        }
    }
}

/// Has `interval` passed since every enabled collection last finished this
/// job?
///
/// A collection with no record of the job is seeded as "finished now" rather
/// than treated as due, so the first run after a fresh deploy lands one
/// interval out instead of immediately. Startup is already covered by the
/// reconcile the consumer runs on `Connected`; without the seed, a new
/// deployment would also kick off a full deep pass a minute after boot.
async fn due(pool: &PgPool, kind: &str, interval: Duration) -> bool {
    match ingest_state::last_finished(pool, kind).await {
        Ok(None) => {
            match ingest_state::seed_schedule(pool, kind).await {
                Ok(seeded) if seeded > 0 => {
                    log::info!("scheduling {kind} to first run in {}s", interval.as_secs())
                }
                Ok(_) => {}
                Err(error) => log::warn!("could not seed the {kind} schedule: {error}"),
            }
            false
        }
        Ok(Some(finished)) => {
            let elapsed = chrono::Utc::now().signed_duration_since(finished);
            elapsed.to_std().map(|e| e >= interval).unwrap_or(false)
        }
        Err(error) => {
            log::warn!("could not read the {kind} schedule: {error}");
            false
        }
    }
}

/// The hourly state sweep plus targeted activity recovery.
async fn sweep(pool: &PgPool, das: &DasClient) -> anyhow::Result<()> {
    // Its own pipeline: the consumer owns its `DecodeContext` mutably, and a
    // fresh one also picks up registry changes without waiting for a restart.
    let pipeline = Pipeline::new(
        pool.clone(),
        das.clone(),
        reconcile::context(pool).await?,
        "reconcile",
    );
    let from = ingest_state::last_processed_slot(pool, crate::consumer::STREAM).await?;
    let report = reconcile::run(pool, das, &pipeline, from).await?;
    report.log("scheduled reconcile");
    Ok(())
}

/// The weekly deep pass: supply, burned assets and attribute changes, through
/// the DAS backfill's own idempotent path.
async fn deep(pool: &PgPool, das: &DasClient) -> anyhow::Result<()> {
    let started_at = chrono::Utc::now();
    let report = backfill::run(pool, das, &BackfillOptions::default(), |_| {}).await?;
    let totals = report.totals();
    log::info!(
        "deep reconcile finished: inserted={} updated={} unchanged={} attributes=+{}/-{} \
         documents={} corrections={}",
        totals.inserted,
        totals.updated,
        totals.unchanged,
        totals.attributes_written,
        totals.attributes_removed,
        totals.documents,
        u64::from(!report.is_noop()),
    );
    for warning in &report.warnings {
        log::warn!("deep reconcile: {warning}");
    }

    // Its own `kind`, so the deep pass's cadence is readable separately from
    // the backfill row it also refreshes.
    for collection in &report.collections {
        for warning in &collection.warnings {
            log::warn!("deep reconcile {}: {warning}", collection.slug);
        }
        let Some(id) = collection_id(pool, &collection.slug).await else {
            continue;
        };
        let state = ingest_state::BackfillState {
            collection_id: id,
            kind: reconcile::DEEP_KIND.to_string(),
            status: collection.status.clone(),
            cursor: json!({"mode": "reconcile_deep"}),
            progress: json!({
                "members": collection.members,
                "inserted": collection.counts.inserted,
                "updated": collection.counts.updated,
                "unchanged": collection.counts.unchanged,
                "attributes_written": collection.counts.attributes_written,
                "attributes_removed": collection.counts.attributes_removed,
                "documents": collection.counts.documents,
                "documents_failed": collection.documents_failed,
                "missing": collection.missing_total,
                "corrections": collection.counts.inserted
                    + collection.counts.updated
                    + collection.counts.attributes_written
                    + collection.counts.attributes_removed
                    + collection.counts.documents,
                "duration_ms": collection.elapsed.as_millis(),
            }),
            last_error: None,
            started_at: Some(started_at),
            finished_at: Some(chrono::Utc::now()),
            updated_at: chrono::Utc::now(),
        };
        ingest_state::put_backfill_state(pool, &state).await?;
    }
    Ok(())
}

async fn collection_id(pool: &PgPool, slug: &str) -> Option<i32> {
    indexer_data_model::registry::by_slug(pool, slug)
        .await
        .ok()
        .flatten()
        .map(|c| c.id)
}
