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

use indexer_config::{RarityConfig, ReconcileConfig};
use indexer_das::backfill::{self, BackfillOptions};
use indexer_das::DasClient;
use indexer_data_model::{ingest_state, rarity, registry, PgPool};
use serde_json::json;
use tokio::sync::watch;

use crate::pipeline::Pipeline;
use crate::{probe, reconcile};

/// How often due-ness is checked. Well under the shortest useful interval, so
/// a job starts close to when it falls due without polling the database hard.
///
/// 10 s rather than 60: the tip probe's default interval is 30 s, and a 60 s
/// tick would swallow it whole — the configured interval has to be the thing
/// that governs, not the tick. Each tick is one indexed `min(finished_at)`
/// query per job.
const TICK: Duration = Duration::from_secs(10);

/// Runs the schedule until the shutdown signal fires.
///
/// Errors are logged and the loop continues: a reconciliation that cannot
/// reach DAS must not take the live pipeline down with it, and the next tick
/// is a minute away.
pub async fn run(
    pool: PgPool,
    das: DasClient,
    config: ReconcileConfig,
    rarity_config: RarityConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    // Two independent schedules share one task. The early return is per
    // schedule, not for the whole loop: RECONCILE_INTERVAL_SECS=0 must not
    // silently freeze ranks, which is a different subsystem with a different
    // knob.
    if !config.enabled() {
        log::info!("periodic reconciliation disabled (RECONCILE_INTERVAL_SECS=0)");
    }
    if !rarity_config.enabled() {
        log::info!("periodic rarity drain disabled (RARITY_INTERVAL_SECS=0)");
    }
    if !config.tip_enabled() {
        log::info!("reconciliation tip probe disabled (RECONCILE_TIP_INTERVAL_SECS=0)");
    }
    if !config.enabled() && !rarity_config.enabled() && !config.tip_enabled() {
        return;
    }
    let tip_every = Duration::from_secs(config.tip_interval_secs);
    let rarity_every = Duration::from_secs(rarity_config.interval_secs);
    let sweep_every = Duration::from_secs(config.interval_secs);
    let deep_every = Duration::from_secs(config.deep_interval_secs);
    log::info!(
        "tip probe every {}s, reconciling every {}s, deep pass every {}s, at {} rpc/s",
        tip_every.as_secs(),
        sweep_every.as_secs(),
        deep_every.as_secs(),
        config.rps
    );

    // `das` arrives already rate-limited, and deliberately as the same
    // instance the consumer's spawned reconcile holds: `with_rate_limit` here
    // would install a *second* limiter and the two halves would together ask
    // for twice `config.rps`. Throttled at all because the live writer is not,
    // and this is the half that can wait.
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
                // The rarity drain first, and every tick: the flag is the
                // trigger, so a collection a writer touched is re-ranked on the
                // next minute rather than at the interval. The interval is only
                // the backstop for a flag that was somehow missed. It is also
                // the cheapest job (~110 ms for everything) and needs no DAS,
                // so it runs even when Helius is down.
                if rarity_config.enabled()
                    && !run_job("rarity drain", drain_rarity(&pool, rarity_every), &mut shutdown)
                        .await
                {
                    return;
                }
                // The tip probe before the full sweep: it is 3 calls against
                // 23 and answers the same question sooner. They share the DAS
                // rate budget, and `run_job` runs one job at a time, so a
                // sweep in progress simply delays the next probe by its own
                // duration rather than competing with it.
                if config.tip_enabled()
                    && due(&pool, probe::KIND, tip_every).await
                    && !run_job("tip probe", tip(&pool, &das), &mut shutdown).await
                {
                    return;
                }
                if config.enabled()
                    && due(&pool, reconcile::KIND, sweep_every).await
                    && !run_job("scheduled reconcile", sweep(&pool, &das), &mut shutdown).await
                {
                    return;
                }
                if config.enabled()
                    && due(&pool, reconcile::DEEP_KIND, deep_every).await
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

/// The tip probe: what moved since we last looked, without re-reading
/// everything to find out.
async fn tip(pool: &PgPool, das: &DasClient) -> anyhow::Result<()> {
    let started_at = chrono::Utc::now();
    // Its own pipeline, for the same reason the sweep builds one: a fresh
    // `DecodeContext` picks up registry changes without waiting for a restart.
    let pipeline = Pipeline::new(
        pool.clone(),
        das.clone(),
        reconcile::context(pool).await?,
        "reconcile",
    );
    let report = probe::run(pool, das, &pipeline).await?;
    if !report.is_noop() {
        report.log("tip probe");
    }
    probe::write_state(pool, &report, started_at).await?;
    Ok(())
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

/// `backfill_state.kind` for the rarity pass.
///
/// Its own kind, not the reconcile one: a *failing* backfill still stamps
/// `finished_at`, so sharing a kind would make a collection whose ranks were
/// never computed look freshly ranked.
pub const RARITY_KIND: &str = "rarity";

/// Recomputes every collection a writer flagged, plus every collection whose
/// last pass is older than the backstop interval.
///
/// The flag is the real trigger — `assets::upsert_batch`,
/// `assets::set_membership_and_flag` and `attributes::sync_trait_facets_and_flag`
/// set it transactionally with the write that invalidated the ranks — so this
/// is usually a single cheap query returning nothing.
async fn drain_rarity(pool: &PgPool, backstop: Duration) -> anyhow::Result<()> {
    let mut targets = rarity::dirty_collections(pool).await?;
    if due(pool, RARITY_KIND, backstop).await {
        for collection in registry::list_enabled(pool).await? {
            if !targets.contains(&collection.id) {
                targets.push(collection.id);
            }
        }
        targets.sort_unstable();
    }
    for collection_id in targets {
        let started_at = chrono::Utc::now();
        if !rarity::is_rankable(pool, collection_id).await? {
            // Nothing to rank, and nothing wrong: a collection whose metadata
            // host is gone carries no facetable trait type. Clear the flag so
            // the drain does not spin on it, and leave the ranks null.
            rarity::clear_dirty(pool, collection_id).await?;
            continue;
        }
        let outcome = rarity::recompute(pool, collection_id).await?;
        if outcome.skipped {
            // Another replica holds the lock during a rolling deploy. The flag
            // still stands, so the winner's pass covers this one.
            log::info!("rarity: collection {collection_id} is being ranked elsewhere");
            continue;
        }
        if outcome.changed > 0 {
            log::info!(
                "rarity: collection {collection_id} re-ranked {} of {} asset(s), version {}",
                outcome.changed,
                outcome.ranked,
                outcome.version
            );
        }
        let state = ingest_state::BackfillState {
            collection_id,
            kind: RARITY_KIND.to_string(),
            status: "done".to_string(),
            cursor: json!({"mode": "rarity"}),
            progress: json!({
                "ranked": outcome.ranked,
                // The same word every other periodic job uses for "rows this
                // run had to fix", so one metric spans them all.
                "corrections": outcome.changed,
                "version": outcome.version,
                "duration_ms": (chrono::Utc::now() - started_at).num_milliseconds(),
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
