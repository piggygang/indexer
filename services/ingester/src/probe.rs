//! The reconciliation tip probe: ask DAS what changed, instead of re-reading
//! everything to find out.
//!
//! The full sweep reads all 17,820 tracked assets through `getAssetBatch` —
//! 23 RPC calls and ~35 s — to discover the handful that moved. `searchAssets`
//! sorted by `recent_action` answers the same question directly: **3 calls and
//! ~1.4 s**, with every stale asset measured inside the first five results.
//! That is what makes a 30-second cadence affordable where an hourly full
//! sweep was not.
//!
//! Two properties this leans on, and their limits:
//!
//! * **`recent_action` is a tip, not a cursor.** The ordering is not stable
//!   under concurrent activity and there is no "changed since" bound, so the
//!   page is read once and never paged through. The stopping rule is
//!   empirical — read down until enough consecutive assets already agree —
//!   which is why the full sweep stays as the backstop rather than being
//!   deleted.
//! * **It finds *disagreements*, not events.** An asset that left and came
//!   back inside one interval agrees with DAS and stays invisible, exactly as
//!   `reconcile`'s own state diff does.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use anyhow::Context;
use indexer_das::{DasClient, SearchFilter};
use indexer_data_model::activity::{self, AssetRef};
use indexer_data_model::assets::{self, StateInput};
use indexer_data_model::types::MembershipRule;
use indexer_data_model::{registry, PgPool};
use serde_json::json;

use crate::pipeline::{Outcome, Pipeline};
use crate::reconcile;

/// `backfill_state.kind` for the probe, so its cadence and counters read
/// separately from the full sweep's.
pub const KIND: &str = "reconcile_tip";

/// Assets read per filter. One page, never paged.
pub const PAGE: u32 = 100;

/// Consecutive already-agreeing assets that end the walk.
///
/// Every disagreement measured on mainnet sat within the first five results,
/// so 20 is roughly a 4× margin. Too small and a burst of unrelated activity
/// hides a real change behind it; too large and the probe reads a page it did
/// not need.
pub const AGREE_STREAK: usize = 20;

/// Never-crawled assets given a full history walk per run.
///
/// A probe hit with no recorded activity has no useful floor, so recovering it
/// means walking its whole signature history — affordable for a handful,
/// ruinous for thousands. The rest keep their corrected owner and wait for the
/// next run.
const MAX_DEEP_CRAWLS: usize = 5;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    /// Distinct DAS queries issued — one per filter.
    pub targets: usize,
    /// Assets read across all pages before the stopping rule fired.
    pub scanned: usize,
    /// Of those, ones we track.
    pub tracked: usize,
    /// Ones whose owner or burned flag disagreed with the database.
    pub stale: usize,
    /// Assets given a full history walk because they had no floor.
    pub deep_crawled: usize,
    /// Rows whose owner or burned flag actually moved.
    pub updated: u64,
    pub activity: Outcome,
    pub elapsed_ms: u128,
}

impl Report {
    /// Events the recovery walk wrote for what the probe found.
    pub fn recovered_activity(&self) -> u64 {
        self.activity.recorded
    }

    pub fn is_noop(&self) -> bool {
        self.stale == 0 && self.updated == 0 && self.activity.is_noop()
    }

    pub fn log(&self, label: &str) {
        log::info!(
            "{label} finished: targets={} scanned={} tracked={} stale={} deep={} \
             updated={} recorded={} in {}ms",
            self.targets,
            self.scanned,
            self.tracked,
            self.stale,
            self.deep_crawled,
            self.updated,
            self.activity.recorded,
            self.elapsed_ms,
        );
    }
}

/// The DAS queries that cover every enabled collection.
///
/// Derived from the registry by `match`ing on [`MembershipRule`], one arm per
/// rule and never on a slug — the same discipline `spec::addresses_for`
/// follows. Deduplicated, because two collections can share a verified creator
/// and one query then answers for both.
pub async fn targets(pool: &PgPool) -> anyhow::Result<Vec<SearchFilter>> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for collection in registry::list_enabled(pool).await? {
        let Some(rule) = collection.membership_rule else {
            continue;
        };
        let filter = match rule {
            // No certified collection: the verified creator is the only
            // collection-wide handle that exists.
            MembershipRule::TmAllowlist => collection
                .verified_creator
                .clone()
                .map(SearchFilter::Creator),
            // Both of these are addressed by their collection account.
            MembershipRule::TmCollection | MembershipRule::CoreCollection => {
                collection.address.clone().map(SearchFilter::Collection)
            }
        };
        let Some(filter) = filter else {
            log::warn!(
                "{}: rule {rule:?} with no address or creator — not probed",
                collection.slug
            );
            continue;
        };
        if seen.insert(format!("{filter:?}")) {
            out.push(filter);
        }
    }
    Ok(out)
}

/// One probe pass: read each filter's newest assets, correct what disagrees.
pub async fn run(pool: &PgPool, das: &DasClient, pipeline: &Pipeline) -> anyhow::Result<Report> {
    let started = Instant::now();
    let mut report = Report::default();
    let mut stale: Vec<(AssetRef, indexer_das::Asset)> = Vec::new();

    for filter in targets(pool).await? {
        report.targets += 1;
        let page = das
            .search_recent(&filter, PAGE)
            .await
            .with_context(|| format!("searchAssets {filter:?}"))?;

        let ids: Vec<String> = page.items.iter().map(|a| a.id.clone()).collect();
        let known: BTreeMap<String, AssetRef> = activity::assets_by_address(pool, &ids)
            .await?
            .into_iter()
            .map(|a| (a.address.clone(), a))
            .collect();

        let mut agreeing = 0usize;
        for asset in page.items {
            report.scanned += 1;
            // A creator query legitimately returns assets outside the
            // allowlist; they are not news and must not end the walk either.
            let Some(stored) = known.get(&asset.id) else {
                continue;
            };
            report.tracked += 1;

            // The same asymmetries the sweep's diff applies: DAS not knowing an
            // owner is not the same as there being none, and burning is
            // monotone.
            let das_owner = (!asset.burnt)
                .then(|| asset.owner().map(str::to_string))
                .flatten();
            let owner_moved = das_owner.is_some() && stored.owner != das_owner;
            let newly_burned = asset.burnt && !stored.burned;

            if owner_moved || newly_burned {
                agreeing = 0;
                report.stale += 1;
                stale.push((stored.clone(), asset));
            } else {
                agreeing += 1;
                if agreeing >= AGREE_STREAK {
                    break;
                }
            }
        }
    }

    if !stale.is_empty() {
        apply(pool, das, pipeline, &stale, &mut report).await?;
    }
    report.elapsed_ms = started.elapsed().as_millis();
    Ok(report)
}

/// Corrects the state, then rebuilds the timeline, for everything the probe
/// found — through the same writer and the same recovery walk the sweep uses.
async fn apply(
    pool: &PgPool,
    das: &DasClient,
    pipeline: &Pipeline,
    stale: &[(AssetRef, indexer_das::Asset)],
    report: &mut Report,
) -> anyhow::Result<()> {
    // State first, grouped by collection because the writer is scoped to one.
    // The owner is what the Explorer reads; the timeline can lag it by a few
    // seconds without lying.
    //
    // `apply_state`, not `upsert_batch`: the probe never fetched a document,
    // and `upsert_batch` writes `name`, `symbol` and `metadata_uri`
    // unconditionally, so it would blank the operator's re-hosted metadata on
    // every asset it corrected. This path touches owner and burned and nothing
    // else — which is also why it needs no document read at all.
    let mut by_collection: BTreeMap<i32, Vec<StateInput>> = BTreeMap::new();
    for (stored, asset) in stale {
        by_collection
            .entry(stored.collection_id)
            .or_default()
            .push(StateInput {
                address: asset.id.clone(),
                owner: (!asset.burnt)
                    .then(|| asset.owner().map(str::to_string))
                    .flatten(),
                burned: asset.burnt,
            });
    }
    // Read BEFORE the state it stamps, so it stays a conservative lower bound
    // on the observation — exactly as `assets.owner_slot` documents.
    let slot = das.get_slot().await.context("getSlot")? as i64;
    for (collection_id, inputs) in by_collection {
        let mut tx = pool.begin().await?;
        report.updated += assets::apply_state(&mut tx, collection_id, slot, &inputs).await?;
        tx.commit().await?;
    }

    // Then the timeline. An asset with a recorded event walks back to it; one
    // with none has no floor at all, so it gets a full history crawl — bounded,
    // because that is the expensive case.
    let mut deep = 0usize;
    for (stored, _) in stale {
        let floor = match stored.last_activity_slot {
            Some(slot) => slot,
            None if deep < MAX_DEEP_CRAWLS => {
                deep += 1;
                report.deep_crawled += 1;
                0
            }
            // Over budget: the owner is already corrected and the next run
            // will pick the timeline up.
            None => continue,
        };
        match reconcile::recover_asset(pool, das, pipeline, stored, floor).await {
            Ok((_, outcome)) => report.activity.add(outcome),
            Err(error) => log::warn!("probe recovering {}: {error:#}", stored.address),
        }
    }
    Ok(())
}

/// Records the run against every enabled collection, so `schedule::due` can
/// read the cadence back the way it does for every other periodic job.
pub async fn write_state(
    pool: &PgPool,
    report: &Report,
    started_at: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<()> {
    use indexer_data_model::ingest_state;
    for collection in registry::list_enabled(pool).await? {
        let state = ingest_state::BackfillState {
            collection_id: collection.id,
            kind: KIND.to_string(),
            status: "done".to_string(),
            cursor: json!({"mode": "reconcile_tip"}),
            progress: json!({
                "targets": report.targets,
                "scanned": report.scanned,
                "tracked": report.tracked,
                "stale": report.stale,
                "deep_crawled": report.deep_crawled,
                "updated": report.updated,
                "recorded": report.activity.recorded,
                // The same word every other periodic job uses for "rows this
                // run had to fix", so one metric spans them all.
                "corrections": report.stale,
                "duration_ms": report.elapsed_ms,
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
