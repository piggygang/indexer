//! Closing the gap after a disconnect.
//!
//! This transport has **no replay** — `fromSlot` is a LaserStream gRPC
//! feature — so a durable cursor plus reconciliation is the only gap
//! mechanism there is. The consumer therefore reconciles on *every*
//! `Connected`, which makes cold start, crash restart and mid-run reconnect
//! one code path instead of three.
//!
//! Two tiers:
//!
//! 1. **State sweep, always.** `getAssetBatch` over every tracked address
//!    (~18 calls, ~200 credits) through the backfill's own `upsert_batch`,
//!    whose `EXCLUDED.owner_slot > assets.owner_slot` guard means a sweep can
//!    never clobber a newer live observation. Recovers current owner/burned
//!    and discovers Core assets minted during the gap. It does **not** recover
//!    activity.
//! 2. **Targeted activity recovery.** For assets the sweep disagreed with,
//!    `getSignaturesForAddress` back to the cursor and `getTransaction` for
//!    each — fed through the *same* decoder and the *same* writer as the live
//!    path, tagged `source = 'reconcile'`. Identical semantics is what makes
//!    the acceptance criterion checkable.
//!
//! What this cannot recover, stated rather than papered over: an ownership
//! round-trip inside one gap (the state diff sees no change), and a
//! transaction that never names the asset. Neither invents an activity row.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use anyhow::Context;
use indexer_das::backfill::merge;
use indexer_das::DasClient;
use indexer_data_model::activity::{self, AssetRef};
use indexer_data_model::assets::{self, AssetInput, BatchCounts};
use indexer_data_model::integrity::{self, Integrity};
use indexer_data_model::types::MembershipRule;
use indexer_data_model::{ingest_state, registry, PgPool};
use indexer_ingest::decode::DecodeContext;
use serde_json::{json, Value};

use crate::pipeline::{Outcome, Pipeline};

/// Signatures fetched per asset per page.
const SIGNATURE_PAGE: u32 = 1_000;

/// Beyond this many disagreeing assets a targeted recovery stops being
/// meaningful. The sweep is still written and the overflow is flagged
/// `ownership_dirty` so the rebuild and the activity backfill can pick it up.
///
/// The cursor keeps advancing on purpose. Holding it back — which an earlier
/// version of this message claimed to do, and did not — would make every
/// reconnect replay an ever-growing span of history without ever catching up.
/// An overflow is a spike in the drift metric and an operator's problem, not
/// something to paper over by refusing to make progress.
const MAX_CANDIDATES: usize = 2_000;

/// `backfill_state.kind` for the periodic state sweep.
pub const KIND: &str = "reconcile";

/// `backfill_state.kind` for the weekly deep pass.
pub const DEEP_KIND: &str = "reconcile_deep";

/// What one collection's reconciliation corrected.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectionReport {
    pub collection_id: i32,
    pub slug: String,
    /// Assets DAS returned for this collection.
    pub swept: u64,
    /// Assets whose stored state disagreed with DAS.
    pub candidates: u64,
    /// Signatures the targeted recovery walked.
    pub signatures: u64,
    /// What the state sweep actually wrote.
    pub state: BatchCounts,
    /// What the targeted recovery wrote.
    pub activity: Outcome,
    /// Core assets that left the collection.
    pub membership_removed: u64,
    /// Assets whose ownership intervals were re-derived from stored activity.
    pub rebuilt: u64,
}

impl CollectionReport {
    /// The drift metric: how much this run had to correct.
    ///
    /// Deliberately the same definition the backfills use for
    /// `--expect-unchanged` — `BatchCounts::is_noop` plus the activity the
    /// recovery had to write — so "corrections trend to zero" and "re-running
    /// changes nothing" are the same claim measured the same way.
    pub fn corrections(&self) -> u64 {
        self.state.inserted
            + self.state.updated
            + self.state.attributes_written
            + self.state.attributes_removed
            + self.state.documents
            + self.activity.recorded
            + self.activity.dirty
            + self.activity.parked
            + self.membership_removed
            + self.rebuilt
    }

    fn progress(&self, elapsed_ms: u128, overflowed: bool, integrity: &Integrity) -> Value {
        json!({
            "swept": self.swept,
            "candidates": self.candidates,
            "corrections": self.corrections(),
            "signatures": self.signatures,
            "inserted": self.state.inserted,
            "updated": self.state.updated,
            "unchanged": self.state.unchanged,
            "attributes_written": self.state.attributes_written,
            "attributes_removed": self.state.attributes_removed,
            "documents": self.state.documents,
            "recorded": self.activity.recorded,
            "redelivered": self.activity.redelivered,
            "dirty": self.activity.dirty,
            "parked": self.activity.parked,
            "hydrated": self.activity.hydrated,
            "membership_removed": self.membership_removed,
            "rebuilt": self.rebuilt,
            "overflowed": overflowed,
            "owner_mismatch": integrity.owner_mismatch,
            "allowlist_violation": integrity.allowlist_violation,
            "symbol_mismatch": integrity.symbol_mismatch,
            "ownership_dirty": integrity.ownership_dirty,
            "duration_ms": elapsed_ms,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub collections: Vec<CollectionReport>,
    /// More disagreeing assets than [`MAX_CANDIDATES`] — the spike an alert
    /// should fire on.
    pub overflowed: bool,
    /// How healthy the database was when the run finished. All zeroes is the
    /// acceptance criterion.
    pub integrity: Integrity,
}

impl Report {
    pub fn swept(&self) -> u64 {
        self.collections.iter().map(|c| c.swept).sum()
    }

    pub fn candidates(&self) -> u64 {
        self.collections.iter().map(|c| c.candidates).sum()
    }

    pub fn signatures(&self) -> u64 {
        self.collections.iter().map(|c| c.signatures).sum()
    }

    pub fn recorded(&self) -> u64 {
        self.collections.iter().map(|c| c.activity.recorded).sum()
    }

    /// Corrections across every collection — the number that should trend to
    /// zero on a healthy stream.
    pub fn corrections(&self) -> u64 {
        self.collections.iter().map(|c| c.corrections()).sum()
    }

    pub fn is_noop(&self) -> bool {
        self.corrections() == 0
    }

    /// Emits the run as one counter bundle, and names every collection that
    /// actually corrected something.
    ///
    /// The migration asks reconciliation to "diff and log"; this is the log
    /// half, and `corrections` is the number ALG-628 should alert on when it
    /// stops trending to zero.
    pub fn log(&self, label: &str) {
        log::info!(
            "{label} finished: swept={} candidates={} corrections={} signatures={} \
             recorded={} dirty={} parked={} removed={} rebuilt={} overflowed={} \
             integrity(owner={} allowlist={} symbol={} dirty={})",
            self.swept(),
            self.candidates(),
            self.corrections(),
            self.signatures(),
            self.recorded(),
            self.collections
                .iter()
                .map(|c| c.activity.dirty)
                .sum::<u64>(),
            self.collections
                .iter()
                .map(|c| c.activity.parked)
                .sum::<u64>(),
            self.collections
                .iter()
                .map(|c| c.membership_removed)
                .sum::<u64>(),
            self.collections.iter().map(|c| c.rebuilt).sum::<u64>(),
            self.overflowed,
            self.integrity.owner_mismatch,
            self.integrity.allowlist_violation,
            self.integrity.symbol_mismatch,
            self.integrity.ownership_dirty,
        );
        for collection in self.collections.iter().filter(|c| c.corrections() > 0) {
            log::info!(
                "{label} corrected {}: {} change(s) — inserted={} updated={} \
                 attributes=+{}/-{} activity={} removed={} rebuilt={}",
                collection.slug,
                collection.corrections(),
                collection.state.inserted,
                collection.state.updated,
                collection.state.attributes_written,
                collection.state.attributes_removed,
                collection.activity.recorded,
                collection.membership_removed,
                collection.rebuilt,
            );
        }
        if !self.integrity.is_healthy() {
            log::warn!(
                "integrity views are not empty after {label}: {:?}",
                self.integrity
            );
        }
    }
}

/// Runs both tiers and records what it corrected.
///
/// `from` is the durable cursor; `None` means we have never checkpointed and
/// the sweep alone is the baseline. Every collection's outcome is persisted to
/// `backfill_state` under [`KIND`], so a run is readable after the fact
/// without scrollback — the same discipline both backfills follow.
pub async fn run(
    pool: &PgPool,
    das: &DasClient,
    pipeline: &Pipeline,
    from: Option<u64>,
) -> anyhow::Result<Report> {
    let started = Instant::now();
    let started_at = chrono::Utc::now();
    let mut report = Report::default();
    let mut candidates: Vec<AssetRef> = Vec::new();

    for collection in registry::list_enabled(pool).await? {
        let Some(rule) = collection.membership_rule else {
            continue;
        };
        let mut counts = CollectionReport {
            collection_id: collection.id,
            slug: collection.slug.clone(),
            ..CollectionReport::default()
        };
        let stored: BTreeMap<String, AssetRef> = current_state(pool, collection.id)
            .await?
            .into_iter()
            .map(|r| (r.address.clone(), r))
            .collect();

        // The slot is read BEFORE the data call so it is a conservative lower
        // bound on the observation, exactly as `assets.owner_slot` documents.
        let slot = das.get_slot().await.context("getSlot")?;
        let found = enumerate(pool, das, &collection, rule, &stored).await?;

        // Core assets can leave a collection when the update authority moves
        // them. Enumeration is authoritative for that rule and only that rule:
        // an allowlist is a closed list, and a `tm_collection` sweep is built
        // from what we already store, so neither can observe a departure.
        if rule == MembershipRule::CoreCollection {
            let present: BTreeSet<&str> = found.iter().map(|a| a.id.as_str()).collect();
            let gone: Vec<String> = stored
                .keys()
                .filter(|address| !present.contains(address.as_str()))
                .cloned()
                .collect();
            if !gone.is_empty() {
                log::warn!(
                    "{}: {} asset(s) left the collection: {}",
                    collection.slug,
                    gone.len(),
                    gone.iter().take(5).cloned().collect::<Vec<_>>().join(", ")
                );
            }
            counts.membership_removed =
                assets::set_membership(pool, collection.id, &gone, true).await?;
            // An asset that came back is a member again. The writer's guard
            // makes the common case (nothing changed) a true no-op.
            let back: Vec<String> = found.iter().map(|a| a.id.clone()).collect();
            assets::set_membership(pool, collection.id, &back, false).await?;
        }

        // Read back the documents we are not re-fetching. Without this the
        // sweep hands `merge` a `None` document and reverts name, image and
        // attributes to whatever DAS has cached — which would both corrupt the
        // operator's re-hosted metadata and keep the corrections metric
        // permanently above zero. `backfill.rs` does exactly this for the same
        // reason.
        let addresses: Vec<String> = found.iter().map(|a| a.id.clone()).collect();
        let mut documents: BTreeMap<String, (String, Value)> = BTreeMap::new();
        for (address, uri, json) in
            assets::stored_documents(pool, collection.id, &addresses).await?
        {
            documents.insert(address, (uri, json));
        }

        let mut inputs: Vec<AssetInput> = Vec::new();
        for asset in &found {
            counts.swept += 1;
            let document = documents
                .get(&asset.id)
                .map(|(uri, json)| (uri.as_str(), json));
            let input = merge(asset, document);
            // The diff must mirror the writer's own policy, or an asset it
            // refuses to change becomes a permanent candidate and burns a
            // `getSignaturesForAddress` call on every reconnect forever.
            // Two asymmetries matter:
            //   * `input.owner == None` means DAS does not know, not that
            //     the asset has no owner — and `upsert_batch` will not
            //     clobber a known owner with unknown.
            //   * burning is monotone, so only DAS asserting a burn we
            //     have not recorded is news.
            if let Some(known) = stored.get(&asset.id) {
                let owner_moved = input.owner.is_some() && known.owner != input.owner;
                let newly_burned = input.burned && !known.burned;
                if owner_moved || newly_burned {
                    log::debug!(
                        "candidate {}: db(owner={:?} burned={}) das(owner={:?} burned={})",
                        known.address,
                        known.owner.as_deref(),
                        known.burned,
                        input.owner.as_deref(),
                        input.burned
                    );
                    counts.candidates += 1;
                    candidates.push(known.clone());
                }
            }
            inputs.push(input);
        }

        for chunk in inputs.chunks(500) {
            let mut tx = pool.begin().await?;
            counts
                .state
                .add(assets::upsert_batch(&mut tx, collection.id, slot, chunk).await?);
            tx.commit().await?;
        }
        report.collections.push(counts);
    }

    // Assets already flagged by an out-of-order live event are candidates too.
    for dirty in activity::dirty_assets(pool, MAX_CANDIDATES as i64).await? {
        if !candidates.iter().any(|c| c.id == dirty.id) {
            candidates.push(dirty);
        }
    }

    if candidates.len() > MAX_CANDIDATES {
        report.overflowed = true;
        log::error!(
            "{} assets disagree with DAS, over the {MAX_CANDIDATES} cap — the sweep is \
             written, the first {MAX_CANDIDATES} are recovered and the rest are flagged \
             ownership_dirty; run `indexer-admin rebuild-ownership` or \
             `backfill-activity --reclassify` to clear them",
            candidates.len()
        );
        // Flagged rather than forgotten: `ownership_dirty` is the queue both
        // the rebuild and the next reconcile already read.
        for overflow in &candidates[MAX_CANDIDATES..] {
            activity::mark_dirty(pool, overflow.id).await?;
        }
        candidates.truncate(MAX_CANDIDATES);
    }

    let floor = from.unwrap_or(0) as i64;
    for candidate in &candidates {
        let (signatures, outcome) = recover_asset(pool, das, pipeline, candidate, floor)
            .await
            .unwrap_or_else(|error| {
                log::warn!("recovering {}: {error:#}", candidate.address);
                (0, Outcome::default())
            });
        if let Some(counts) = report
            .collections
            .iter_mut()
            .find(|c| c.collection_id == candidate.collection_id)
        {
            counts.signatures += signatures;
            counts.activity.add(outcome);
        }
    }

    // Self-heal, after the recovery has had its chance to supply the missing
    // events: re-derive intervals for every asset an out-of-order write
    // flagged. That is the other half of the writer contract — an event stored
    // but not applied needs something to rebuild the history — and until now
    // only `indexer-admin rebuild-ownership` ever did it.
    //
    // An asset whose stored activity is genuinely incomplete stays mismatched
    // and is reported rather than papered over; ALG-622's crawl is the fix for
    // those, and the integrity counters are how they surface.
    for dirty in activity::dirty_assets(pool, MAX_CANDIDATES as i64).await? {
        let mut tx = pool.begin().await?;
        let rebuilt = activity::rebuild_ownership(&mut tx, dirty.id).await?;
        tx.commit().await?;
        if rebuilt.was_dirty {
            if let Some(counts) = report
                .collections
                .iter_mut()
                .find(|c| c.collection_id == dirty.collection_id)
            {
                counts.rebuilt += 1;
            }
        }
    }

    report.integrity = integrity::snapshot(pool).await?;

    let elapsed = started.elapsed();
    for counts in &report.collections {
        let state = ingest_state::BackfillState {
            collection_id: counts.collection_id,
            kind: KIND.to_string(),
            status: "done".into(),
            cursor: json!({"mode": "reconcile", "from_slot": from}),
            progress: counts.progress(elapsed.as_millis(), report.overflowed, &report.integrity),
            last_error: None,
            started_at: Some(started_at),
            finished_at: Some(chrono::Utc::now()),
            updated_at: chrono::Utc::now(),
        };
        ingest_state::put_backfill_state(pool, &state).await?;
    }

    Ok(report)
}

/// Asks DAS what the collection looks like now, one arm per membership rule.
///
/// A Core collection grows on its own, so it must be *enumerated* rather than
/// re-read by id: a mint during the gap is invisible to a list built from what
/// we already store.
async fn enumerate(
    pool: &PgPool,
    das: &DasClient,
    collection: &registry::CollectionRow,
    rule: MembershipRule,
    stored: &BTreeMap<String, AssetRef>,
) -> anyhow::Result<Vec<indexer_das::Asset>> {
    match rule {
        MembershipRule::TmAllowlist | MembershipRule::TmCollection => {
            let addresses: Vec<String> = match rule {
                MembershipRule::TmAllowlist => registry::allowlist(pool, collection.id).await?,
                _ => stored.keys().cloned().collect(),
            };
            let mut found = Vec::new();
            for chunk in addresses.chunks(1_000) {
                found.extend(
                    das.get_asset_batch(chunk)
                        .await
                        .context("getAssetBatch")?
                        .found,
                );
            }
            Ok(found)
        }
        MembershipRule::CoreCollection => {
            let Some(address) = collection.address.as_deref() else {
                return Ok(Vec::new());
            };
            let mut found = Vec::new();
            let mut page = 1u32;
            loop {
                let result = das
                    .search_assets(address, page, 1_000, false)
                    .await
                    .context("searchAssets")?;
                let count = result.items.len();
                found.extend(result.items);
                if count < 1_000 {
                    break;
                }
                page += 1;
            }
            Ok(found)
        }
    }
}

/// Walks one asset's signatures back to the cursor and replays them through
/// the live decoder.
async fn recover_asset(
    pool: &PgPool,
    das: &DasClient,
    pipeline: &Pipeline,
    asset: &AssetRef,
    floor: i64,
) -> anyhow::Result<(u64, Outcome)> {
    let mut before: Option<String> = None;
    let mut seen = 0u64;
    let mut outcome = Outcome::default();

    loop {
        let page = das
            .get_signatures_for_address(&asset.address, before.as_deref(), SIGNATURE_PAGE)
            .await?;
        if page.is_empty() {
            break;
        }
        let last = page.last().map(|s| s.signature.clone());

        for info in &page {
            if info.slot <= floor {
                return Ok((seen, outcome));
            }
            seen += 1;
            // getSignaturesForAddress carries blockTime, so the recovery path
            // never needs getBlockTime at all.
            if let Some(time) = info.block_time_utc() {
                pipeline.block_times().insert(info.slot, time).await;
            }
            if info.failed() {
                activity::park_signature(pool, asset.id, &info.signature, info.slot, true).await?;
                outcome.parked += 1;
                continue;
            }
            match das.get_transaction(&info.signature).await? {
                Some(transaction) => {
                    outcome.add(
                        pipeline
                            .replay(&info.signature, info.slot, &transaction)
                            .await?,
                    );
                }
                None => {
                    activity::park_signature(pool, asset.id, &info.signature, info.slot, false)
                        .await?;
                    outcome.parked += 1;
                }
            }
        }

        if page.len() < SIGNATURE_PAGE as usize {
            break;
        }
        before = last;
    }

    Ok((seen, outcome))
}

async fn current_state(pool: &PgPool, collection_id: i32) -> anyhow::Result<Vec<AssetRef>> {
    Ok(activity::assets_in_collection(pool, collection_id).await?)
}

/// Seeds the cursor on a database that has never checkpointed, so the first
/// reconciliation is honestly "since the backfill ran" rather than "since the
/// genesis block".
pub async fn seed_cursor(pool: &PgPool, stream: &str) -> anyhow::Result<Option<u64>> {
    if let Some(slot) = ingest_state::last_processed_slot(pool, stream).await? {
        return Ok(Some(slot));
    }
    let backfilled = ingest_state::backfilled_slot(pool).await?;
    let Some(slot) = backfilled.filter(|s| *s > 0) else {
        return Ok(None);
    };
    // reset() rather than checkpoint(): this runs before the stream starts, so
    // "only with the ingester stopped" holds.
    ingest_state::reset(pool, stream, slot as u64).await?;
    log::info!("seeded {stream} cursor at slot {slot} from the DAS backfill");
    Ok(Some(slot as u64))
}

/// The decoder context, rebuilt whenever the registry might have changed.
pub async fn context(pool: &PgPool) -> anyhow::Result<DecodeContext> {
    Ok(DecodeContext {
        core_collections: crate::spec::core_collections(pool).await?,
        // Empty on the live path: every balance a 2026 validator produces
        // carries its own `owner`. The map exists for the archival crawl,
        // whose 2021 transactions predate that field.
        token_account_owners: Default::default(),
    })
}
