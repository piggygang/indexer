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
use std::time::{Duration, Instant};

use anyhow::Context;
use futures_util::StreamExt;
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

/// Concurrent `getAssetBatch` calls during enumeration.
///
/// The Developer plan allows 10 DAS requests/second — a separate bucket from
/// RPC's 50/s — and `RECONCILE_RPS` already caps the client. Six in flight
/// turns ~30 s of serial round trips into ~5 s without approaching the limit.
const FETCH_CONCURRENCY: usize = 6;

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

    fn progress(
        &self,
        elapsed_ms: u128,
        phases: &Phases,
        overflowed: bool,
        integrity: &Integrity,
    ) -> Value {
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
            // Where the run's wall clock went. Run-level, not per collection,
            // and recorded because "the sweep is slow" is otherwise
            // unanswerable after the fact: the state pass and the recovery walk
            // have completely different cost drivers, and only one of them is
            // fixed by reading fewer assets.
            "enumerate_ms": phases.enumerate.as_millis(),
            "write_ms": phases.write.as_millis(),
            "recover_ms": phases.recover.as_millis(),
        })
    }
}

/// Wall clock per phase of one run.
#[derive(Debug, Default, Clone, Copy)]
struct Phases {
    /// Asking DAS what exists and what it owns.
    enumerate: Duration,
    /// Writing the state back.
    write: Duration,
    /// Walking each candidate's signatures and replaying what is missing.
    recover: Duration,
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
/// `from` is the durable cursor, recorded in `backfill_state.cursor.from_slot`
/// so a run says what it resumed from. It is **not** the recovery floor —
/// tier 2 walks each candidate back to that asset's own `last_activity_slot`.
/// Those two were the same value once, and it made the scheduled sweep a
/// no-op: the cursor is always ahead of anything the stream missed. Do not
/// re-couple them.
///
/// Every collection's outcome is persisted to `backfill_state` under [`KIND`],
/// so a run is readable after the fact without scrollback — the same
/// discipline both backfills follow.
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
    let mut phases = Phases::default();

    // One slot for the whole sweep, read BEFORE any data call so it stays a
    // conservative lower bound on every observation it stamps — exactly as
    // `assets.owner_slot` documents. Per-collection reads bought nothing: a
    // later collection got a *higher* floor for data of the same age, which is
    // the direction that loses a write, and each one was a round trip.
    let slot = das.get_slot().await.context("getSlot")?;

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

        let enumerating = Instant::now();
        let found = enumerate(pool, das, &collection, rule, &stored).await?;
        phases.enumerate += enumerating.elapsed();

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
            // Membership moves the population every trait frequency is
            // measured against, so both directions re-rank the collection —
            // and this is the one such write with no other trace, since the
            // "came back" call's return value is deliberately discarded.
            let mut tx = pool.begin().await?;
            counts.membership_removed =
                assets::set_membership_and_flag(&mut tx, collection.id, &gone, true).await?;
            // An asset that came back is a member again. The writer's guard
            // makes the common case (nothing changed) a true no-op.
            let back: Vec<String> = found.iter().map(|a| a.id.clone()).collect();
            assets::set_membership_and_flag(&mut tx, collection.id, &back, false).await?;
            tx.commit().await?;
        }

        // Assets we already have take the state-only path; only the ones this
        // sweep is *discovering* need a full row.
        //
        // That split is what removed the sweep's largest read. It used to load
        // every stored document — 17 820 JSONB blobs a pass — purely so
        // `merge` would not hand `upsert_batch` a `None` document and blank the
        // operator's re-hosted metadata. `apply_state` cannot touch those
        // columns at all, so for an asset we already know the question does not
        // arise; and a genuinely new asset (a Core mint) is rare enough that
        // reading documents for just those few costs nothing.
        let fresh: Vec<&indexer_das::Asset> = found
            .iter()
            .filter(|asset| !stored.contains_key(&asset.id))
            .collect();
        let mut documents: BTreeMap<String, (String, Value)> = BTreeMap::new();
        if !fresh.is_empty() {
            let addresses: Vec<String> = fresh.iter().map(|a| a.id.clone()).collect();
            for (address, uri, json) in
                assets::stored_documents(pool, collection.id, &addresses).await?
            {
                documents.insert(address, (uri, json));
            }
        }

        let mut inserts: Vec<AssetInput> = Vec::new();
        let mut updates: Vec<assets::StateInput> = Vec::new();
        for asset in &found {
            counts.swept += 1;
            let owner = (!asset.burnt)
                .then(|| asset.owner().map(str::to_string))
                .flatten();

            // The diff must mirror the writer's own policy, or an asset it
            // refuses to change becomes a permanent candidate and burns a
            // `getSignaturesForAddress` call on every reconnect forever.
            // Two asymmetries matter:
            //   * `owner == None` means DAS does not know, not that the asset
            //     has no owner — and neither writer will clobber a known owner
            //     with unknown.
            //   * burning is monotone, so only DAS asserting a burn we
            //     have not recorded is news.
            match stored.get(&asset.id) {
                Some(known) => {
                    let owner_moved = owner.is_some() && known.owner != owner;
                    let newly_burned = asset.burnt && !known.burned;
                    if owner_moved || newly_burned {
                        log::debug!(
                            "candidate {}: db(owner={:?} burned={}) das(owner={:?} burned={})",
                            known.address,
                            known.owner.as_deref(),
                            known.burned,
                            owner.as_deref(),
                            asset.burnt
                        );
                        counts.candidates += 1;
                        candidates.push(known.clone());
                    }
                    updates.push(assets::StateInput {
                        address: asset.id.clone(),
                        owner,
                        burned: asset.burnt,
                    });
                }
                None => {
                    let document = documents
                        .get(&asset.id)
                        .map(|(uri, json)| (uri.as_str(), json));
                    inserts.push(merge(asset, document));
                }
            }
        }

        let writing = Instant::now();
        for chunk in inserts.chunks(500) {
            let mut tx = pool.begin().await?;
            counts
                .state
                .add(assets::upsert_batch(&mut tx, collection.id, slot, chunk).await?);
            tx.commit().await?;
        }
        for chunk in updates.chunks(2_000) {
            let mut tx = pool.begin().await?;
            counts.state.updated +=
                assets::apply_state(&mut tx, collection.id, slot, chunk).await?;
            tx.commit().await?;
        }
        phases.write += writing.elapsed();
        report.collections.push(counts);
    }

    // Assets already flagged by an out-of-order live event are candidates too.
    for dirty in activity::dirty_assets(pool, MAX_CANDIDATES as i64).await? {
        if !candidates.iter().any(|c| c.id == dirty.id) {
            candidates.push(dirty);
        }
    }

    // …and so are assets whose ownership is right but whose timeline is not.
    //
    // The state diff above cannot find these: it compares `assets.owner` with
    // DAS, and a previous sweep already patched the owner to match. The
    // transfer that moved it stayed missing, so the asset agrees with DAS,
    // disagrees with its own history, and would never be looked at again.
    // `drifted_assets` is the standing backlog of exactly that, and folding it
    // in here is what makes the sweep converge instead of plateauing.
    for drifted in activity::drifted_assets(pool, MAX_CANDIDATES as i64).await? {
        if !candidates.iter().any(|c| c.id == drifted.id) {
            candidates.push(drifted);
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

    let recovering = Instant::now();
    for candidate in &candidates {
        // The floor is per asset: the newest slot we already have an event
        // for. This is the bug this module shipped with — `from` is the live
        // cursor, which on a healthy ingester is *now*, so a transfer the
        // WebSocket dropped was by definition below it. `recover_asset`
        // returned on its first comparison and every scheduled sweep reported
        // `signatures=0 recorded=0` while tier 1 quietly patched the owner
        // column. The gap between the two is the whole failure.
        //
        // An asset we have never recorded anything for falls back to the
        // cursor rather than to 0. That is not timidity: 35% of tracked assets
        // are in that state because the archival backfill has never covered
        // the whole catalogue, and walking each one's full history inline would
        // turn an hourly sweep into an archival crawl. `backfill-activity` owns
        // that; this owns the gap between what we recorded and what moved.
        let floor = candidate
            .last_activity_slot
            .unwrap_or(from.unwrap_or(0) as i64);
        // Unbounded, deliberately and visibly: the sweep's own MAX_CANDIDATES
        // cap is what bounds it today, and a per-asset budget without a resume
        // cursor would re-buy the same newest N every run instead of making
        // progress. Both belong with the repair watermark, not here.
        let (signatures, outcome) = recover_asset(pool, das, pipeline, candidate, floor, u64::MAX)
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
    phases.recover = recovering.elapsed();

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
            progress: counts.progress(
                elapsed.as_millis(),
                &phases,
                report.overflowed,
                &report.integrity,
            ),
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
            // Concurrent, because the sweep's wall clock was almost entirely
            // serial round trips: 18 sequential `getAssetBatch` calls, each
            // returning a 1 000-asset payload, is ~30 s of mostly waiting.
            // `FETCH_CONCURRENCY` is well inside the Developer plan's 10 DAS
            // requests/second, and the client's own rate limiter enforces the
            // ceiling regardless.
            let batches: Vec<Vec<String>> =
                addresses.chunks(1_000).map(<[String]>::to_vec).collect();
            let results: Vec<_> = futures_util::stream::iter(batches)
                .map(|chunk| async move { das.get_asset_batch(&chunk).await })
                .buffer_unordered(FETCH_CONCURRENCY)
                .collect()
                .await;

            let mut found = Vec::new();
            for result in results {
                found.extend(result.context("getAssetBatch")?.found);
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

/// Walks one asset's signatures back to `floor` and replays them through the
/// live decoder, stopping after `budget` signatures.
///
/// Shared by every caller that needs to rebuild a timeline: the scheduled
/// sweep and the tip probe. There is exactly one implementation on purpose — a
/// second would drift — and exactly one signature, so an unbounded walk has to
/// be written as `u64::MAX` at a call site rather than hidden behind a
/// pleasant-sounding wrapper.
///
/// `budget` counts signatures *considered*, which is the thing that costs: one
/// `getTransaction` each, plus one `getSignaturesForAddress` per 1 000. The
/// floor is the real bound in the healthy case; the budget is what stops an
/// asset with a floor of 0, or one whose floor never advances because the walk
/// records nothing, from being unbounded.
///
/// A truncated walk is logged and is **not** recorded as covered anywhere —
/// there is no resume cursor yet, so the next run repeats it. That is a bounded
/// leak rather than a fix, and closing it needs a per-asset repair watermark.
pub(crate) async fn recover_asset(
    pool: &PgPool,
    das: &DasClient,
    pipeline: &Pipeline,
    asset: &AssetRef,
    floor: i64,
    budget: u64,
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
            if seen >= budget {
                log::info!(
                    "recovering {}: stopped at the {budget}-signature budget, \
                     still above floor {floor}",
                    asset.address
                );
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
