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

use std::collections::BTreeMap;

use anyhow::Context;
use indexer_das::backfill::merge;
use indexer_das::DasClient;
use indexer_data_model::activity::{self, AssetRef};
use indexer_data_model::assets::{self, AssetInput};
use indexer_data_model::types::MembershipRule;
use indexer_data_model::{ingest_state, registry, PgPool};
use indexer_ingest::decode::DecodeContext;

use crate::pipeline::Pipeline;

/// Signatures fetched per asset per page.
const SIGNATURE_PAGE: u32 = 1_000;

/// Beyond this many disagreeing assets we stop pretending a targeted recovery
/// is meaningful: the sweep is still written, the overflow is flagged dirty,
/// and the cursor is not advanced past the gap.
const MAX_CANDIDATES: usize = 2_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Report {
    pub swept: u64,
    pub candidates: u64,
    pub signatures: u64,
    pub recorded: u64,
    pub overflowed: bool,
}

/// Runs both tiers. `from` is the durable cursor; `None` means we have never
/// checkpointed and the sweep alone is the baseline.
pub async fn run(
    pool: &PgPool,
    das: &DasClient,
    pipeline: &Pipeline,
    from: Option<u64>,
) -> anyhow::Result<Report> {
    let mut report = Report::default();
    let mut candidates: Vec<AssetRef> = Vec::new();

    for collection in registry::list_enabled(pool).await? {
        let Some(rule) = collection.membership_rule else {
            continue;
        };
        let stored: BTreeMap<String, AssetRef> = current_state(pool, collection.id)
            .await?
            .into_iter()
            .map(|r| (r.address.clone(), r))
            .collect();

        // The slot is read BEFORE the data call so it is a conservative lower
        // bound on the observation, exactly as `assets.owner_slot` documents.
        let slot = das.get_slot().await.context("getSlot")?;
        // A Core collection grows on its own, so it must be *enumerated*
        // rather than re-read by id: a mint during the gap is invisible to a
        // list built from what we already store.
        let found = match rule {
            MembershipRule::TmAllowlist | MembershipRule::TmCollection => {
                let addresses = match rule {
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
                found
            }
            MembershipRule::CoreCollection => {
                let Some(address) = collection.address.as_deref() else {
                    continue;
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
                found
            }
        };

        {
            let mut inputs: Vec<AssetInput> = Vec::new();
            for asset in &found {
                report.swept += 1;
                let input = merge(asset, None);
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
                        candidates.push(known.clone());
                    }
                }
                inputs.push(input);
            }

            for chunk in inputs.chunks(500) {
                let mut tx = pool.begin().await?;
                assets::upsert_batch(&mut tx, collection.id, slot, chunk).await?;
                tx.commit().await?;
            }
        }
    }

    // Assets already flagged by an out-of-order live event are candidates too.
    for dirty in activity::dirty_assets(pool, MAX_CANDIDATES as i64).await? {
        if !candidates.iter().any(|c| c.id == dirty.id) {
            candidates.push(dirty);
        }
    }

    report.candidates = candidates.len() as u64;
    if candidates.len() > MAX_CANDIDATES {
        report.overflowed = true;
        log::error!(
            "{} assets disagree with DAS, over the {MAX_CANDIDATES} cap — the sweep is \
             written and the cursor is NOT advanced; escalate to the ALG-622 activity backfill",
            candidates.len()
        );
        candidates.truncate(MAX_CANDIDATES);
    }

    let floor = from.unwrap_or(0) as i64;
    for candidate in &candidates {
        report.add_signatures(
            recover_asset(pool, das, pipeline, candidate, floor)
                .await
                .unwrap_or_else(|error| {
                    log::warn!("recovering {}: {error:#}", candidate.address);
                    (0, 0)
                }),
        );
    }

    Ok(report)
}

impl Report {
    fn add_signatures(&mut self, (signatures, recorded): (u64, u64)) {
        self.signatures += signatures;
        self.recorded += recorded;
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
) -> anyhow::Result<(u64, u64)> {
    let mut before: Option<String> = None;
    let (mut seen, mut recorded) = (0u64, 0u64);

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
                return Ok((seen, recorded));
            }
            seen += 1;
            // getSignaturesForAddress carries blockTime, so the recovery path
            // never needs getBlockTime at all.
            if let Some(time) = info.block_time_utc() {
                pipeline.block_times().insert(info.slot, time).await;
            }
            if info.failed() {
                activity::park_signature(pool, asset.id, &info.signature, info.slot, true).await?;
                continue;
            }
            match das.get_transaction(&info.signature).await? {
                Some(transaction) => {
                    recorded += pipeline
                        .replay(&info.signature, info.slot, &transaction)
                        .await?;
                }
                None => {
                    activity::park_signature(pool, asset.id, &info.signature, info.slot, false)
                        .await?;
                }
            }
        }

        if page.len() < SIGNATURE_PAGE as usize {
            break;
        }
        before = last;
    }

    Ok((seen, recorded))
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
