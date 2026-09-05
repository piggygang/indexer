//! Applying decoded events to the database.
//!
//! One transaction per (signature × asset), because the writer contract asks
//! for it and because holding exactly one asset lock at a time makes
//! asset↔asset deadlock structurally impossible.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use chrono::{DateTime, Utc};
use indexer_das::backfill::merge;
use indexer_das::DasClient;
use indexer_data_model::activity::{self, AssetRef, LiveEvent};
use indexer_data_model::assets;
use indexer_data_model::types::EventKind;
use indexer_data_model::PgPool;
use indexer_ingest::decode::{self, CoreTouch, DecodeContext, DecodedKind};
use indexer_ingest::TransactionUpdate;
use serde_json::{json, Value};

use crate::blocktime::BlockTimes;

/// How long to keep asking DAS about a freshly minted Core asset before giving
/// up on its attributes. DAS needs a moment to index a new mint, and the
/// acceptance criterion allows 30 s.
const HYDRATE_BACKOFF_MS: [u64; 4] = [1_000, 3_000, 7_000, 15_000];

pub struct Pipeline {
    pool: PgPool,
    das: DasClient,
    block_times: BlockTimes,
    context: DecodeContext,
    source: &'static str,
}

/// What one transaction produced.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Outcome {
    pub recorded: u64,
    pub redelivered: u64,
    pub dirty: u64,
    pub parked: u64,
    pub hydrated: u64,
    /// Decoded events whose address we do not track. Expected: the filter
    /// matches whole transactions, which routinely touch other tokens.
    pub untracked: u64,
}

impl Outcome {
    /// Did this produce any change at all? The reconciliation metric's
    /// definition of a correction, alongside `BatchCounts::is_noop`.
    pub fn is_noop(&self) -> bool {
        self.recorded == 0 && self.dirty == 0 && self.parked == 0 && self.hydrated == 0
    }

    pub fn add(&mut self, other: Outcome) {
        self.recorded += other.recorded;
        self.redelivered += other.redelivered;
        self.dirty += other.dirty;
        self.parked += other.parked;
        self.hydrated += other.hydrated;
        self.untracked += other.untracked;
    }
}

impl Pipeline {
    pub fn new(pool: PgPool, das: DasClient, context: DecodeContext, source: &'static str) -> Self {
        Self {
            pool,
            das,
            block_times: BlockTimes::new(),
            context,
            source,
        }
    }

    pub fn block_times(&self) -> &BlockTimes {
        &self.block_times
    }

    pub fn context_mut(&mut self) -> &mut DecodeContext {
        &mut self.context
    }

    /// Decodes and applies one live transaction.
    pub async fn handle(&self, update: &TransactionUpdate) -> anyhow::Result<Outcome> {
        self.handle_as(update, self.source).await
    }

    /// Replays a transaction fetched by the gap recovery through the *same*
    /// decoder and writer, tagged `reconcile`. Sharing the path is what makes
    /// "no missing activity in the gap" comparable to the live behaviour.
    pub async fn replay(
        &self,
        signature: &str,
        slot: i64,
        transaction: &Value,
    ) -> anyhow::Result<Outcome> {
        let update = TransactionUpdate {
            filters: Vec::new(),
            slot: slot as u64,
            signature: signature.to_string(),
            failed: false,
            account_keys: Vec::new(),
            raw: indexer_ingest::RawPayload::Json(transaction.clone()),
        };
        // The whole `Outcome`, not just `recorded`: a recovery that parked a
        // signature or flagged an asset dirty corrected something too, and the
        // drift metric counts all of it.
        self.handle_as(&update, "reconcile").await
    }

    async fn handle_as(&self, update: &TransactionUpdate, source: &str) -> anyhow::Result<Outcome> {
        let decoded = decode::decode_transaction(update, &self.context);
        if decoded.is_empty() {
            return Ok(Outcome::default());
        }

        // Carries the marketplace program id to ALG-622 without this crate
        // ever naming one, so a transfer can be reclassified into a priced
        // sale later without re-fetching the transaction.
        let details = json!({
            "programs": decoded.programs,
            "fee_payer": decoded.fee_payer,
        });

        let mut outcome = Outcome::default();
        let slot = i64::try_from(update.slot).context("slot out of range")?;

        // Core assets are resolved against DAS first: a brand-new mint has no
        // `assets` row yet, and creating it here is what lets the very same
        // transaction's activity row satisfy the `asset_id` FK.
        let mut core_events: Vec<(String, DecodedKind, Option<String>, Option<String>)> =
            Vec::new();
        for touch in &decoded.core {
            match self.resolve_core(touch, slot).await {
                Ok(Some(event)) => {
                    outcome.hydrated += 1;
                    core_events.push(event);
                }
                Ok(None) => {}
                Err(error) => log::warn!("core resolve {}: {error:#}", touch.asset),
            }
        }

        let mut wanted: BTreeSet<String> =
            decoded.events.iter().map(|e| e.address.clone()).collect();
        wanted.extend(core_events.iter().map(|(address, ..)| address.clone()));
        if wanted.is_empty() {
            return Ok(outcome);
        }

        let addresses: Vec<String> = wanted.into_iter().collect();
        let known: BTreeMap<String, AssetRef> = activity::assets_by_address(&self.pool, &addresses)
            .await?
            .into_iter()
            .map(|r| (r.address.clone(), r))
            .collect();

        // Resolve the block time once per transaction. Without it no activity
        // row may be written at all, so the signature is parked instead.
        let block_time = self.block_times.get(&self.das, slot).await;
        let Some(block_time) = block_time else {
            for asset in known.values() {
                activity::park_signature(&self.pool, asset.id, &update.signature, slot, false)
                    .await?;
                outcome.parked += 1;
            }
            log::error!(
                "slot {slot} has no block time; parked {} signature(s) for {}",
                outcome.parked,
                update.signature
            );
            return Ok(outcome);
        };

        for event in &decoded.events {
            let Some(asset) = known.get(&event.address) else {
                outcome.untracked += 1;
                continue;
            };
            let mut event_details = details.clone();
            event_details["instruction"] = json!(event.instruction);
            outcome.add(
                self.apply(
                    asset,
                    &update.signature,
                    event.seq,
                    slot,
                    block_time,
                    kind_of(event.kind),
                    event.from_owner.as_deref(),
                    event.to_owner.as_deref(),
                    &event_details,
                    source,
                )
                .await?,
            );
        }

        for (address, kind, from, to) in &core_events {
            let Some(asset) = known.get(address) else {
                outcome.untracked += 1;
                continue;
            };
            let mut event_details = details.clone();
            event_details["instruction"] = json!("core");
            outcome.add(
                self.apply(
                    asset,
                    &update.signature,
                    0,
                    slot,
                    block_time,
                    kind_of(*kind),
                    from.as_deref(),
                    to.as_deref(),
                    &event_details,
                    source,
                )
                .await?,
            );
        }

        Ok(outcome)
    }

    /// One asset, one transaction. Retries once on a commit-time conflict —
    /// the exclusion constraint is deferred, so a genuine overlap only
    /// surfaces at `commit()` and would otherwise look like a transport fault.
    #[allow(clippy::too_many_arguments)]
    async fn apply(
        &self,
        asset: &AssetRef,
        signature: &str,
        seq: i16,
        slot: i64,
        block_time: DateTime<Utc>,
        kind: EventKind,
        from_owner: Option<&str>,
        to_owner: Option<&str>,
        details: &Value,
        source: &str,
    ) -> anyhow::Result<Outcome> {
        let event = LiveEvent {
            asset_id: asset.id,
            collection_id: asset.collection_id,
            signature,
            seq,
            slot,
            block_time,
            kind,
            from_owner,
            to_owner,
            // The live path never prices a sale: it has no venue registry and
            // no reason to re-read a transaction. It records an honest
            // transfer and hands the program ids to ALG-622 in `details`.
            price_lamports: None,
            marketplace: None,
            details: Some(details),
            source,
        };

        for attempt in 0..2 {
            let mut tx = self.pool.begin().await?;
            let applied = activity::record(&mut tx, &event).await?;
            match tx.commit().await {
                Ok(()) => {
                    return Ok(Outcome {
                        recorded: u64::from(!applied.is_redelivery()),
                        redelivered: u64::from(applied.is_redelivery()),
                        dirty: u64::from(applied.dirty),
                        ..Outcome::default()
                    })
                }
                Err(error) if attempt == 0 && activity::is_retryable_conflict(&error) => {
                    log::warn!("{signature}/{}: retrying after {error}", asset.address);
                }
                Err(error) => {
                    // A second conflict means the history genuinely disagrees;
                    // flag it for the rebuild rather than killing the stream.
                    if activity::is_retryable_conflict(&error) {
                        activity::mark_dirty(&self.pool, asset.id).await?;
                        log::error!(
                            "{signature}/{}: ownership conflict persisted, flagged dirty",
                            asset.address
                        );
                        return Ok(Outcome {
                            dirty: 1,
                            ..Outcome::default()
                        });
                    }
                    return Err(error.into());
                }
            }
        }
        Ok(Outcome::default())
    }

    /// Classifies a Metaplex Core touch from DAS state rather than from the
    /// instruction.
    ///
    /// Decoding Core's Borsh discriminators would mean hardcoding protocol
    /// constants for a rare event; asking DAS what the asset looks like now is
    /// self-correcting and reuses the backfill's own merge. The cost is stated
    /// in the README: two ownership changes for one asset inside the DAS read
    /// window would attribute the first transfer's receiver to the final
    /// owner, which reconciliation later heals.
    async fn resolve_core(
        &self,
        touch: &CoreTouch,
        slot: i64,
    ) -> anyhow::Result<Option<(String, DecodedKind, Option<String>, Option<String>)>> {
        let address = touch.asset.as_str();
        let stored = activity::assets_by_address(&self.pool, &[address.to_string()])
            .await?
            .into_iter()
            .next();

        let batch = self.das.get_asset_batch(&[address.to_string()]).await?;
        let Some(asset) = batch.found.into_iter().next() else {
            return Ok(None);
        };

        match stored {
            // Known asset: compare DAS against what we hold.
            Some(stored) => {
                if asset.burnt && !stored.burned {
                    return Ok(Some((
                        address.to_string(),
                        DecodedKind::Burn,
                        stored.owner,
                        None,
                    )));
                }
                let das_owner = asset.owner().map(str::to_string);
                if das_owner.is_some() && das_owner != stored.owner {
                    return Ok(Some((
                        address.to_string(),
                        DecodedKind::Transfer,
                        stored.owner,
                        das_owner,
                    )));
                }
                // No ownership change: a metadata update. Refresh the row so
                // the Explorer sees it, but write no activity.
                self.upsert(&asset, stored.collection_id, slot).await?;
                Ok(None)
            }
            // Unknown asset: a new Core mint. Create the row first so the
            // activity row's FK resolves, then report the mint.
            None => {
                // The decoder already told us which registered collection the
                // instruction named, so the row is filed without asking DAS
                // for a grouping.
                let Some(collection_id) = self.collection_id(&touch.collection).await? else {
                    return Ok(None);
                };
                let asset = self.hydrate(address, asset).await;
                self.upsert(&asset, collection_id, slot).await?;
                Ok(Some((
                    address.to_string(),
                    DecodedKind::Mint,
                    None,
                    asset.owner().map(str::to_string),
                )))
            }
        }
    }

    /// Re-reads a fresh mint until DAS has its attributes, or the budget runs
    /// out. A mint is visible on chain before DAS has indexed its metadata.
    async fn hydrate(&self, address: &str, first: indexer_das::Asset) -> indexer_das::Asset {
        if first.attributes().is_some_and(|a| !a.is_empty()) {
            return first;
        }
        for delay in HYDRATE_BACKOFF_MS {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            match self.das.get_asset_batch(&[address.to_string()]).await {
                Ok(batch) => {
                    if let Some(asset) = batch.found.into_iter().next() {
                        if asset.attributes().is_some_and(|a| !a.is_empty()) {
                            return asset;
                        }
                    }
                }
                Err(error) => log::warn!("hydrating {address}: {error}"),
            }
        }
        log::warn!("{address}: no attributes from DAS within the hydration budget");
        first
    }

    async fn upsert(
        &self,
        asset: &indexer_das::Asset,
        collection_id: i32,
        slot: i64,
    ) -> anyhow::Result<()> {
        let input = merge(asset, None);
        let mut tx = self.pool.begin().await?;
        assets::upsert_batch(&mut tx, collection_id, slot, std::slice::from_ref(&input)).await?;
        tx.commit().await?;
        Ok(())
    }

    /// The registered collection with this address. Matching on the address
    /// the registry supplied keeps slugs out of pipeline code.
    async fn collection_id(&self, address: &str) -> anyhow::Result<Option<i32>> {
        Ok(indexer_data_model::registry::list_enabled(&self.pool)
            .await?
            .into_iter()
            .find(|c| c.address.as_deref() == Some(address))
            .map(|c| c.id))
    }
}

const fn kind_of(kind: DecodedKind) -> EventKind {
    match kind {
        // A marketplace-mediated change is recorded honestly as a transfer;
        // `sale` needs a price, which `activity_sale_has_price` enforces and
        // ALG-622 supplies. The program ids are in `details` so it can
        // reclassify without re-fetching.
        DecodedKind::Transfer => EventKind::Transfer,
        DecodedKind::Mint => EventKind::Mint,
        DecodedKind::Burn => EventKind::Burn,
    }
}
