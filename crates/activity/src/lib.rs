//! Historical activity backfill (ALG-622): the full transaction timeline per
//! NFT, from archival RPC.
//!
//! The live pipeline only sees what happens after it connects, and this
//! transport has no replay — so every 2021-era mint, transfer and marketplace
//! sale has to be crawled. What comes out is `activity` rows (including priced
//! `sale`s), the `ownership_history` intervals derived from them, and the raw
//! `asset_signatures` list the crawl walked.
//!
//! Three rules this crate inherits and keeps:
//!
//! - **No SQL here.** Every statement lives in
//!   [`indexer_data_model::activity`], the same split `crates/das` follows, so
//!   the writer invariants stay provable against Postgres with no network.
//! - **No on-chain address in Rust.** Marketplace program ids are addresses
//!   like any other and live in `config/marketplaces.toml`; see
//!   [`marketplaces`].
//! - **It is not an [`indexer_ingest::IngestSource`]**, and must not become
//!   one. That trait is the streaming interface with a slot cursor; a
//!   backfill's cursor lives in `backfill_state`.
//!
//! Writes go through [`indexer_data_model::activity::record`] with
//! `source = "backfill"` — the same entry point and the same contract as the
//! live path, which is what makes the two results comparable. An event that
//! predates an asset's frontier (because the live pipeline already recorded
//! something newer) is stored and flags `ownership_dirty`; the crawl then runs
//! the rebuild itself, so a backfill over a running system converges rather
//! than leaving work behind.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use indexer_das::DasClient;
use indexer_data_model::activity::{self, AssetRef, LiveEvent};
use indexer_data_model::types::MembershipRule;
use indexer_data_model::{ingest_state, registry, PgPool};
use serde_json::json;

pub mod chain;
pub mod classify;
pub mod crawl;
pub mod marketplaces;

pub use marketplaces::Venues;

/// `backfill_state.kind` for this pass — assigned to ALG-622 by
/// `20260829000500_ingest_state.sql`, and matching its `^[a-z_]+$` CHECK.
pub const KIND: &str = "activity";

/// Assets per cursor commit. Small: each asset is several RPC calls, and a
/// crash should not replay much.
const DEFAULT_BATCH: usize = 25;

#[derive(Debug, Clone)]
pub struct Options {
    pub slug: Option<String>,
    /// Crawl exactly one asset, by its on-chain address. The hand-verification
    /// path: pick an old pig, crawl it, diff the timeline against an explorer.
    pub address: Option<String>,
    /// Continue from `backfill_state` instead of restarting at the first
    /// asset. Opt-in, like the DAS backfill: writes are idempotent, so
    /// restarting is always safe.
    pub resume: bool,
    pub limit: Option<usize>,
    pub batch: usize,
    /// Concurrent per-asset crawls. The RPC rate limit is enforced in the DAS
    /// client and shared across them, so this only controls how many are in
    /// flight.
    pub concurrency: usize,
    /// Throw away each asset's derived rows and re-derive them. The raw
    /// signatures survive.
    pub reclassify: bool,
    /// Database-only pass: promote already-stored transfers to sales using the
    /// current venue registry, with no network at all.
    pub reprice_only: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            slug: None,
            address: None,
            resume: false,
            limit: None,
            batch: DEFAULT_BATCH,
            concurrency: 4,
            reclassify: false,
            reprice_only: false,
        }
    }
}

/// Emitted once per committed batch so the caller owns the printing.
#[derive(Debug, Clone)]
pub struct BatchProgress {
    pub slug: String,
    pub assets: usize,
    pub counts: Counts,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub assets: u64,
    pub signatures: u64,
    pub events: u64,
    pub sales: u64,
    pub redelivered: u64,
    /// Assets whose timeline needed the token-account expansion.
    pub expanded: u64,
    /// Assets whose derived history still disagrees with DAS *after* the
    /// write and any rebuild — the acceptance criterion, asked of the same
    /// view the acceptance query uses.
    pub mismatched: u64,
    /// Assets with no ownership events to check — Metaplex Core, whose
    /// instructions the RPC does not parse.
    pub unverifiable: u64,
    pub rebuilt: u64,
    /// Events dropped for want of a `blockTime`; their signatures stay
    /// unclassified.
    pub undated: u64,
    pub repriced: u64,
}

impl Counts {
    pub fn add(&mut self, other: Counts) {
        self.assets += other.assets;
        self.signatures += other.signatures;
        self.events += other.events;
        self.sales += other.sales;
        self.redelivered += other.redelivered;
        self.expanded += other.expanded;
        self.mismatched += other.mismatched;
        self.unverifiable += other.unverifiable;
        self.rebuilt += other.rebuilt;
        self.undated += other.undated;
        self.repriced += other.repriced;
    }

    /// Did this change anything? Backs `--expect-unchanged`. Reading and
    /// re-deriving are not changes; writing rows is.
    pub fn is_noop(&self) -> bool {
        self.events == 0 && self.signatures == 0 && self.repriced == 0 && self.rebuilt == 0
    }
}

#[derive(Debug, Clone)]
pub struct CollectionReport {
    pub slug: String,
    pub rule: MembershipRule,
    pub counts: Counts,
    pub status: String,
    pub elapsed: Duration,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Report {
    pub collections: Vec<CollectionReport>,
    pub warnings: Vec<String>,
}

impl Report {
    pub fn totals(&self) -> Counts {
        let mut total = Counts::default();
        for collection in &self.collections {
            total.add(collection.counts);
        }
        total
    }

    pub fn is_noop(&self) -> bool {
        self.totals().is_noop()
    }
}

/// Runs the backfill over every enabled collection, or just `options.slug`.
///
/// A collection that fails is recorded and the run moves on, exactly as the
/// DAS backfill does: one asset with a pathological history must not block the
/// other collections.
pub async fn run<F>(
    pool: &PgPool,
    das: &DasClient,
    venues: &Venues,
    options: &Options,
    mut progress: F,
) -> Result<Report>
where
    F: FnMut(&BatchProgress),
{
    let collections = match &options.slug {
        Some(slug) => vec![registry::by_slug(pool, slug)
            .await?
            .with_context(|| format!("collection {slug} not found"))?],
        None => registry::list_enabled(pool).await?,
    };
    if let Some(address) = &options.address {
        // One asset: skip the cursor entirely. A single-asset run is a
        // verification, not progress, and must never claim a collection is
        // further along than it is.
        let asset = activity::assets_by_address(pool, std::slice::from_ref(address))
            .await?
            .into_iter()
            .next()
            .with_context(|| {
                format!("{address} is not a tracked asset — run the DAS backfill first")
            })?;
        let venue_count = venues.len();
        log::info!("crawling one asset against {venue_count} venue(s)");
        let core = core_collections(pool).await?;
        let outcome = crawl::crawl_asset(das, venues, &core, &asset).await?;
        let counts = write_asset(pool, &asset, &outcome, options).await?;
        return Ok(Report {
            collections: vec![CollectionReport {
                slug: address.clone(),
                rule: MembershipRule::TmAllowlist,
                counts,
                status: "done".into(),
                elapsed: std::time::Duration::default(),
                warnings: Vec::new(),
            }],
            warnings: Vec::new(),
        });
    }

    let mut report = Report::default();
    if venues.is_empty() {
        report.warnings.push(
            "no marketplaces configured: every marketplace move stays an honest transfer".into(),
        );
    }

    for collection in collections {
        if !collection.enabled {
            report
                .warnings
                .push(format!("{}: disabled, skipped", collection.slug));
            continue;
        }
        let Some(rule) = collection.membership_rule else {
            report.warnings.push(format!(
                "{}: no membership rule (unresolvable registry row), skipped",
                collection.slug
            ));
            continue;
        };

        let started = Instant::now();
        let outcome = backfill_collection(pool, das, venues, options, &collection, &mut progress)
            .await
            .map(|(counts, warnings)| CollectionReport {
                slug: collection.slug.clone(),
                rule,
                counts,
                status: "done".into(),
                elapsed: started.elapsed(),
                warnings,
            });

        match outcome {
            Ok(collection_report) => report.collections.push(collection_report),
            Err(error) => {
                let message = format!("{error:#}");
                mark_failed(pool, collection.id, &message).await;
                report
                    .warnings
                    .push(format!("{}: FAILED — {message}", collection.slug));
                report.collections.push(CollectionReport {
                    slug: collection.slug.clone(),
                    rule,
                    counts: Counts::default(),
                    status: "failed".into(),
                    elapsed: started.elapsed(),
                    warnings: Vec::new(),
                });
            }
        }
    }
    Ok(report)
}

/// One collection: keyset-page its assets, crawl each, commit the cursor with
/// the batch.
async fn backfill_collection<F>(
    pool: &PgPool,
    das: &DasClient,
    venues: &Venues,
    options: &Options,
    collection: &registry::CollectionRow,
    progress: &mut F,
) -> Result<(Counts, Vec<String>)>
where
    F: FnMut(&BatchProgress),
{
    let mut counts = Counts::default();
    let mut warnings = Vec::new();

    if options.reprice_only {
        counts.repriced = reprice(pool, venues, collection.id).await?;
        write_state(pool, collection.id, 0, &counts, "done", None).await?;
        return Ok((counts, warnings));
    }

    let previous = ingest_state::backfill_state(pool, collection.id, KIND).await?;
    let mut after_id = match (options.resume, &previous) {
        (true, Some(state)) => state
            .cursor
            .get("after_asset_id")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0),
        _ => 0,
    };
    let started_at = match (options.resume, &previous) {
        (true, Some(state)) => state.started_at.unwrap_or_else(chrono::Utc::now),
        _ => chrono::Utc::now(),
    };

    let core_collections = core_collections(pool).await?;
    let batch = options.batch.max(1) as i64;
    let mut processed = 0usize;

    loop {
        if options.limit.is_some_and(|limit| processed >= limit) {
            break;
        }
        let mut assets = activity::assets_after(pool, collection.id, after_id, batch).await?;
        if assets.is_empty() {
            break;
        }
        if let Some(limit) = options.limit {
            assets.truncate(limit - processed);
        }
        let started = Instant::now();

        // Crawls run concurrently; the writes do not. Every write is its own
        // per-asset transaction (the schema asks for it — GiST inserts and
        // deferred constraints make a giant transaction expensive), and doing
        // them in order keeps the cursor honest.
        let crawled = futures_util::future::join_all(assets.iter().map(|asset| {
            let core_collections = &core_collections;
            async move {
                (
                    asset,
                    crawl::crawl_asset(das, venues, core_collections, asset).await,
                )
            }
        }))
        .await;

        let mut batch_counts = Counts::default();
        for (asset, result) in crawled {
            let outcome = result.with_context(|| format!("crawling {}", asset.address))?;
            batch_counts.add(write_asset(pool, asset, &outcome, options).await?);
            after_id = asset.id;
            processed += 1;
        }

        counts.add(batch_counts);
        write_state(
            pool,
            collection.id,
            after_id,
            &counts,
            "running",
            Some(started_at),
        )
        .await?;
        progress(&BatchProgress {
            slug: collection.slug.clone(),
            assets: assets.len(),
            counts: batch_counts,
            elapsed: started.elapsed(),
        });
    }

    // A `--limit` run has not backfilled the collection, so it must not claim
    // it did — the next unlimited run picks up from the same cursor.
    let status = if options.limit.is_none() {
        "done"
    } else {
        "running"
    };
    write_state(
        pool,
        collection.id,
        after_id,
        &counts,
        status,
        Some(started_at),
    )
    .await?;

    if counts.unverifiable > 0 && counts.events == 0 && counts.signatures > 0 {
        warnings.push(format!(
            "{}: {} asset(s) yielded signatures but no ownership events. Metaplex Core \
             instructions are Borsh and the RPC does not parse them, so their timeline is \
             stored raw and left unclassified rather than guessed at",
            collection.slug, counts.unverifiable
        ));
    }
    if counts.mismatched > 0 {
        warnings.push(format!(
            "{}: {} asset(s) still disagree with DAS after expansion — see \
             integrity_owner_mismatch",
            collection.slug, counts.mismatched
        ));
    }
    if counts.undated > 0 {
        warnings.push(format!(
            "{}: {} event(s) had no blockTime and were left unclassified",
            collection.slug, counts.undated
        ));
    }
    Ok((counts, warnings))
}

/// Writes one asset's crawl: signatures, then events in order, then the
/// rebuild if anything landed out of order.
async fn write_asset(
    pool: &PgPool,
    asset: &AssetRef,
    outcome: &crawl::AssetCrawl,
    options: &Options,
) -> Result<Counts> {
    let mut counts = Counts {
        assets: 1,
        undated: outcome.undated as u64,
        ..Counts::default()
    };
    if outcome.verdict == chain::Verdict::Unverifiable {
        counts.unverifiable += 1;
    }
    if outcome.queried > 1 {
        counts.expanded += 1;
    }

    let mut tx = pool.begin().await?;
    if options.reclassify {
        activity::reset_for_reclassify(&mut tx, asset.id).await?;
    }
    counts.signatures +=
        activity::record_signatures(&mut *tx, asset.id, &outcome.signatures).await?;

    let mut dirty = false;
    let mut classified: Vec<String> = Vec::new();
    for event in &outcome.events {
        let applied = activity::record(
            &mut tx,
            &LiveEvent {
                asset_id: asset.id,
                collection_id: asset.collection_id,
                signature: &event.signature,
                seq: event.seq,
                slot: event.slot,
                block_time: event.block_time,
                kind: event.kind,
                from_owner: event.from_owner.as_deref(),
                to_owner: event.to_owner.as_deref(),
                price_lamports: event.price_lamports,
                marketplace: event.marketplace.as_deref(),
                details: Some(&event.details),
                source: "backfill",
            },
        )
        .await?;
        if applied.is_redelivery() {
            counts.redelivered += 1;
        } else {
            counts.events += 1;
            if event.kind == indexer_data_model::types::EventKind::Sale {
                counts.sales += 1;
            }
        }
        dirty |= applied.dirty;
        classified.push(event.signature.clone());
    }
    activity::mark_classified(&mut *tx, asset.id, &classified).await?;
    tx.commit().await?;

    // An event older than what the live pipeline already recorded is stored
    // but not applied — that is the writer contract. The rebuild is the other
    // half of it, and running it here is what lets a backfill over a live
    // system converge instead of leaving `ownership_dirty` set.
    if dirty {
        let mut tx = pool.begin().await?;
        activity::rebuild_ownership(&mut tx, asset.id).await?;
        tx.commit().await?;
        counts.rebuilt += 1;
    }

    // Asked after the write and the rebuild, not of the crawl's own verdict:
    // an event the writer stored out of order is a disagreement until the
    // rebuild re-derives the intervals, and reporting the earlier reading
    // would overstate the failures by every asset the pass then repaired.
    if !activity::owner_agrees(pool, asset.id).await? {
        counts.mismatched += 1;
    }
    Ok(counts)
}

/// The database-only pass: promote stored transfers to sales using the current
/// venue registry. No network — the price the classifier derived at crawl time
/// is in `details`.
async fn reprice(pool: &PgPool, venues: &Venues, collection_id: i32) -> Result<u64> {
    const PAGE: i64 = 500;
    let mut after_id = 0i64;
    let mut repriced = 0u64;
    loop {
        let candidates = activity::reprice_candidates(pool, collection_id, after_id, PAGE).await?;
        if candidates.is_empty() {
            return Ok(repriced);
        }
        for candidate in &candidates {
            after_id = candidate.id;
            let programs: Vec<String> = candidate
                .details
                .get("programs")
                .and_then(serde_json::Value::as_array)
                .map(|list| {
                    list.iter()
                        .filter_map(|p| p.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let (Some(venue), Some(price)) = (
                venues.find(&programs),
                candidate
                    .details
                    .pointer("/price_candidate/lamports")
                    .and_then(serde_json::Value::as_i64)
                    .filter(|price| *price > 0),
            ) else {
                continue;
            };
            if activity::promote_to_sale(pool, candidate.id, price, Some(venue)).await? {
                repriced += 1;
            }
        }
    }
}

/// Metaplex Core collection addresses, for the decoder's structural
/// recognition.
async fn core_collections(pool: &PgPool) -> Result<BTreeSet<String>> {
    Ok(registry::list_enabled(pool)
        .await?
        .into_iter()
        .filter(|c| c.membership_rule == Some(MembershipRule::CoreCollection))
        .filter_map(|c| c.address)
        .collect())
}

async fn write_state(
    pool: &PgPool,
    collection_id: i32,
    after_id: i64,
    counts: &Counts,
    status: &str,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<()> {
    let state = ingest_state::BackfillState {
        collection_id,
        kind: KIND.to_string(),
        status: status.to_string(),
        cursor: json!({"mode": "activity", "after_asset_id": after_id}),
        progress: json!({
            "assets": counts.assets,
            "signatures": counts.signatures,
            "events": counts.events,
            "sales": counts.sales,
            "expanded": counts.expanded,
            "mismatched": counts.mismatched,
            "unverifiable": counts.unverifiable,
            "rebuilt": counts.rebuilt,
            "undated": counts.undated,
            "repriced": counts.repriced,
        }),
        last_error: None,
        started_at,
        finished_at: (status == "done").then(chrono::Utc::now),
        updated_at: chrono::Utc::now(),
    };
    ingest_state::put_backfill_state(pool, &state).await?;
    Ok(())
}

/// Records a failure without losing the cursor. Never returns `Err` — the run
/// is already failing and a second error would hide the first.
async fn mark_failed(pool: &PgPool, collection_id: i32, error: &str) {
    let previous = ingest_state::backfill_state(pool, collection_id, KIND)
        .await
        .ok()
        .flatten();
    let state = ingest_state::BackfillState {
        collection_id,
        kind: KIND.to_string(),
        status: "failed".into(),
        cursor: previous
            .as_ref()
            .map(|s| s.cursor.clone())
            .unwrap_or_else(|| json!({})),
        progress: previous
            .as_ref()
            .map(|s| s.progress.clone())
            .unwrap_or_else(|| json!({})),
        last_error: Some(error.chars().take(2000).collect()),
        started_at: previous.as_ref().and_then(|s| s.started_at),
        finished_at: None,
        updated_at: chrono::Utc::now(),
    };
    if let Err(error) = ingest_state::put_backfill_state(pool, &state).await {
        log::error!("could not record the failure for collection {collection_id}: {error}");
    }
}
