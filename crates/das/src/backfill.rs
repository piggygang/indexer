//! The backfill pass itself: enumerate a collection's members, ask DAS about
//! them, fetch the off-chain documents that are actually stale, and write the
//! batch and its cursor in one transaction.
//!
//! Membership is decided by `match`ing on [`MembershipRule`] — one arm per
//! rule, never on a slug — so onboarding a collection stays a TOML change.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Context;
use futures_util::StreamExt;
use serde_json::{json, Value};

use indexer_data_model::assets::{self, AssetDocument, AssetInput, BatchCounts};
use indexer_data_model::registry::CollectionRow;
use indexer_data_model::types::{ImageStatus, MembershipRule};
use indexer_data_model::{attributes, ingest_state, registry, PgPool};

use crate::asset::{self, Asset};
use crate::client::{DasClient, Reachability};

/// `backfill_state.kind` for this pass — assigned to ALG-621 by
/// `20260829000500_ingest_state.sql`. Matches its `^[a-z_]+$` CHECK.
pub const KIND: &str = "das_assets";

/// `backfill_state.kind` for the opt-in image reachability pass.
pub const IMAGE_KIND: &str = "image_status";

/// A faceted trait type this wide is per-asset-unique and should be in the
/// collection's `facet_exclude` — see [`CollectionReport::warnings`].
const FACET_CARDINALITY_WARN_RATIO: f64 = 0.5;

#[derive(Debug, Clone)]
pub struct BackfillOptions {
    pub slug: Option<String>,
    pub resume: bool,
    pub limit: Option<usize>,
    pub batch: usize,
    pub fetch_concurrency: usize,
    pub das_only: bool,
    pub refetch_documents: bool,
    pub check_images: bool,
    pub recheck_images_after_days: i32,
}

impl Default for BackfillOptions {
    fn default() -> Self {
        Self {
            slug: None,
            resume: false,
            limit: None,
            batch: crate::client::MAX_BATCH,
            fetch_concurrency: 16,
            das_only: false,
            refetch_documents: false,
            check_images: false,
            recheck_images_after_days: 30,
        }
    }
}

/// Emitted once per committed batch, so the caller can print progress in its
/// own style without this crate owning stdout.
#[derive(Debug, Clone)]
pub struct BatchProgress {
    pub slug: String,
    pub batch: usize,
    pub batches: Option<usize>,
    pub slot: i64,
    pub counts: BatchCounts,
    pub documents_wanted: usize,
    pub documents_failed: usize,
    pub missing: usize,
    pub elapsed: Duration,
}

#[derive(Debug, Clone)]
pub struct CollectionReport {
    pub slug: String,
    pub rule: MembershipRule,
    /// Rows in the browse population after the pass.
    pub members: i64,
    pub counts: BatchCounts,
    /// Ids DAS does not know. Capped for the report; the count is exact.
    pub missing: Vec<String>,
    pub missing_total: usize,
    pub documents_failed: usize,
    pub images_ok: u64,
    pub images_dead: u64,
    pub status: String,
    pub elapsed: Duration,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct BackfillReport {
    pub collections: Vec<CollectionReport>,
    pub warnings: Vec<String>,
}

impl BackfillReport {
    pub fn totals(&self) -> BatchCounts {
        let mut total = BatchCounts::default();
        for collection in &self.collections {
            total.add(collection.counts);
        }
        total
    }

    /// Did the whole run change nothing? Backs `--expect-unchanged`.
    pub fn is_noop(&self) -> bool {
        self.totals().is_noop()
    }
}

/// How many missing ids the report carries verbatim (the count is always
/// exact); enough to act on, bounded so a pathological run cannot blow up
/// `backfill_state.progress`.
const MISSING_SAMPLE: usize = 500;

/// Runs the backfill over every enabled collection, or just `options.slug`.
///
/// A collection that fails is recorded and the run moves on to the next one —
/// one dead metadata host must not block the other three collections — and
/// the error surfaces through the report's status plus a non-zero exit from
/// the caller.
pub async fn run<F>(
    pool: &PgPool,
    das: &DasClient,
    options: &BackfillOptions,
    mut progress: F,
) -> anyhow::Result<BackfillReport>
where
    F: FnMut(&BatchProgress),
{
    let collections = match &options.slug {
        Some(slug) => vec![registry::by_slug(pool, slug)
            .await?
            .with_context(|| format!("collection {slug} not found"))?],
        None => registry::list_enabled(pool).await?,
    };

    let mut report = BackfillReport::default();
    for collection in collections {
        if !collection.enabled {
            report
                .warnings
                .push(format!("{}: disabled, skipped", collection.slug));
            continue;
        }
        let Some(rule) = collection.membership_rule else {
            // `collections_enabled_resolvable` makes this unreachable for an
            // enabled row, but the type is Option and silence would be worse.
            report.warnings.push(format!(
                "{}: no membership rule (unresolvable registry row), skipped",
                collection.slug
            ));
            continue;
        };

        match backfill_collection(pool, das, options, &collection, rule, &mut progress).await {
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
                    members: assets::member_count(pool, collection.id)
                        .await
                        .unwrap_or(-1),
                    counts: BatchCounts::default(),
                    missing: Vec::new(),
                    missing_total: 0,
                    documents_failed: 0,
                    images_ok: 0,
                    images_dead: 0,
                    status: "failed".into(),
                    elapsed: Duration::ZERO,
                    warnings: Vec::new(),
                });
            }
        }
    }

    if report
        .collections
        .iter()
        .any(|c| c.counts.inserted > 0 || c.counts.attributes_written > 0)
    {
        // Stated obligation of the assets migration: a per-asset-unique trait
        // leaves thousands of singleton values that mislead the facet planner.
        assets::analyze_after_backfill(pool)
            .await
            .context("ANALYZE after backfill")?;
    }

    Ok(report)
}

async fn backfill_collection<F>(
    pool: &PgPool,
    das: &DasClient,
    options: &BackfillOptions,
    collection: &CollectionRow,
    rule: MembershipRule,
    progress: &mut F,
) -> anyhow::Result<CollectionReport>
where
    F: FnMut(&BatchProgress),
{
    let started = Instant::now();
    let batch_size = options.batch.clamp(1, crate::client::MAX_BATCH);

    // One arm per rule. `tm_collection` has no registry row today, but the
    // arm exists so adding one stays a TOML change.
    let source = match rule {
        MembershipRule::TmAllowlist => {
            let mints = registry::allowlist(pool, collection.id).await?;
            anyhow::ensure!(
                !mints.is_empty(),
                "{} is a tm_allowlist collection with no mints — run `seed` first",
                collection.slug
            );
            Source::Allowlist(mints)
        }
        MembershipRule::CoreCollection | MembershipRule::TmCollection => {
            let address = collection
                .address
                .clone()
                .with_context(|| format!("{} has no address but rule {rule:?}", collection.slug))?;
            Source::Search(address)
        }
    };

    let previous = ingest_state::backfill_state(pool, collection.id, KIND).await?;
    let mut cursor = Cursor::start(&source);
    if options.resume {
        if let Some(state) = previous.as_ref() {
            match Cursor::resume(&state.cursor, &source) {
                Some(resumed) => cursor = resumed,
                None => log::warn!(
                    "{}: stored cursor does not match rule {rule:?}, restarting",
                    collection.slug
                ),
            }
        }
    }

    let started_at = previous
        .as_ref()
        .filter(|_| options.resume)
        .and_then(|s| s.started_at)
        .unwrap_or_else(chrono::Utc::now);

    let mut totals = BatchCounts::default();
    let mut missing: Vec<String> = Vec::new();
    let mut missing_total = 0usize;
    let mut documents_failed = 0usize;
    let mut processed = 0usize;
    let mut batch_index = 0usize;
    let mut slot_seen = 0i64;
    let mut complete = false;

    loop {
        if options.limit.is_some_and(|limit| processed >= limit) {
            break;
        }
        let batch_started = Instant::now();

        // Read the slot BEFORE the data call: the response reflects chain
        // state at or after it, so this is a conservative lower bound for
        // `assets.owner_slot`. Reading it afterwards could stamp a slot newer
        // than the observation and defeat the stale-writer guard forever.
        let slot = das.get_slot().await.context("getSlot")?;
        slot_seen = slot;

        let (found, batch_missing) = match &source {
            Source::Allowlist(mints) => {
                let start = cursor.next_index;
                if start >= mints.len() {
                    complete = true;
                    break;
                }
                let mut end = (start + batch_size).min(mints.len());
                if let Some(limit) = options.limit {
                    end = end.min(start + limit.saturating_sub(processed));
                }
                let result = das
                    .get_asset_batch(&mints[start..end])
                    .await
                    .context("getAssetBatch")?;
                cursor.next_index = end;
                processed += end - start;
                if cursor.next_index >= mints.len() {
                    complete = true;
                }
                (result.found, result.missing)
            }
            Source::Search(address) => {
                let page = das
                    .search_assets(
                        address,
                        cursor.next_page,
                        batch_size as u32,
                        cursor.next_page == 1,
                    )
                    .await
                    .context("searchAssets")?;
                let full_page = page.items.len();
                if full_page == 0 {
                    complete = true;
                    break;
                }
                cursor.next_page += 1;
                let mut items = page.items;
                // The allowlist arm bounds its own slice; a search page can
                // only be trimmed after the fact.
                if let Some(limit) = options.limit {
                    items.truncate(limit.saturating_sub(processed));
                }
                processed += items.len();
                // A short page means the collection is exhausted.
                if full_page < batch_size {
                    complete = true;
                }
                (items, Vec::new())
            }
        };

        missing_total += batch_missing.len();
        for id in &batch_missing {
            if missing.len() < MISSING_SAMPLE {
                missing.push(id.clone());
            }
        }
        if !batch_missing.is_empty() {
            log::warn!(
                "{}: {} id(s) unknown to DAS in this batch (first: {})",
                collection.slug,
                batch_missing.len(),
                batch_missing.first().map(String::as_str).unwrap_or("-")
            );
        }

        let wanted = if options.das_only {
            Vec::new()
        } else {
            documents_to_fetch(pool, collection, &found, options.refetch_documents).await?
        };
        let mut documents = fetch_documents(das, &wanted, options.fetch_concurrency).await;
        let failed_here = wanted.len() - documents.len();
        documents_failed += failed_here;

        // For assets whose document we deliberately skipped, read the stored
        // copy back. The document is authoritative over DAS's cache, so
        // dropping it here would let a second pass revert name/image/
        // attributes to DAS's stale values — corrupting the data and making
        // every re-run report changes.
        let unfetched: Vec<String> = found
            .iter()
            .map(|a| a.id.clone())
            .filter(|id| !documents.contains_key(id))
            .collect();
        if !unfetched.is_empty() {
            for (address, source_uri, metadata_json) in
                assets::stored_documents(pool, collection.id, &unfetched).await?
            {
                documents.insert(address, (source_uri, metadata_json));
            }
        }

        let inputs: Vec<AssetInput> = found
            .iter()
            .map(|a| {
                merge(
                    a,
                    documents
                        .get(a.id.as_str())
                        .map(|(uri, doc)| (uri.as_str(), doc)),
                )
            })
            .collect();

        let mut tx = pool.begin().await?;
        let counts = assets::upsert_batch(&mut tx, collection.id, slot, &inputs).await?;
        totals.add(counts);

        let state = ingest_state::BackfillState {
            collection_id: collection.id,
            kind: KIND.into(),
            status: "running".into(),
            cursor: cursor.to_json(&source),
            progress: progress_json(
                started_at,
                slot,
                &totals,
                missing_total,
                documents_failed,
                &missing,
            ),
            last_error: None,
            started_at: Some(started_at),
            finished_at: None,
            updated_at: chrono::Utc::now(),
        };
        ingest_state::put_backfill_state(&mut *tx, &state).await?;
        tx.commit().await?;

        batch_index += 1;
        progress(&BatchProgress {
            slug: collection.slug.clone(),
            batch: batch_index,
            batches: source.batches(batch_size),
            slot,
            counts,
            documents_wanted: wanted.len(),
            documents_failed: failed_here,
            missing: batch_missing.len(),
            elapsed: batch_started.elapsed(),
        });

        if complete {
            break;
        }
    }

    let (images_ok, images_dead) = if options.check_images {
        check_images(pool, das, collection, options).await?
    } else {
        (0, 0)
    };

    // A `--limit` run has NOT backfilled the collection, so it must not claim
    // it did — the next unlimited run picks up from the same cursor.
    let status = if complete && options.limit.is_none() {
        "done"
    } else {
        "running"
    };
    let finished_at = (status == "done").then(chrono::Utc::now);
    let final_state = ingest_state::BackfillState {
        collection_id: collection.id,
        kind: KIND.into(),
        status: status.into(),
        cursor: cursor.to_json(&source),
        progress: progress_json(
            started_at,
            slot_seen,
            &totals,
            missing_total,
            documents_failed,
            &missing,
        ),
        last_error: None,
        started_at: Some(started_at),
        finished_at,
        updated_at: chrono::Utc::now(),
    };
    ingest_state::put_backfill_state(pool, &final_state).await?;

    let members = assets::member_count(pool, collection.id).await?;
    let mut warnings = Vec::new();
    if documents_failed > 0 {
        let reason = if collection.metadata_uri_template.is_none() {
            "no metadata_uri_template and the on-chain host did not answer"
        } else {
            "the re-host did not answer"
        };
        warnings.push(format!(
            "{}: {documents_failed} metadata document(s) unreachable ({reason})",
            collection.slug
        ));
    }
    warnings.extend(facet_cardinality_warnings(pool, collection, members).await?);

    Ok(CollectionReport {
        slug: collection.slug.clone(),
        rule,
        members,
        counts: totals,
        missing,
        missing_total,
        documents_failed,
        images_ok,
        images_dead,
        status: status.into(),
        elapsed: started.elapsed(),
        warnings,
    })
}

enum Source {
    Allowlist(Vec<String>),
    Search(String),
}

impl Source {
    fn batches(&self, batch_size: usize) -> Option<usize> {
        match self {
            Self::Allowlist(mints) => Some(mints.len().div_ceil(batch_size)),
            // A dynamic collection's size is not known until it is walked.
            Self::Search(_) => None,
        }
    }

    fn mode(&self) -> &'static str {
        match self {
            Self::Allowlist(_) => "allowlist",
            Self::Search(_) => "search",
        }
    }
}

/// Where the next batch starts. Persisted inside the same transaction as the
/// batch it follows, so it can never claim progress that was rolled back.
struct Cursor {
    next_index: usize,
    next_page: u32,
}

impl Cursor {
    fn start(_source: &Source) -> Self {
        Self {
            next_index: 0,
            next_page: 1,
        }
    }

    /// `None` when the stored cursor was written for a different membership
    /// rule — the registry changed under us and restarting is the safe move.
    fn resume(stored: &Value, source: &Source) -> Option<Self> {
        if stored.get("mode").and_then(Value::as_str)? != source.mode() {
            return None;
        }
        Some(Self {
            next_index: stored
                .get("next_index")
                .and_then(Value::as_u64)
                .unwrap_or(0) as usize,
            next_page: stored
                .get("next_page")
                .and_then(Value::as_u64)
                .unwrap_or(1)
                .max(1) as u32,
        })
    }

    fn to_json(&self, source: &Source) -> Value {
        match source {
            Source::Allowlist(mints) => json!({
                "mode": "allowlist",
                "next_index": self.next_index,
                "total": mints.len(),
            }),
            Source::Search(_) => json!({"mode": "search", "next_page": self.next_page}),
        }
    }
}

fn progress_json(
    started_at: chrono::DateTime<chrono::Utc>,
    slot: i64,
    counts: &BatchCounts,
    missing_total: usize,
    documents_failed: usize,
    missing: &[String],
) -> Value {
    json!({
        "run_started_at": started_at.to_rfc3339(),
        "slot": slot,
        "inserted": counts.inserted,
        "updated": counts.updated,
        "unchanged": counts.unchanged,
        "skipped_foreign": counts.skipped_foreign,
        "invalid": counts.invalid,
        "documents": counts.documents,
        "documents_failed": documents_failed,
        "attributes_written": counts.attributes_written,
        "attributes_removed": counts.attributes_removed,
        "missing": missing_total,
        "missing_sample": missing,
    })
}

/// Which assets need their off-chain document fetched.
///
/// A document is fetched when there is none stored, or when the URI we would
/// fetch today differs from the one recorded. That second clause is what
/// makes adding a `metadata_uri_template` to a collection later — Pig Mud,
/// once its metadata is re-hosted — fill in the gap on the next run with no
/// code change. It also means a second run over an unchanged collection
/// issues zero HTTP requests.
async fn documents_to_fetch(
    pool: &PgPool,
    collection: &CollectionRow,
    found: &[Asset],
    refetch: bool,
) -> anyhow::Result<Vec<(String, String)>> {
    let addresses: Vec<String> = found.iter().map(|a| a.id.clone()).collect();
    let stored = assets::document_state(pool, collection.id, &addresses).await?;
    let stored: HashMap<&str, &assets::DocumentState> =
        stored.iter().map(|s| (s.address.as_str(), s)).collect();

    let mut wanted = Vec::new();
    for asset in found {
        let Some(uri) = collection.metadata_source_uri(&asset.id, asset.json_uri()) else {
            continue;
        };
        let needed = match stored.get(asset.id.as_str()) {
            None => true,
            Some(state) => {
                !state.has_document || state.metadata_source_uri.as_deref() != Some(uri.as_str())
            }
        };
        if refetch || needed {
            wanted.push((asset.id.clone(), uri));
        }
    }
    Ok(wanted)
}

/// Fetches documents concurrently. A failure is never fatal: the asset keeps
/// whatever DAS said and, crucially, is left out of the attribute delete
/// scope so its stored attributes survive.
async fn fetch_documents(
    das: &DasClient,
    wanted: &[(String, String)],
    concurrency: usize,
) -> HashMap<String, (String, Value)> {
    futures_util::stream::iter(wanted.iter().cloned())
        .map(|(address, uri)| async move {
            match das.fetch_document(&uri).await {
                Ok(Some(document)) => Some((address, (uri, document))),
                Ok(None) => None,
                Err(error) => {
                    log::warn!("{address}: metadata fetch failed: {error}");
                    None
                }
            }
        })
        .buffer_unordered(concurrency.max(1))
        .filter_map(|result| async move { result })
        .collect()
        .await
}

/// Combines what DAS says with the document we fetched.
///
/// The document wins wherever it has an answer: it is the operator's
/// re-hosted copy, whereas DAS's cached metadata may predate the re-host or
/// be missing entirely. Where there is no document, DAS's own values are the
/// fallback — which is what lets an unrevealed collection whose host is dead
/// still get its assets, names and owners.
pub fn merge(das_asset: &Asset, document: Option<(&str, &Value)>) -> AssetInput {
    let doc = document.map(|(_, value)| value);

    let name = doc
        .and_then(|d| asset::document_string(d, "name"))
        .or_else(|| das_asset.name().map(str::to_string))
        .unwrap_or_default();
    let symbol = doc
        .and_then(|d| asset::document_string(d, "symbol"))
        .or_else(|| das_asset.symbol().map(str::to_string));
    let image_uri = doc
        .and_then(asset::document_image)
        .or_else(|| das_asset.image().map(str::to_string));
    let attributes = doc
        .and_then(asset::document_attributes)
        .or_else(|| das_asset.attributes());

    AssetInput {
        address: das_asset.id.clone(),
        name,
        symbol,
        metadata_uri: das_asset.json_uri().map(str::to_string),
        // Only a successful fetch records where the metadata actually came
        // from; the column means "the URI we really read".
        metadata_source_uri: document.map(|(uri, _)| uri.to_string()),
        image_uri,
        burned: das_asset.burnt,
        // `assets_burned_has_no_owner`: a burned asset has no owner, even
        // when DAS still reports the last one.
        owner: (!das_asset.burnt)
            .then(|| das_asset.owner().map(str::to_string))
            .flatten(),
        attributes,
        document: document.map(|(uri, value)| AssetDocument {
            metadata_json: value.clone(),
            source_uri: uri.to_string(),
        }),
    }
}

/// The opt-in reachability pass. Undetermined probes leave both columns
/// alone, so the next run retries them and a fully-determined collection is a
/// genuine no-op.
async fn check_images(
    pool: &PgPool,
    das: &DasClient,
    collection: &CollectionRow,
    options: &BackfillOptions,
) -> anyhow::Result<(u64, u64)> {
    const PAGE: i64 = 200;
    let mut after_id = 0i64;
    let (mut ok, mut dead) = (0u64, 0u64);

    loop {
        let candidates = assets::image_candidates(
            pool,
            collection.id,
            after_id,
            options.recheck_images_after_days,
            PAGE,
        )
        .await?;
        if candidates.is_empty() {
            break;
        }
        after_id = candidates.last().map(|(id, _, _)| *id).unwrap_or(after_id);

        let probed: Vec<(String, ImageStatus)> = futures_util::stream::iter(
            candidates.into_iter().map(|(_, address, uri)| async move {
                match das.probe_image(&uri).await {
                    Reachability::Ok => Some((address, ImageStatus::Ok)),
                    Reachability::Dead => Some((address, ImageStatus::Dead)),
                    Reachability::Undetermined => None,
                }
            }),
        )
        .buffer_unordered(options.fetch_concurrency.max(1))
        .filter_map(|result| async move { result })
        .collect()
        .await;

        ok += probed.iter().filter(|(_, s)| *s == ImageStatus::Ok).count() as u64;
        dead += probed
            .iter()
            .filter(|(_, s)| *s == ImageStatus::Dead)
            .count() as u64;
        assets::set_image_status(pool, collection.id, &probed).await?;

        let state = ingest_state::BackfillState {
            collection_id: collection.id,
            kind: IMAGE_KIND.into(),
            status: "running".into(),
            cursor: json!({"mode": "keyset", "after_id": after_id}),
            progress: json!({"ok": ok, "dead": dead}),
            last_error: None,
            started_at: Some(chrono::Utc::now()),
            finished_at: None,
            updated_at: chrono::Utc::now(),
        };
        ingest_state::put_backfill_state(pool, &state).await?;
    }

    Ok((ok, dead))
}

/// Flags a *faceted* trait type whose values are nearly per-asset-unique —
/// the pathology that bloats `/facets` and pushes real values out of the
/// planner's MCV list. The fix is a `facet_exclude` entry in
/// `config/collections.toml` plus `seed`, never code.
async fn facet_cardinality_warnings(
    pool: &PgPool,
    collection: &CollectionRow,
    members: i64,
) -> anyhow::Result<Vec<String>> {
    if members <= 0 {
        return Ok(Vec::new());
    }
    let card = attributes::trait_cardinality(pool, collection.id).await?;
    Ok(card
        .into_iter()
        .filter(|t| t.is_facet && t.values as f64 > t.assets as f64 * FACET_CARDINALITY_WARN_RATIO)
        .map(|t| {
            format!(
                "{}: trait type {:?} has {} distinct values over {} assets — \
                 consider facet_exclude in config/collections.toml",
                collection.slug, t.name, t.values, t.assets
            )
        })
        .collect())
}

async fn mark_failed(pool: &PgPool, collection_id: i32, error: &str) {
    let existing = ingest_state::backfill_state(pool, collection_id, KIND)
        .await
        .ok()
        .flatten();
    let state = ingest_state::BackfillState {
        collection_id,
        kind: KIND.into(),
        status: "failed".into(),
        cursor: existing
            .as_ref()
            .map(|s| s.cursor.clone())
            .unwrap_or_else(|| json!({})),
        progress: existing
            .as_ref()
            .map(|s| s.progress.clone())
            .unwrap_or_else(|| json!({})),
        last_error: Some(error.chars().take(2000).collect()),
        started_at: existing.as_ref().and_then(|s| s.started_at),
        finished_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    };
    if let Err(e) = ingest_state::put_backfill_state(pool, &state).await {
        log::error!("could not record backfill failure for {collection_id}: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn das_asset() -> Asset {
        serde_json::from_value(json!({
            "id": bs58::encode([7u8; 32]).into_string(),
            "interface": "V1_NFT",
            "burnt": false,
            "ownership": {"owner": bs58::encode([9u8; 32]).into_string()},
            "content": {
                "json_uri": "https://dead.invalid/1.json",
                "metadata": {
                    "name": "stale name",
                    "symbol": "SYN",
                    "attributes": [{"trait_type": "Background", "value": "Stale"}],
                },
                "links": {"image": "https://dead.invalid/1.png"},
            },
        }))
        .unwrap()
    }

    #[test]
    fn without_a_document_das_supplies_everything() {
        let input = merge(&das_asset(), None);
        assert_eq!(input.name, "stale name");
        assert_eq!(
            input.metadata_uri.as_deref(),
            Some("https://dead.invalid/1.json")
        );
        assert_eq!(
            input.metadata_source_uri, None,
            "nothing was fetched, so nothing is claimed to have been"
        );
        assert_eq!(input.attributes.unwrap()[0].value, "Stale");
    }

    /// The re-hosted document is the operator's own copy and outranks DAS's
    /// cache, which may predate the re-host.
    #[test]
    fn the_document_wins_over_das() {
        let document = json!({
            "name": "#6545",
            "symbol": "PSG",
            "image": "https://rehost.invalid/6545.png",
            "attributes": [{"trait_type": "Background", "value": "Yellow"}],
        });
        let input = merge(
            &das_asset(),
            Some(("https://rehost.invalid/1.json", &document)),
        );
        assert_eq!(input.name, "#6545");
        assert_eq!(
            input.image_uri.as_deref(),
            Some("https://rehost.invalid/6545.png")
        );
        assert_eq!(
            input.metadata_source_uri.as_deref(),
            Some("https://rehost.invalid/1.json")
        );
        assert_eq!(input.attributes.unwrap()[0].value, "Yellow");
        assert!(input.document.is_some());
    }

    #[test]
    fn a_burned_asset_never_carries_an_owner() {
        let mut asset = das_asset();
        asset.burnt = true;
        let input = merge(&asset, None);
        assert!(input.burned);
        assert_eq!(input.owner, None);
    }

    /// An asset with no name anywhere becomes `""`, never NULL — the column
    /// is NOT NULL and `number` is generated from it.
    #[test]
    fn a_nameless_asset_becomes_the_empty_string() {
        let asset: Asset = serde_json::from_value(json!({"id": "SYN"})).unwrap();
        assert_eq!(merge(&asset, None).name, "");
    }

    #[test]
    fn a_cursor_from_another_rule_is_refused() {
        let allowlist = Source::Allowlist(vec!["a".into()]);
        let stored = json!({"mode": "search", "next_page": 4});
        assert!(Cursor::resume(&stored, &allowlist).is_none());

        let stored = json!({"mode": "allowlist", "next_index": 3000, "total": 10000});
        assert_eq!(
            Cursor::resume(&stored, &allowlist).unwrap().next_index,
            3000
        );
    }

    #[test]
    fn allowlist_batches_are_counted_but_a_dynamic_collection_is_not() {
        assert_eq!(
            Source::Allowlist(vec![String::new(); 10_000]).batches(1000),
            Some(10)
        );
        assert_eq!(
            Source::Allowlist(vec![String::new(); 2073]).batches(1000),
            Some(3)
        );
        assert_eq!(Source::Search("x".into()).batches(1000), None);
    }
}
