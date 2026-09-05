//! Asset, attribute and document writers — the DAS backfill's whole database
//! surface (ALG-621), reused by the live pipeline (ALG-623) and reconciliation
//! (ALG-624).
//!
//! Everything here is set-based: one statement per concern, one `jsonb`
//! payload per statement, so a 1 000-asset batch costs a fixed handful of
//! round trips instead of one per trait. No HTTP happens in this crate — the
//! caller hands over already-fetched data, which is what lets every invariant
//! below be tested against Postgres with no network.
//!
//! Three properties the SQL is built around, because downstream depends on
//! them:
//!
//! - **A re-run is a true no-op.** Every upsert carries a
//!   `WHERE … IS DISTINCT FROM …` guard, so an unchanged row produces no new
//!   tuple and never fires `assets_updated_at`. `updated_at` therefore means
//!   "something actually changed", which is what makes "re-running causes zero
//!   duplicates" checkable with a single `count(*) WHERE updated_at > $t0`.
//! - **Unknown never overwrites known.** A failed metadata fetch, or a
//!   DAS-only run, leaves `metadata_source_uri`/`image_uri` and the existing
//!   attributes alone rather than clearing them.
//! - **Ownership only moves forward.** `owner` and `owner_slot` are written
//!   together under `EXCLUDED.owner_slot > assets.owner_slot`, so a stale
//!   observation can never clobber a newer one.

use serde_json::{json, Value};
use sqlx::{PgExecutor, Postgres, Transaction};

use crate::seed::is_pubkey;
use crate::types::ImageStatus;

/// One `(trait_type, value)` pair of an asset, in source order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraitInput {
    pub trait_type: String,
    pub value: String,
    /// Index in the source metadata's `attributes` array (detail-page order).
    pub position: i16,
}

/// The fetched off-chain JSON, stored verbatim in `asset_documents` — the
/// durable copy, since the original hosts die.
#[derive(Debug, Clone, PartialEq)]
pub struct AssetDocument {
    pub metadata_json: Value,
    pub source_uri: String,
}

/// One asset as observed by a backfill pass.
#[derive(Debug, Clone, PartialEq)]
pub struct AssetInput {
    /// Mint (Token Metadata) or asset id (Core).
    pub address: String,
    /// `assets.name` is `NOT NULL DEFAULT ''`; an unnamed asset is `""`,
    /// never `NULL`, and `number` is generated from it by Postgres.
    pub name: String,
    pub symbol: Option<String>,
    /// The URI recorded on chain.
    pub metadata_uri: Option<String>,
    /// The URI actually fetched. `None` when nothing was fetched — the
    /// existing value is then kept.
    pub metadata_source_uri: Option<String>,
    pub image_uri: Option<String>,
    pub burned: bool,
    /// `None` = not known. Never clears a stored owner.
    pub owner: Option<String>,
    /// `None` = attributes were **not observed** this pass, so whatever is
    /// stored survives. `Some(vec![])` = observed and genuinely empty, so the
    /// asset's attributes are deleted. That distinction is what lets a
    /// collection whose metadata host is dead (Pig Mud) be re-run later,
    /// after a re-host, and fill in with no code change.
    pub attributes: Option<Vec<TraitInput>>,
    pub document: Option<AssetDocument>,
}

/// What one [`upsert_batch`] actually changed. Every field is a count of
/// rows, so a run's report and the idempotency proof read the same numbers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchCounts {
    pub inserted: u64,
    pub updated: u64,
    pub unchanged: u64,
    /// Addresses already filed under a *different* collection. Never
    /// re-filed — `assets.address` is globally unique, so silently moving one
    /// would corrupt the other collection.
    pub skipped_foreign: u64,
    /// Addresses rejected before binding (not base58 32-byte keys). One bad
    /// value must not abort the whole batch on the `is_pubkey()` CHECK.
    pub invalid: u64,
    pub attributes_written: u64,
    pub attributes_removed: u64,
    pub documents: u64,
}

impl BatchCounts {
    /// Did this batch change anything at all? Backs `--expect-unchanged`.
    pub fn is_noop(&self) -> bool {
        self.inserted == 0
            && self.updated == 0
            && self.attributes_written == 0
            && self.attributes_removed == 0
            && self.documents == 0
    }

    pub fn add(&mut self, other: BatchCounts) {
        self.inserted += other.inserted;
        self.updated += other.updated;
        self.unchanged += other.unchanged;
        self.skipped_foreign += other.skipped_foreign;
        self.invalid += other.invalid;
        self.attributes_written += other.attributes_written;
        self.attributes_removed += other.attributes_removed;
        self.documents += other.documents;
    }
}

/// Upserts a batch of assets with their attributes and documents.
///
/// `owner_slot` is the slot read **before** the DAS call that produced these
/// assets — a conservative lower bound on when the ownership was observed,
/// exactly as `assets.owner_slot` documents. Reading it afterwards could
/// stamp a slot newer than the observation and let stale data outrank a
/// newer write forever.
///
/// Runs entirely inside the caller's transaction, so the caller can advance
/// its `backfill_state` cursor in the same commit and never claim progress it
/// did not persist.
pub async fn upsert_batch(
    tx: &mut Transaction<'_, Postgres>,
    collection_id: i32,
    owner_slot: i64,
    assets: &[AssetInput],
) -> sqlx::Result<BatchCounts> {
    let mut counts = BatchCounts::default();
    if assets.is_empty() {
        return Ok(counts);
    }

    // Reject malformed addresses and de-duplicate: `ON CONFLICT DO UPDATE`
    // raises 21000 ("cannot affect row a second time") if one statement
    // touches the same key twice, and a bisected DAS retry can legitimately
    // hand us the same id twice. First occurrence wins.
    let mut seen = std::collections::HashSet::with_capacity(assets.len());
    let mut rows: Vec<&AssetInput> = Vec::with_capacity(assets.len());
    for asset in assets {
        if !is_pubkey(&asset.address) {
            counts.invalid += 1;
            continue;
        }
        if seen.insert(asset.address.as_str()) {
            rows.push(asset);
        }
    }
    if rows.is_empty() {
        return Ok(counts);
    }

    let payload = Value::Array(
        rows.iter()
            .map(|a| {
                // A garbage owner must cost the owner, not the asset.
                let owner = a.owner.as_deref().filter(|o| is_pubkey(o));
                json!({
                    "address": a.address,
                    "name": a.name,
                    "symbol": a.symbol,
                    "metadata_uri": a.metadata_uri,
                    "metadata_source_uri": a.metadata_source_uri,
                    "image_uri": a.image_uri,
                    "burned": a.burned,
                    "owner": owner,
                })
            })
            .collect(),
    );

    let changed: Vec<bool> = sqlx::query_scalar(UPSERT_ASSETS)
        .bind(collection_id)
        .bind(&payload)
        .bind(owner_slot)
        .fetch_all(&mut **tx)
        .await?;
    counts.inserted = changed.iter().filter(|inserted| **inserted).count() as u64;
    counts.updated = changed.len() as u64 - counts.inserted;

    // Anything missing from the id map exists under another collection: the
    // upsert's `WHERE a.collection_id = $1` refused to re-file it.
    let addresses: Vec<String> = rows.iter().map(|a| a.address.clone()).collect();
    let present: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM assets WHERE collection_id = $1 AND address = ANY($2::text[])",
    )
    .bind(collection_id)
    .bind(&addresses)
    .fetch_one(&mut **tx)
    .await?;
    let present = present.max(0) as u64;
    counts.skipped_foreign = rows.len() as u64 - present;
    counts.unchanged = present.saturating_sub(counts.inserted + counts.updated);

    counts.documents = write_documents(tx, collection_id, &rows).await?;

    let (removed, written) = write_attributes(tx, collection_id, &rows).await?;
    counts.attributes_removed = removed;
    counts.attributes_written = written;

    Ok(counts)
}

/// The asset upsert. Every clause here is load-bearing — see the module docs.
///
/// Note what the change predicate compares in the ownership arm: the **owner**
/// alone, never `owner_slot`. Each pass reads a fresh, higher slot, so
/// including the slot would make every re-run rewrite every row and destroy
/// the "a re-run changes nothing" proof. The column therefore means "a lower
/// bound on when this owner was last observed to *change*", not "when we last
/// asked" — conservative in the safe direction, since it can only cause a
/// later writer to re-apply an already-correct owner, never to skip a real
/// one. A row updated for some other reason does carry the newer slot along.
const UPSERT_ASSETS: &str = "\
INSERT INTO assets AS a
    (address, collection_id, name, symbol, metadata_uri, metadata_source_uri,
     image_uri, burned, owner, owner_slot)
SELECT x.address, $1, x.name, x.symbol, x.metadata_uri, x.metadata_source_uri, x.image_uri,
       x.burned,
       CASE WHEN x.burned THEN NULL ELSE x.owner END,
       CASE WHEN x.burned OR x.owner IS NULL THEN NULL ELSE $3::bigint END
  FROM jsonb_to_recordset($2::jsonb) AS x(
       address text, name text, symbol text, metadata_uri text,
       metadata_source_uri text, image_uri text, burned boolean, owner text)
ON CONFLICT (address) DO UPDATE SET
    name                = EXCLUDED.name,
    symbol              = EXCLUDED.symbol,
    metadata_uri        = EXCLUDED.metadata_uri,
    metadata_source_uri = coalesce(EXCLUDED.metadata_source_uri, a.metadata_source_uri),
    image_uri           = coalesce(EXCLUDED.image_uri, a.image_uri),
    burned              = a.burned OR EXCLUDED.burned,
    owner      = CASE WHEN a.burned OR EXCLUDED.burned THEN NULL
                      WHEN a.owner_slot IS NULL OR EXCLUDED.owner_slot > a.owner_slot
                           THEN EXCLUDED.owner
                      ELSE a.owner END,
    owner_slot = CASE WHEN a.burned OR EXCLUDED.burned THEN NULL
                      WHEN a.owner_slot IS NULL OR EXCLUDED.owner_slot > a.owner_slot
                           THEN EXCLUDED.owner_slot
                      ELSE a.owner_slot END
WHERE a.collection_id = $1
  AND ((a.name, a.symbol, a.metadata_uri, a.metadata_source_uri, a.image_uri)
       IS DISTINCT FROM
       (EXCLUDED.name, EXCLUDED.symbol, EXCLUDED.metadata_uri,
        coalesce(EXCLUDED.metadata_source_uri, a.metadata_source_uri),
        coalesce(EXCLUDED.image_uri, a.image_uri))
    OR (EXCLUDED.burned AND NOT a.burned)
    OR (NOT (a.burned OR EXCLUDED.burned)
        AND (a.owner_slot IS NULL OR EXCLUDED.owner_slot > a.owner_slot)
        AND a.owner IS DISTINCT FROM EXCLUDED.owner))
RETURNING (xmax = 0) AS inserted";

/// Stores the fetched JSON. `fetched_at` deliberately does not move when the
/// content is identical (jsonb equality is semantic, so key reordering is not
/// a change) — otherwise every re-run would rewrite the whole table and the
/// idempotency proof would be worthless.
async fn write_documents(
    tx: &mut Transaction<'_, Postgres>,
    collection_id: i32,
    rows: &[&AssetInput],
) -> sqlx::Result<u64> {
    let payload = Value::Array(
        rows.iter()
            .filter_map(|a| {
                a.document.as_ref().map(|doc| {
                    json!({
                        "address": a.address,
                        "metadata_json": doc.metadata_json,
                        "source_uri": doc.source_uri,
                    })
                })
            })
            .collect(),
    );
    if payload.as_array().is_some_and(Vec::is_empty) {
        return Ok(0);
    }

    let result = sqlx::query(
        "INSERT INTO asset_documents (asset_id, metadata_json, source_uri) \
         SELECT a.id, x.metadata_json, x.source_uri \
           FROM jsonb_to_recordset($2::jsonb) \
                  AS x(address text, metadata_json jsonb, source_uri text) \
           JOIN assets a ON a.address = x.address AND a.collection_id = $1 \
         ON CONFLICT (asset_id) DO UPDATE \
            SET metadata_json = EXCLUDED.metadata_json, \
                source_uri    = EXCLUDED.source_uri, \
                fetched_at    = now() \
          WHERE (asset_documents.metadata_json, asset_documents.source_uri) \
             IS DISTINCT FROM (EXCLUDED.metadata_json, EXCLUDED.source_uri)",
    )
    .bind(collection_id)
    .bind(&payload)
    .execute(&mut **tx)
    .await?;
    Ok(result.rows_affected())
}

/// Interns the dictionary and replaces the batch's attributes, returning
/// `(removed, written)`.
async fn write_attributes(
    tx: &mut Transaction<'_, Postgres>,
    collection_id: i32,
    rows: &[&AssetInput],
) -> sqlx::Result<(u64, u64)> {
    // Only assets whose attributes were OBSERVED this pass are in scope; an
    // asset we could not fetch keeps whatever it has.
    let scope: Vec<String> = rows
        .iter()
        .filter(|a| a.attributes.is_some())
        .map(|a| a.address.clone())
        .collect();
    if scope.is_empty() {
        return Ok((0, 0));
    }

    let mut pairs = Vec::new();
    for asset in rows {
        for trait_input in asset.attributes.iter().flatten() {
            pairs.push(json!({
                "address": asset.address,
                "trait_type": trait_input.trait_type,
                "value": trait_input.value,
                "position": trait_input.position,
            }));
        }
    }
    let payload = Value::Array(pairs);

    // The dictionary is interned set-wise and no ids come back to Rust: the
    // replace statement resolves them by joining the existing unique indexes.
    // Per-row `ensure_trait_*` would be ~90 000 round trips for one
    // collection with a per-asset-unique trait.
    sqlx::query(
        "INSERT INTO trait_types (collection_id, name, is_facet) \
         SELECT c.id, x.name, NOT (x.name = ANY(c.facet_exclude)) \
           FROM collections c, \
                (SELECT DISTINCT trait_type AS name \
                   FROM jsonb_to_recordset($2::jsonb) AS t(trait_type text)) x \
          WHERE c.id = $1 \
         ON CONFLICT (collection_id, name) DO NOTHING",
    )
    .bind(collection_id)
    .bind(&payload)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "INSERT INTO trait_values (trait_type_id, value) \
         SELECT DISTINCT tt.id, x.value \
           FROM jsonb_to_recordset($2::jsonb) AS x(trait_type text, value text) \
           JOIN trait_types tt ON tt.collection_id = $1 AND tt.name = x.trait_type \
         ON CONFLICT (trait_type_id, value) DO NOTHING",
    )
    .bind(collection_id)
    .bind(&payload)
    .execute(&mut **tx)
    .await?;

    let (removed, written): (i64, i64) = sqlx::query_as(REPLACE_ATTRIBUTES)
        .bind(collection_id)
        .bind(&payload)
        .bind(&scope)
        .fetch_one(&mut **tx)
        .await?;
    Ok((removed.max(0) as u64, written.max(0) as u64))
}

/// `$2` are the observed rows, `$3` the assets whose attributes were observed
/// at all. The two are deliberately separate: an asset absent from `$3` keeps
/// its stored attributes, while an asset in `$3` with no rows in `$2` has
/// them all deleted.
///
/// `DISTINCT ON` is mandatory rather than tidy — 2021-era metadata that
/// repeats a `(trait_type, value)` pair would otherwise raise SQLSTATE 21000.
/// The delete set (`NOT EXISTS in input`) and the insert set (`input`) are
/// disjoint, so both may live in one statement.
const REPLACE_ATTRIBUTES: &str = "\
WITH input AS (
    SELECT a.id AS asset_id, tt.id AS trait_type_id, tv.id AS trait_value_id, x.position
      FROM jsonb_to_recordset($2::jsonb)
             AS x(address text, trait_type text, value text, position smallint)
      JOIN assets       a  ON a.address = x.address AND a.collection_id = $1
      JOIN trait_types  tt ON tt.collection_id = $1 AND tt.name = x.trait_type
      JOIN trait_values tv ON tv.trait_type_id = tt.id AND tv.value = x.value
), scope AS (
    SELECT a.id AS asset_id FROM assets a
     WHERE a.collection_id = $1 AND a.address = ANY($3::text[])
), removed AS (
    DELETE FROM asset_attributes aa USING scope s
     WHERE aa.asset_id = s.asset_id
       AND NOT EXISTS (SELECT 1 FROM input i
                        WHERE i.asset_id = aa.asset_id
                          AND i.trait_value_id = aa.trait_value_id)
    RETURNING 1
), written AS (
    INSERT INTO asset_attributes
        (asset_id, collection_id, trait_type_id, trait_value_id, position)
    SELECT DISTINCT ON (asset_id, trait_value_id)
           asset_id, $1, trait_type_id, trait_value_id, position
      FROM input
     ORDER BY asset_id, trait_value_id, position
    ON CONFLICT (asset_id, trait_value_id) DO UPDATE
       SET position = EXCLUDED.position, trait_type_id = EXCLUDED.trait_type_id
     WHERE (asset_attributes.position, asset_attributes.trait_type_id)
        IS DISTINCT FROM (EXCLUDED.position, EXCLUDED.trait_type_id)
    RETURNING 1
)
SELECT (SELECT count(*) FROM removed)::bigint,
       (SELECT count(*) FROM written)::bigint";

/// Records image reachability for the opt-in `--check-images` pass. Only
/// determined outcomes are passed in: a timeout leaves both columns untouched
/// so the next pass retries it.
pub async fn set_image_status<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
    checks: &[(String, ImageStatus)],
) -> sqlx::Result<u64> {
    if checks.is_empty() {
        return Ok(0);
    }
    let payload = Value::Array(
        checks
            .iter()
            .map(|(address, status)| json!({ "address": address, "status": status.as_str() }))
            .collect(),
    );
    let result = sqlx::query(
        "UPDATE assets a \
            SET image_status = x.status, image_checked_at = now() \
           FROM jsonb_to_recordset($2::jsonb) AS x(address text, status text) \
          WHERE a.address = x.address AND a.collection_id = $1",
    )
    .bind(collection_id)
    .bind(&payload)
    .execute(exec)
    .await?;
    Ok(result.rows_affected())
}

/// Refreshes planner statistics. Stated obligation of
/// `20260829000300_assets_attributes.sql`: a per-asset-unique trait leaves
/// thousands of singleton values that would otherwise push the real facet
/// values out of the MCV list and mislead the facet planner.
///
/// Must run outside a transaction (`ANALYZE` takes SHARE UPDATE EXCLUSIVE and
/// is concurrent-safe).
pub async fn analyze_after_backfill<'e>(exec: impl PgExecutor<'e>) -> sqlx::Result<()> {
    sqlx::query("ANALYZE assets, trait_types, trait_values, asset_attributes")
        .execute(exec)
        .await?;
    Ok(())
}

/// What the database already knows about one asset's off-chain document, so
/// the backfill can skip a fetch it does not need. A second pass over an
/// unchanged collection issues **zero** HTTP requests because of this.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct DocumentState {
    pub address: String,
    pub metadata_source_uri: Option<String>,
    pub has_document: bool,
}

/// Document state for a batch of addresses. Addresses with no `assets` row
/// yet are simply absent — the caller treats those as "needs fetching".
pub async fn document_state<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
    addresses: &[String],
) -> sqlx::Result<Vec<DocumentState>> {
    sqlx::query_as(
        "SELECT a.address, a.metadata_source_uri, (d.asset_id IS NOT NULL) AS has_document \
           FROM assets a \
           LEFT JOIN asset_documents d ON d.asset_id = a.id \
          WHERE a.collection_id = $1 AND a.address = ANY($2::text[])",
    )
    .bind(collection_id)
    .bind(addresses)
    .fetch_all(exec)
    .await
}

/// Assets whose image reachability is still unknown or stale, keyset-ordered
/// so the pass resumes from the database alone.
pub async fn image_candidates<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
    after_id: i64,
    recheck_after_days: i32,
    limit: i64,
) -> sqlx::Result<Vec<(i64, String, String)>> {
    sqlx::query_as(
        "SELECT a.id, a.address, a.image_uri \
           FROM assets a \
          WHERE a.collection_id = $1 AND a.image_uri IS NOT NULL AND a.id > $2 \
            AND (a.image_status = 'unknown' \
                 OR a.image_checked_at < now() - make_interval(days => $3)) \
          ORDER BY a.id LIMIT $4",
    )
    .bind(collection_id)
    .bind(after_id)
    .bind(recheck_after_days)
    .bind(limit)
    .fetch_all(exec)
    .await
}

/// Rows in the browse population — `collection_id AND membership_status =
/// 'member'`, the predicate every query over the population applies. Burned
/// assets are included: this is the number the supply acceptance check
/// compares against the registry's mint count, not `collection_stats.supply`
/// (which excludes burned).
pub async fn member_count<'e>(exec: impl PgExecutor<'e>, collection_id: i32) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        "SELECT count(*)::bigint FROM assets \
          WHERE collection_id = $1 AND membership_status = 'member'",
    )
    .bind(collection_id)
    .fetch_one(exec)
    .await
}

/// Flips membership for a batch of addresses, returning how many rows moved.
///
/// The one writer of `membership_status`, which the migration reserves for
/// reconciliation: *"Core assets can leave a collection (update authority
/// moves them); reconciliation (ALG-624) flips this instead of deleting
/// history."* Deleting the asset would take its activity and ownership
/// intervals with it; the row stays and drops out of the browse population.
///
/// `removed_at` moves with the status because `assets_removed_pair` requires
/// the two to agree, and the `IS DISTINCT FROM` guard keeps a re-run a true
/// no-op — the same discipline every other writer here follows.
pub async fn set_membership<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
    addresses: &[String],
    removed: bool,
) -> sqlx::Result<u64> {
    if addresses.is_empty() {
        return Ok(0);
    }
    let done = sqlx::query(
        "UPDATE assets \
            SET membership_status = CASE WHEN $3 THEN 'removed' ELSE 'member' END, \
                removed_at = CASE WHEN $3 THEN now() ELSE NULL END \
          WHERE collection_id = $1 AND address = ANY($2::text[]) \
            AND membership_status IS DISTINCT FROM \
                (CASE WHEN $3 THEN 'removed' ELSE 'member' END)",
    )
    .bind(collection_id)
    .bind(addresses)
    .bind(removed)
    .execute(exec)
    .await?;
    Ok(done.rows_affected())
}

/// Documents already stored for a batch of addresses, as
/// `(address, source_uri, metadata_json)`.
///
/// The backfill reads these back for assets whose document it deliberately
/// did **not** re-fetch. Without it, a second pass would fall back to DAS's
/// cached name/image/attributes and overwrite the better values the first
/// pass derived from the operator's re-hosted JSON — which would both corrupt
/// the data and make every re-run look like a change.
pub async fn stored_documents<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
    addresses: &[String],
) -> sqlx::Result<Vec<(String, String, Value)>> {
    sqlx::query_as(
        "SELECT a.address, d.source_uri, d.metadata_json \
           FROM assets a \
           JOIN asset_documents d ON d.asset_id = a.id \
          WHERE a.collection_id = $1 AND a.address = ANY($2::text[])",
    )
    .bind(collection_id)
    .bind(addresses)
    .fetch_all(exec)
    .await
}

/// Member addresses of one collection — the subscription filter for a
/// `tm_collection`, whose certified collection mint never appears in a
/// member's transfer, so the members themselves must be the filter.
pub async fn member_addresses<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
) -> sqlx::Result<Vec<String>> {
    sqlx::query_scalar(
        "SELECT address FROM assets \
          WHERE collection_id = $1 AND membership_status = 'member' \
          ORDER BY address",
    )
    .bind(collection_id)
    .fetch_all(exec)
    .await
}
