//! The NFT detail page's reads (ALG-626).
//!
//! `/v1/nfts/{id}` is the one endpoint that deliberately steps outside the
//! browse population. The contract: *"Addressable even when the asset has left
//! its collection (`membershipStatus: removed`) or has been burned — both
//! remain valid pages with valid history."* So there is **no
//! `membership_status = 'member'` predicate here**, only `collections.enabled`,
//! which is what keeps the disabled `bench-*` fixtures out of the public API.
//!
//! Five small queries rather than one CTE: each maps to exactly one index, each
//! is independently testable, and the whole page sits behind the API's 15 s
//! response cache, so the round trips are amortized over a burst rather than
//! paid per request.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

use crate::types::{EventKind, Standard};

/// The asset row plus the collection identity `NftDetail` needs. The standard
/// is the collection's — the assets migration says so: *"a collection cannot
/// mix standards; NftDetail.standard comes from the join."*
///
/// No `Eq`: `rarity_score` is an `Option<f64>`.
#[derive(Debug, Clone, PartialEq, FromRow)]
pub struct NftRow {
    pub id: i64,
    pub address: String,
    pub collection_id: i32,
    pub name: String,
    pub number: Option<i32>,
    pub symbol: Option<String>,
    pub metadata_uri: Option<String>,
    pub metadata_source_uri: Option<String>,
    pub image_uri: Option<String>,
    pub image_status: String,
    pub image_checked_at: Option<DateTime<Utc>>,
    pub burned: bool,
    pub owner: Option<String>,
    pub owner_slot: Option<i64>,
    pub membership_status: String,
    pub removed_at: Option<DateTime<Utc>>,
    pub last_activity_at: Option<DateTime<Utc>>,
    pub rarity_score: Option<f64>,
    pub rarity_rank: Option<i32>,
    pub updated_at: DateTime<Utc>,
    pub collection_slug: String,
    pub collection_name: String,
    pub collection_image_url: Option<String>,
    pub standard: Option<Standard>,
    /// The collection's rarity fence, so a re-rank invalidates this page's
    /// cache entry instead of waiting out the TTL.
    pub rarity_version: i32,
}

/// One attribute of one asset, in display order.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct AssetAttribute {
    pub trait_type: String,
    pub value: String,
    pub position: i32,
    /// False = shown as a chip on the detail page but excluded from `/facets`.
    /// The detail page renders these; the facet query filters them out. Never
    /// filter on it here.
    pub is_facet: bool,
}

/// The open ownership interval, if the asset has one.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct OpenInterval {
    pub owner: String,
    pub from_slot: i64,
    pub from_ts: DateTime<Utc>,
    pub opened_by_signature: Option<String>,
}

/// The mint event, if the activity backfill has reached this asset.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct MintInfo {
    pub minted_at: DateTime<Utc>,
    pub mint_slot: i64,
    pub signature: String,
}

/// The counts above the timeline. Zeroes for an asset with no activity.
#[derive(Debug, Clone, Default, PartialEq, Eq, FromRow)]
pub struct ActivitySummary {
    pub sales_count: i64,
    pub transfer_count: i64,
    /// Distinct owners across the asset's history — not the interval count, so
    /// a wallet that buys the same pig twice counts once.
    pub owner_count: i64,
    pub last_sale_price_lamports: Option<i64>,
    pub last_sale_at: Option<DateTime<Utc>>,
    pub last_sale_marketplace: Option<String>,
}

const NFT_COLUMNS: &str = "a.id, a.address, a.collection_id, a.name, a.number, a.symbol, \
     a.metadata_uri, a.metadata_source_uri, a.image_uri, a.image_status, a.image_checked_at, \
     a.burned, a.owner, a.owner_slot, a.membership_status, a.removed_at, a.last_activity_at, \
     a.rarity_score, a.rarity_rank, a.updated_at, c.slug AS collection_slug, c.name AS collection_name, \
     c.image_url AS collection_image_url, c.standard, c.rarity_version";

/// One asset by its public address — the mint for Token Metadata, the asset id
/// for Core. Assets of disabled collections are invisible, exactly as a
/// disabled collection's slug is a 404.
pub async fn by_address(pool: &PgPool, address: &str) -> sqlx::Result<Option<NftRow>> {
    sqlx::query_as::<_, NftRow>(&format!(
        "SELECT {NFT_COLUMNS} FROM assets a \
           JOIN collections c ON c.id = a.collection_id \
          WHERE a.address = $1 AND c.enabled"
    ))
    .bind(address)
    .fetch_optional(pool)
    .await
}

/// Every attribute of one asset, including the non-facetable ones.
///
/// `position` is `NOT NULL DEFAULT 0` and duplicate `(type, value)` pairs
/// collapse to their lowest position, so it is not a total order — the trait
/// type breaks the tie, which is also the order the contract asks clients to
/// render in (`position, traitType, value`).
pub async fn attributes(pool: &PgPool, asset_id: i64) -> sqlx::Result<Vec<AssetAttribute>> {
    sqlx::query_as::<_, AssetAttribute>(
        "SELECT tt.name AS trait_type, tv.value, aa.position::int AS position, tt.is_facet \
           FROM asset_attributes aa \
           JOIN trait_types  tt ON tt.id = aa.trait_type_id \
           JOIN trait_values tv ON tv.id = aa.trait_value_id \
          WHERE aa.asset_id = $1 \
          ORDER BY aa.position, tt.name, tv.value",
    )
    .bind(asset_id)
    .fetch_all(pool)
    .await
}

/// The asset's open ownership interval, or `None` when it has no history (or
/// was burned, which closes the last one).
///
/// Served by the partial index `ownership_history_open`, whose predicate is
/// this `WHERE` clause verbatim.
pub async fn open_interval(pool: &PgPool, asset_id: i64) -> sqlx::Result<Option<OpenInterval>> {
    sqlx::query_as::<_, OpenInterval>(
        "SELECT h.owner, h.from_slot, h.from_ts, o.signature AS opened_by_signature \
           FROM ownership_history h \
           LEFT JOIN activity o ON o.id = h.opened_by \
          WHERE h.asset_id = $1 AND h.to_slot IS NULL",
    )
    .bind(asset_id)
    .fetch_optional(pool)
    .await
}

/// The asset's mint event. Null for every asset the activity backfill has not
/// reached — the contract says so: *"there is no mint-date column; this comes
/// from the mint event."*
pub async fn mint_info(pool: &PgPool, asset_id: i64) -> sqlx::Result<Option<MintInfo>> {
    sqlx::query_as::<_, MintInfo>(
        "SELECT block_time AS minted_at, slot AS mint_slot, signature \
           FROM activity WHERE asset_id = $1 AND kind = 'mint' \
          ORDER BY slot, id LIMIT 1",
    )
    .bind(asset_id)
    .fetch_optional(pool)
    .await
}

/// The `ActivitySummary` strip.
///
/// One statement: the kind counts and the last sale come off the asset's
/// timeline index, and `owner_count` is a correlated subquery over
/// `ownership_history` rather than a join, so an asset with activity but no
/// derived history still reports the other five numbers.
pub async fn activity_summary(pool: &PgPool, asset_id: i64) -> sqlx::Result<ActivitySummary> {
    sqlx::query_as::<_, ActivitySummary>(
        "SELECT count(*) FILTER (WHERE kind = 'sale')::bigint     AS sales_count, \
                count(*) FILTER (WHERE kind = 'transfer')::bigint AS transfer_count, \
                (SELECT count(DISTINCT owner)::bigint FROM ownership_history \
                  WHERE asset_id = $1)                            AS owner_count, \
                (SELECT price_lamports FROM activity \
                  WHERE asset_id = $1 AND kind = 'sale' \
                  ORDER BY slot DESC, id DESC LIMIT 1)            AS last_sale_price_lamports, \
                (SELECT block_time FROM activity \
                  WHERE asset_id = $1 AND kind = 'sale' \
                  ORDER BY slot DESC, id DESC LIMIT 1)            AS last_sale_at, \
                (SELECT marketplace FROM activity \
                  WHERE asset_id = $1 AND kind = 'sale' \
                  ORDER BY slot DESC, id DESC LIMIT 1)            AS last_sale_marketplace \
           FROM activity WHERE asset_id = $1 AND kind = ANY($2::text[])",
    )
    .bind(asset_id)
    .bind(EventKind::public_strings())
    .fetch_one(pool)
    .await
}
