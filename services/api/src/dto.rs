//! The response bodies of the frozen v1 contract (ALG-625).
//!
//! One struct per schema in `openapi/v1.yaml`, field for field (ALG-626 added
//! the detail half). Two rules from the contract shape every type here:
//!
//! * **camelCase on the wire**, snake_case in Rust — `rename_all`, never a
//!   hand-written name;
//! * **every property is present**, with an explicit `null` rather than
//!   omission, so clients get `T | null` and never `T | null | undefined`.
//!   That means no `skip_serializing_if` anywhere in this file. Ever.

use chrono::{DateTime, Utc};
use indexer_data_model::{browse::AssetCard, registry::CollectionRow, stats};
use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Collection {
    pub slug: String,
    pub name: String,
    pub standard: Option<String>,
    pub membership_rule: Option<String>,
    pub address: Option<String>,
    pub symbol: Option<String>,
    pub image_url: Option<String>,
    pub stats: Option<CollectionStats>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionStats {
    pub supply: i32,
    pub holders: i32,
    pub burned: i32,
    pub indexed: i32,
    pub activity24h: i32,
    pub activity7d: i32,
    pub last_activity_at: Option<DateTime<Utc>>,
    pub holder_distribution: Vec<HolderBucket>,
    pub as_of: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HolderBucket {
    pub label: String,
    pub min_count: i32,
    pub max_count: Option<i32>,
    pub holders: i64,
    pub assets: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionRef {
    pub slug: String,
    pub name: String,
    pub image_url: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NftSummary {
    pub address: String,
    pub name: String,
    pub number: Option<i32>,
    pub image_uri: Option<String>,
    pub image_status: String,
    pub burned: bool,
    pub owner: Option<String>,
    pub last_activity_at: Option<DateTime<Utc>>,
    /// Rank 1 is the rarest. Null for a collection with no facetable trait
    /// types, and until the first rarity pass has run.
    pub rarity_rank: Option<i32>,
    pub rarity_score: Option<f64>,
    pub collection: CollectionRef,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Page<T> {
    pub data: Vec<T>,
    pub next_cursor: Option<String>,
    /// Always equal to `next_cursor.is_some()`. Never computed with a COUNT —
    /// the page query asks for one row more than it needs instead.
    pub has_more: bool,
}

impl<T> Page<T> {
    pub fn new(data: Vec<T>, next_cursor: Option<String>) -> Self {
        Self {
            data,
            has_more: next_cursor.is_some(),
            next_cursor,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetValue {
    pub value: String,
    pub count: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Facet {
    pub trait_type: String,
    pub values: Vec<FacetValue>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FacetsResponse {
    pub total: i64,
    pub facets: Vec<Facet>,
}

impl Collection {
    pub fn new(row: &CollectionRow, stats: Option<CollectionStats>) -> Self {
        Self {
            slug: row.slug.clone(),
            name: row.name.clone(),
            // Enum values on the wire are the Postgres CHECK literals, which
            // is exactly what these types already serialize as.
            standard: row.standard.map(|s| s.as_str().to_string()),
            membership_rule: row.membership_rule.map(membership_rule),
            address: row.address.clone(),
            symbol: row.symbol.clone(),
            image_url: row.image_url.clone(),
            stats,
        }
    }

    pub fn reference(row: &CollectionRow) -> CollectionRef {
        CollectionRef {
            slug: row.slug.clone(),
            name: row.name.clone(),
            image_url: row.image_url.clone(),
        }
    }
}

/// `MembershipRule` has no `as_str` in `data-model` (only `Standard` does), and
/// the wire value must be the CHECK literal.
fn membership_rule(rule: indexer_data_model::types::MembershipRule) -> String {
    use indexer_data_model::types::MembershipRule::*;
    match rule {
        CoreCollection => "core_collection",
        TmCollection => "tm_collection",
        TmAllowlist => "tm_allowlist",
    }
    .to_string()
}

impl CollectionStats {
    pub fn new(row: &stats::CollectionStats, buckets: Vec<stats::HolderBucket>) -> Self {
        Self {
            supply: row.supply,
            holders: row.holders,
            burned: row.burned,
            indexed: row.indexed,
            activity24h: row.activity_24h,
            activity7d: row.activity_7d,
            last_activity_at: row.last_activity_at,
            holder_distribution: buckets
                .into_iter()
                .map(|b| HolderBucket {
                    label: b.label,
                    min_count: b.min_count,
                    max_count: b.max_count,
                    holders: b.holders,
                    assets: b.assets,
                })
                .collect(),
            as_of: Utc::now(),
        }
    }
}

impl NftSummary {
    pub fn new(card: &AssetCard, collection: CollectionRef) -> Self {
        Self {
            address: card.address.clone(),
            name: card.name.clone(),
            number: card.number,
            image_uri: card.image_uri.clone(),
            image_status: card.image_status.clone(),
            burned: card.burned,
            owner: card.owner.clone(),
            last_activity_at: card.last_activity_at,
            rarity_rank: card.rarity_rank,
            rarity_score: card.rarity_score,
            collection,
        }
    }
}

/// `NftDetail` — the summary plus everything that needs a second table.
///
/// `#[serde(flatten)]` rather than a nested object, because the contract
/// composes it with `allOf`, which merges on the wire: 11 summary keys plus 12
/// here, flat, 23 in total.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NftDetail {
    #[serde(flatten)]
    pub summary: NftSummary,
    pub standard: Option<String>,
    pub symbol: Option<String>,
    pub membership_status: String,
    pub removed_at: Option<DateTime<Utc>>,
    pub metadata_uri: Option<String>,
    pub metadata_source_uri: Option<String>,
    pub image_checked_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub attributes: Vec<Attribute>,
    pub ownership: OwnerCard,
    pub mint: MintInfo,
    pub activity_summary: ActivitySummary,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Attribute {
    pub trait_type: String,
    pub value: String,
    pub position: i32,
    /// False = rendered as a chip but excluded from `/facets`, so the client
    /// must not link it to a filtered browse URL.
    pub is_facet: bool,
    /// Reserved by the contract — always null until ALG-627 ships rarity.
    pub rarity_pct: Option<f64>,
}

/// The owner card. `heldSince` and friends are null when the open interval
/// disagrees with the observed owner — the contract would rather say nothing
/// than attribute a date to the wrong wallet.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerCard {
    pub owner: Option<String>,
    pub owner_slot: Option<i64>,
    pub held_since: Option<DateTime<Utc>>,
    pub held_since_slot: Option<i64>,
    pub acquired_by_signature: Option<String>,
}

/// Always an object; every field is null until the activity backfill has
/// reached this asset.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MintInfo {
    pub minted_at: Option<DateTime<Utc>>,
    pub mint_slot: Option<i64>,
    pub signature: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivitySummary {
    pub sales_count: i64,
    pub transfer_count: i64,
    pub owner_count: i64,
    pub last_sale_price_lamports: Option<String>,
    pub last_sale_at: Option<DateTime<Utc>>,
    pub last_sale_marketplace: Option<String>,
}

/// One timeline entry. `id` is an `Int64String` — opaque, never parsed by a
/// client and never a path segment; `slot` stays a JSON number because Solana's
/// own RPC types it as one.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityEvent {
    pub id: String,
    pub kind: String,
    pub signature: String,
    pub seq: i16,
    pub slot: i64,
    pub block_time: DateTime<Utc>,
    pub from_owner: Option<String>,
    pub to_owner: Option<String>,
    pub price_lamports: Option<String>,
    pub marketplace: Option<String>,
}

/// The collection feed's event: the same shape plus the card it happened to.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionActivityEvent {
    #[serde(flatten)]
    pub event: ActivityEvent,
    pub nft: NftSummary,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnershipInterval {
    pub owner: String,
    pub from_slot: i64,
    pub from_ts: DateTime<Utc>,
    pub to_slot: Option<i64>,
    pub to_ts: Option<DateTime<Utc>>,
    pub is_current: bool,
    pub opened_by_signature: Option<String>,
    pub closed_by_signature: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletPortfolio {
    pub address: String,
    pub total_count: i64,
    pub collections: Vec<WalletCollectionHolding>,
    /// Always empty in v1. Badge membership is a registry concept, and
    /// hard-coding a set of slugs here would contradict this project's rule
    /// that collection membership is data, never code — the contract says so
    /// itself and tells clients to derive it.
    pub badges: Vec<Badge>,
    pub nfts: Page<NftSummary>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletCollectionHolding {
    pub collection: CollectionRef,
    pub count: i64,
    pub holder_rank: Option<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Badge {
    pub code: String,
    pub label: String,
    pub collection_slug: Option<String>,
    pub value: Option<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Holder {
    pub address: String,
    pub count: i64,
    pub rank: Option<i64>,
}

/// Top-N, not a page: `/holders` groups the collection's owner rows on every
/// request, so the list is capped rather than cursor-paginated. No
/// `nextCursor`, no `hasMore`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HoldersResponse {
    pub data: Vec<Holder>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResponse {
    pub query: String,
    pub interpreted_as: String,
    pub route: Option<SearchRoute>,
    pub wallet: Option<WalletHit>,
    pub groups: Vec<SearchGroup>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchRoute {
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletHit {
    pub address: String,
    pub total_count: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchGroup {
    pub collection: CollectionRef,
    pub total: i64,
    pub nfts: Vec<NftSummary>,
}

/// A lamport amount on the wire: a decimal string, so the full u64 range round
/// trips exactly and nobody divides by 1e9 in floating point.
pub fn lamports(value: Option<i64>) -> Option<String> {
    value.map(|v| v.to_string())
}

impl ActivityEvent {
    pub fn new(row: &indexer_data_model::timeline::ActivityEvent) -> Self {
        Self {
            id: row.id.to_string(),
            kind: row.kind.clone(),
            signature: row.signature.clone(),
            seq: row.seq,
            slot: row.slot,
            block_time: row.block_time,
            from_owner: row.from_owner.clone(),
            to_owner: row.to_owner.clone(),
            price_lamports: lamports(row.price_lamports),
            marketplace: row.marketplace.clone(),
        }
    }
}

impl OwnershipInterval {
    pub fn new(row: &indexer_data_model::timeline::OwnershipInterval) -> Self {
        Self {
            owner: row.owner.clone(),
            from_slot: row.from_slot,
            from_ts: row.from_ts,
            to_slot: row.to_slot,
            to_ts: row.to_ts,
            is_current: row.is_current(),
            opened_by_signature: row.opened_by_signature.clone(),
            closed_by_signature: row.closed_by_signature.clone(),
        }
    }
}

impl ActivitySummary {
    pub fn new(row: &indexer_data_model::nft::ActivitySummary) -> Self {
        Self {
            sales_count: row.sales_count,
            transfer_count: row.transfer_count,
            owner_count: row.owner_count,
            last_sale_price_lamports: lamports(row.last_sale_price_lamports),
            last_sale_at: row.last_sale_at,
            last_sale_marketplace: row.last_sale_marketplace.clone(),
        }
    }
}
