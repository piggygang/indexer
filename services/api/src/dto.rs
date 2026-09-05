//! The response bodies of the frozen v1 contract (ALG-625).
//!
//! One struct per schema in `openapi/v1.yaml`, field for field. Two rules from
//! the contract shape every type here:
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

#[derive(Debug, Serialize)]
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
    /// Reserved by the contract — always null until ALG-627 ships rarity.
    pub rarity_rank: Option<i32>,
    /// Reserved by the contract — always null until ALG-627 ships rarity.
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
            rarity_rank: None,
            rarity_score: None,
            collection,
        }
    }
}
