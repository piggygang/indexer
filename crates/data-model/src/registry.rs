//! Typed access to the collections / tokens registry.

use sqlx::{FromRow, PgPool};

use crate::types::{MembershipRule, Standard};

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct CollectionRow {
    pub id: i32,
    pub slug: String,
    pub name: String,
    /// `None` only on disabled placeholders (DB CHECK).
    pub standard: Option<Standard>,
    pub address: Option<String>,
    pub verified_creator: Option<String>,
    pub update_authority: Option<String>,
    pub symbol: Option<String>,
    pub image_url: Option<String>,
    /// Off-chain metadata location override with a `{mint}` placeholder;
    /// `None` = use the on-chain URI.
    pub metadata_uri_template: Option<String>,
    pub facet_exclude: Vec<String>,
    pub enabled: bool,
    /// `Some` for every enabled row (DB CHECK `collections_enabled_resolvable`).
    pub membership_rule: Option<MembershipRule>,
}

const COLLECTION_COLUMNS: &str = "id, slug, name, standard, address, verified_creator, \
     update_authority, symbol, image_url, metadata_uri_template, facet_exclude, enabled, \
     membership_rule";

impl CollectionRow {
    /// The URI the backfill should fetch for an asset: the template with
    /// `{mint}` substituted when set, else the on-chain URI.
    pub fn metadata_source_uri(&self, address: &str, on_chain_uri: Option<&str>) -> Option<String> {
        match &self.metadata_uri_template {
            Some(template) => Some(template.replace("{mint}", address)),
            None => on_chain_uri.map(str::to_owned),
        }
    }
}

/// All collections in registry order (`id` = first-seed order).
pub async fn list(pool: &PgPool, enabled_only: bool) -> sqlx::Result<Vec<CollectionRow>> {
    let sql = format!(
        "SELECT {COLLECTION_COLUMNS} FROM collections \
         WHERE NOT $1::boolean OR enabled ORDER BY id"
    );
    sqlx::query_as::<_, CollectionRow>(&sql)
        .bind(enabled_only)
        .fetch_all(pool)
        .await
}

/// The collections the API serves and the pipelines index.
pub async fn list_enabled(pool: &PgPool) -> sqlx::Result<Vec<CollectionRow>> {
    list(pool, true).await
}

pub async fn by_slug(pool: &PgPool, slug: &str) -> sqlx::Result<Option<CollectionRow>> {
    let sql = format!("SELECT {COLLECTION_COLUMNS} FROM collections WHERE slug = $1");
    sqlx::query_as::<_, CollectionRow>(&sql)
        .bind(slug)
        .fetch_optional(pool)
        .await
}

/// The closed mint list of a `tm_allowlist` collection (ALG-621's
/// `getAssetBatch` input). Empty for the other rules.
pub async fn allowlist(pool: &PgPool, collection_id: i32) -> sqlx::Result<Vec<String>> {
    sqlx::query_scalar("SELECT mint FROM collection_mints WHERE collection_id = $1 ORDER BY mint")
        .bind(collection_id)
        .fetch_all(pool)
        .await
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct TokenRow {
    pub mint: String,
    pub symbol: String,
    pub name: String,
    pub decimals: i16,
    pub logo_uri: Option<String>,
    pub enabled: bool,
}

pub async fn list_tokens(pool: &PgPool, enabled_only: bool) -> sqlx::Result<Vec<TokenRow>> {
    sqlx::query_as::<_, TokenRow>(
        "SELECT mint, symbol, name, decimals, logo_uri, enabled FROM tokens \
         WHERE NOT $1::boolean OR enabled ORDER BY created_at, mint",
    )
    .bind(enabled_only)
    .fetch_all(pool)
    .await
}
