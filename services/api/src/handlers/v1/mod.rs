//! The frozen v1 contract: eleven endpoints, four tags.
//!
//! ALG-625 built the browse half (the registry, one collection, the grid, the
//! facet sidebar); ALG-626 added the detail half (an NFT, its timeline, its
//! ownership history, a wallet portfolio) plus the collection feed, the holder
//! list and search, which completes the document.
//!
//! Every SQL statement lives in `indexer-data-model`; what is here is parameter
//! parsing, the contract's response shapes, and the caching/validator
//! behaviour it promises.
//!
//! Two rules the whole scope obeys and which are easy to get backwards:
//!
//! * **A disabled collection is a 404**, not an empty result — and
//!   `registry::by_slug` happily returns disabled rows, including the
//!   `bench-*` synthetic collections that live in the same table. Every
//!   slug lookup goes through [`enabled_collection`], and every *address*
//!   lookup joins `collections.enabled` inside the query, so an asset of a
//!   disabled collection is invisible everywhere: detail, portfolio and search.
//! * **Unknown input is never a 4xx** where the data could legitimately change
//!   under a bookmark: an unknown trait type or value, an unknown `?collection=`
//!   slug, an unknown wallet and an unindexed search term are all `200`. Only a
//!   malformed *shape* (a non-base58 id, a non-integer limit) is `400`.

pub mod collections;
pub mod nfts;
pub mod search;
pub mod wallets;

use actix_web::{http::header, web, HttpRequest, HttpResponse};
use indexer_data_model::registry::{self, CollectionRow};
use indexer_data_model::{stats, PgPool};
use serde::Serialize;

use crate::cache::{Cached, ResponseCache};
use crate::dto;
use crate::error::{ApiError, Code};

pub(crate) type Result<T> = std::result::Result<T, ApiError>;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/v1")
            .service(collections::list_collections)
            .service(collections::get_collection)
            .service(collections::browse_collection_nfts)
            .service(collections::get_collection_facets)
            .service(collections::get_collection_activity)
            .service(collections::get_collection_holders)
            .service(nfts::get_nft)
            .service(nfts::get_nft_activity)
            .service(nfts::get_nft_owners)
            .service(wallets::get_wallet_portfolio)
            .service(search::search),
    );
}

/// Looks a collection up, refusing anything not publicly served.
///
/// Disabled rows are 404 rather than 403 or an empty body — the contract says
/// "unknown or disabled collection slugs". This is also what keeps the
/// disabled `bench-*` benchmark collections out of the public API.
pub(crate) async fn enabled_collection(pool: &PgPool, slug: &str) -> Result<CollectionRow> {
    match registry::by_slug(pool, slug).await? {
        Some(row) if row.enabled => Ok(row),
        _ => Err(ApiError::not_found(
            format!("no enabled collection with slug `{slug}`"),
            serde_json::json!({ "slug": slug }),
        )),
    }
}

pub(crate) async fn collection_stats(
    pool: &PgPool,
    collection_id: i32,
) -> Result<Option<dto::CollectionStats>> {
    let Some(row) = stats::one(pool, collection_id).await? else {
        return Ok(None);
    };
    let buckets = stats::holder_buckets(pool, collection_id).await?;
    Ok(Some(dto::CollectionStats::new(&row, buckets)))
}

/// Renders a body through the cache and applies the contract's validators.
///
/// `If-None-Match` is answered with `304` and **all** the same headers: the
/// contract says the validators are repeated so a client can refresh its
/// freshness window without a second round trip.
pub(crate) async fn respond<T, F, Fut>(
    req: &HttpRequest,
    cache: &ResponseCache,
    key: &str,
    build: F,
) -> Result<HttpResponse>
where
    T: Serialize,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let cached = match cache.get(key) {
        Some(hit) => hit,
        None => {
            let body = serde_json::to_string(&build().await?).map_err(|error| {
                log::error!("serializing {key}: {error}");
                ApiError::new(
                    Code::Internal,
                    "internal server error",
                    serde_json::Value::Null,
                )
            })?;
            let fresh = Cached::new(body);
            cache.put(key.to_string(), fresh.clone());
            fresh
        }
    };

    let matched = req
        .headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|candidate| candidate.split(',').any(|e| e.trim() == cached.etag));

    let mut builder = if matched {
        HttpResponse::NotModified()
    } else {
        HttpResponse::Ok()
    };
    builder
        .insert_header((header::ETAG, cached.etag.clone()))
        .insert_header((
            header::CACHE_CONTROL,
            format!(
                "public, max-age={}, stale-while-revalidate={}",
                crate::cache::TTL.as_secs(),
                crate::cache::TTL.as_secs() * 2
            ),
        ))
        // Only the encoding half: the CORS middleware appends `Origin` (and
        // the preflight request headers) itself, and naming it here too would
        // list it twice.
        .insert_header((header::VARY, "Accept-Encoding"));

    Ok(if matched {
        builder.finish()
    } else {
        builder
            .content_type("application/json")
            .body(cached.body.clone())
    })
}

/// The `(cursor position)` part of a cache key — a page is only cacheable
/// together with where it starts.
pub(crate) fn position_key(after: Option<(i64, i64)>) -> String {
    after.map_or(String::new(), |(key, id)| format!("{key}|{id}"))
}

/// A cache key whose variable half is free text.
///
/// Joining user text with a separator lets two different requests share a key —
/// one trait value or search term containing the separator is enough — and a
/// shared key serves one request's body to the other. The cursor's fingerprint
/// already hashes a part list separator-safely, so the readable prefix stays
/// and the free text becomes a digest.
pub(crate) fn text_key(prefix: &str, parts: &[&str]) -> String {
    format!("{prefix}:{}", crate::cursor::fingerprint(parts))
}
