//! The browse half of the frozen v1 contract (ALG-625).
//!
//! Four endpoints, all `tags: [collections]`: the registry, one collection,
//! the grid and the facet sidebar. Every SQL statement lives in
//! `indexer-data-model`; what is here is parameter parsing, the contract's
//! response shapes, and the caching/validator behaviour it promises.
//!
//! Two behaviours the contract is explicit about and which are easy to get
//! backwards:
//!
//! * **A disabled collection is a 404**, not an empty result — and
//!   `registry::by_slug` happily returns disabled rows, including the
//!   `bench-*` synthetic collections that live in the same table. Every
//!   lookup here goes through [`enabled_collection`].
//! * **Unknown filter input is never a 4xx**, so a bookmarked URL survives a
//!   metadata refresh. An unknown trait *type* yields an empty page and
//!   `facets: []`; an unknown *value* still counts its type as selected, so it
//!   matches nothing and every other type's counts collapse to zero.
//!   `facets::resolve_selections` already draws exactly that distinction.

use actix_web::http::StatusCode;
use actix_web::{get, http::header, web, HttpRequest, HttpResponse};
use indexer_data_model::browse::{self, BrowseQuery, CursorKey};
use indexer_data_model::registry::{self, CollectionRow};
use indexer_data_model::{facets, stats, PgPool};
use serde::Serialize;

use crate::cache::{Cached, ResponseCache};
use crate::error::{ApiError, Code};
use crate::{cursor, dto, query};

type Result<T> = std::result::Result<T, ApiError>;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/v1")
            .service(list_collections)
            .service(get_collection)
            .service(browse_collection_nfts)
            .service(get_collection_facets),
    );
}

#[get("/collections")]
async fn list_collections(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
) -> Result<HttpResponse> {
    // The envelope is paginated for consistency, not need — four rows fit in
    // one page. We never mint a cursor here, so any cursor presented was
    // issued for something else.
    if query::cursor(req.query_string()).is_some() {
        return Err(ApiError::cursor(
            "cursor was issued for a different endpoint",
        ));
    }
    query::limit(req.query_string(), StatusCode::BAD_REQUEST)?;

    respond(&req, &cache, "collections", || async {
        let rows = registry::list_enabled(pool.get_ref()).await?;
        let mut data = Vec::with_capacity(rows.len());
        for row in &rows {
            data.push(dto::Collection::new(
                row,
                collection_stats(&pool, row.id).await?,
            ));
        }
        Ok(dto::Page::new(data, None))
    })
    .await
}

#[get("/collections/{slug}")]
async fn get_collection(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
    slug: web::Path<String>,
) -> Result<HttpResponse> {
    let row = enabled_collection(&pool, &slug).await?;
    respond(&req, &cache, &format!("collection:{}", row.id), || async {
        Ok(dto::Collection::new(
            &row,
            collection_stats(&pool, row.id).await?,
        ))
    })
    .await
}

#[get("/collections/{slug}/nfts")]
async fn browse_collection_nfts(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
    slug: web::Path<String>,
) -> Result<HttpResponse> {
    let row = enabled_collection(&pool, &slug).await?;
    let raw = req.query_string();
    let filters = query::filters(raw)?;
    let sort = query::sort(raw)?;
    // Browse is the only operation that declares 422, so it is the only one
    // where an over-large limit can be reported as one.
    let limit = query::limit(raw, StatusCode::UNPROCESSABLE_ENTITY)?;

    let fingerprint = cursor::fingerprint(row.id, sort, &filters.canonical(), filters.q.as_deref());
    let after = match query::cursor(raw) {
        Some(raw) => Some(cursor::decode(&raw, sort, &fingerprint)?),
        None => None,
    };

    let key = format!(
        "nfts:{}:{}:{}:{}:{}:{}",
        row.id,
        sort.as_str(),
        filters.canonical(),
        filters.q.as_deref().unwrap_or_default(),
        limit,
        after.as_ref().map_or(String::new(), |k| format!(
            "{}|{}|{}",
            k.number, k.text, k.id
        )),
    );
    respond(&req, &cache, &key, || async {
        let Some(selections) =
            facets::resolve_selections(pool.get_ref(), row.id, &filters.traits).await?
        else {
            // Unknown trait type: nothing can match, and that is a 200.
            return Ok(dto::Page::new(Vec::new(), None));
        };

        // One row more than the page, so `hasMore` never costs a COUNT.
        let mut cards = browse::browse(
            pool.get_ref(),
            &BrowseQuery {
                collection_id: row.id,
                selections,
                q: filters.q.clone(),
                sort,
                after: after.clone(),
                limit: limit + 1,
            },
        )
        .await?;

        let next = (cards.len() as i64 > limit).then(|| {
            cards.truncate(limit as usize);
            let last = cards.last().expect("limit >= 1");
            cursor::encode(
                sort,
                &CursorKey {
                    number: last.sort_number,
                    text: last.sort_text.clone(),
                    id: last.id,
                },
                &fingerprint,
            )
        });
        let reference = dto::Collection::reference(&row);
        Ok(dto::Page::new(
            cards
                .iter()
                .map(|card| dto::NftSummary::new(card, clone_reference(&reference)))
                .collect(),
            next,
        ))
    })
    .await
}

#[get("/collections/{slug}/facets")]
async fn get_collection_facets(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
    slug: web::Path<String>,
) -> Result<HttpResponse> {
    let row = enabled_collection(&pool, &slug).await?;
    let raw = req.query_string();
    let filters = query::filters(raw)?;
    let key = format!(
        "facets:{}:{}:{}",
        row.id,
        filters.canonical(),
        filters.q.as_deref().unwrap_or_default()
    );

    respond(&req, &cache, &key, || async {
        let Some(selections) =
            facets::resolve_selections(pool.get_ref(), row.id, &filters.traits).await?
        else {
            return Ok(dto::FacetsResponse {
                total: 0,
                facets: Vec::new(),
            });
        };
        let q = filters.q.as_deref();
        let counts =
            facets::disjunctive_facet_counts(pool.get_ref(), row.id, &selections, q).await?;
        // `total` is the set under EVERY filter; the counts above each leave
        // their own type's filter out. Different predicates, one response.
        let total = browse::filtered_total(pool.get_ref(), row.id, &selections, q).await?;

        // The rows arrive ordered by trait type, then count descending, then
        // value — which is the contract's required ordering, so grouping in
        // arrival order preserves it.
        let mut facets_out: Vec<dto::Facet> = Vec::new();
        for count in counts {
            match facets_out.last_mut() {
                Some(last) if last.trait_type == count.trait_type => {
                    last.values.push(dto::FacetValue {
                        value: count.value,
                        count: count.count,
                    })
                }
                _ => facets_out.push(dto::Facet {
                    trait_type: count.trait_type,
                    values: vec![dto::FacetValue {
                        value: count.value,
                        count: count.count,
                    }],
                }),
            }
        }
        Ok(dto::FacetsResponse {
            total,
            facets: facets_out,
        })
    })
    .await
}

/// Looks a collection up, refusing anything not publicly served.
///
/// Disabled rows are 404 rather than 403 or an empty body — the contract says
/// "unknown or disabled collection slugs". This is also what keeps the
/// disabled `bench-*` benchmark collections out of the public API.
async fn enabled_collection(pool: &PgPool, slug: &str) -> Result<CollectionRow> {
    match registry::by_slug(pool, slug).await? {
        Some(row) if row.enabled => Ok(row),
        _ => Err(ApiError::not_found(
            format!("no enabled collection with slug `{slug}`"),
            serde_json::json!({ "slug": slug }),
        )),
    }
}

async fn collection_stats(
    pool: &PgPool,
    collection_id: i32,
) -> Result<Option<dto::CollectionStats>> {
    let Some(row) = stats::one(pool, collection_id).await? else {
        return Ok(None);
    };
    let buckets = stats::holder_buckets(pool, collection_id).await?;
    Ok(Some(dto::CollectionStats::new(&row, buckets)))
}

fn clone_reference(reference: &dto::CollectionRef) -> dto::CollectionRef {
    dto::CollectionRef {
        slug: reference.slug.clone(),
        name: reference.name.clone(),
        image_url: reference.image_url.clone(),
    }
}

/// Renders a body through the cache and applies the contract's validators.
///
/// `If-None-Match` is answered with `304` and **all** the same headers: the
/// contract says the validators are repeated so a client can refresh its
/// freshness window without a second round trip.
async fn respond<T, F, Fut>(
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
