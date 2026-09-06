//! The `collections` tag: the registry, one collection, the grid, the facet
//! sidebar, the activity feed and the holder list.
//!
//! The first four are ALG-625; the last two complete the tag in ALG-626 and
//! reuse its machinery — the feed is the per-asset timeline scoped by
//! `collection_id` with an `NftSummary` embedded, and the holder list is the
//! ranking query a portfolio's `holderRank` already needs.

use actix_web::http::StatusCode;
use actix_web::{get, web, HttpRequest, HttpResponse};
use indexer_data_model::browse::{self, BrowseQuery, CursorKey};
use indexer_data_model::{facets, registry, stats, timeline, PgPool};

use super::{collection_stats, enabled_collection, position_key, respond, text_key, Result};
use crate::cache::ResponseCache;
use crate::error::ApiError;
use crate::{cursor, dto, query};

#[get("/collections")]
pub async fn list_collections(
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
pub async fn get_collection(
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
pub async fn browse_collection_nfts(
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

    let canonical = filters.canonical();
    let fingerprint = cursor::fingerprint(&[
        &row.id.to_string(),
        sort.as_str(),
        &canonical,
        filters.q.as_deref().unwrap_or_default(),
    ]);
    let after = match query::cursor(raw) {
        Some(raw) => Some(cursor::decode(&raw, sort.as_str(), &fingerprint)?),
        None => None,
    };

    let key = text_key(
        "nfts",
        &[
            &row.id.to_string(),
            sort.as_str(),
            &canonical,
            filters.q.as_deref().unwrap_or_default(),
            &limit.to_string(),
            &after.as_ref().map_or(String::new(), |k| {
                format!("{}|{}|{}", k.number, k.text, k.id)
            }),
        ],
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
                sort.as_str(),
                &CursorKey {
                    number: last.sort_number,
                    text: last.sort_text.clone(),
                    id: last.id,
                },
                sort.key_is_text(),
                &fingerprint,
            )
        });
        let reference = dto::Collection::reference(&row);
        Ok(dto::Page::new(
            cards
                .iter()
                .map(|card| dto::NftSummary::new(card, reference.clone()))
                .collect(),
            next,
        ))
    })
    .await
}

#[get("/collections/{slug}/facets")]
pub async fn get_collection_facets(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
    slug: web::Path<String>,
) -> Result<HttpResponse> {
    let row = enabled_collection(&pool, &slug).await?;
    let raw = req.query_string();
    let filters = query::filters(raw)?;
    let key = text_key(
        "facets",
        &[
            &row.id.to_string(),
            &filters.canonical(),
            filters.q.as_deref().unwrap_or_default(),
        ],
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

#[get("/collections/{slug}/activity")]
pub async fn get_collection_activity(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
    slug: web::Path<String>,
) -> Result<HttpResponse> {
    let row = enabled_collection(&pool, &slug).await?;
    let raw = req.query_string();
    let kinds = query::kinds(raw)?;
    let limit = query::limit(raw, StatusCode::BAD_REQUEST)?;

    let selected = kinds.join(",");
    let fingerprint = cursor::fingerprint(&["collection-activity", &row.id.to_string(), &selected]);
    let after = match query::cursor(raw) {
        Some(raw) => Some(cursor::decode_scalar(&raw, cursor::ACTIVITY, &fingerprint)?),
        None => None,
    };

    let key = format!(
        "collection-activity:{}:{selected}:{limit}:{}",
        row.id,
        position_key(after)
    );
    respond(&req, &cache, &key, || async {
        let mut rows = timeline::collection_activity(
            pool.get_ref(),
            row.id,
            &kinds,
            after.map(|(key, id)| timeline::Position { key, id }),
            limit + 1,
        )
        .await?;
        let next = (rows.len() as i64 > limit).then(|| {
            rows.truncate(limit as usize);
            let last = rows.last().expect("limit >= 1");
            cursor::encode_scalar(cursor::ACTIVITY, last.slot, last.id, &fingerprint)
        });
        let reference = dto::Collection::reference(&row);
        Ok(dto::Page::new(
            rows.iter()
                .map(|row| dto::CollectionActivityEvent {
                    event: dto::ActivityEvent::new(&row.event()),
                    nft: dto::NftSummary::new(&row.card(), reference.clone()),
                })
                .collect(),
            next,
        ))
    })
    .await
}

#[get("/collections/{slug}/holders")]
pub async fn get_collection_holders(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
    slug: web::Path<String>,
) -> Result<HttpResponse> {
    let row = enabled_collection(&pool, &slug).await?;
    // This path declares its `limit` inline: default 25, not the shared 24.
    // Top-N only — it groups the collection's owner rows on every request, so
    // the contract caps it rather than paginating it.
    let limit = query::limit_with(req.query_string(), 25, 100, StatusCode::BAD_REQUEST)?;

    let key = format!("holders:{}:{limit}", row.id);
    respond(&req, &cache, &key, || async {
        let holders = stats::top_holders(pool.get_ref(), row.id, limit).await?;
        Ok(dto::HoldersResponse {
            data: holders
                .into_iter()
                .map(|h| dto::Holder {
                    address: h.address,
                    count: h.count,
                    rank: Some(h.rank),
                })
                .collect(),
        })
    })
    .await
}
