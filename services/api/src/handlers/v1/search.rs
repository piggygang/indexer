//! The `search` tag: one box that understands what is pasted into it.
//!
//! Three interpretations and one routing rule, both straight from the contract:
//!
//! * a base58 address **resolves by lookup** — a known mint routes to that NFT,
//!   a wallet holding indexed assets routes to that wallet, anything else
//!   resolves to nothing. An address never produces text groups;
//! * `#N` (or a bare integer) matches the token number;
//! * anything else is a case-insensitive substring of the name.
//!
//! `route` precedence is *exact mint > wallet with holdings > exact collection
//! slug*, and it is *"never returned when the best hit is fuzzy"* — so a
//! substring match that happens to be popular still routes nowhere. Like the
//! wallet endpoint, this never 404s: nothing indexed is `200` with an empty
//! result.

use actix_web::http::StatusCode;
use actix_web::{get, web, HttpRequest, HttpResponse};
use indexer_data_model::registry::CollectionRow;
use indexer_data_model::{nft, registry, search as model, wallet, PgPool};

use super::{respond, text_key, Result};
use crate::cache::ResponseCache;
use crate::{dto, query};

#[get("/search")]
pub async fn search(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
) -> Result<HttpResponse> {
    let raw = req.query_string();
    let q = query::required_q(raw)?;
    let slug = query::slug(raw, "collection")?;
    // Inline `limit` on this path: at most 25 NFT results per collection group,
    // default 10.
    let limit = query::limit_with(raw, 10, 25, StatusCode::BAD_REQUEST)?;

    let key = text_key(
        "search",
        &[&q, slug.as_deref().unwrap_or_default(), &limit.to_string()],
    );
    respond(&req, &cache, &key, || async {
        let enabled = registry::list_enabled(pool.get_ref()).await?;
        let filter = slug.as_ref().map(|slug| {
            enabled
                .iter()
                .find(|c| &c.slug == slug)
                .map_or(-1, |c| c.id)
        });

        let interpreted = interpret(&q);
        let mut route = None;
        let mut wallet_hit = None;
        let mut groups = Vec::new();

        match &interpreted {
            Interpretation::Address => {
                // Exact mint first, then a wallet that actually holds something
                // indexed. Both are lookups, so neither is ever "fuzzy".
                if nft::by_address(pool.get_ref(), &q).await?.is_some() {
                    route = Some(dto::SearchRoute {
                        kind: "nft".into(),
                        id: q.clone(),
                    });
                } else {
                    let total = wallet::total_count(pool.get_ref(), &q).await?;
                    if total > 0 {
                        route = Some(dto::SearchRoute {
                            kind: "wallet".into(),
                            id: q.clone(),
                        });
                        // `minimum: 1` — a wallet holding nothing is `null`,
                        // never a hit reporting zero.
                        wallet_hit = Some(dto::WalletHit {
                            address: q.clone(),
                            total_count: total,
                        });
                    }
                }
            }
            Interpretation::Number(n) => {
                groups = grouped(
                    pool.get_ref(),
                    &enabled,
                    &model::Match::Number(*n),
                    filter,
                    limit,
                )
                .await?;
            }
            Interpretation::Text => {
                // An exact slug is the weakest of the three routes, and the
                // only one that can accompany groups.
                if let Some(row) = enabled.iter().find(|c| c.slug == q) {
                    route = Some(dto::SearchRoute {
                        kind: "collection".into(),
                        id: row.slug.clone(),
                    });
                }
                groups = grouped(
                    pool.get_ref(),
                    &enabled,
                    &model::Match::Text(q.clone()),
                    filter,
                    limit,
                )
                .await?;
            }
        }

        Ok(dto::SearchResponse {
            query: q.clone(),
            interpreted_as: interpreted.as_str().to_string(),
            route,
            wallet: wallet_hit,
            groups,
        })
    })
    .await
}

enum Interpretation {
    Address,
    Number(i32),
    Text,
}

impl Interpretation {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Address => "address",
            Self::Number(_) => "number",
            Self::Text => "text",
        }
    }
}

/// How the server parses the input. An address is recognised by shape alone —
/// whether it resolves is the route's business, and an unknown one is still
/// `interpretedAs: address` (the contract's `nothing` example says so).
fn interpret(q: &str) -> Interpretation {
    if query::address(q, "q").is_ok() {
        return Interpretation::Address;
    }
    let digits = q.strip_prefix('#').unwrap_or(q);
    // `#0001` is the same pig as `#1`; `number` is an integer either way. The
    // 9-digit cap keeps the parse inside `int`, which is the column's type.
    if !digits.is_empty() && digits.len() <= 9 && digits.bytes().all(|b| b.is_ascii_digit()) {
        if let Ok(n) = digits.parse::<i32>() {
            return Interpretation::Number(n);
        }
    }
    Interpretation::Text
}

/// Turns flat hits into the contract's groups: most hits first, each capped.
async fn grouped(
    pool: &PgPool,
    enabled: &[CollectionRow],
    matcher: &model::Match,
    filter: Option<i32>,
    limit: i64,
) -> Result<Vec<dto::SearchGroup>> {
    let hits = model::grouped(pool, matcher, filter, limit).await?;
    let mut groups: Vec<dto::SearchGroup> = Vec::new();
    for hit in &hits {
        let Some(collection) = enabled
            .iter()
            .find(|c| c.id == hit.collection_id)
            .map(dto::Collection::reference)
        else {
            continue;
        };
        let summary = dto::NftSummary::new(&hit.card, collection.clone());
        match groups.last_mut() {
            Some(last) if last.collection.slug == collection.slug => last.nfts.push(summary),
            _ => groups.push(dto::SearchGroup {
                collection,
                total: hit.total,
                nfts: vec![summary],
            }),
        }
    }
    groups.sort_by(|a, b| {
        b.total
            .cmp(&a.total)
            .then(a.collection.slug.cmp(&b.collection.slug))
    });
    Ok(groups)
}
