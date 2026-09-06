//! The `wallets` tag: one wallet's holdings across every indexed collection.
//!
//! The contract is emphatic that this endpoint never 404s — *"an unknown wallet
//! is `200` with `totalCount: 0` and empty arrays … the Explorer needs the
//! empty state"* — and its response list omits `404` entirely. Only a
//! malformed address is a `400`.
//!
//! The response is two-level and deliberately not nested: an unpaginated
//! `collections` summary (bounded by the number of enabled collections) plus a
//! single flat keyset grid. There is no `sort` parameter because the backing
//! index, `assets_owner_collection`, orders by collection then id.

use actix_web::http::StatusCode;
use actix_web::{get, web, HttpRequest, HttpResponse};
use indexer_data_model::registry::CollectionRow;
use indexer_data_model::{registry, stats, wallet, PgPool};

use super::{position_key, respond, Result};
use crate::cache::ResponseCache;
use crate::{cursor, dto, query};

#[get("/wallets/{address}/nfts")]
pub async fn get_wallet_portfolio(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
    address: web::Path<String>,
) -> Result<HttpResponse> {
    let owner = query::address(&address, "address")?.to_string();
    let raw = req.query_string();
    let limit = query::limit(raw, StatusCode::BAD_REQUEST)?;
    let slug = query::slug(raw, "collection")?;

    // An unknown or disabled `?collection=` narrows the grid to nothing rather
    // than 404ing: this path declares no 404, and the contract's rule is that
    // unknown *filter* input stays a 200 so a bookmark survives a registry
    // change. `collections` and `totalCount` ignore the filter either way.
    let enabled = registry::list_enabled(pool.get_ref()).await?;
    let filter = match &slug {
        Some(slug) => match enabled.iter().find(|c| &c.slug == slug) {
            Some(row) => Some(row.id),
            // -1 matches no collection, which is exactly the intent.
            None => Some(-1),
        },
        None => None,
    };

    let fingerprint = cursor::fingerprint(&["wallet", &owner, slug.as_deref().unwrap_or_default()]);
    let after = match query::cursor(raw) {
        Some(raw) => Some(cursor::decode_scalar(&raw, cursor::WALLET, &fingerprint)?),
        None => None,
    };

    let key = format!(
        "wallet:{owner}:{}:{limit}:{}",
        slug.as_deref().unwrap_or_default(),
        position_key(after)
    );
    respond(&req, &cache, &key, || async {
        let total_count = wallet::total_count(pool.get_ref(), &owner).await?;
        let holdings = wallet::holdings(pool.get_ref(), &owner).await?;
        let held_in: Vec<i32> = holdings.iter().map(|h| h.collection_id).collect();
        // One ranking query for every collection the wallet holds, sharing its
        // window with `/holders` so the two can never disagree.
        let ranks = stats::holder_ranks(pool.get_ref(), &owner, &held_in).await?;

        let mut rows = wallet::page(
            pool.get_ref(),
            &owner,
            filter,
            after.map(|(collection, id)| (collection as i32, id)),
            limit + 1,
        )
        .await?;
        let next = (rows.len() as i64 > limit).then(|| {
            rows.truncate(limit as usize);
            let last = rows.last().expect("limit >= 1");
            cursor::encode_scalar(
                cursor::WALLET,
                last.collection_id as i64,
                last.card.id,
                &fingerprint,
            )
        });

        Ok(dto::WalletPortfolio {
            address: owner.clone(),
            total_count,
            collections: holdings
                .iter()
                .filter_map(|holding| {
                    let row = reference(&enabled, holding.collection_id)?;
                    Some(dto::WalletCollectionHolding {
                        collection: row,
                        count: holding.count,
                        holder_rank: ranks
                            .iter()
                            .find(|r| r.collection_id == holding.collection_id)
                            .map(|r| r.rank),
                    })
                })
                .collect(),
            // Always empty in v1, by the contract's own argument: badge
            // membership is a registry concept, and a hard-coded slug set here
            // would contradict "membership is data, never code".
            badges: Vec::new(),
            nfts: dto::Page::new(
                rows.iter()
                    .filter_map(|row| {
                        Some(dto::NftSummary::new(
                            &row.card,
                            reference(&enabled, row.collection_id)?,
                        ))
                    })
                    .collect(),
                next,
            ),
        })
    })
    .await
}

fn reference(enabled: &[CollectionRow], collection_id: i32) -> Option<dto::CollectionRef> {
    enabled
        .iter()
        .find(|c| c.id == collection_id)
        .map(dto::Collection::reference)
}
