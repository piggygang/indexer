//! The `nfts` tag: one asset, its timeline, its ownership history.
//!
//! `/nfts/{id}` is the one endpoint that deliberately steps outside the browse
//! population — the contract calls a removed or burned asset "a valid page with
//! valid history" — so the only guard is that its collection is enabled, which
//! `nft::by_address` applies inside the query. That is the address-keyed
//! equivalent of `enabled_collection`, and it is what keeps the disabled
//! `bench-*` fixtures invisible here too.

use actix_web::http::StatusCode;
use actix_web::{get, web, HttpRequest, HttpResponse};
use indexer_data_model::nft::{self, NftRow};
use indexer_data_model::{timeline, PgPool};

use super::{position_key, respond, Result};
use crate::cache::ResponseCache;
use crate::error::ApiError;
use crate::{cursor, dto, query};

#[get("/nfts/{id}")]
pub async fn get_nft(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
    id: web::Path<String>,
) -> Result<HttpResponse> {
    let row = asset(&pool, &id).await?;
    respond(&req, &cache, &format!("nft:{}", row.id), || async {
        let attributes = nft::attributes(pool.get_ref(), row.id).await?;
        let open = nft::open_interval(pool.get_ref(), row.id).await?;
        let mint = nft::mint_info(pool.get_ref(), row.id).await?;
        let summary = nft::activity_summary(pool.get_ref(), row.id).await?;

        // "`heldSince` comes from the open ownership interval, and is null when
        // that interval disagrees with the observed owner rather than
        // attributing a date to the wrong wallet." That disagreement is the
        // same predicate `integrity_owner_mismatch` encodes, applied to the
        // interval we already have.
        let agrees = open
            .as_ref()
            .is_some_and(|interval| Some(&interval.owner) == row.owner.as_ref());
        let held = agrees.then(|| open.as_ref().expect("agrees implies Some"));

        Ok(dto::NftDetail {
            summary: dto::NftSummary {
                address: row.address.clone(),
                name: row.name.clone(),
                number: row.number,
                image_uri: row.image_uri.clone(),
                image_status: row.image_status.clone(),
                burned: row.burned,
                owner: row.owner.clone(),
                last_activity_at: row.last_activity_at,
                rarity_rank: None,
                rarity_score: None,
                collection: dto::CollectionRef {
                    slug: row.collection_slug.clone(),
                    name: row.collection_name.clone(),
                    image_url: row.collection_image_url.clone(),
                },
            },
            standard: row.standard.map(|s| s.as_str().to_string()),
            symbol: row.symbol.clone(),
            membership_status: row.membership_status.clone(),
            removed_at: row.removed_at,
            metadata_uri: row.metadata_uri.clone(),
            metadata_source_uri: row.metadata_source_uri.clone(),
            image_checked_at: row.image_checked_at,
            updated_at: row.updated_at,
            attributes: attributes
                .into_iter()
                .map(|a| dto::Attribute {
                    trait_type: a.trait_type,
                    value: a.value,
                    position: a.position,
                    is_facet: a.is_facet,
                    rarity_pct: None,
                })
                .collect(),
            ownership: dto::OwnerCard {
                owner: row.owner.clone(),
                owner_slot: row.owner_slot,
                held_since: held.map(|i| i.from_ts),
                held_since_slot: held.map(|i| i.from_slot),
                acquired_by_signature: held.and_then(|i| i.opened_by_signature.clone()),
            },
            mint: dto::MintInfo {
                minted_at: mint.as_ref().map(|m| m.minted_at),
                mint_slot: mint.as_ref().map(|m| m.mint_slot),
                signature: mint.map(|m| m.signature),
            },
            activity_summary: dto::ActivitySummary::new(&summary),
        })
    })
    .await
}

#[get("/nfts/{id}/activity")]
pub async fn get_nft_activity(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
    id: web::Path<String>,
) -> Result<HttpResponse> {
    let row = asset(&pool, &id).await?;
    let raw = req.query_string();
    let kinds = query::kinds(raw)?;
    let limit = query::limit(raw, StatusCode::BAD_REQUEST)?;

    let selected = kinds.join(",");
    let fingerprint = cursor::fingerprint(&["nft-activity", &row.address, &selected]);
    let after = match query::cursor(raw) {
        Some(raw) => Some(cursor::decode_scalar(&raw, cursor::ACTIVITY, &fingerprint)?),
        None => None,
    };

    let key = format!(
        "nft-activity:{}:{selected}:{limit}:{}",
        row.id,
        position_key(after)
    );
    respond(&req, &cache, &key, || async {
        let mut rows = timeline::asset_activity(
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
        Ok(dto::Page::new(
            rows.iter().map(dto::ActivityEvent::new).collect(),
            next,
        ))
    })
    .await
}

#[get("/nfts/{id}/owners")]
pub async fn get_nft_owners(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    cache: web::Data<ResponseCache>,
    id: web::Path<String>,
) -> Result<HttpResponse> {
    let row = asset(&pool, &id).await?;
    let raw = req.query_string();
    let limit = query::limit(raw, StatusCode::BAD_REQUEST)?;

    let fingerprint = cursor::fingerprint(&["nft-owners", &row.address]);
    let after = match query::cursor(raw) {
        Some(raw) => Some(cursor::decode_scalar(&raw, cursor::OWNERS, &fingerprint)?),
        None => None,
    };

    let key = format!("nft-owners:{}:{limit}:{}", row.id, position_key(after));
    respond(&req, &cache, &key, || async {
        let mut rows = timeline::owners(
            pool.get_ref(),
            row.id,
            after.map(|(key, id)| timeline::Position { key, id }),
            limit + 1,
        )
        .await?;
        let next = (rows.len() as i64 > limit).then(|| {
            rows.truncate(limit as usize);
            let last = rows.last().expect("limit >= 1");
            cursor::encode_scalar(cursor::OWNERS, last.from_slot, last.id, &fingerprint)
        });
        Ok(dto::Page::new(
            rows.iter().map(dto::OwnershipInterval::new).collect(),
            next,
        ))
    })
    .await
}

/// Resolves `{id}` to an asset.
///
/// A malformed id is `400 invalid_parameter` naming the parameter — claiming
/// `404` would say we looked, and a client cannot tell a typo from a burn. A
/// well-formed id we do not index is the `404`.
async fn asset(pool: &PgPool, id: &str) -> Result<NftRow> {
    let address = query::address(id, "id")?;
    nft::by_address(pool, address).await?.ok_or_else(|| {
        ApiError::not_found(
            format!("no indexed NFT with address `{address}`"),
            serde_json::json!({ "id": address }),
        )
    })
}
