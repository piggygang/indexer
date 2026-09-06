//! The wallet portfolio (ALG-626).
//!
//! Population: `owner = $1 AND membership_status = 'member'`, in enabled
//! collections. Burned assets can never appear — `assets_burned_has_no_owner`
//! makes a burned asset ownerless — which is the contract's stated behaviour,
//! not a gap: *"Burned assets never appear (a burned asset has no owner)."*
//!
//! Every query here is served by `assets_owner_collection (owner,
//! collection_id, id) WHERE owner IS NOT NULL`, which is also why the grid has
//! no `sort` parameter: *"the backing index orders by collection then id."*

use sqlx::{FromRow, PgPool};

use crate::browse::AssetCard;

/// One card plus the collection it belongs to, so a cross-collection grid can
/// attach the right badge without a second lookup.
///
/// No `Eq`: the embedded card carries an `Option<f64>` rarity score.
#[derive(Debug, Clone, PartialEq, FromRow)]
pub struct PortfolioCard {
    pub collection_id: i32,
    #[sqlx(flatten)]
    pub card: AssetCard,
}

/// How many of one collection a wallet holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromRow)]
pub struct Holding {
    pub collection_id: i32,
    pub count: i64,
}

const CARD_COLUMNS: &str = "a.collection_id, a.id, a.address, a.name, a.number, a.image_uri, \
     a.image_status, a.burned, a.owner, a.last_activity_at, a.rarity_score, a.rarity_rank, \
     0::bigint AS sort_number, ''::text AS sort_text";

/// Assets held across every enabled collection — the contract's `totalCount`,
/// which `?collection=` deliberately does not narrow.
pub async fn total_count(pool: &PgPool, owner: &str) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        "SELECT count(*)::bigint FROM assets a \
           JOIN collections c ON c.id = a.collection_id \
          WHERE a.owner = $1 AND a.membership_status = 'member' AND c.enabled",
    )
    .bind(owner)
    .fetch_one(pool)
    .await
}

/// The grouping, count descending. Collections with no holdings are absent —
/// `WalletCollectionHolding.count` has `minimum: 1`, so a zero must never be
/// emitted as a row.
pub async fn holdings(pool: &PgPool, owner: &str) -> sqlx::Result<Vec<Holding>> {
    sqlx::query_as::<_, Holding>(
        "SELECT a.collection_id, count(*)::bigint AS count FROM assets a \
           JOIN collections c ON c.id = a.collection_id \
          WHERE a.owner = $1 AND a.membership_status = 'member' AND c.enabled \
          GROUP BY a.collection_id ORDER BY count DESC, a.collection_id",
    )
    .bind(owner)
    .fetch_all(pool)
    .await
}

/// One keyset page of the flat grid, ordered by collection then id.
///
/// `collection` narrows only this grid; the caller's `collections` array and
/// `totalCount` are computed without it.
pub async fn page(
    pool: &PgPool,
    owner: &str,
    collection: Option<i32>,
    after: Option<(i32, i64)>,
    limit: i64,
) -> sqlx::Result<Vec<PortfolioCard>> {
    // Placeholders are numbered by appearance: $1 owner, $2 collection filter,
    // then the optional keyset pair, then the limit.
    let (keyset, limit_param) = match after {
        Some(_) => (" AND (a.collection_id, a.id) > ($3::int, $4::bigint)", "$5"),
        None => ("", "$3"),
    };
    let sql = format!(
        "SELECT {CARD_COLUMNS} FROM assets a \
           JOIN collections c ON c.id = a.collection_id \
          WHERE a.owner = $1 AND a.membership_status = 'member' AND c.enabled \
            AND ($2::int IS NULL OR a.collection_id = $2){keyset} \
          ORDER BY a.collection_id, a.id LIMIT {limit_param}"
    );
    let mut q = sqlx::query_as::<_, PortfolioCard>(&sql)
        .bind(owner)
        .bind(collection);
    if let Some((collection_id, id)) = after {
        q = q.bind(collection_id).bind(id);
    }
    q.bind(limit).fetch_all(pool).await
}
