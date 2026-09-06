//! Collection stats for the API (ALG-625).
//!
//! Everything here is a read of `collection_stats` plus the holder cohorts,
//! which are a `GROUP BY` over per-owner counts rather than a view column —
//! the bucket edges are a presentation choice and the contract says so
//! explicitly ("an array rather than fixed fields so the bucket edges can be
//! retuned without a contract change").
//!
//! These are the numbers behind a short-TTL cache in the API, not per-request
//! work: `holders` and the cohorts both scan the collection's assets.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgExecutor};

/// One row of `collection_stats`, plus the cohorts.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct CollectionStats {
    pub collection_id: i32,
    /// Members excluding burned.
    pub supply: i32,
    pub holders: i32,
    pub activity_24h: i32,
    pub activity_7d: i32,
    pub burned: i32,
    /// The browse population — members including burned. The unfiltered
    /// result count for the grid.
    pub indexed: i32,
    pub last_activity_at: Option<DateTime<Utc>>,
}

/// A holder cohort: wallets holding between `min_count` and `max_count`
/// assets, and how many assets they hold between them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HolderBucket {
    pub label: String,
    pub min_count: i32,
    /// `None` is unbounded.
    pub max_count: Option<i32>,
    pub holders: i64,
    pub assets: i64,
}

/// Stats for every enabled collection, in registry order.
pub async fn all<'e>(exec: impl PgExecutor<'e>) -> sqlx::Result<Vec<CollectionStats>> {
    sqlx::query_as(
        "SELECT s.collection_id, s.supply, s.holders, s.activity_24h, s.activity_7d, \
                s.burned, s.indexed, s.last_activity_at \
           FROM collection_stats s JOIN collections c ON c.id = s.collection_id \
          WHERE c.enabled ORDER BY c.id",
    )
    .fetch_all(exec)
    .await
}

/// Stats for one collection.
pub async fn one<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
) -> sqlx::Result<Option<CollectionStats>> {
    sqlx::query_as(
        "SELECT collection_id, supply, holders, activity_24h, activity_7d, \
                burned, indexed, last_activity_at \
           FROM collection_stats WHERE collection_id = $1",
    )
    .bind(collection_id)
    .fetch_optional(exec)
    .await
}

/// The cohort edges. Open-ended at the top, so the last bucket's `max_count`
/// is `None` and every wallet lands in exactly one.
const BUCKETS: [(&str, i32, Option<i32>); 4] = [
    ("1", 1, Some(1)),
    ("2-5", 2, Some(5)),
    ("6-20", 6, Some(20)),
    ("21+", 21, None),
];

/// Holder cohorts for one collection.
///
/// Counted over the same population as `holders` — members with an owner, so
/// burned assets (which have none) drop out and the `assets` column sums to
/// `supply`, exactly as the contract states.
pub async fn holder_buckets<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
) -> sqlx::Result<Vec<HolderBucket>> {
    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT held, count(*)::bigint FROM ( \
             SELECT count(*)::bigint AS held FROM assets \
              WHERE collection_id = $1 AND membership_status = 'member' AND owner IS NOT NULL \
              GROUP BY owner) per_owner \
          GROUP BY held",
    )
    .bind(collection_id)
    .fetch_all(exec)
    .await?;

    Ok(BUCKETS
        .iter()
        .map(|(label, min, max)| {
            let (holders, assets) = rows
                .iter()
                .filter(|(held, _)| {
                    *held >= i64::from(*min) && max.is_none_or(|m| *held <= i64::from(m))
                })
                .fold((0, 0), |(h, a), (held, wallets)| {
                    (h + wallets, a + held * wallets)
                });
            HolderBucket {
                label: (*label).to_string(),
                min_count: *min,
                max_count: *max,
                holders,
                assets,
            }
        })
        .collect())
}

/// One holder of one collection — the contract's `Holder`, and the counting
/// half of `WalletCollectionHolding`.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct Holder {
    pub collection_id: i32,
    pub address: String,
    pub count: i64,
    pub rank: i64,
}

/// Holders ranked by holdings descending, over the same population as
/// `holders` and `holder_buckets` — members with an owner, so burned assets
/// drop out and the counts sum to `supply`, exactly as the contract states.
///
/// `RANK()`, not `DENSE_RANK()`: the contract says *"ties share the lower rank
/// and skip the next values"*. The window has to be computed in a subquery,
/// because a `WHERE` on the outer query is applied *before* window functions
/// and would rank only the surviving rows.
const RANKED: &str = "\
    WITH per_owner AS ( \
        SELECT a.collection_id, a.owner, count(*)::bigint AS held \
          FROM assets a JOIN collections c ON c.id = a.collection_id \
         WHERE a.collection_id = ANY($1::int[]) AND a.membership_status = 'member' \
           AND a.owner IS NOT NULL AND c.enabled \
         GROUP BY a.collection_id, a.owner), \
    ranked AS ( \
        SELECT collection_id, owner, held, \
               rank() OVER (PARTITION BY collection_id ORDER BY held DESC)::bigint AS rank \
          FROM per_owner)";

/// The top holders of one collection, ranked.
pub async fn top_holders<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
    limit: i64,
) -> sqlx::Result<Vec<Holder>> {
    sqlx::query_as::<_, Holder>(&format!(
        "{RANKED} SELECT collection_id, owner AS address, held AS count, rank FROM ranked \
          ORDER BY rank, address LIMIT $2"
    ))
    .bind(vec![collection_id])
    .bind(limit)
    .fetch_all(exec)
    .await
}

/// One wallet's rank in each of the collections it holds.
///
/// Shares [`RANKED`] with [`top_holders`], so a portfolio's `holderRank` and
/// the same collection's `/holders` listing can never disagree about a wallet's
/// position.
pub async fn holder_ranks<'e>(
    exec: impl PgExecutor<'e>,
    owner: &str,
    collections: &[i32],
) -> sqlx::Result<Vec<Holder>> {
    sqlx::query_as::<_, Holder>(&format!(
        "{RANKED} SELECT collection_id, owner AS address, held AS count, rank FROM ranked \
          WHERE owner = $2 ORDER BY held DESC, collection_id"
    ))
    .bind(collections)
    .bind(owner)
    .fetch_all(exec)
    .await
}
