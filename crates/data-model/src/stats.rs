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
