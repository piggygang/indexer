//! The browse grid and the filtered result count (ALG-625).
//!
//! The population is stated once in the assets migration and applied here
//! verbatim: `collection_id = $1 AND membership_status = 'member'` — burned
//! assets are **in**, because the UI greys them rather than hiding them, and
//! the contract has no burned filter.
//!
//! Filtering and `q` reuse [`crate::facets::binds`], so this query, the
//! filtered total and the facet counts are bound from one place. The contract
//! promises the grid and the sidebar never disagree; sharing the predicate is
//! what makes that structural rather than a thing to remember.
//!
//! Sorting is keyset, never offset, and every `ORDER BY` here matches an
//! `assets_browse_*` index expression exactly — including the NULL sentinels
//! (`coalesce(number, 2147483647)`, `coalesce(last_activity_slot, -1)`), which
//! exist so a cursor can never lose the NULL rows. One index serves both
//! directions through a backward scan.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

use crate::facets::{self, TraitSelection};

/// How the grid is ordered. `rarity` is reserved by the contract and rejected
/// at the HTTP layer with `422`, so it has no variant here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sort {
    Number,
    NumberDesc,
    Name,
    NameDesc,
    Activity,
    ActivityDesc,
}

impl Sort {
    /// The contract's wire value, and what a cursor records so it can be
    /// rejected when replayed against a different sort.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Number => "number",
            Self::NumberDesc => "-number",
            Self::Name => "name",
            Self::NameDesc => "-name",
            Self::Activity => "activity",
            Self::ActivityDesc => "-activity",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "number" => Self::Number,
            "-number" => Self::NumberDesc,
            "name" => Self::Name,
            "-name" => Self::NameDesc,
            "activity" => Self::Activity,
            "-activity" => Self::ActivityDesc,
            _ => return None,
        })
    }

    const fn descending(self) -> bool {
        matches!(self, Self::NumberDesc | Self::NameDesc | Self::ActivityDesc)
    }

    /// The sort key as SQL, with the NULL sentinel the matching index stores.
    const fn key_sql(self) -> &'static str {
        match self {
            Self::Number | Self::NumberDesc => "coalesce(a.number, 2147483647)",
            Self::Name | Self::NameDesc => "a.name",
            Self::Activity | Self::ActivityDesc => "coalesce(a.last_activity_slot, -1)",
        }
    }

    /// Is the key a number (`$k` binds as bigint) or text? Public because the
    /// cursor codec has to encode the same distinction.
    pub const fn key_is_text(self) -> bool {
        matches!(self, Self::Name | Self::NameDesc)
    }
}

/// One page's worth of keyset position: the sort key of the last row returned,
/// plus its id as the tiebreaker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorKey {
    /// Numeric sort key. Unused when the sort is by name.
    pub number: i64,
    /// Text sort key. Unused when the sort is numeric.
    pub text: String,
    pub id: i64,
}

#[derive(Debug, Clone)]
pub struct BrowseQuery {
    pub collection_id: i32,
    pub selections: Vec<TraitSelection>,
    pub q: Option<String>,
    pub sort: Sort,
    pub after: Option<CursorKey>,
    /// Rows to return. The caller asks for one more than the page size to
    /// learn `hasMore` without a `COUNT`.
    pub limit: i64,
}

/// One grid card — the `NftSummary` row shape.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct AssetCard {
    pub id: i64,
    pub address: String,
    pub name: String,
    pub number: Option<i32>,
    pub image_uri: Option<String>,
    pub image_status: String,
    pub burned: bool,
    pub owner: Option<String>,
    pub last_activity_at: Option<DateTime<Utc>>,
    /// The keyset position of this row, so the caller can mint a cursor
    /// without re-deriving which column the sort used.
    pub sort_number: i64,
    pub sort_text: String,
}

/// The filter half of the predicate, shared by the page and the total.
///
/// `$2`/`$3` are the parallel (type, value) arrays, `$4` the distinct selected
/// types and `$5` their count; an asset is in the set when it satisfies every
/// selected type. `$6`/`$7`/`$8` are `q` and its two LIKE patterns — the same
/// three predicates the facet query applies, in the same order.
const FILTERED_BASE: &str = "\
    SELECT a.id, a.address, a.name, a.number, a.image_uri, a.image_status, a.burned, \
           a.owner, a.last_activity_at, a.last_activity_slot \
      FROM assets a \
     WHERE a.collection_id = $1 AND a.membership_status = 'member' \
       AND ($6::text IS NULL \
            OR a.address LIKE $8 \
            OR a.number = CASE WHEN $6 ~ '^#?[0-9]{1,9}$' THEN ltrim($6, '#')::int END \
            OR a.name ILIKE $7) \
       AND ($5 = 0 OR (SELECT count(DISTINCT s.t) \
                         FROM unnest($2::int[], $3::int[]) AS s(t, v) \
                         JOIN asset_attributes aa \
                           ON aa.asset_id = a.id AND aa.trait_value_id = s.v) = $5)";

/// One page of cards under the active filters.
///
/// Ordering and the keyset predicate are built from [`Sort`] rather than bound,
/// because a column name cannot be a bind parameter; every fragment is a
/// compile-time constant chosen by an exhaustive `match`, so no user input
/// reaches the SQL text.
pub async fn browse(pool: &PgPool, query: &BrowseQuery) -> sqlx::Result<Vec<AssetCard>> {
    let b = facets::binds(&query.selections, query.q.as_deref());
    let sort = query.sort;
    let key = sort.key_sql();
    let dir = if sort.descending() { "DESC" } else { "ASC" };

    // `(key, id) > (k, i)` as an explicit conjunction rather than a row
    // comparison: the text and numeric keys bind differently, and spelling it
    // out keeps the planner on the composite index either way.
    let keyset = match (query.after.is_some(), sort.descending(), sort.key_is_text()) {
        (false, _, _) => String::new(),
        (true, false, false) => format!(" AND ({key}, a.id) > ($9::bigint, $10::bigint)"),
        (true, true, false) => format!(" AND ({key}, a.id) < ($9::bigint, $10::bigint)"),
        (true, false, true) => format!(" AND ({key}, a.id) > ($9::text, $10::bigint)"),
        (true, true, true) => format!(" AND ({key}, a.id) < ($9::text, $10::bigint)"),
    };
    // Postgres numbers placeholders by appearance, so the limit is `$9` when
    // there is no cursor and `$11` when the keyset clause consumed two.
    let limit_param = if query.after.is_some() { "$11" } else { "$9" };
    let sql = format!(
        "WITH base AS ({FILTERED_BASE}{keyset}) \
         SELECT id, address, name, number, image_uri, image_status, burned, owner, \
                last_activity_at, \
                {number_key}::bigint AS sort_number, {text_key}::text AS sort_text \
           FROM base a \
          ORDER BY {key} {dir}, a.id {dir} \
          LIMIT {limit_param}",
        number_key = if sort.key_is_text() { "0" } else { key },
        text_key = if sort.key_is_text() { key } else { "''" },
    );

    let mut q = sqlx::query_as::<_, AssetCard>(&sql)
        .bind(query.collection_id)
        .bind(&b.types)
        .bind(&b.values)
        .bind(&b.distinct_types)
        .bind(b.distinct_types.len() as i32)
        .bind(&b.q)
        .bind(&b.like)
        .bind(&b.prefix);
    if let Some(after) = &query.after {
        if sort.key_is_text() {
            q = q.bind(after.text.clone());
        } else {
            q = q.bind(after.number);
        }
        q = q.bind(after.id);
    }
    // Same reasoning as the facet query: planned with the real parameter
    // values every time, because a generic plan treats the array sizes and the
    // `q IS NULL` branch as unknowns and can pick nested loops.
    q.bind(query.limit).persistent(false).fetch_all(pool).await
}

/// The size of the filtered result set — the contract's `FacetsResponse.total`.
///
/// Built from the same `FILTERED_BASE` as the page, so the number the sidebar
/// shows is by construction the number of cards the grid will yield.
pub async fn filtered_total(
    pool: &PgPool,
    collection_id: i32,
    selections: &[TraitSelection],
    q: Option<&str>,
) -> sqlx::Result<i64> {
    let b = facets::binds(selections, q);
    sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM ({FILTERED_BASE}) base"
    ))
    .bind(collection_id)
    .bind(&b.types)
    .bind(&b.values)
    .bind(&b.distinct_types)
    .bind(b.distinct_types.len() as i32)
    .bind(&b.q)
    .bind(&b.like)
    .bind(&b.prefix)
    .persistent(false)
    .fetch_one(pool)
    .await
}
