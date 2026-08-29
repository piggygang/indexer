//! Facet counts — the ALG-619 acceptance query (`< 100 ms` on the full Piggy
//! dataset) and what the browse/facets endpoints (ALG-625) build on.
//!
//! Semantics (API contract): values of one trait type OR together, distinct
//! trait types AND together; facet counts are DISJUNCTIVE — the counts for
//! trait type T are computed with T's own filter removed and every other
//! filter (other trait types, `q`) applied.

use std::collections::BTreeMap;

use sqlx::{FromRow, PgPool};

/// One active trait type with the selected value ids (OR within). An empty
/// `trait_value_ids` (unknown value string) matches nothing — the type still
/// counts as selected, so every other type's counts collapse to zero while
/// T's own values stay visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraitSelection {
    pub trait_type_id: i32,
    pub trait_value_ids: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct FacetCount {
    pub trait_type_id: i32,
    pub trait_type: String,
    pub trait_value_id: i32,
    pub value: String,
    pub count: i64,
}

/// Resolves `trait[<Type>]=<Value>` pairs (exact, case-sensitive) to ids.
/// `None` when a trait TYPE is unknown for the collection — nothing can
/// match, so callers return empty facets without querying.
pub async fn resolve_selections(
    pool: &PgPool,
    collection_id: i32,
    filters: &BTreeMap<String, Vec<String>>,
) -> sqlx::Result<Option<Vec<TraitSelection>>> {
    let mut selections = Vec::with_capacity(filters.len());
    for (trait_type, values) in filters {
        let type_id: Option<i32> =
            sqlx::query_scalar("SELECT id FROM trait_types WHERE collection_id = $1 AND name = $2")
                .bind(collection_id)
                .bind(trait_type)
                .fetch_optional(pool)
                .await?;
        let Some(trait_type_id) = type_id else {
            return Ok(None);
        };
        let trait_value_ids: Vec<i32> = sqlx::query_scalar(
            "SELECT id FROM trait_values WHERE trait_type_id = $1 AND value = ANY($2) ORDER BY id",
        )
        .bind(trait_type_id)
        .bind(values)
        .fetch_all(pool)
        .await?;
        selections.push(TraitSelection {
            trait_type_id,
            trait_value_ids,
        });
    }
    Ok(Some(selections))
}

/// Single-pass disjunctive facet query. Binds: `$1` collection, `$2`/`$3`
/// parallel arrays (selected type id, selected value id), `$4` distinct
/// selected type ids, `$5` = len($4), `$6` raw `q`, `$7` `q` as a
/// substring LIKE pattern (names), `$8` `q` as a prefix LIKE pattern (ids).
///
/// Population = member assets of the collection, burned included — the same
/// predicate browse uses (see the assets migration header).
///
/// `sat` = which selected types each candidate asset satisfies. An asset is
/// counted for a (type, value) pair when it satisfies every selected type,
/// or when it fails exactly one and that one is the pair's own type.
pub const DISJUNCTIVE_SQL: &str = "\
WITH base AS (
    SELECT a.id
    FROM assets a
    WHERE a.collection_id = $1 AND a.membership_status = 'member'
      AND ($6::text IS NULL
           OR a.address LIKE $8
           OR a.number = CASE WHEN $6 ~ '^#?[0-9]{1,9}$' THEN ltrim($6, '#')::int END
           OR a.name ILIKE $7)
),
sat AS (
    SELECT aa.asset_id, s.t AS trait_type_id
    FROM asset_attributes aa
    JOIN unnest($2::int[], $3::int[]) AS s(t, v) ON s.v = aa.trait_value_id
    JOIN base b ON b.id = aa.asset_id
    GROUP BY 1, 2
),
per_asset AS (
    SELECT asset_id, count(*)::int AS ok_count, array_agg(trait_type_id) AS ok_types
    FROM sat
    GROUP BY asset_id
)
SELECT tt.id AS trait_type_id, tt.name AS trait_type, tv.id AS trait_value_id, tv.value,
       count(*)::bigint AS count
FROM base b
LEFT JOIN per_asset pa ON pa.asset_id = b.id
JOIN asset_attributes aa ON aa.asset_id = b.id
JOIN trait_values tv ON tv.id = aa.trait_value_id
JOIN trait_types tt ON tt.id = tv.trait_type_id AND tt.collection_id = $1 AND tt.is_facet
WHERE coalesce(pa.ok_count, 0) = $5
   OR (coalesce(pa.ok_count, 0) = $5 - 1
       AND tt.id = ANY($4::int[])
       AND NOT (tt.id = ANY(coalesce(pa.ok_types, '{}'::int[]))))
GROUP BY tt.id, tt.name, tv.id, tv.value
ORDER BY tt.name, count DESC, tv.value";

struct Binds {
    types: Vec<i32>,
    values: Vec<i32>,
    distinct_types: Vec<i32>,
    q: Option<String>,
    like: Option<String>,
    prefix: Option<String>,
}

fn binds(selections: &[TraitSelection], q: Option<&str>) -> Binds {
    let mut types = Vec::new();
    let mut values = Vec::new();
    let mut distinct_types = Vec::new();
    for s in selections {
        if !distinct_types.contains(&s.trait_type_id) {
            distinct_types.push(s.trait_type_id);
        }
        for v in &s.trait_value_ids {
            types.push(s.trait_type_id);
            values.push(*v);
        }
    }
    let q = q
        .map(str::trim)
        .filter(|q| !q.is_empty())
        .map(str::to_owned);
    let escaped = q.as_deref().map(|q| {
        q.replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    });
    let like = escaped.as_deref().map(|e| format!("%{e}%"));
    let prefix = escaped.as_deref().map(|e| format!("{e}%"));
    Binds {
        types,
        values,
        distinct_types,
        q,
        like,
        prefix,
    }
}

/// Disjunctive facet counts for one collection under the active selections
/// and optional text search. Rows are ordered by trait type name, then count
/// descending, then value.
pub async fn disjunctive_facet_counts(
    pool: &PgPool,
    collection_id: i32,
    selections: &[TraitSelection],
    q: Option<&str>,
) -> sqlx::Result<Vec<FacetCount>> {
    let b = binds(selections, q);
    // Unnamed statement: planned with the actual parameter values every
    // time. A cached generic plan would treat `$6 IS NULL OR ...` and the
    // array sizes as unknowns and can pick nested loops over 80k rows; a
    // ~1 ms re-plan is the cheaper side of that trade.
    sqlx::query_as::<_, FacetCount>(DISJUNCTIVE_SQL)
        .persistent(false)
        .bind(collection_id)
        .bind(&b.types)
        .bind(&b.values)
        .bind(&b.distinct_types)
        .bind(b.distinct_types.len() as i32)
        .bind(&b.q)
        .bind(&b.like)
        .bind(&b.prefix)
        .fetch_all(pool)
        .await
}

/// `EXPLAIN (ANALYZE, BUFFERS)` of [`disjunctive_facet_counts`] with the same
/// binds — the acceptance evidence `indexer-admin bench` prints.
pub async fn explain_disjunctive(
    pool: &PgPool,
    collection_id: i32,
    selections: &[TraitSelection],
    q: Option<&str>,
) -> sqlx::Result<Vec<String>> {
    let b = binds(selections, q);
    let sql = format!("EXPLAIN (ANALYZE, BUFFERS) {DISJUNCTIVE_SQL}");
    sqlx::query_scalar::<_, String>(&sql)
        .persistent(false)
        .bind(collection_id)
        .bind(&b.types)
        .bind(&b.values)
        .bind(&b.distinct_types)
        .bind(b.distinct_types.len() as i32)
        .bind(&b.q)
        .bind(&b.like)
        .bind(&b.prefix)
        .fetch_all(pool)
        .await
}

/// Unfiltered counts from the `facet_counts` view (same row shape and order).
pub async fn facet_counts(pool: &PgPool, collection_id: i32) -> sqlx::Result<Vec<FacetCount>> {
    sqlx::query_as::<_, FacetCount>(
        "SELECT trait_type_id, trait_type, trait_value_id, value, count::bigint AS count \
           FROM facet_counts WHERE collection_id = $1 \
          ORDER BY trait_type, count DESC, value",
    )
    .bind(collection_id)
    .fetch_all(pool)
    .await
}
