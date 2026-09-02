//! Trait dictionary maintenance shared by the backfill (ALG-621), the live
//! pipeline (ALG-623), the seeder and the synthetic generator.

use sqlx::PgExecutor;

/// Returns the trait type's id, creating it if needed. `is_facet` is derived
/// from `collections.facet_exclude` at creation and never flipped here.
pub async fn ensure_trait_type<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
    name: &str,
) -> sqlx::Result<i32> {
    sqlx::query_scalar(
        "INSERT INTO trait_types (collection_id, name, is_facet) \
         SELECT c.id, $2, NOT ($2 = ANY(c.facet_exclude)) FROM collections c WHERE c.id = $1 \
         ON CONFLICT (collection_id, name) DO UPDATE SET name = EXCLUDED.name \
         RETURNING id",
    )
    .bind(collection_id)
    .bind(name)
    .fetch_one(exec)
    .await
}

/// Returns the trait value's id, creating it if needed.
pub async fn ensure_trait_value<'e>(
    exec: impl PgExecutor<'e>,
    trait_type_id: i32,
    value: &str,
) -> sqlx::Result<i32> {
    sqlx::query_scalar(
        "INSERT INTO trait_values (trait_type_id, value) VALUES ($1, $2) \
         ON CONFLICT (trait_type_id, value) DO UPDATE SET value = EXCLUDED.value \
         RETURNING id",
    )
    .bind(trait_type_id)
    .bind(value)
    .fetch_one(exec)
    .await
}

/// Re-applies `collections.facet_exclude` to the existing trait types of one
/// collection. Returns the number of rows that changed.
pub async fn sync_trait_facets<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
) -> sqlx::Result<u64> {
    let result = sqlx::query(
        "UPDATE trait_types tt \
            SET is_facet = NOT (tt.name = ANY(c.facet_exclude)) \
           FROM collections c \
          WHERE c.id = tt.collection_id AND c.id = $1 \
            AND tt.is_facet IS DISTINCT FROM NOT (tt.name = ANY(c.facet_exclude))",
    )
    .bind(collection_id)
    .execute(exec)
    .await?;
    Ok(result.rows_affected())
}

/// One trait type's spread over a collection, for the backfill's
/// facet-cardinality warning: a *faceted* trait type whose distinct-value
/// count approaches the collection's asset count is per-asset-unique (Girl
/// Gang's `Name`), which bloats `/facets` and pushes the real values out of
/// the planner's MCV list. The fix is one `facet_exclude` entry in
/// `config/collections.toml` plus `seed` — never code.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct TraitCardinality {
    pub name: String,
    pub is_facet: bool,
    /// Distinct values of this trait type.
    pub values: i64,
    /// Assets carrying this trait type.
    pub assets: i64,
}

/// Trait types of one collection, widest first.
pub async fn trait_cardinality<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
) -> sqlx::Result<Vec<TraitCardinality>> {
    sqlx::query_as(
        "SELECT tt.name, tt.is_facet, \
                count(DISTINCT tv.id)::bigint AS values, \
                count(DISTINCT aa.asset_id)::bigint AS assets \
           FROM trait_types tt \
           JOIN trait_values tv ON tv.trait_type_id = tt.id \
           JOIN asset_attributes aa ON aa.trait_value_id = tv.id \
          WHERE tt.collection_id = $1 \
          GROUP BY tt.id, tt.name, tt.is_facet \
          ORDER BY count(DISTINCT tv.id) DESC, tt.name",
    )
    .bind(collection_id)
    .fetch_all(exec)
    .await
}
