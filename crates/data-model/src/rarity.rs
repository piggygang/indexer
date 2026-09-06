//! Statistical rarity: score, rank, and the bookkeeping that keeps them fresh
//! (ALG-627).
//!
//! The formula is stated once, in
//! `migrations/20260906000800_rarity.sql`, and implemented once, in
//! [`RECOMPUTE`]. Everything else here is scheduling and proof.
//!
//! Two properties the rest of the system leans on:
//!
//! * **The population is the browse population, verbatim** — `collection_id AND
//!   membership_status = 'member'`, burned included. Any other choice would let
//!   the grid, the facet counts and the rank disagree about what exists.
//! * **A pass is a true no-op when nothing changed.** The `IS DISTINCT FROM`
//!   guard is what makes `rarity --expect-unchanged` a proof rather than a
//!   hope, and it is only meaningful because the sum is exact: `numeric`
//!   addition is associative, `float8` addition is not, and the same query
//!   really does return three different answers under three planner settings.

use serde::Serialize;
use sqlx::{PgExecutor, PgPool};

/// The advisory-lock class for a rarity pass. Transaction-scoped, so it cannot
/// leak across a pooled connection the way a session lock would.
const LOCK_CLASS: i32 = 627;

/// What one [`recompute`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Outcome {
    pub collection_id: i32,
    /// Assets that came out of the ranking. 0 for a collection with no
    /// facetable trait types — which is a fact about the metadata, not a
    /// failure.
    pub ranked: i64,
    /// Rows whose score or rank actually moved. 0 is what
    /// `--expect-unchanged` demands.
    pub changed: i64,
    /// The collection's fence after this pass. Bumped only when `changed > 0`.
    pub version: i32,
    /// Another pass held the lock; this one did nothing and the flag stands.
    pub skipped: bool,
}

impl Outcome {
    pub fn is_noop(&self) -> bool {
        self.changed == 0
    }
}

/// Scores and ranks one collection, and reports how much moved.
///
/// Absence is modelled by the `LEFT JOIN` leaving `trait_value_id` NULL and by
/// `IS NOT DISTINCT FROM` matching that NULL to its own frequency bucket — so
/// "carries no Earring" is a value with a count like any other, which is the
/// whole formula in one join condition.
///
/// An asset that falls out of the ranking (its collection lost every facetable
/// trait type, or the asset was removed from the collection) is cleared in the
/// same statement, so a stale rank can never outlive the population it was
/// computed over.
const RECOMPUTE: &str = "\
WITH pop AS ( \
    SELECT a.id FROM assets a \
     WHERE a.collection_id = $1 AND a.membership_status = 'member' \
), n AS (SELECT count(*)::numeric AS n FROM pop), \
types AS ( \
    SELECT tt.id FROM trait_types tt WHERE tt.collection_id = $1 AND tt.is_facet \
), cell AS ( \
    SELECT p.id AS asset_id, t.id AS trait_type_id, aa.trait_value_id \
      FROM pop p CROSS JOIN types t \
      LEFT JOIN asset_attributes aa \
             ON aa.asset_id = p.id AND aa.trait_type_id = t.id \
), freq AS ( \
    SELECT trait_type_id, trait_value_id, count(*)::numeric AS c \
      FROM cell GROUP BY trait_type_id, trait_value_id \
), scored AS ( \
    SELECT c.asset_id, round(sum(round((SELECT n FROM n) / f.c, 12)), 6) AS score \
      FROM cell c JOIN freq f \
        ON f.trait_type_id = c.trait_type_id \
       AND f.trait_value_id IS NOT DISTINCT FROM c.trait_value_id \
     GROUP BY c.asset_id \
), ranked AS ( \
    SELECT asset_id, score, \
           row_number() OVER (ORDER BY score DESC, asset_id)::int AS rank \
      FROM scored \
), cleared AS ( \
    UPDATE assets a SET rarity_score = NULL, rarity_rank = NULL \
     WHERE a.collection_id = $1 AND a.rarity_rank IS NOT NULL \
       AND NOT EXISTS (SELECT 1 FROM ranked r WHERE r.asset_id = a.id) \
    RETURNING 1 \
), written AS ( \
    UPDATE assets a \
       SET rarity_score = r.score::double precision, rarity_rank = r.rank \
      FROM ranked r \
     WHERE a.id = r.asset_id \
       AND (a.rarity_score, a.rarity_rank) \
           IS DISTINCT FROM (r.score::double precision, r.rank) \
    RETURNING 1 \
) \
SELECT (SELECT count(*) FROM ranked)::bigint, \
       ((SELECT count(*) FROM cleared) + (SELECT count(*) FROM written))::bigint";

/// Recomputes one collection's scores and ranks.
///
/// Takes a transaction-scoped advisory lock and **skips** rather than blocks
/// when another pass holds it: two ingester replicas overlapping during a
/// rolling deploy should cost one cheap round trip, not two full recomputes
/// serialized behind each other.
pub async fn recompute(pool: &PgPool, collection_id: i32) -> sqlx::Result<Outcome> {
    let mut tx = pool.begin().await?;
    let outcome = recompute_in(&mut tx, collection_id).await?;
    tx.commit().await?;
    Ok(outcome)
}

/// Recomputes and reports without writing — `rarity --dry-run`.
///
/// The same statement against the same snapshot, rolled back; the counts are
/// exactly what a real pass would have written.
pub async fn preview(pool: &PgPool, collection_id: i32) -> sqlx::Result<Outcome> {
    let mut tx = pool.begin().await?;
    let outcome = recompute_in(&mut tx, collection_id).await?;
    tx.rollback().await?;
    Ok(outcome)
}

/// The pass itself, inside a caller's transaction.
pub async fn recompute_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    collection_id: i32,
) -> sqlx::Result<Outcome> {
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1, $2)")
        .bind(LOCK_CLASS)
        .bind(collection_id)
        .fetch_one(&mut **tx)
        .await?;
    if !acquired {
        return Ok(Outcome {
            collection_id,
            skipped: true,
            ..Outcome::default()
        });
    }

    let (ranked, changed): (i64, i64) = sqlx::query_as(RECOMPUTE)
        .bind(collection_id)
        // Planned with the real parameter values: the trait-type count and the
        // population size decide between a hash aggregate and a sort, and a
        // generic plan treats both as unknowns.
        .persistent(false)
        .fetch_one(&mut **tx)
        .await?;

    // The flag always clears — the pass ran. The version moves only when rows
    // did, so a cursor is fenced by a real change and never by a heartbeat.
    let version: i32 = sqlx::query_scalar(
        "UPDATE collections \
            SET rarity_dirty = false, \
                rarity_version = rarity_version + CASE WHEN $2 > 0 THEN 1 ELSE 0 END \
          WHERE id = $1 RETURNING rarity_version",
    )
    .bind(collection_id)
    .bind(changed)
    .fetch_one(&mut **tx)
    .await?;

    Ok(Outcome {
        collection_id,
        ranked,
        changed,
        version,
        skipped: false,
    })
}

/// Flags a collection's ranks as stale.
///
/// Called from the SQL layer rather than from each pipeline, so it is
/// transactional with the write that invalidated the ranks and no future
/// caller can forget it: [`crate::assets::upsert_batch`] covers the live
/// pipeline, the reconcile sweep and the DAS backfill at once,
/// [`crate::assets::set_membership`] covers a Core asset leaving or rejoining,
/// and [`crate::attributes::sync_trait_facets`] covers a `facet_exclude` edit,
/// which re-ranks a collection with no asset write at all.
pub async fn mark_dirty<'e>(exec: impl PgExecutor<'e>, collection_id: i32) -> sqlx::Result<()> {
    sqlx::query("UPDATE collections SET rarity_dirty = true WHERE id = $1 AND NOT rarity_dirty")
        .bind(collection_id)
        .execute(exec)
        .await?;
    Ok(())
}

/// Clears the flag without recomputing.
///
/// For a collection with nothing to rank: the drain has looked, there is
/// nothing to do, and leaving the flag set would make it look at the same
/// collection every minute forever.
pub async fn clear_dirty<'e>(exec: impl PgExecutor<'e>, collection_id: i32) -> sqlx::Result<()> {
    sqlx::query("UPDATE collections SET rarity_dirty = false WHERE id = $1 AND rarity_dirty")
        .bind(collection_id)
        .execute(exec)
        .await?;
    Ok(())
}

/// Enabled collections whose ranks are stale, in registry order.
pub async fn dirty_collections<'e>(exec: impl PgExecutor<'e>) -> sqlx::Result<Vec<i32>> {
    sqlx::query_scalar("SELECT id FROM collections WHERE enabled AND rarity_dirty ORDER BY id")
        .fetch_all(exec)
        .await
}

/// A collection's cursor fence. Rarity cursors carry it; every other sort is
/// unaffected.
pub async fn version<'e>(exec: impl PgExecutor<'e>, collection_id: i32) -> sqlx::Result<i32> {
    sqlx::query_scalar("SELECT rarity_version FROM collections WHERE id = $1")
        .bind(collection_id)
        .fetch_one(exec)
        .await
}

/// Does the collection have anything to rank?
///
/// False for Pig Mud, whose only trait is a per-asset-unique `Name` the
/// registry excludes from facets, because its metadata host is gone. Ranking it
/// would mean a 2073-way tie; NULL is the honest answer, and the contract
/// permits it.
pub async fn is_rankable<'e>(exec: impl PgExecutor<'e>, collection_id: i32) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM trait_types WHERE collection_id = $1 AND is_facet)",
    )
    .bind(collection_id)
    .fetch_one(exec)
    .await
}

/// One term of one asset's score — what `rarity --explain` prints.
/// `Eq` is deliberately absent: `term` is an `f64`, and this is a report, not
/// a key.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Term {
    pub trait_type: String,
    /// `None` is the absent bucket, which is a value like any other.
    pub value: Option<String>,
    /// Members of the population sharing this cell.
    pub carriers: i64,
    /// `N / carriers`, to 12 dp — this term's contribution.
    pub term: f64,
}

/// The per-term breakdown behind one asset's score.
///
/// The auditable form of the formula: an operator can add the column up by
/// hand and get the stored score back.
pub async fn explain(pool: &PgPool, address: &str) -> sqlx::Result<Vec<Term>> {
    sqlx::query_as::<_, Term>(
        "WITH target AS (SELECT id, collection_id FROM assets WHERE address = $1), \
         pop AS ( \
            SELECT a.id FROM assets a JOIN target t ON t.collection_id = a.collection_id \
             WHERE a.membership_status = 'member' \
         ), n AS (SELECT count(*)::numeric AS n FROM pop), \
         types AS ( \
            SELECT tt.id, tt.name FROM trait_types tt JOIN target t ON t.collection_id = tt.collection_id \
             WHERE tt.is_facet \
         ), cell AS ( \
            SELECT p.id AS asset_id, ty.id AS trait_type_id, aa.trait_value_id \
              FROM pop p CROSS JOIN types ty \
              LEFT JOIN asset_attributes aa \
                     ON aa.asset_id = p.id AND aa.trait_type_id = ty.id \
         ), freq AS ( \
            SELECT trait_type_id, trait_value_id, count(*)::numeric AS c \
              FROM cell GROUP BY trait_type_id, trait_value_id \
         ) \
         SELECT ty.name AS trait_type, tv.value, f.c::bigint AS carriers, \
                round((SELECT n FROM n) / f.c, 12)::double precision AS term \
           FROM cell c \
           JOIN target t ON t.id = c.asset_id \
           JOIN types ty ON ty.id = c.trait_type_id \
           JOIN freq f ON f.trait_type_id = c.trait_type_id \
                      AND f.trait_value_id IS NOT DISTINCT FROM c.trait_value_id \
           LEFT JOIN trait_values tv ON tv.id = c.trait_value_id \
          ORDER BY term DESC, ty.name",
    )
    .bind(address)
    .persistent(false)
    .fetch_all(pool)
    .await
}

/// The stored score and rank of every member, for the independent
/// recomputation to compare against.
pub async fn stored(
    pool: &PgPool,
    collection_id: i32,
) -> sqlx::Result<Vec<(i64, Option<f64>, Option<i32>)>> {
    sqlx::query_as(
        "SELECT id, rarity_score, rarity_rank FROM assets \
          WHERE collection_id = $1 AND membership_status = 'member' ORDER BY id",
    )
    .bind(collection_id)
    .fetch_all(pool)
    .await
}

/// Every `(asset, facetable trait type, value)` cell of the population, with
/// absence spelled out as a `None` value.
///
/// The input to the independent recomputation. Deliberately a different query
/// shape from [`RECOMPUTE`] — it joins nothing and aggregates nothing, so
/// agreement between the two is evidence rather than tautology.
pub async fn cells(
    pool: &PgPool,
    collection_id: i32,
) -> sqlx::Result<Vec<(i64, i32, Option<i32>)>> {
    sqlx::query_as(
        "SELECT p.id, t.id, aa.trait_value_id \
           FROM assets p \
           CROSS JOIN trait_types t \
           LEFT JOIN asset_attributes aa ON aa.asset_id = p.id AND aa.trait_type_id = t.id \
          WHERE p.collection_id = $1 AND p.membership_status = 'member' \
            AND t.collection_id = $1 AND t.is_facet \
          ORDER BY p.id, t.id",
    )
    .bind(collection_id)
    .persistent(false)
    .fetch_all(pool)
    .await
}

/// The result of recomputing a collection's ranks a second way and comparing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Verification {
    pub collection_id: i32,
    pub members: usize,
    pub ranked: usize,
    pub score_mismatches: usize,
    pub rank_mismatches: usize,
    /// The first disagreement, spelled out, so a failure names a row.
    pub first: Option<String>,
}

impl Verification {
    /// The acceptance criterion — "ranks match an independent recomputation" —
    /// in one place.
    pub fn is_clean(&self) -> bool {
        self.score_mismatches == 0 && self.rank_mismatches == 0
    }
}

/// Scale of the per-term intermediate, matching `round(…, 12)` in [`RECOMPUTE`].
const TERM_SCALE: i128 = 1_000_000_000_000;
/// Scale of the stored score, matching `round(…, 6)`.
const SCORE_SCALE: i128 = 1_000_000;

/// `round(a / b)` for positive integers, half away from zero — what Postgres's
/// `round(numeric, n)` does.
fn round_div(a: i128, b: i128) -> i128 {
    (2 * a + b) / (2 * b)
}

/// Recomputes a collection's scores and ranks independently and compares them
/// with what is stored.
///
/// Independent in the two ways that matter. The **inputs** come from
/// [`cells`], which joins nothing and aggregates nothing, rather than from the
/// aggregating CTEs [`RECOMPUTE`] uses. The **arithmetic** is exact `i128`
/// fixed point rather than Postgres `numeric`, so agreement is not two copies
/// of one rounding bug agreeing with each other — and because both are exact,
/// the comparison needs no epsilon, which is what keeps a near-tie testable.
pub async fn verify(pool: &PgPool, collection_id: i32) -> sqlx::Result<Verification> {
    use std::collections::HashMap;

    let stored = stored(pool, collection_id).await?;
    let cells = cells(pool, collection_id).await?;
    let n = stored.len() as i128;

    let mut frequency: HashMap<(i32, Option<i32>), i128> = HashMap::new();
    for (_, trait_type_id, trait_value_id) in &cells {
        *frequency
            .entry((*trait_type_id, *trait_value_id))
            .or_default() += 1;
    }

    let mut totals: HashMap<i64, i128> = HashMap::new();
    for (asset_id, trait_type_id, trait_value_id) in &cells {
        let carriers = frequency[&(*trait_type_id, *trait_value_id)];
        *totals.entry(*asset_id).or_default() += round_div(n * TERM_SCALE, carriers);
    }

    // (score in units of 1e-6, asset id) — the ORDER BY of the ranking window,
    // reproduced.
    let mut scored: Vec<(i128, i64)> = totals
        .iter()
        .map(|(id, total)| (round_div(*total, TERM_SCALE / SCORE_SCALE), *id))
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let expected: HashMap<i64, (i128, i32)> = scored
        .iter()
        .enumerate()
        .map(|(i, (score, id))| (*id, (*score, i as i32 + 1)))
        .collect();

    let mut result = Verification {
        collection_id,
        members: stored.len(),
        ranked: expected.len(),
        ..Verification::default()
    };
    for (asset_id, score, rank) in &stored {
        // A stored 6-dp score round-trips through f64 exactly at this
        // magnitude, so the comparison stays on integers.
        let stored_score = score.map(|s| (s * SCORE_SCALE as f64).round() as i128);
        let (want_score, want_rank) = match expected.get(asset_id) {
            Some((s, r)) => (Some(*s), Some(*r)),
            None => (None, None),
        };
        if stored_score != want_score {
            result.score_mismatches += 1;
            result.first.get_or_insert_with(|| {
                format!("asset {asset_id}: score {stored_score:?}, recomputed {want_score:?}")
            });
        }
        if *rank != want_rank {
            result.rank_mismatches += 1;
            result.first.get_or_insert_with(|| {
                format!("asset {asset_id}: rank {rank:?}, recomputed {want_rank:?}")
            });
        }
    }
    Ok(result)
}
