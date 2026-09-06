//! Statistical rarity, proven rather than asserted (ALG-627).
//!
//! Four layers, deliberately redundant, because "the ranks are right" is the
//! whole issue:
//!
//! 1. a **hand-computed fixture** whose scores are literals with the arithmetic
//!    written out — the only test that catches someone changing the formula and
//!    updating the oracle to match;
//! 2. an **independent recomputation** in exact integer arithmetic
//!    ([`rarity::verify`]), from different queries, so agreement is evidence
//!    and not two copies of one rounding bug agreeing with each other;
//! 3. **structural invariants** — `1..N` with no gap, no duplicate, and no pair
//!    out of order;
//! 4. the **triggers**: a mint, a membership move and a `facet_exclude` edit
//!    each flag the collection, and two concurrent passes do not collide.
//!
//! Every address is a synthetic base58 key (CLAUDE.md).
//! Ignored without a database: `cargo test --workspace -- --include-ignored`.

use indexer_data_model::assets::{self, AssetInput, TraitInput};
use indexer_data_model::synth::{self, SyntheticSpec};
use indexer_data_model::{attributes, integrity, rarity, PgPool};

fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

async fn collection(pool: &PgPool, slug: &str, seed: u8, facet_exclude: &[&str]) -> i32 {
    sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, enabled, \
                                  facet_exclude) \
         VALUES ($1, 'Synthetic', 'token_metadata', $2, 'SYN', true, $3) RETURNING id",
    )
    .bind(slug)
    .bind(pk(seed))
    .bind(
        facet_exclude
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

/// One fixture asset: `(address seed, name, [(trait type, value)])`.
type Fixture<'a> = (u8, &'a str, Vec<(&'a str, &'a str)>);

/// Writes assets through the real writer, so the dirty flag is set the way
/// production sets it.
async fn write(pool: &PgPool, collection_id: i32, assets_in: &[Fixture<'_>]) {
    let inputs: Vec<AssetInput> = assets_in
        .iter()
        .map(|(seed, name, traits)| AssetInput {
            address: pk(*seed),
            name: (*name).to_string(),
            symbol: None,
            metadata_uri: None,
            metadata_source_uri: None,
            image_uri: None,
            burned: false,
            owner: None,
            attributes: Some(
                traits
                    .iter()
                    .enumerate()
                    .map(|(i, (t, v))| TraitInput {
                        trait_type: (*t).to_string(),
                        value: (*v).to_string(),
                        position: i as i16,
                    })
                    .collect(),
            ),
            document: None,
        })
        .collect();
    let mut tx = pool.begin().await.unwrap();
    assets::upsert_batch(&mut tx, collection_id, 100, &inputs)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    attributes::sync_trait_facets(pool, collection_id)
        .await
        .unwrap();
}

async fn ranks(pool: &PgPool, collection_id: i32) -> Vec<(String, Option<f64>, Option<i32>)> {
    sqlx::query_as(
        "SELECT name, rarity_score, rarity_rank FROM assets \
          WHERE collection_id = $1 AND membership_status = 'member' \
          ORDER BY coalesce(rarity_rank, 2147483647), id",
    )
    .bind(collection_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn is_dirty(pool: &PgPool, collection_id: i32) -> bool {
    sqlx::query_scalar("SELECT rarity_dirty FROM collections WHERE id = $1")
        .bind(collection_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "needs DATABASE_URL"]
async fn the_score_is_the_sum_of_inverse_trait_frequencies(pool: PgPool) {
    // Four assets, two facetable trait types, N = 4.
    //
    //        Background   Head
    //   #1   Pink (1/4)   Crown (1/4)
    //   #2   Blue (3/4)   Cap   (2/4)
    //   #3   Blue (3/4)   Cap   (2/4)
    //   #4   Blue (3/4)   ABSENT (1/4)   <- the absent bucket is a value
    //
    // score(#1) = 4/1 + 4/1               = 8.0
    // score(#4) = 4/3 + 4/1 = 1.333333 + 4 = 5.333333
    // score(#2) = score(#3) = 4/3 + 4/2   = 1.333333 + 2 = 3.333333  (a tie)
    let cid = collection(&pool, "syn-formula", 1, &[]).await;
    write(
        &pool,
        cid,
        &[
            (10, "#1", vec![("Background", "Pink"), ("Head", "Crown")]),
            (11, "#2", vec![("Background", "Blue"), ("Head", "Cap")]),
            (12, "#3", vec![("Background", "Blue"), ("Head", "Cap")]),
            (13, "#4", vec![("Background", "Blue")]),
        ],
    )
    .await;

    let outcome = rarity::recompute(&pool, cid).await.unwrap();
    assert_eq!(outcome.ranked, 4);
    assert_eq!(outcome.changed, 4);
    assert_eq!(outcome.version, 1);

    let rows = ranks(&pool, cid).await;
    let scores: Vec<(String, f64, i32)> = rows
        .iter()
        .map(|(n, s, r)| (n.clone(), s.unwrap(), r.unwrap()))
        .collect();
    assert_eq!(scores[0], ("#1".into(), 8.0, 1));
    assert_eq!(scores[1], ("#4".into(), 5.333333, 2));
    // The tie is broken by id, which is insertion order — deterministic, and
    // the reason `rank` is a `row_number` rather than a `rank`.
    assert_eq!(scores[2], ("#2".into(), 3.333333, 3));
    assert_eq!(scores[3], ("#3".into(), 3.333333, 4));

    // An asset that carries no Earring is NOT rewarded for the empty slot:
    // #4 outranks #2 only because "Crown-less and Blue" is rarer than "Cap",
    // and it scores 4/1 for the absent Head because it is the only one.
    assert!(scores[1].1 > scores[2].1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "needs DATABASE_URL"]
async fn ranks_match_an_independent_recomputation(pool: PgPool) {
    // Sparse on purpose: `coverage` below 1.0 leaves later trait types absent
    // on some assets, which is the bucket a full-coverage fixture never
    // exercises — and it is 65% of one trait type on the real Core collection.
    let report = synth::seed_synthetic(
        &pool,
        &SyntheticSpec {
            slug: "bench-rarity".into(),
            name: "Bench".into(),
            assets: 900,
            unique_trait: true,
            coverage: 0.4,
            seed: 0.21,
        },
    )
    .await
    .unwrap();
    let cid = report.collection_id;
    sqlx::query("UPDATE collections SET enabled = true, address = $2 WHERE id = $1")
        .bind(cid)
        .bind(pk(7))
        .execute(&pool)
        .await
        .unwrap();

    let outcome = rarity::recompute(&pool, cid).await.unwrap();
    assert_eq!(outcome.ranked, 900);

    // The acceptance criterion: a second implementation, over different
    // queries and in exact integer arithmetic, agrees exactly.
    let check = rarity::verify(&pool, cid).await.unwrap();
    assert!(check.is_clean(), "{check:?}");
    assert_eq!(check.members, 900);
    assert_eq!(check.ranked, 900);

    // Structural invariants: 1..N with no gap and no duplicate.
    let (count, distinct, max): (i64, i64, i32) = sqlx::query_as(
        "SELECT count(*)::bigint, count(DISTINCT rarity_rank)::bigint, max(rarity_rank) \
           FROM assets WHERE collection_id = $1 AND membership_status = 'member'",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((count, distinct, max as i64), (900, 900, 900));

    // No pair is out of order: a better rank never has a lower score.
    let inversions: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM assets a JOIN assets b \
            ON a.collection_id = b.collection_id AND a.rarity_rank < b.rarity_rank \
         WHERE a.collection_id = $1 AND a.rarity_score < b.rarity_score",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(inversions, 0);

    // The fixture must actually contain the absent bucket, or this test is
    // quietly weaker than it looks.
    let sparse: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM ( \
            SELECT a.id FROM assets a WHERE a.collection_id = $1 \
             GROUP BY a.id \
            HAVING (SELECT count(*) FROM asset_attributes aa \
                     JOIN trait_types tt ON tt.id = aa.trait_type_id AND tt.is_facet \
                    WHERE aa.asset_id = a.id) \
                 < (SELECT count(*) FROM trait_types WHERE collection_id = $1 AND is_facet)) x",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(sparse > 0, "the fixture must have assets missing a trait");

    // Re-running writes nothing — what `rarity --expect-unchanged` proves.
    let again = rarity::recompute(&pool, cid).await.unwrap();
    assert!(again.is_noop(), "{again:?}");
    assert_eq!(
        again.version, outcome.version,
        "a no-op must not move the fence"
    );
    assert!(rarity::verify(&pool, cid).await.unwrap().is_clean());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_new_mint_flags_the_collection_and_moves_the_ranks(pool: PgPool) {
    let cid = collection(&pool, "syn-mint", 1, &[]).await;
    write(
        &pool,
        cid,
        &[
            (10, "#1", vec![("Background", "Pink"), ("Head", "Crown")]),
            (11, "#2", vec![("Background", "Blue"), ("Head", "Cap")]),
            (12, "#3", vec![("Background", "Blue"), ("Head", "Cap")]),
        ],
    )
    .await;
    let first = rarity::recompute(&pool, cid).await.unwrap();
    assert!(!is_dirty(&pool, cid).await, "a pass clears the flag");
    let before = ranks(&pool, cid).await;

    // A mint through the real writer — the path a Core mint takes.
    write(
        &pool,
        cid,
        &[(13, "#4", vec![("Background", "Blue"), ("Head", "Crown")])],
    )
    .await;
    assert!(
        is_dirty(&pool, cid).await,
        "the writer must flag the collection: N changed, so every frequency did"
    );

    let after_mint = rarity::recompute(&pool, cid).await.unwrap();
    assert_eq!(after_mint.ranked, 4);
    // Three rows move, not four — and the one that does not is the point.
    //
    //   before (N=3):  #1 = 3/1 + 3/1 = 6
    //   after  (N=4):  #1 = 4/1 + 4/2 = 6   <- Pink stayed unique and Crown
    //                                          doubled; the two changes cancel
    //   #2/#3:  3/2 + 3/2 = 3  ->  4/3 + 4/2 = 3.333333
    //   #4 is new.
    //
    // So the writer's `IS DISTINCT FROM` guard skips #1: an unchanged row is a
    // true no-op even inside a pass that rewrote everything else.
    assert_eq!(
        after_mint.changed, 3,
        "only the rows whose score actually moved are written"
    );
    let unchanged: f64 = sqlx::query_scalar(
        "SELECT rarity_score FROM assets WHERE collection_id = $1 AND name = '#1'",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(unchanged, 6.0);
    assert_eq!(
        after_mint.version,
        first.version + 1,
        "a pass that changed rows moves the cursor fence"
    );

    let after = ranks(&pool, cid).await;
    assert_ne!(
        before.iter().map(|r| r.1).collect::<Vec<_>>(),
        after.iter().take(3).map(|r| r.1).collect::<Vec<_>>(),
        "the scores of the pre-existing assets must move"
    );
    assert!(rarity::verify(&pool, cid).await.unwrap().is_clean());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "needs DATABASE_URL"]
async fn membership_and_facet_changes_flag_the_collection_too(pool: PgPool) {
    // The two staleness sources the issue's list misses. Neither writes an
    // asset attribute, so neither would be noticed without an explicit flag.
    let cid = collection(&pool, "syn-flags", 1, &[]).await;
    write(
        &pool,
        cid,
        &[
            (10, "#1", vec![("Background", "Pink"), ("Name", "#1")]),
            (11, "#2", vec![("Background", "Blue"), ("Name", "#2")]),
        ],
    )
    .await;
    rarity::recompute(&pool, cid).await.unwrap();
    assert!(!is_dirty(&pool, cid).await);

    // A Core asset leaving shrinks the population every frequency is over.
    let mut tx = pool.begin().await.unwrap();
    let moved = assets::set_membership_and_flag(&mut tx, cid, &[pk(11)], true)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(moved, 1);
    assert!(is_dirty(&pool, cid).await, "membership out must flag");

    let outcome = rarity::recompute(&pool, cid).await.unwrap();
    assert_eq!(outcome.ranked, 1, "a removed asset leaves the population");
    let removed_rank: Option<i32> =
        sqlx::query_scalar("SELECT rarity_rank FROM assets WHERE address = $1")
            .bind(pk(11))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        removed_rank.is_none(),
        "a stale rank must not outlive the population it was computed over"
    );

    // …and coming back flags it again, through the return value reconcile
    // otherwise discards.
    let mut tx = pool.begin().await.unwrap();
    assets::set_membership_and_flag(&mut tx, cid, &[pk(11)], false)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(is_dirty(&pool, cid).await, "membership back must flag");
    rarity::recompute(&pool, cid).await.unwrap();

    // A facet_exclude edit re-ranks with no asset write at all.
    sqlx::query("UPDATE collections SET facet_exclude = ARRAY['Name'] WHERE id = $1")
        .bind(cid)
        .execute(&pool)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    let changed = attributes::sync_trait_facets_and_flag(&mut tx, cid)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(changed, 1);
    assert!(is_dirty(&pool, cid).await, "a facet_exclude edit must flag");
    let outcome = rarity::recompute(&pool, cid).await.unwrap();
    assert!(
        outcome.changed > 0,
        "dropping a trait type from the score changes the score"
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_collection_with_no_facetable_traits_stays_null(pool: PgPool) {
    // Pig Mud's shape: the only trait is a per-asset-unique `Name`, excluded
    // from facets, because its metadata host is gone. Ranking it would mean an
    // N-way tie; null is the honest answer and the contract permits it.
    let cid = collection(&pool, "syn-nameonly", 1, &["Name"]).await;
    write(
        &pool,
        cid,
        &[
            (10, "#1", vec![("Name", "#1")]),
            (11, "#2", vec![("Name", "#2")]),
        ],
    )
    .await;

    assert!(!rarity::is_rankable(&pool, cid).await.unwrap());
    let outcome = rarity::recompute(&pool, cid).await.unwrap();
    assert_eq!(outcome.ranked, 0);
    assert_eq!(outcome.changed, 0);
    assert!(ranks(&pool, cid)
        .await
        .iter()
        .all(|(_, score, rank)| score.is_none() && rank.is_none()));

    // …and the integrity view does not flag it, because there is nothing wrong.
    assert_eq!(integrity::snapshot(&pool).await.unwrap().rarity_broken, 0);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "needs DATABASE_URL"]
async fn integrity_flags_a_collection_that_owes_a_pass(pool: PgPool) {
    let cid = collection(&pool, "syn-integrity", 1, &[]).await;
    write(
        &pool,
        cid,
        &[
            (10, "#1", vec![("Background", "Pink")]),
            (11, "#2", vec![("Background", "Blue")]),
        ],
    )
    .await;
    // Ranked nowhere yet: the drift counter is what makes that visible in
    // production, where a null rank still serializes and still sorts.
    assert_eq!(integrity::snapshot(&pool).await.unwrap().rarity_broken, 1);

    rarity::recompute(&pool, cid).await.unwrap();
    assert_eq!(integrity::snapshot(&pool).await.unwrap().rarity_broken, 0);

    // A hand-corrupted rank is caught too.
    sqlx::query("UPDATE assets SET rarity_rank = 1 WHERE collection_id = $1")
        .bind(cid)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(integrity::snapshot(&pool).await.unwrap().rarity_broken, 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_concurrent_pass_skips_instead_of_blocking(pool: PgPool) {
    let cid = collection(&pool, "syn-lock", 1, &[]).await;
    write(
        &pool,
        cid,
        &[
            (10, "#1", vec![("Background", "Pink")]),
            (11, "#2", vec![("Background", "Blue")]),
        ],
    )
    .await;

    // Hold the collection's lock in one transaction, the way a second ingester
    // replica would during a rolling deploy.
    let mut holder = pool.begin().await.unwrap();
    let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(627, $1)")
        .bind(cid)
        .fetch_one(&mut *holder)
        .await
        .unwrap();
    assert!(taken);

    let outcome = rarity::recompute(&pool, cid).await.unwrap();
    assert!(outcome.skipped, "the loser skips rather than blocking");
    assert_eq!(outcome.changed, 0);
    assert!(
        is_dirty(&pool, cid).await,
        "a skipped pass leaves the flag standing, so the winner covers it"
    );

    holder.rollback().await.unwrap();
    let outcome = rarity::recompute(&pool, cid).await.unwrap();
    assert!(!outcome.skipped);
    assert_eq!(outcome.ranked, 2);
}
