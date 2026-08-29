//! Schema invariants, each a fresh database with the migrations applied
//! (`#[sqlx::test]` is also the migration smoke test). Ignored without a
//! database: run with `cargo test --workspace -- --include-ignored`.

use indexer_data_model::types::{MembershipRule, Standard};
use indexer_data_model::{ingest_state, registry, PgPool};

const CHECK_VIOLATION: &str = "23514";
const UNIQUE_VIOLATION: &str = "23505";
const EXCLUSION_VIOLATION: &str = "23P01";

fn code(err: sqlx::Error) -> String {
    err.as_database_error()
        .and_then(|e| e.code())
        .map(|c| c.to_string())
        .unwrap_or_else(|| format!("not a database error: {err}"))
}

/// Deterministic, obviously synthetic 32-byte keys (`seed` >= 1 so the
/// base58 form is 43–44 chars, never a run of leading '1's).
fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

fn sig(seed: u8) -> String {
    bs58::encode([seed; 64]).into_string()
}

async fn insert_collection(
    pool: &PgPool,
    slug: &str,
    standard: Option<&str>,
    address: Option<&str>,
    creator: Option<&str>,
    enabled: bool,
) -> sqlx::Result<i32> {
    sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, address, verified_creator, enabled) \
         VALUES ($1, $1, $2, $3, $4, $5) RETURNING id",
    )
    .bind(slug)
    .bind(standard)
    .bind(address)
    .bind(creator)
    .bind(enabled)
    .fetch_one(pool)
    .await
}

async fn insert_asset(pool: &PgPool, collection_id: i32, address: &str, name: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO assets (address, collection_id, name) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(address)
    .bind(collection_id)
    .bind(name)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn migrations_rerun_is_noop(pool: PgPool) {
    indexer_data_model::migrate(&pool).await.unwrap();
    indexer_data_model::migrate(&pool).await.unwrap();
    let applied: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE success")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(applied, 5);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn registry_checks(pool: PgPool) {
    // Enabled rows must resolve to a membership rule.
    let err = insert_collection(
        &pool,
        "tm-nothing",
        Some("token_metadata"),
        None,
        None,
        true,
    )
    .await
    .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);
    let err = insert_collection(&pool, "core-noaddr", Some("core"), None, None, true)
        .await
        .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);
    let err = insert_collection(&pool, "no-standard", None, None, None, true)
        .await
        .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);
    // Creator / symbol are Token Metadata concepts.
    let err = insert_collection(
        &pool,
        "core-creator",
        Some("core"),
        Some(&pk(1)),
        Some(&pk(2)),
        true,
    )
    .await
    .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);
    // Addresses must look like pubkeys; slugs must be slugs.
    let err = insert_collection(
        &pool,
        "bad-addr",
        Some("core"),
        Some("not-base58-0OIl"),
        None,
        true,
    )
    .await
    .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);
    let err = insert_collection(&pool, "Bad Slug", Some("core"), Some(&pk(1)), None, true)
        .await
        .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);

    // Accepted shapes and their derived rule.
    let placeholder = insert_collection(&pool, "later", None, None, None, false)
        .await
        .unwrap();
    let core = insert_collection(&pool, "core", Some("core"), Some(&pk(1)), None, true)
        .await
        .unwrap();
    let certified = insert_collection(
        &pool,
        "tm-cert",
        Some("token_metadata"),
        Some(&pk(2)),
        None,
        true,
    )
    .await
    .unwrap();
    let allowlist = insert_collection(
        &pool,
        "tm-list",
        Some("token_metadata"),
        None,
        Some(&pk(3)),
        true,
    )
    .await
    .unwrap();
    // Certified collection wins over a creator when both are present.
    let both = insert_collection(
        &pool,
        "tm-both",
        Some("token_metadata"),
        Some(&pk(4)),
        Some(&pk(3)),
        true,
    )
    .await
    .unwrap();

    let rows = registry::list(&pool, false).await.unwrap();
    let rule = |id: i32| rows.iter().find(|r| r.id == id).unwrap().membership_rule;
    assert_eq!(rule(placeholder), None);
    assert_eq!(rule(core), Some(MembershipRule::CoreCollection));
    assert_eq!(rule(certified), Some(MembershipRule::TmCollection));
    assert_eq!(rule(allowlist), Some(MembershipRule::TmAllowlist));
    assert_eq!(rule(both), Some(MembershipRule::TmCollection));

    // Text-backed enums round-trip; enabled filter works.
    let core_row = rows.iter().find(|r| r.id == core).unwrap();
    assert_eq!(core_row.standard, Some(Standard::Core));
    assert_eq!(core_row.address.as_deref(), Some(pk(1).as_str()));
    let placeholder_row = rows.iter().find(|r| r.id == placeholder).unwrap();
    assert_eq!(placeholder_row.standard, None);
    let enabled = registry::list_enabled(&pool).await.unwrap();
    assert_eq!(enabled.len(), 4);
    assert!(enabled
        .iter()
        .all(|r| r.enabled && r.membership_rule.is_some()));
    assert_eq!(
        registry::by_slug(&pool, "tm-list")
            .await
            .unwrap()
            .unwrap()
            .id,
        allowlist
    );

    // A NULL standard must not slip through either CHECK.
    let err = sqlx::query(
        "INSERT INTO collections (slug, name, address, enabled) VALUES ('leak-a', 'leak', $1, true)",
    )
    .bind(pk(6))
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION, "enabled without a standard");
    let err = sqlx::query(
        "INSERT INTO collections (slug, name, symbol, verified_creator) VALUES ('leak-b', 'leak', 'SYM', $1)",
    )
    .bind(pk(6))
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(
        code(err),
        CHECK_VIOLATION,
        "TM-only fields without a standard"
    );

    // A placeholder cannot be enabled by flipping the flag alone.
    let err = sqlx::query("UPDATE collections SET enabled = true WHERE id = $1")
        .bind(placeholder)
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn allowlist_mint_belongs_to_one_collection(pool: PgPool) {
    let a = insert_collection(&pool, "a", Some("token_metadata"), None, Some(&pk(1)), true)
        .await
        .unwrap();
    let b = insert_collection(&pool, "b", Some("token_metadata"), None, Some(&pk(1)), true)
        .await
        .unwrap();
    sqlx::query("INSERT INTO collection_mints (mint, collection_id) VALUES ($1, $2)")
        .bind(pk(10))
        .bind(a)
        .execute(&pool)
        .await
        .unwrap();
    let err = sqlx::query("INSERT INTO collection_mints (mint, collection_id) VALUES ($1, $2)")
        .bind(pk(10))
        .bind(b)
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(code(err), UNIQUE_VIOLATION);
    assert_eq!(registry::allowlist(&pool, a).await.unwrap(), vec![pk(10)]);
    assert!(registry::allowlist(&pool, b).await.unwrap().is_empty());
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn asset_number_is_generated_from_name(pool: PgPool) {
    let c = insert_collection(&pool, "c", Some("core"), Some(&pk(1)), None, true)
        .await
        .unwrap();
    let cases = [
        ("#7687", Some(7687)),
        ("Pig Mud #348", Some(348)),
        ("Piggy SOL Gang #12", Some(12)),
        ("no number", None),
        ("#12345678901", None),
        ("", None),
    ];
    for (i, (name, expected)) in cases.iter().enumerate() {
        let id = insert_asset(&pool, c, &pk(20 + i as u8), name).await;
        let number: Option<i32> = sqlx::query_scalar("SELECT number FROM assets WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(number, *expected, "name {name:?}");
    }
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn burned_asset_cannot_have_owner(pool: PgPool) {
    let c = insert_collection(&pool, "c", Some("core"), Some(&pk(1)), None, true)
        .await
        .unwrap();
    let id = insert_asset(&pool, c, &pk(2), "#1").await;
    // An owner must carry the slot of its observation.
    let err = sqlx::query("UPDATE assets SET owner = $2 WHERE id = $1")
        .bind(id)
        .bind(pk(9))
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);
    sqlx::query("UPDATE assets SET owner = $2, owner_slot = 100 WHERE id = $1")
        .bind(id)
        .bind(pk(9))
        .execute(&pool)
        .await
        .unwrap();
    let err = sqlx::query("UPDATE assets SET burned = true WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);
    sqlx::query("UPDATE assets SET burned = true, owner = NULL WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    // Membership status pairs with removed_at; removed assets leave supply.
    let other = insert_asset(&pool, c, &pk(3), "#2").await;
    let err = sqlx::query("UPDATE assets SET membership_status = 'removed' WHERE id = $1")
        .bind(other)
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);
    sqlx::query(
        "UPDATE assets SET membership_status = 'removed', removed_at = now() WHERE id = $1",
    )
    .bind(other)
    .execute(&pool)
    .await
    .unwrap();
    let supply: i32 =
        sqlx::query_scalar("SELECT supply FROM collection_stats WHERE collection_id = $1")
            .bind(c)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(supply, 0, "one burned, one removed");
}

async fn insert_interval(
    exec: impl sqlx::PgExecutor<'_>,
    asset_id: i64,
    owner: &str,
    from_slot: i64,
    to_slot: Option<i64>,
) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        "INSERT INTO ownership_history (asset_id, owner, from_slot, from_ts, to_slot, to_ts) \
         VALUES ($1, $2, $3, now(), $4, CASE WHEN $4::bigint IS NULL THEN NULL ELSE now() END) \
         RETURNING id",
    )
    .bind(asset_id)
    .bind(owner)
    .bind(from_slot)
    .bind(to_slot)
    .fetch_one(exec)
    .await
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn ownership_intervals_never_overlap(pool: PgPool) {
    let c = insert_collection(&pool, "c", Some("core"), Some(&pk(1)), None, true)
        .await
        .unwrap();
    let asset = insert_asset(&pool, c, &pk(2), "#1").await;

    // History: A [100,200) -> B [200,300) -> C [300, open).
    insert_interval(&pool, asset, &pk(11), 100, Some(200))
        .await
        .unwrap();
    insert_interval(&pool, asset, &pk(12), 200, Some(300))
        .await
        .unwrap();
    let open = insert_interval(&pool, asset, &pk(13), 300, None)
        .await
        .unwrap();

    // A second open interval overlaps the existing one (both unbounded).
    let err = insert_interval(&pool, asset, &pk(14), 400, None)
        .await
        .unwrap_err();
    assert_eq!(code(err), EXCLUSION_VIOLATION);
    // A closed interval inside a closed one overlaps too.
    let err = insert_interval(&pool, asset, &pk(14), 150, Some(160))
        .await
        .unwrap_err();
    assert_eq!(code(err), EXCLUSION_VIOLATION);
    // Same-slot hand-off is the empty range: legal.
    insert_interval(&pool, asset, &pk(15), 200, Some(200))
        .await
        .unwrap();
    // to_slot and to_ts must be set together; to >= from.
    let err = sqlx::query(
        "INSERT INTO ownership_history (asset_id, owner, from_slot, from_ts, to_slot) \
         VALUES ($1, $2, 500, now(), 600)",
    )
    .bind(asset)
    .bind(pk(16))
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);
    let err = insert_interval(&pool, asset, &pk(16), 600, Some(500))
        .await
        .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);

    // DEFERRABLE: open the next interval before closing the current one,
    // inside one transaction.
    let mut tx = pool.begin().await.unwrap();
    insert_interval(&mut *tx, asset, &pk(17), 400, None)
        .await
        .unwrap();
    sqlx::query("UPDATE ownership_history SET to_slot = 400, to_ts = now() WHERE id = $1")
        .bind(open)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let open_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ownership_history WHERE asset_id = $1 AND to_slot IS NULL",
    )
    .bind(asset)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(open_count, 1);

    // Derived owner (open interval) vs observed owner: the integrity view
    // flags a disagreement and clears once assets.owner catches up.
    let mismatches: i64 = sqlx::query_scalar("SELECT count(*) FROM integrity_owner_mismatch")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(mismatches, 1, "assets.owner is NULL, derived owner is set");
    sqlx::query("UPDATE assets SET owner = $2, owner_slot = 400 WHERE id = $1")
        .bind(asset)
        .bind(pk(17))
        .execute(&pool)
        .await
        .unwrap();
    let mismatches: i64 = sqlx::query_scalar("SELECT count(*) FROM integrity_owner_mismatch")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(mismatches, 0);
}

#[allow(clippy::too_many_arguments)]
async fn insert_event(
    pool: &PgPool,
    asset_id: i64,
    collection_id: i32,
    signature: &str,
    seq: i16,
    slot: i64,
    kind: &str,
    from: Option<&str>,
    to: Option<&str>,
    price: Option<i64>,
) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        "INSERT INTO activity (asset_id, collection_id, signature, seq, slot, block_time, kind, from_owner, to_owner, price_lamports, marketplace, source) \
         VALUES ($1, $2, $3, $4, $5, now(), $6, $7, $8, $9, CASE WHEN $9::bigint IS NULL THEN NULL ELSE 'Magic Eden' END, 'manual') \
         RETURNING id",
    )
    .bind(asset_id)
    .bind(collection_id)
    .bind(signature)
    .bind(seq)
    .bind(slot)
    .bind(kind)
    .bind(from)
    .bind(to)
    .bind(price)
    .fetch_one(pool)
    .await
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn activity_key_shapes_and_trigger(pool: PgPool) {
    let c = insert_collection(&pool, "c", Some("core"), Some(&pk(1)), None, true)
        .await
        .unwrap();
    let a1 = insert_asset(&pool, c, &pk(2), "#1").await;
    let a2 = insert_asset(&pool, c, &pk(3), "#2").await;
    let (w1, w2) = (pk(21), pk(22));

    // One signature, two assets (a swap-style tx): two rows.
    insert_event(&pool, a1, c, &sig(1), 0, 100, "burn", Some(&w1), None, None)
        .await
        .unwrap();
    insert_event(&pool, a2, c, &sig(1), 0, 100, "mint", None, Some(&w1), None)
        .await
        .unwrap();
    // Same asset, same tx, second event: seq disambiguates; a duplicate is rejected.
    insert_event(
        &pool,
        a2,
        c,
        &sig(1),
        1,
        100,
        "transfer",
        Some(&w1),
        Some(&w2),
        None,
    )
    .await
    .unwrap();
    let err = insert_event(
        &pool,
        a2,
        c,
        &sig(1),
        1,
        100,
        "transfer",
        Some(&w1),
        Some(&w2),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(code(err), UNIQUE_VIOLATION);

    // Contract nullability shapes.
    for (kind, from, to, price) in [
        ("mint", Some(w1.as_str()), Some(w2.as_str()), None), // mint has no sender
        ("burn", Some(w1.as_str()), Some(w2.as_str()), None), // burn has no receiver
        ("sale", Some(w1.as_str()), Some(w2.as_str()), None), // sale needs a price
        ("transfer", Some(w1.as_str()), Some(w2.as_str()), Some(1)), // price only on sales
        ("transfer", Some(w1.as_str()), None, None),          // transfer needs a receiver
        ("listing", Some(w1.as_str()), Some(w2.as_str()), None), // unknown kind
    ] {
        let err = insert_event(&pool, a1, c, &sig(2), 0, 101, kind, from, to, price)
            .await
            .unwrap_err();
        assert_eq!(
            code(err),
            CHECK_VIOLATION,
            "{kind} {from:?} {to:?} {price:?}"
        );
    }
    insert_event(
        &pool,
        a1,
        c,
        &sig(3),
        0,
        102,
        "sale",
        Some(&w1),
        Some(&w2),
        Some(1_500_000_000),
    )
    .await
    .unwrap();
    // The sender may be unknown (escrow-era marketplaces).
    insert_event(
        &pool,
        a1,
        c,
        &sig(7),
        0,
        103,
        "transfer",
        None,
        Some(&w2),
        None,
    )
    .await
    .unwrap();
    // The denormalized collection must be the asset's collection.
    let other = insert_collection(&pool, "other", Some("core"), Some(&pk(5)), None, true)
        .await
        .unwrap();
    let err = insert_event(
        &pool,
        a1,
        other,
        &sig(8),
        0,
        104,
        "transfer",
        Some(&w1),
        Some(&w2),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(code(err), "23503", "foreign key violation");

    // Trigger: one multi-row insert leaves last_activity at the max slot.
    sqlx::query(
        "INSERT INTO activity (asset_id, collection_id, signature, seq, slot, block_time, kind, from_owner, to_owner, source) \
         VALUES ($1, $2, $3, 0, 500, now(), 'transfer', $5, $6, 'manual'), \
                ($1, $2, $4, 0, 300, now(), 'transfer', $6, $5, 'manual')",
    )
    .bind(a1)
    .bind(c)
    .bind(sig(4))
    .bind(sig(5))
    .bind(&w1)
    .bind(&w2)
    .execute(&pool)
    .await
    .unwrap();
    let (last_slot, last_at): (Option<i64>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT last_activity_slot, last_activity_at FROM assets WHERE id = $1")
            .bind(a1)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(last_slot, Some(500));
    assert!(last_at.is_some());
    // An older event never moves it backwards.
    insert_event(
        &pool,
        a1,
        c,
        &sig(6),
        0,
        50,
        "transfer",
        Some(&w1),
        Some(&w2),
        None,
    )
    .await
    .unwrap();
    let last_slot: Option<i64> =
        sqlx::query_scalar("SELECT last_activity_slot FROM assets WHERE id = $1")
            .bind(a1)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(last_slot, Some(500));

    // Stats view counts the collection's recent events.
    let (activity_24h, supply): (i32, i32) = sqlx::query_as(
        "SELECT activity_24h, supply FROM collection_stats WHERE collection_id = $1",
    )
    .bind(c)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(activity_24h, 8);
    assert_eq!(supply, 2);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn ingest_state_is_monotonic(pool: PgPool) {
    assert_eq!(
        ingest_state::last_processed_slot(&pool, "mock:test")
            .await
            .unwrap(),
        None
    );
    ingest_state::checkpoint(&pool, "mock:test", 10)
        .await
        .unwrap();
    ingest_state::checkpoint(&pool, "mock:test", 5)
        .await
        .unwrap();
    assert_eq!(
        ingest_state::last_processed_slot(&pool, "mock:test")
            .await
            .unwrap(),
        Some(10)
    );
    ingest_state::reset(&pool, "mock:test", 5).await.unwrap();
    assert_eq!(
        ingest_state::last_processed_slot(&pool, "mock:test")
            .await
            .unwrap(),
        Some(5)
    );

    let c = insert_collection(&pool, "c", Some("core"), Some(&pk(1)), None, true)
        .await
        .unwrap();
    assert!(ingest_state::backfill_state(&pool, c, "das_assets")
        .await
        .unwrap()
        .is_none());
    let state = ingest_state::BackfillState {
        collection_id: c,
        kind: "das_assets".into(),
        status: "running".into(),
        cursor: serde_json::json!({ "page": 3 }),
        progress: serde_json::json!({ "processed": 3000 }),
        last_error: None,
        started_at: Some(chrono::Utc::now()),
        finished_at: None,
        updated_at: chrono::Utc::now(),
    };
    ingest_state::put_backfill_state(&pool, &state)
        .await
        .unwrap();
    let read = ingest_state::backfill_state(&pool, c, "das_assets")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.status, "running");
    assert_eq!(read.cursor["page"], 3);
    let err = ingest_state::put_backfill_state(
        &pool,
        &ingest_state::BackfillState {
            kind: "Bad Kind".into(),
            ..state
        },
    )
    .await
    .unwrap_err();
    assert_eq!(code(err), CHECK_VIOLATION);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn attributes_cannot_cross_collections(pool: PgPool) {
    let cx = insert_collection(&pool, "cx", Some("core"), Some(&pk(1)), None, true)
        .await
        .unwrap();
    let cy = insert_collection(&pool, "cy", Some("core"), Some(&pk(2)), None, true)
        .await
        .unwrap();
    let asset = insert_asset(&pool, cx, &pk(3), "#1").await;
    let hat_y = indexer_data_model::attributes::ensure_trait_type(&pool, cy, "Hat")
        .await
        .unwrap();
    let red_y = indexer_data_model::attributes::ensure_trait_value(&pool, hat_y, "red")
        .await
        .unwrap();
    // cy's trait on cx's asset: rejected whichever collection_id is claimed.
    for claimed in [cx, cy] {
        let err = sqlx::query(
            "INSERT INTO asset_attributes (asset_id, collection_id, trait_type_id, trait_value_id) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(asset)
        .bind(claimed)
        .bind(hat_y)
        .bind(red_y)
        .execute(&pool)
        .await
        .unwrap_err();
        assert_eq!(code(err), "23503", "claimed collection {claimed}");
    }
    let hat_x = indexer_data_model::attributes::ensure_trait_type(&pool, cx, "Hat")
        .await
        .unwrap();
    let red_x = indexer_data_model::attributes::ensure_trait_value(&pool, hat_x, "red")
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO asset_attributes (asset_id, collection_id, trait_type_id, trait_value_id) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(asset)
    .bind(cx)
    .bind(hat_x)
    .bind(red_x)
    .execute(&pool)
    .await
    .unwrap();
    let counts = indexer_data_model::facets::facet_counts(&pool, cy)
        .await
        .unwrap();
    assert!(counts.is_empty(), "cy has no assets: {counts:?}");
    let counts = indexer_data_model::facets::facet_counts(&pool, cx)
        .await
        .unwrap();
    assert_eq!(counts.len(), 1);
    assert_eq!((counts[0].trait_type.as_str(), counts[0].count), ("Hat", 1));
}
