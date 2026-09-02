//! Writer semantics for the DAS backfill (ALG-621), each on a fresh database.
//! No network: the point of keeping the SQL in this crate is that every
//! invariant the backfill depends on is provable against Postgres alone.
//! Ignored without a database: `cargo test --workspace -- --include-ignored`.

use chrono::{DateTime, Utc};
use indexer_data_model::assets::{self, AssetDocument, AssetInput, BatchCounts, TraitInput};
use indexer_data_model::types::ImageStatus;
use indexer_data_model::PgPool;
use serde_json::json;

/// Deterministic, obviously synthetic 32-byte keys — no on-chain address ever
/// appears in a test (CLAUDE.md).
fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

async fn collection(pool: &PgPool, slug: &str, facet_exclude: &[&str]) -> i32 {
    sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, \
                                  facet_exclude, enabled) \
         VALUES ($1, $1, 'token_metadata', $2, 'SYN', $3, true) RETURNING id",
    )
    .bind(slug)
    .bind(pk(200))
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

fn asset(address: &str) -> AssetInput {
    AssetInput {
        address: address.to_string(),
        name: "#1".into(),
        symbol: Some("SYN".into()),
        metadata_uri: Some("https://example.invalid/on-chain.json".into()),
        metadata_source_uri: None,
        image_uri: None,
        burned: false,
        owner: None,
        attributes: None,
        document: None,
    }
}

fn traits(pairs: &[(&str, &str)]) -> Option<Vec<TraitInput>> {
    Some(
        pairs
            .iter()
            .enumerate()
            .map(|(i, (t, v))| TraitInput {
                trait_type: (*t).to_string(),
                value: (*v).to_string(),
                position: i as i16,
            })
            .collect(),
    )
}

async fn write(pool: &PgPool, collection_id: i32, slot: i64, rows: &[AssetInput]) -> BatchCounts {
    let mut tx = pool.begin().await.unwrap();
    let counts = assets::upsert_batch(&mut tx, collection_id, slot, rows)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    counts
}

async fn attribute_pairs(pool: &PgPool, address: &str) -> Vec<(String, String, i16)> {
    sqlx::query_as(
        "SELECT tt.name, tv.value, aa.position \
           FROM asset_attributes aa \
           JOIN assets a ON a.id = aa.asset_id \
           JOIN trait_types tt ON tt.id = aa.trait_type_id \
           JOIN trait_values tv ON tv.id = aa.trait_value_id \
          WHERE a.address = $1 ORDER BY tt.name, tv.value",
    )
    .bind(address)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn updated_ats(pool: &PgPool) -> Vec<(String, DateTime<Utc>)> {
    sqlx::query_as("SELECT address, updated_at FROM assets ORDER BY address")
        .fetch_all(pool)
        .await
        .unwrap()
}

async fn owner_of(pool: &PgPool, address: &str) -> (Option<String>, Option<i64>) {
    sqlx::query_as("SELECT owner, owner_slot FROM assets WHERE address = $1")
        .bind(address)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The acceptance criterion "rerun causes zero duplicates", mechanized: the
/// second pass must report nothing changed AND leave every `updated_at` and
/// `fetched_at` untouched, which only holds if the upserts' `IS DISTINCT
/// FROM` guards make an unchanged row a true no-op.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn upsert_batch_is_idempotent(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut a = asset(&pk(1));
    a.owner = Some(pk(50));
    a.attributes = traits(&[("Background", "Pink"), ("Head", "Crown")]);
    a.document = Some(AssetDocument {
        metadata_json: json!({"name": "#1", "attributes": [{"trait_type": "Head"}]}),
        source_uri: "https://example.invalid/1.json".into(),
    });

    let first = write(&pool, cid, 100, std::slice::from_ref(&a)).await;
    assert_eq!(first.inserted, 1);
    assert_eq!(first.attributes_written, 2);
    assert_eq!(first.documents, 1);

    let stamps = updated_ats(&pool).await;
    let fetched: DateTime<Utc> = sqlx::query_scalar("SELECT fetched_at FROM asset_documents")
        .fetch_one(&pool)
        .await
        .unwrap();

    // Re-run at a higher slot: a fresh DAS snapshot observing the same state.
    let second = write(&pool, cid, 200, std::slice::from_ref(&a)).await;
    assert_eq!(
        second,
        BatchCounts {
            unchanged: 1,
            ..BatchCounts::default()
        },
        "a second identical pass must change nothing"
    );
    assert!(second.is_noop());
    assert_eq!(updated_ats(&pool).await, stamps, "updated_at must not move");

    let fetched_again: DateTime<Utc> = sqlx::query_scalar("SELECT fetched_at FROM asset_documents")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(fetched_again, fetched, "fetched_at must not move");

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM asset_attributes")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2, "no duplicate attribute rows");
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn attributes_are_replaced_not_merged(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut a = asset(&pk(1));
    a.attributes = traits(&[("Background", "Pink"), ("Head", "Crown")]);
    write(&pool, cid, 100, std::slice::from_ref(&a)).await;

    // Drop one trait, change the other's value.
    a.attributes = traits(&[("Background", "Blue")]);
    let counts = write(&pool, cid, 101, std::slice::from_ref(&a)).await;
    assert_eq!(counts.attributes_removed, 2, "both stale rows deleted");
    assert_eq!(counts.attributes_written, 1);

    assert_eq!(
        attribute_pairs(&pool, &pk(1)).await,
        vec![("Background".to_string(), "Blue".to_string(), 0)]
    );
}

/// `attributes: None` means "not observed" — the Pig Mud path, where the
/// metadata host is dead. Whatever is stored must survive, so that a later
/// run after a re-host fills the gap with no code change.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn attributes_none_leaves_existing(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut a = asset(&pk(1));
    a.attributes = traits(&[("Background", "Pink")]);
    write(&pool, cid, 100, std::slice::from_ref(&a)).await;

    a.attributes = None;
    let counts = write(&pool, cid, 101, std::slice::from_ref(&a)).await;
    assert_eq!(counts.attributes_removed, 0);
    assert_eq!(counts.attributes_written, 0);
    assert_eq!(attribute_pairs(&pool, &pk(1)).await.len(), 1);
}

/// `Some(vec![])` is the opposite: observed, and genuinely empty.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn attributes_some_empty_deletes_all(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut a = asset(&pk(1));
    a.attributes = traits(&[("Background", "Pink")]);
    write(&pool, cid, 100, std::slice::from_ref(&a)).await;

    a.attributes = Some(vec![]);
    let counts = write(&pool, cid, 101, std::slice::from_ref(&a)).await;
    assert_eq!(counts.attributes_removed, 1);
    assert!(attribute_pairs(&pool, &pk(1)).await.is_empty());
}

/// One asset's attributes must not be touched while another's are replaced in
/// the same batch — the delete is scoped, not collection-wide.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn attribute_delete_is_scoped_to_observed_assets(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut first = asset(&pk(1));
    first.attributes = traits(&[("Background", "Pink")]);
    let mut second = asset(&pk(2));
    second.attributes = traits(&[("Background", "Blue")]);
    write(&pool, cid, 100, &[first.clone(), second.clone()]).await;

    // Only the second asset is observed this time.
    second.attributes = traits(&[("Background", "Green")]);
    first.attributes = None;
    let counts = write(&pool, cid, 101, &[first, second]).await;
    assert_eq!(counts.attributes_removed, 1, "only the observed asset's");
    assert_eq!(attribute_pairs(&pool, &pk(1)).await.len(), 1);
    assert_eq!(
        attribute_pairs(&pool, &pk(2)).await,
        vec![("Background".to_string(), "Green".to_string(), 0)]
    );
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn owner_slot_guard_rejects_a_stale_observation(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut a = asset(&pk(1));
    a.owner = Some(pk(50));
    write(&pool, cid, 100, std::slice::from_ref(&a)).await;
    assert_eq!(owner_of(&pool, &pk(1)).await, (Some(pk(50)), Some(100)));

    // An older snapshot must not win.
    a.owner = Some(pk(51));
    let stale = write(&pool, cid, 50, std::slice::from_ref(&a)).await;
    assert!(stale.is_noop(), "a stale slot changes nothing");
    assert_eq!(owner_of(&pool, &pk(1)).await, (Some(pk(50)), Some(100)));

    // A newer one does.
    let fresh = write(&pool, cid, 150, std::slice::from_ref(&a)).await;
    assert_eq!(fresh.updated, 1);
    assert_eq!(owner_of(&pool, &pk(1)).await, (Some(pk(51)), Some(150)));
}

/// DAS not knowing the owner must never clear one we already have.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn unknown_owner_does_not_clobber(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut a = asset(&pk(1));
    a.owner = Some(pk(50));
    write(&pool, cid, 100, std::slice::from_ref(&a)).await;

    a.owner = None;
    let counts = write(&pool, cid, 200, std::slice::from_ref(&a)).await;
    assert!(counts.is_noop());
    assert_eq!(owner_of(&pool, &pk(1)).await, (Some(pk(50)), Some(100)));
}

/// A malformed owner costs the owner, not the asset.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn malformed_owner_is_dropped_but_the_asset_lands(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut a = asset(&pk(1));
    a.owner = Some("not-a-pubkey".into());
    let counts = write(&pool, cid, 100, std::slice::from_ref(&a)).await;
    assert_eq!(counts.inserted, 1);
    assert_eq!(owner_of(&pool, &pk(1)).await, (None, None));
}

/// Burning is irreversible on chain, so `true -> false` is a DAS artifact.
/// Setting it must also clear the owner, or `assets_burned_has_no_owner`
/// rejects the row.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn burn_is_monotone_and_clears_owner(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut a = asset(&pk(1));
    a.owner = Some(pk(50));
    write(&pool, cid, 100, std::slice::from_ref(&a)).await;

    a.burned = true;
    a.owner = None;
    let burn = write(&pool, cid, 150, std::slice::from_ref(&a)).await;
    assert_eq!(burn.updated, 1);
    assert_eq!(owner_of(&pool, &pk(1)).await, (None, None));

    a.burned = false;
    a.owner = Some(pk(51));
    let unburn = write(&pool, cid, 200, std::slice::from_ref(&a)).await;
    assert!(unburn.is_noop(), "an asset never comes back from a burn");
    let burned: bool = sqlx::query_scalar("SELECT burned FROM assets WHERE address = $1")
        .bind(pk(1))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(burned);
}

/// A `--das-only` pass, or a failed document fetch, must not wipe the URIs an
/// earlier successful pass derived from the off-chain JSON.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn source_uri_and_image_are_sticky(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut a = asset(&pk(1));
    a.metadata_source_uri = Some("https://example.invalid/rehost/1.json".into());
    a.image_uri = Some("https://example.invalid/rehost/1.png".into());
    write(&pool, cid, 100, std::slice::from_ref(&a)).await;

    a.metadata_source_uri = None;
    a.image_uri = None;
    let counts = write(&pool, cid, 200, std::slice::from_ref(&a)).await;
    assert!(counts.is_noop());

    let (source, image): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT metadata_source_uri, image_uri FROM assets WHERE address = $1")
            .bind(pk(1))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        source.as_deref(),
        Some("https://example.invalid/rehost/1.json")
    );
    assert_eq!(
        image.as_deref(),
        Some("https://example.invalid/rehost/1.png")
    );
}

/// `assets.address` is globally unique, so an address already filed under
/// another collection must be reported, never silently re-filed.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn foreign_collection_address_is_skipped(pool: PgPool) {
    let a_id = collection(&pool, "a", &[]).await;
    let b_id = collection(&pool, "b", &[]).await;
    write(&pool, a_id, 100, &[asset(&pk(1))]).await;

    let counts = write(&pool, b_id, 200, &[asset(&pk(1)), asset(&pk(2))]).await;
    assert_eq!(counts.skipped_foreign, 1);
    assert_eq!(counts.inserted, 1);

    let owner_collection: i32 =
        sqlx::query_scalar("SELECT collection_id FROM assets WHERE address = $1")
            .bind(pk(1))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(owner_collection, a_id, "the original collection keeps it");
}

/// 2021-era metadata repeats pairs; without `DISTINCT ON` the upsert raises
/// SQLSTATE 21000 and takes the whole batch down.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn duplicate_trait_pair_in_one_asset_does_not_error(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut a = asset(&pk(1));
    a.attributes = traits(&[("Background", "Pink"), ("Background", "Pink")]);
    let counts = write(&pool, cid, 100, std::slice::from_ref(&a)).await;
    assert_eq!(counts.attributes_written, 1);
    assert_eq!(attribute_pairs(&pool, &pk(1)).await.len(), 1);
}

/// The same address twice in one batch would otherwise raise 21000 on the
/// asset upsert; a bisected DAS retry can legitimately produce it.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn duplicate_address_in_one_batch_does_not_error(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let counts = write(&pool, cid, 100, &[asset(&pk(1)), asset(&pk(1))]).await;
    assert_eq!(counts.inserted, 1);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn invalid_address_is_counted_not_fatal(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let counts = write(&pool, cid, 100, &[asset("nope"), asset(&pk(1))]).await;
    assert_eq!(counts.invalid, 1);
    assert_eq!(counts.inserted, 1);
}

/// `facet_exclude` must reach `trait_types.is_facet` on creation, so a
/// per-asset-unique trait never enters the facet population.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn facet_exclude_is_applied_on_intern(pool: PgPool) {
    let cid = collection(&pool, "c", &["Name"]).await;
    let mut a = asset(&pk(1));
    a.attributes = traits(&[("Name", "#1"), ("Background", "Pink")]);
    write(&pool, cid, 100, std::slice::from_ref(&a)).await;

    let facets: Vec<(String, bool)> = sqlx::query_as(
        "SELECT name, is_facet FROM trait_types WHERE collection_id = $1 ORDER BY name",
    )
    .bind(cid)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        facets,
        vec![
            ("Background".to_string(), true),
            ("Name".to_string(), false)
        ]
    );

    // The excluded type is still stored on the asset (detail page), just not
    // faceted — and the facet view must agree.
    assert_eq!(attribute_pairs(&pool, &pk(1)).await.len(), 2);
    let faceted: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM facet_counts WHERE collection_id = $1 AND trait_type = 'Name'",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(faceted, 0);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn trait_cardinality_reports_the_widest_first(pool: PgPool) {
    use indexer_data_model::attributes;

    let cid = collection(&pool, "c", &[]).await;
    let rows: Vec<AssetInput> = (1..=5u8)
        .map(|n| {
            let mut a = asset(&pk(n));
            a.attributes =
                traits(&[("Name", &format!("#{n}")), ("Background", "Pink")]).map(|mut t| {
                    t[0].value = format!("#{n}");
                    t
                });
            a
        })
        .collect();
    write(&pool, cid, 100, &rows).await;

    let card = attributes::trait_cardinality(&pool, cid).await.unwrap();
    assert_eq!(card[0].name, "Name");
    assert_eq!(card[0].values, 5, "per-asset-unique");
    assert_eq!(card[0].assets, 5);
    assert!(card[0].is_facet, "not excluded in this collection");
    assert_eq!(card[1].name, "Background");
    assert_eq!(card[1].values, 1);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn image_status_is_recorded(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    write(&pool, cid, 100, &[asset(&pk(1)), asset(&pk(2))]).await;

    let updated = assets::set_image_status(
        &pool,
        cid,
        &[(pk(1), ImageStatus::Ok), (pk(2), ImageStatus::Dead)],
    )
    .await
    .unwrap();
    assert_eq!(updated, 2);

    let rows: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT address, image_status, image_checked_at IS NOT NULL \
           FROM assets WHERE collection_id = $1 ORDER BY address",
    )
    .bind(cid)
    .fetch_all(&pool)
    .await
    .unwrap();
    let statuses: Vec<&str> = rows.iter().map(|(_, s, _)| s.as_str()).collect();
    assert!(statuses.contains(&"ok") && statuses.contains(&"dead"));
    assert!(rows.iter().all(|(_, _, checked)| *checked));
}

/// The asset backfill must leave `image_status` alone — the contract defines
/// `unknown` as "not checked, load optimistically", and only the opt-in pass
/// may move it.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn upsert_never_touches_image_status(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    write(&pool, cid, 100, &[asset(&pk(1))]).await;
    assets::set_image_status(&pool, cid, &[(pk(1), ImageStatus::Dead)])
        .await
        .unwrap();

    let mut a = asset(&pk(1));
    a.name = "#renamed".into();
    write(&pool, cid, 200, &[a]).await;

    let status: String = sqlx::query_scalar("SELECT image_status FROM assets WHERE address = $1")
        .bind(pk(1))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "dead");
}

/// The backfill writes no ownership intervals, so ALG-624's "empty means
/// healthy" integrity view stays clean.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn backfill_leaves_ownership_history_empty(pool: PgPool) {
    let cid = collection(&pool, "c", &[]).await;
    let mut a = asset(&pk(1));
    a.owner = Some(pk(50));
    write(&pool, cid, 100, &[a]).await;

    let intervals: i64 = sqlx::query_scalar("SELECT count(*) FROM ownership_history")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(intervals, 0);

    let mismatches: i64 = sqlx::query_scalar("SELECT count(*) FROM integrity_owner_mismatch")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(mismatches, 0);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn analyze_runs(pool: PgPool) {
    assets::analyze_after_backfill(&pool).await.unwrap();
}
