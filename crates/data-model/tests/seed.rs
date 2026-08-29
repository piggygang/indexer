//! The committed `config/collections.toml` against a fresh database: applied
//! twice it is a no-op, and it never deletes.

use std::path::Path;

use chrono::{DateTime, Utc};
use indexer_data_model::attributes::ensure_trait_type;
use indexer_data_model::seed::{self, ApplyOptions, Outcome};
use indexer_data_model::{registry, PgPool};

fn committed() -> seed::Seed {
    seed::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/collections.toml"))
        .unwrap()
}

async fn counts(pool: &PgPool) -> (i64, i64, i64) {
    let c: i64 = sqlx::query_scalar("SELECT count(*) FROM collections")
        .fetch_one(pool)
        .await
        .unwrap();
    let m: i64 = sqlx::query_scalar("SELECT count(*) FROM collection_mints")
        .fetch_one(pool)
        .await
        .unwrap();
    let t: i64 = sqlx::query_scalar("SELECT count(*) FROM tokens")
        .fetch_one(pool)
        .await
        .unwrap();
    (c, m, t)
}

async fn is_facet(pool: &PgPool, trait_type_id: i32) -> bool {
    sqlx::query_scalar("SELECT is_facet FROM trait_types WHERE id = $1")
        .bind(trait_type_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn updated_ats(pool: &PgPool) -> Vec<(String, DateTime<Utc>)> {
    sqlx::query_as("SELECT slug, updated_at FROM collections ORDER BY id")
        .fetch_all(pool)
        .await
        .unwrap()
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn seed_twice_is_idempotent(pool: PgPool) {
    let seed = committed();

    let first = seed::apply(&pool, &seed, ApplyOptions::default())
        .await
        .unwrap();
    assert!(
        first
            .collections
            .iter()
            .all(|c| c.outcome == Outcome::Inserted),
        "{first:?}"
    );
    assert!(
        first
            .collections
            .iter()
            .all(|c| c.mints_new == c.mints_in_file as u64),
        "{first:?}"
    );
    assert!(first.tokens.iter().all(|t| t.outcome == Outcome::Inserted));
    assert!(first.warnings.is_empty(), "{:?}", first.warnings);
    assert_eq!(counts(&pool).await, (4, 10_000 + 5_000 + 2_073, 1));

    let enabled = registry::list_enabled(&pool).await.unwrap();
    let slugs: Vec<&str> = enabled.iter().map(|c| c.slug.as_str()).collect();
    assert_eq!(
        slugs,
        ["piggy-sol-gang", "piggy-girl-gang", "pig-mud", "piggy-gang"]
    );
    assert!(enabled.iter().all(|c| c.membership_rule.is_some()));
    let pgg = registry::by_slug(&pool, "piggy-girl-gang")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pgg.facet_exclude, ["Name"]);
    assert_eq!(
        registry::allowlist(&pool, pgg.id).await.unwrap().len(),
        5_000
    );
    let tokens = registry::list_tokens(&pool, true).await.unwrap();
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].symbol, "PIGGY");
    assert_eq!(tokens[0].decimals, 9);

    // facet_exclude drives trait_types.is_facet at creation time...
    let name_type = ensure_trait_type(&pool, pgg.id, "Name").await.unwrap();
    let eyes_type = ensure_trait_type(&pool, pgg.id, "Eyes").await.unwrap();
    assert!(!is_facet(&pool, name_type).await);
    assert!(is_facet(&pool, eyes_type).await);
    // ...and the seed re-syncs it (simulate a flag that drifted).
    sqlx::query("UPDATE trait_types SET is_facet = true WHERE id = $1")
        .bind(name_type)
        .execute(&pool)
        .await
        .unwrap();

    let before = updated_ats(&pool).await;
    let second = seed::apply(&pool, &seed, ApplyOptions::default())
        .await
        .unwrap();
    assert!(
        second
            .collections
            .iter()
            .all(|c| c.outcome == Outcome::Unchanged),
        "{second:?}"
    );
    assert!(
        second.collections.iter().all(|c| c.mints_new == 0),
        "{second:?}"
    );
    assert!(second
        .tokens
        .iter()
        .all(|t| t.outcome == Outcome::Unchanged));
    let synced: u64 = second.collections.iter().map(|c| c.facets_synced).sum();
    assert_eq!(synced, 1, "the drifted Name flag was re-synced");
    assert!(!is_facet(&pool, name_type).await);
    assert_eq!(counts(&pool).await, (4, 17_073, 1));
    assert_eq!(
        updated_ats(&pool).await,
        before,
        "an unchanged row keeps its updated_at"
    );
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn seed_never_deletes_and_dry_run_persists_nothing(pool: PgPool) {
    let seed = committed();

    let dry = seed::apply(
        &pool,
        &seed,
        ApplyOptions {
            dry_run: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(dry.dry_run);
    assert_eq!(dry.collections.len(), 4);
    assert_eq!(counts(&pool).await, (0, 0, 0), "dry run rolled back");

    seed::apply(&pool, &seed, ApplyOptions::default())
        .await
        .unwrap();
    let psg = registry::by_slug(&pool, "piggy-sol-gang")
        .await
        .unwrap()
        .unwrap();
    let extra = bs58::encode([7u8; 32]).into_string();
    sqlx::query("INSERT INTO collection_mints (mint, collection_id) VALUES ($1, $2)")
        .bind(&extra)
        .bind(psg.id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO collections (slug, name, enabled) VALUES ('legacy', 'Legacy', false)")
        .execute(&pool)
        .await
        .unwrap();

    let report = seed::apply(&pool, &seed, ApplyOptions::default())
        .await
        .unwrap();
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.contains("piggy-sol-gang") && w.contains("10001")),
        "{:?}",
        report.warnings
    );
    assert!(
        report.warnings.iter().any(|w| w.contains("legacy")),
        "{:?}",
        report.warnings
    );
    let still_there: i64 =
        sqlx::query_scalar("SELECT count(*) FROM collection_mints WHERE mint = $1")
            .bind(&extra)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(still_there, 1);
    assert_eq!(counts(&pool).await, (5, 17_074, 1));
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn seed_rejects_a_mint_owned_by_another_collection(pool: PgPool) {
    let seed = committed();
    seed::apply(&pool, &seed, ApplyOptions::default())
        .await
        .unwrap();
    // Move one PSG mint under piggy-gang behind the seed's back.
    let piggy_gang = registry::by_slug(&pool, "piggy-gang")
        .await
        .unwrap()
        .unwrap();
    let psg_mint = &seed.mints["piggy-sol-gang"][0];
    sqlx::query("UPDATE collection_mints SET collection_id = $2 WHERE mint = $1")
        .bind(psg_mint)
        .bind(piggy_gang.id)
        .execute(&pool)
        .await
        .unwrap();
    let err = seed::apply(&pool, &seed, ApplyOptions::default())
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("already belongs to collection piggy-gang"),
        "{err:#}"
    );
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn seed_refuses_identity_changes_once_assets_exist(pool: PgPool) {
    let mut seed = committed();
    seed::apply(&pool, &seed, ApplyOptions::default())
        .await
        .unwrap();
    let piggy_gang = registry::by_slug(&pool, "piggy-gang")
        .await
        .unwrap()
        .unwrap();
    sqlx::query("INSERT INTO assets (address, collection_id, name) VALUES ($1, $2, '#1')")
        .bind(bs58::encode([9u8; 32]).into_string())
        .bind(piggy_gang.id)
        .execute(&pool)
        .await
        .unwrap();

    let new_address = bs58::encode([8u8; 32]).into_string();
    seed.file
        .collections
        .iter_mut()
        .find(|c| c.slug == "piggy-gang")
        .unwrap()
        .address = Some(new_address.clone());
    let err = seed::apply(&pool, &seed, ApplyOptions::default())
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("refusing to change the identity"),
        "{err:#}"
    );
    assert_eq!(
        registry::by_slug(&pool, "piggy-gang")
            .await
            .unwrap()
            .unwrap()
            .address,
        piggy_gang.address,
        "nothing was applied"
    );

    let report = seed::apply(
        &pool,
        &seed,
        ApplyOptions {
            allow_identity_change: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(report
        .collections
        .iter()
        .any(|c| c.slug == "piggy-gang" && c.outcome == Outcome::Updated));
    assert_eq!(
        registry::by_slug(&pool, "piggy-gang")
            .await
            .unwrap()
            .unwrap()
            .address,
        Some(new_address)
    );
    // Cosmetic changes (name) never trip the guard.
    seed.file.collections[0].name = "Piggy SOL Gang (renamed)".into();
    seed::apply(&pool, &seed, ApplyOptions::default())
        .await
        .unwrap();
}
