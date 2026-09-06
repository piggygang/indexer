//! The read paths the detail half is built on (ALG-626), against Postgres.
//!
//! These assert what the HTTP layer above them cannot reach or would only
//! prove indirectly: that the detail page shows traits the facet sidebar hides,
//! that a keyset page is exactly a slice of the ordered scan at every page
//! size, and that a search term made of `LIKE` metacharacters is a literal.
//!
//! Every address is a synthetic base58 key (CLAUDE.md).
//! Ignored without a database: `cargo test --workspace -- --include-ignored`.

use chrono::{TimeZone, Utc};
use indexer_data_model::activity::{self, LiveEvent};
use indexer_data_model::assets::{self, AssetInput, TraitInput};
use indexer_data_model::types::EventKind;
use indexer_data_model::{attributes, facets, nft, search, stats, timeline, PgPool};

fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

/// A synthetic 87-character signature. Callers pass a seed of 1 or more:
/// base58 renders each leading zero byte as one character, so all-zero bytes
/// encode to 64 `1`s and fail `is_signature`'s 86-88 range.
fn sig(seed: u8) -> String {
    bs58::encode([seed; 64]).into_string()
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

fn trait_input(trait_type: &str, value: &str, position: i16) -> TraitInput {
    TraitInput {
        trait_type: trait_type.to_string(),
        value: value.to_string(),
        position,
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "needs DATABASE_URL"]
async fn the_detail_page_shows_the_traits_the_facets_exclude(pool: PgPool) {
    // `Name` is per-asset-unique, so Piggy Sol Gang excludes it from facets —
    // and the contract still requires the detail page to render it as a chip
    // with `isFacet: false`.
    let cid = collection(&pool, "syn-facets", 1, &["Name"]).await;
    let mut tx = pool.begin().await.unwrap();
    assets::upsert_batch(
        &mut tx,
        cid,
        100,
        &[AssetInput {
            address: pk(2),
            name: "#1".into(),
            symbol: None,
            metadata_uri: None,
            metadata_source_uri: None,
            image_uri: None,
            burned: false,
            owner: None,
            attributes: Some(vec![
                trait_input("Name", "#1", 0),
                trait_input("Background", "Pink", 1),
                trait_input("Head", "Crown", 2),
            ]),
            document: None,
        }],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    attributes::sync_trait_facets(&pool, cid).await.unwrap();

    let asset_id: i64 = sqlx::query_scalar("SELECT id FROM assets WHERE address = $1")
        .bind(pk(2))
        .fetch_one(&pool)
        .await
        .unwrap();
    let shown = nft::attributes(&pool, asset_id).await.unwrap();
    assert_eq!(shown.len(), 3, "the detail page shows every attribute");
    let name = shown.iter().find(|a| a.trait_type == "Name").unwrap();
    assert!(!name.is_facet, "an excluded trait is shown, flagged");
    assert!(shown
        .iter()
        .filter(|a| a.trait_type != "Name")
        .all(|a| a.is_facet));
    // Ordered by (position, traitType), because `position` alone is not a
    // total order.
    let positions: Vec<i32> = shown.iter().map(|a| a.position).collect();
    assert!(positions.windows(2).all(|w| w[0] <= w[1]));

    // …and the sidebar does not, which is the difference this test exists for.
    let counted = facets::facet_counts(&pool, cid).await.unwrap();
    assert!(
        counted.iter().all(|c| c.trait_type != "Name"),
        "an excluded trait must never reach /facets"
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_keyset_page_is_a_slice_of_the_ordered_scan(pool: PgPool) {
    let cid = collection(&pool, "syn-keyset", 1, &[]).await;
    let asset_id: i64 = sqlx::query_scalar(
        "INSERT INTO assets (address, collection_id, name) VALUES ($1, $2, '#1') RETURNING id",
    )
    .bind(pk(2))
    .bind(cid)
    .fetch_one(&pool)
    .await
    .unwrap();

    let kinds = EventKind::public_strings();
    let owner = |i: u8| pk(100 + i % 5);
    for i in 0..40u8 {
        let (kind, from, to) = if i == 0 {
            (EventKind::Mint, None, Some(owner(0)))
        } else {
            (EventKind::Transfer, Some(owner(i - 1)), Some(owner(i)))
        };
        let mut tx = pool.begin().await.unwrap();
        activity::record(
            &mut tx,
            &LiveEvent {
                asset_id,
                collection_id: cid,
                signature: &sig(i + 1),
                seq: 0,
                slot: 1000 + i as i64,
                block_time: Utc.timestamp_opt(1_600_000_000 + i as i64, 0).unwrap(),
                kind,
                from_owner: from.as_deref(),
                to_owner: to.as_deref(),
                price_lamports: None,
                marketplace: None,
                details: None,
                source: "backfill",
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    // The whole feed in one call is the reference; every page size must
    // reproduce it exactly.
    let whole = timeline::asset_activity(&pool, asset_id, &kinds, None, 1000)
        .await
        .unwrap();
    assert_eq!(whole.len(), 40);
    for size in [1i64, 3, 7, 40] {
        let mut collected = Vec::new();
        let mut after = None;
        loop {
            let page = timeline::asset_activity(&pool, asset_id, &kinds, after, size)
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            let last = page.last().unwrap();
            after = Some(timeline::Position {
                key: last.slot,
                id: last.id,
            });
            collected.extend(page);
        }
        assert_eq!(collected, whole, "page size {size} diverged from the scan");
    }

    // The ownership feed pages the same way, on `(from_slot, id)`.
    let all = timeline::owners(&pool, asset_id, None, 1000).await.unwrap();
    assert_eq!(all.len(), 40);
    assert_eq!(all.iter().filter(|i| i.is_current()).count(), 1);
    let mut collected = Vec::new();
    let mut after = None;
    loop {
        let page = timeline::owners(&pool, asset_id, after, 6).await.unwrap();
        if page.is_empty() {
            break;
        }
        let last = page.last().unwrap();
        after = Some(timeline::Position {
            key: last.from_slot,
            id: last.id,
        });
        collected.extend(page);
    }
    assert_eq!(collected, all);

    // `ownerCount` is distinct wallets, not intervals — five owners, forty
    // hand-offs.
    let summary = nft::activity_summary(&pool, asset_id).await.unwrap();
    assert_eq!(summary.owner_count, 5);
    assert_eq!(summary.transfer_count, 39);
    assert_eq!(summary.sales_count, 0);
    assert!(summary.last_sale_at.is_none() && summary.last_sale_price_lamports.is_none());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "needs DATABASE_URL"]
async fn search_treats_like_metacharacters_literally(pool: PgPool) {
    let cid = collection(&pool, "syn-like", 1, &[]).await;
    for (seed, name) in [(2u8, "100% Piggy"), (3, "100 Piggy"), (4, "Plain")] {
        sqlx::query("INSERT INTO assets (address, collection_id, name) VALUES ($1, $2, $3)")
            .bind(pk(seed))
            .bind(cid)
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
    }

    // Unescaped, `%` would make this match everything.
    let hits = search::grouped(&pool, &search::Match::Text("100%".into()), None, 10)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].card.name, "100% Piggy");
    assert_eq!(hits[0].total, 1, "total is the exact match count");

    // A plain substring still matches both.
    let hits = search::grouped(&pool, &search::Match::Text("100".into()), None, 10)
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert!(hits.iter().all(|h| h.total == 2));

    // The preview is capped but the total is not.
    let hits = search::grouped(&pool, &search::Match::Text("100".into()), None, 1)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].total, 2, "the cap must not truncate the count");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "needs DATABASE_URL"]
async fn holder_ranks_and_the_holder_list_share_one_window(pool: PgPool) {
    let a = collection(&pool, "syn-rank-a", 1, &[]).await;
    let b = collection(&pool, "syn-rank-b", 2, &[]).await;
    let owners = [pk(120), pk(121), pk(122)];
    let mut seed = 10u8;
    // Collection A: 3 / 3 / 1 — a tie at the top. Collection B: only the
    // third wallet holds anything.
    for (cid, counts) in [(a, [3u8, 3, 1]), (b, [0, 0, 2])] {
        for (owner, count) in owners.iter().zip(counts) {
            for _ in 0..count {
                sqlx::query(
                    "INSERT INTO assets (address, collection_id, name, owner, owner_slot) \
                     VALUES ($1, $2, '#1', $3, 1)",
                )
                .bind(pk(seed))
                .bind(cid)
                .bind(owner)
                .execute(&pool)
                .await
                .unwrap();
                seed += 1;
            }
        }
    }

    let top = stats::top_holders(&pool, a, 10).await.unwrap();
    assert_eq!(
        top.iter().map(|h| h.rank).collect::<Vec<_>>(),
        vec![1, 1, 3],
        "ties share the lower rank and skip the next value"
    );

    for owner in &owners {
        let ranks = stats::holder_ranks(&pool, owner, &[a, b]).await.unwrap();
        for rank in &ranks {
            let listed = stats::top_holders(&pool, rank.collection_id, 100)
                .await
                .unwrap();
            let same = listed.iter().find(|h| &h.address == owner).unwrap();
            assert_eq!((rank.rank, rank.count), (same.rank, same.count));
        }
        // A wallet holding nothing in a collection is simply absent.
        assert!(ranks.iter().all(|r| r.count > 0));
    }
    assert_eq!(
        stats::holder_ranks(&pool, &pk(200), &[a, b])
            .await
            .unwrap()
            .len(),
        0,
        "a wallet with no holdings ranks nowhere"
    );
}
