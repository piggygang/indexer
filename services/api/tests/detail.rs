//! The detail half of the contract against a real database (ALG-626).
//!
//! Prism proves the *shape* of a response; these tests prove the behaviour
//! behind it — the parts a schema check structurally cannot see:
//!
//! * a deep timeline paged to exhaustion loses nothing and repeats nothing;
//! * a cursor is refused by every feed except the one that minted it;
//! * `heldSince` is nulled when the derived history disagrees with the observed
//!   owner, rather than dating the wrong wallet;
//! * an unknown wallet is a `200`, an unknown NFT is a `404`, and a malformed
//!   id is a `400` — three different answers the contract is explicit about;
//! * a disabled collection is invisible to detail, portfolio *and* search.
//!
//! Every address and signature is a synthetic base58 key (CLAUDE.md).

use actix_web::{http::header, test, web, App};
use chrono::{DateTime, TimeZone, Utc};
use indexer_data_model::activity::{self, LiveEvent};
use indexer_data_model::synth::{self, SyntheticSpec};
use indexer_data_model::types::EventKind;
use indexer_data_model::PgPool;
use serde_json::Value;

/// Builds the identical route table the binary serves.
macro_rules! app {
    ($pool:expr) => {
        test::init_service(
            App::new()
                .app_data(web::Data::new($pool.clone()))
                .app_data(web::Data::new(indexer_api::cache::ResponseCache::default()))
                .configure(indexer_api::handlers::configure),
        )
        .await
    };
}

/// A macro rather than a helper function: naming actix's `Service` bound would
/// drag `actix-http` in as a dev-dependency for one type parameter.
macro_rules! get {
    ($app:expr, $uri:expr) => {{
        let resp = test::call_service(&$app, test::TestRequest::get().uri($uri).to_request()).await;
        let status = resp.status().as_u16();
        let body = test::read_body(resp).await;
        let json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        (status, json)
    }};
}

fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

fn sig(seed: u8) -> String {
    bs58::encode([seed; 64]).into_string()
}

fn ts(slot: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_600_000_000 + slot, 0).unwrap()
}

/// A `tm_allowlist` collection, which needs only a verified creator to be
/// enabled — the shape with the fewest columns to invent.
async fn collection(pool: &PgPool, slug: &str, seed: u8) -> i32 {
    sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, enabled) \
         VALUES ($1, 'Synthetic', 'token_metadata', $2, 'SYN', true) RETURNING id",
    )
    .bind(slug)
    .bind(pk(seed))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn asset(pool: &PgPool, collection_id: i32, seed: u8, name: &str) -> (i64, String) {
    let address = pk(seed);
    let id = sqlx::query_scalar(
        "INSERT INTO assets (address, collection_id, name) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(&address)
    .bind(collection_id)
    .bind(name)
    .fetch_one(pool)
    .await
    .unwrap();
    (id, address)
}

/// Writes one event the way the live loop does — its own transaction, through
/// the writer contract, so ownership intervals are derived rather than faked.
#[allow(clippy::too_many_arguments)]
async fn write(
    pool: &PgPool,
    asset_id: i64,
    collection_id: i32,
    signature: &str,
    slot: i64,
    kind: EventKind,
    from: Option<&str>,
    to: Option<&str>,
    price: Option<i64>,
) {
    let mut tx = pool.begin().await.unwrap();
    activity::record(
        &mut tx,
        &LiveEvent {
            asset_id,
            collection_id,
            signature,
            seq: 0,
            slot,
            block_time: ts(slot),
            kind,
            from_owner: from,
            to_owner: to,
            price_lamports: price,
            marketplace: price.map(|_| "Synthetic Venue"),
            details: None,
            source: "backfill",
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// A mint plus `transfers` hand-offs, one of every four priced as a sale.
/// Owners rotate through a small set, so distinct owners are fewer than
/// intervals — which is exactly what `ownerCount` must report.
async fn timeline(pool: &PgPool, asset_id: i64, collection_id: i32, transfers: u8) -> usize {
    let owner = |i: u8| pk(100 + i % 7);
    write(
        pool,
        asset_id,
        collection_id,
        &sig(1),
        1000,
        EventKind::Mint,
        None,
        Some(&owner(0)),
        None,
    )
    .await;
    for i in 0..transfers {
        let (kind, price) = if i % 4 == 3 {
            (EventKind::Sale, Some(1_000_000_000 + i as i64))
        } else {
            (EventKind::Transfer, None)
        };
        write(
            pool,
            asset_id,
            collection_id,
            &sig(2 + i),
            1001 + i as i64,
            kind,
            Some(&owner(i)),
            Some(&owner(i + 1)),
            price,
        )
        .await;
    }
    transfers as usize + 1
}

/// Pages a feed to exhaustion, asserting the contract's pagination invariants
/// on every page, and returns the concatenation.
macro_rules! exhaust {
    ($app:expr, $path:expr, $limit:expr) => {{
        let mut rows: Vec<Value> = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0;
        loop {
            let sep = if $path.contains('?') { '&' } else { '?' };
            let uri = match &cursor {
                Some(c) => format!("{}{sep}limit={}&cursor={c}", $path, $limit),
                None => format!("{}{sep}limit={}", $path, $limit),
            };
            let (status, body) = get!($app, &uri);
            assert_eq!(status, 200, "{uri} -> {body}");
            let envelope = body.get("nfts").unwrap_or(&body).clone();
            rows.extend(envelope["data"].as_array().unwrap().iter().cloned());
            // "Always equal to `nextCursor !== null`. Never computed with a COUNT."
            assert_eq!(
                envelope["hasMore"].as_bool().unwrap(),
                !envelope["nextCursor"].is_null(),
                "hasMore must equal nextCursor != null"
            );
            pages += 1;
            assert!(pages < 200, "runaway pagination");
            match envelope["nextCursor"].as_str() {
                Some(next) => cursor = Some(next.to_string()),
                None => break,
            }
        }
        (rows, pages)
    }};
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_deep_timeline_pages_to_exhaustion_at_every_page_size(pool: PgPool) {
    let cid = collection(&pool, "syn-deep", 1).await;
    let (id, address) = asset(&pool, cid, 2, "#1").await;
    // Deeper than the busiest real 2021 pig (30 events), so the acceptance
    // criterion's "50+ events" is actually exercised.
    let total = timeline(&pool, id, cid, 59).await;
    assert_eq!(total, 60);
    let app = app!(pool);

    for limit in [1, 7, 24, 100] {
        let (rows, pages) = exhaust!(app, format!("/v1/nfts/{address}/activity"), limit);
        assert_eq!(rows.len(), total, "limit={limit} lost or duplicated rows");

        let ids: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), total, "limit={limit} repeated an event");

        // Newest first, strictly: `(slot, id)` descending with no plateau.
        let keys: Vec<(i64, i64)> = rows
            .iter()
            .map(|r| {
                (
                    r["slot"].as_i64().unwrap(),
                    r["id"].as_str().unwrap().parse().unwrap(),
                )
            })
            .collect();
        assert!(
            keys.windows(2).all(|w| w[0] > w[1]),
            "limit={limit} is not strictly newest-first"
        );
        // The page query asks for one row more than it needs, so a feed whose
        // length is a multiple of the page size still ends without a trailing
        // empty page.
        assert_eq!(pages, total.div_ceil(limit as usize), "limit={limit}");
    }
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn the_kind_filter_narrows_and_scopes_the_cursor(pool: PgPool) {
    let cid = collection(&pool, "syn-kinds", 1).await;
    let (id, address) = asset(&pool, cid, 2, "#1").await;
    timeline(&pool, id, cid, 59).await;
    let (other_id, other_address) = asset(&pool, cid, 3, "#2").await;
    timeline(&pool, other_id, cid, 59).await;
    let app = app!(pool);

    let (all, _) = exhaust!(app, format!("/v1/nfts/{address}/activity"), 7);
    let (sales, _) = exhaust!(app, format!("/v1/nfts/{address}/activity?kind=sale"), 7);
    assert!(!sales.is_empty());
    assert_eq!(
        sales.len(),
        all.iter().filter(|e| e["kind"] == "sale").count()
    );
    assert!(sales.iter().all(|e| e["kind"] == "sale"));
    // "Present exactly on `sale`."
    assert!(sales.iter().all(|e| !e["priceLamports"].is_null()));
    assert!(all
        .iter()
        .filter(|e| e["kind"] != "sale")
        .all(|e| e["priceLamports"].is_null() && e["marketplace"].is_null()));

    // Repeating a member is the same request; an unknown one is not a member.
    let (status, _) = get!(
        app,
        &format!("/v1/nfts/{address}/activity?kind=sale&kind=sale")
    );
    assert_eq!(status, 200);
    let (status, body) = get!(app, &format!("/v1/nfts/{address}/activity?kind=listing"));
    assert_eq!(status, 400);
    assert_eq!(body["error"], "invalid_parameter");

    // A cursor is valid only for the feed, filter set and asset that issued it.
    let (_, page) = get!(app, &format!("/v1/nfts/{address}/activity?limit=3"));
    let cursor = page["nextCursor"].as_str().unwrap();
    for other in [
        format!("/v1/nfts/{address}/activity?limit=3&kind=sale&cursor={cursor}"),
        format!("/v1/nfts/{other_address}/activity?limit=3&cursor={cursor}"),
        format!("/v1/nfts/{address}/owners?limit=3&cursor={cursor}"),
        format!("/v1/collections/syn-kinds/activity?limit=3&cursor={cursor}"),
    ] {
        let (status, body) = get!(app, &other);
        assert_eq!(status, 400, "{other} must not accept a foreign cursor");
        assert_eq!(body["error"], "invalid_cursor");
    }
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn ownership_history_is_newest_first_with_exactly_one_open_interval(pool: PgPool) {
    let cid = collection(&pool, "syn-owners", 1).await;
    let (id, address) = asset(&pool, cid, 2, "#1").await;
    timeline(&pool, id, cid, 59).await;
    let app = app!(pool);

    let (rows, _) = exhaust!(app, format!("/v1/nfts/{address}/owners"), 4);
    assert_eq!(rows.len(), 60, "one interval per ownership change");
    let slots: Vec<i64> = rows
        .iter()
        .map(|r| r["fromSlot"].as_i64().unwrap())
        .collect();
    assert!(slots.windows(2).all(|w| w[0] > w[1]), "newest first");
    assert_eq!(rows.iter().filter(|r| r["isCurrent"] == true).count(), 1);
    // `isCurrent` is exactly "the interval is open".
    assert!(rows.iter().all(|r| r["isCurrent"] == r["toSlot"].is_null()));
    assert!(rows[0]["isCurrent"] == true && rows[0]["closedBySignature"].is_null());

    // `ownerCount` counts distinct wallets, not intervals: the fixture rotates
    // seven owners through sixty hand-offs.
    let (_, detail) = get!(app, &format!("/v1/nfts/{address}"));
    let distinct: std::collections::HashSet<&str> =
        rows.iter().map(|r| r["owner"].as_str().unwrap()).collect();
    assert_eq!(
        detail["activitySummary"]["ownerCount"].as_u64().unwrap() as usize,
        distinct.len()
    );
    assert!(distinct.len() < rows.len(), "the fixture must reuse owners");
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn held_since_is_null_when_the_history_disagrees_with_the_observed_owner(pool: PgPool) {
    let cid = collection(&pool, "syn-drift", 1).await;
    let (id, address) = asset(&pool, cid, 2, "#1").await;
    timeline(&pool, id, cid, 3).await;
    let app = app!(pool);

    let (_, agreeing) = get!(app, &format!("/v1/nfts/{address}"));
    assert!(!agreeing["ownership"]["heldSince"].is_null());
    assert!(!agreeing["ownership"]["acquiredBySignature"].is_null());

    // A DAS observation moves the owner ahead of the derived history — exactly
    // what `integrity_owner_mismatch` reports and ALG-624 heals.
    sqlx::query("UPDATE assets SET owner = $2, owner_slot = 9999 WHERE id = $1")
        .bind(id)
        .bind(pk(200))
        .execute(&pool)
        .await
        .unwrap();
    let mismatched: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM integrity_owner_mismatch WHERE asset_id = $1)",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(mismatched, "the fixture must actually disagree");

    let app = app!(pool);
    let (_, drifted) = get!(app, &format!("/v1/nfts/{address}"));
    let ownership = &drifted["ownership"];
    // The observed owner is still reported; only the dates are withheld.
    assert_eq!(ownership["owner"], pk(200));
    assert_eq!(ownership["ownerSlot"], 9999);
    assert!(ownership["heldSince"].is_null());
    assert!(ownership["heldSinceSlot"].is_null());
    assert!(ownership["acquiredBySignature"].is_null());
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn burned_removed_and_uncrawled_assets_are_still_valid_pages(pool: PgPool) {
    let cid = collection(&pool, "syn-edges", 1).await;
    let app = app!(pool);

    // Burned: the interval closes, the owner clears, the page survives.
    let (burned_id, burned) = asset(&pool, cid, 2, "#1").await;
    timeline(&pool, burned_id, cid, 3).await;
    write(
        &pool,
        burned_id,
        cid,
        &sig(90),
        2000,
        EventKind::Burn,
        Some(&pk(103)),
        None,
        None,
    )
    .await;
    let (status, body) = get!(app, &format!("/v1/nfts/{burned}"));
    assert_eq!(status, 200);
    assert_eq!(body["burned"], true);
    assert!(body["owner"].is_null());
    assert!(body["ownership"]["owner"].is_null() && body["ownership"]["heldSince"].is_null());
    assert!(
        !body["mint"]["mintedAt"].is_null(),
        "the mint event survives a burn"
    );
    let (owners, _) = exhaust!(app, format!("/v1/nfts/{burned}/owners"), 24);
    assert!(
        owners.iter().all(|o| o["isCurrent"] == false),
        "a burned asset has no open interval"
    );

    // Removed from its collection: still addressable, still reports history.
    let (_, removed) = asset(&pool, cid, 3, "#2").await;
    sqlx::query(
        "UPDATE assets SET membership_status = 'removed', removed_at = now() WHERE address = $1",
    )
    .bind(&removed)
    .execute(&pool)
    .await
    .unwrap();
    let (status, body) = get!(app, &format!("/v1/nfts/{removed}"));
    assert_eq!(status, 200);
    assert_eq!(body["membershipStatus"], "removed");
    assert!(!body["removedAt"].is_null());

    // Never crawled: `mint` is an object of nulls, not null, and both feeds are
    // empty pages rather than 404s.
    let (_, fresh) = asset(&pool, cid, 4, "#3").await;
    let (status, body) = get!(app, &format!("/v1/nfts/{fresh}"));
    assert_eq!(status, 200);
    assert!(body["mint"].is_object());
    for field in ["mintedAt", "mintSlot", "signature"] {
        assert!(body["mint"][field].is_null(), "{field} must be null");
    }
    assert_eq!(body["activitySummary"]["salesCount"], 0);
    assert_eq!(body["activitySummary"]["ownerCount"], 0);
    for feed in ["activity", "owners"] {
        let (status, body) = get!(app, &format!("/v1/nfts/{fresh}/{feed}"));
        assert_eq!(status, 200);
        assert_eq!(body["data"].as_array().unwrap().len(), 0);
        assert_eq!(body["hasMore"], false);
        assert!(body["nextCursor"].is_null());
    }
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn an_unknown_id_is_404_and_a_malformed_one_is_400(pool: PgPool) {
    let cid = collection(&pool, "syn-ids", 1).await;
    asset(&pool, cid, 2, "#1").await;
    let app = app!(pool);

    // Well-formed but not indexed: we looked, and it is not here.
    for path in ["", "/activity", "/owners"] {
        let (status, body) = get!(app, &format!("/v1/nfts/{}{path}", pk(250)));
        assert_eq!(status, 404, "{path}");
        assert_eq!(body["error"], "not_found");
    }
    // Malformed: never a 404, which would claim we looked.
    for bad in ["not-an-address", "0OIl0OIl0OIl0OIl0OIl0OIl0OIl0OIl", "abc"] {
        let (status, body) = get!(app, &format!("/v1/nfts/{bad}"));
        assert_eq!(status, 400, "{bad}");
        assert_eq!(body["error"], "invalid_parameter");
        assert_eq!(body["details"]["parameter"], "id");
    }
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_portfolio_groups_by_collection_and_the_filter_narrows_only_the_grid(pool: PgPool) {
    let a = collection(&pool, "syn-one", 1).await;
    let b = collection(&pool, "syn-two", 2).await;
    let owner = pk(120);
    for (cid, count, seed) in [(a, 5u8, 10u8), (b, 3, 40)] {
        for i in 0..count {
            let (id, _) = asset(&pool, cid, seed + i, &format!("#{}", i + 1)).await;
            sqlx::query("UPDATE assets SET owner = $2, owner_slot = 1 WHERE id = $1")
                .bind(id)
                .bind(&owner)
                .execute(&pool)
                .await
                .unwrap();
        }
    }
    let app = app!(pool);

    let (_, body) = get!(app, &format!("/v1/wallets/{owner}/nfts?limit=100"));
    assert_eq!(body["address"], owner);
    assert_eq!(body["totalCount"], 8);
    assert_eq!(body["badges"].as_array().unwrap().len(), 0);
    let groups = body["collections"].as_array().unwrap();
    assert_eq!(groups.len(), 2);
    // Ordered by count descending, and never a zero — `count` has minimum 1.
    assert_eq!(groups[0]["collection"]["slug"], "syn-one");
    assert_eq!(groups[0]["count"], 5);
    assert_eq!(groups[1]["count"], 3);
    assert!(groups.iter().all(|g| g["count"].as_i64().unwrap() >= 1));
    // Sole holder of both, so rank 1 in each.
    assert!(groups.iter().all(|g| g["holderRank"] == 1));

    // The filter narrows the grid and nothing else.
    let (_, filtered) = get!(
        app,
        &format!("/v1/wallets/{owner}/nfts?limit=100&collection=syn-two")
    );
    assert_eq!(filtered["totalCount"], 8, "totalCount ignores ?collection=");
    assert_eq!(filtered["collections"].as_array().unwrap().len(), 2);
    let cards = filtered["nfts"]["data"].as_array().unwrap();
    assert_eq!(cards.len(), 3);
    assert!(cards.iter().all(|c| c["collection"]["slug"] == "syn-two"));

    // An unknown slug is a 200 with an empty grid, not a 404: unknown filter
    // input never becomes an error.
    let (status, unknown) = get!(
        app,
        &format!("/v1/wallets/{owner}/nfts?collection=no-such-collection")
    );
    assert_eq!(status, 200);
    assert_eq!(unknown["totalCount"], 8);
    assert_eq!(unknown["nfts"]["data"].as_array().unwrap().len(), 0);

    // Paged to exhaustion the grid is the whole portfolio, ordered by
    // collection then id, with no repeats.
    let (rows, pages) = exhaust!(app, format!("/v1/wallets/{owner}/nfts"), 3);
    assert_eq!(rows.len(), 8);
    assert!(pages > 1);
    let addresses: std::collections::HashSet<&str> = rows
        .iter()
        .map(|r| r["address"].as_str().unwrap())
        .collect();
    assert_eq!(addresses.len(), 8);
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn an_unknown_wallet_is_an_empty_200_and_a_malformed_one_is_400(pool: PgPool) {
    collection(&pool, "syn-empty", 1).await;
    let app = app!(pool);

    let (status, body) = get!(app, &format!("/v1/wallets/{}/nfts", pk(250)));
    assert_eq!(status, 200, "an unknown wallet is never a 404");
    assert_eq!(body["address"], pk(250));
    assert_eq!(body["totalCount"], 0);
    assert_eq!(body["collections"].as_array().unwrap().len(), 0);
    assert_eq!(body["badges"].as_array().unwrap().len(), 0);
    assert_eq!(body["nfts"]["data"].as_array().unwrap().len(), 0);
    assert!(body["nfts"]["nextCursor"].is_null());
    assert_eq!(body["nfts"]["hasMore"], false);

    let (status, body) = get!(app, "/v1/wallets/nope/nfts");
    assert_eq!(status, 400);
    assert_eq!(body["details"]["parameter"], "address");
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn holder_ranks_tie_and_agree_with_the_portfolio(pool: PgPool) {
    let cid = collection(&pool, "syn-holders", 1).await;
    // Two wallets hold two each and one holds one: ranks are 1, 1, 3 —
    // "ties share the lower rank and skip the next values".
    let owners = [pk(130), pk(131), pk(132)];
    let counts = [2u8, 2, 1];
    let mut seed = 10u8;
    for (owner, count) in owners.iter().zip(counts) {
        for i in 0..count {
            let (id, _) = asset(&pool, cid, seed, &format!("#{i}")).await;
            seed += 1;
            sqlx::query("UPDATE assets SET owner = $2, owner_slot = 1 WHERE id = $1")
                .bind(id)
                .bind(owner)
                .execute(&pool)
                .await
                .unwrap();
        }
    }
    let app = app!(pool);

    let (status, body) = get!(app, "/v1/collections/syn-holders/holders");
    assert_eq!(status, 200);
    let holders = body["data"].as_array().unwrap();
    assert_eq!(holders.len(), 3);
    assert!(body.get("nextCursor").is_none(), "top-N, not a page");
    let ranks: Vec<i64> = holders
        .iter()
        .map(|h| h["rank"].as_i64().unwrap())
        .collect();
    assert_eq!(ranks, vec![1, 1, 3], "RANK(), not DENSE_RANK()");
    // The counts sum to `stats.supply` — burned assets have no owner.
    let (_, collection) = get!(app, "/v1/collections/syn-holders");
    let total: i64 = holders.iter().map(|h| h["count"].as_i64().unwrap()).sum();
    assert_eq!(total, collection["stats"]["supply"].as_i64().unwrap());

    // The same wallet's rank in its portfolio comes from the same window.
    for holder in holders {
        let address = holder["address"].as_str().unwrap();
        let (_, portfolio) = get!(app, &format!("/v1/wallets/{address}/nfts"));
        let group = &portfolio["collections"][0];
        assert_eq!(group["collection"]["slug"], "syn-holders");
        assert_eq!(group["count"], holder["count"]);
        assert_eq!(group["holderRank"], holder["rank"]);
    }

    let (status, _) = get!(app, "/v1/collections/syn-holders/holders?limit=101");
    assert_eq!(status, 400, "an over-large limit is never clamped");
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_collection_feed_carries_its_cards_newest_first(pool: PgPool) {
    let cid = collection(&pool, "syn-feed", 1).await;
    let (first, _) = asset(&pool, cid, 2, "#1").await;
    let (second, _) = asset(&pool, cid, 3, "#2").await;
    timeline(&pool, first, cid, 7).await;
    timeline(&pool, second, cid, 5).await;
    let app = app!(pool);

    let (rows, pages) = exhaust!(app, "/v1/collections/syn-feed/activity", 5);
    assert_eq!(rows.len(), 8 + 6);
    assert!(pages > 1);
    let keys: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| {
            (
                r["slot"].as_i64().unwrap(),
                r["id"].as_str().unwrap().parse().unwrap(),
            )
        })
        .collect();
    assert!(keys.windows(2).all(|w| w[0] > w[1]), "newest first");
    // Each event carries the card it happened to, so a strip needs no second
    // request.
    assert!(rows
        .iter()
        .all(|r| r["nft"]["collection"]["slug"] == "syn-feed"));
    assert!(rows
        .iter()
        .all(|r| r["nft"]["address"].is_string() && r["nft"]["rarityRank"].is_null()));

    let (mints, _) = exhaust!(app, "/v1/collections/syn-feed/activity?kind=mint", 5);
    assert_eq!(mints.len(), 2, "the latest-mints strip");
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn search_routes_addresses_and_groups_text(pool: PgPool) {
    let a = collection(&pool, "syn-search", 1).await;
    let b = collection(&pool, "syn-other", 2).await;
    let (nft_id, nft) = asset(&pool, a, 10, "Piggy #1").await;
    asset(&pool, a, 11, "Piggy #2").await;
    asset(&pool, b, 12, "Piggy #1").await;
    let owner = pk(140);
    sqlx::query("UPDATE assets SET owner = $2, owner_slot = 1 WHERE id = $1")
        .bind(nft_id)
        .bind(&owner)
        .execute(&pool)
        .await
        .unwrap();
    let app = app!(pool);

    // A pasted mint routes straight to the NFT, and never produces groups.
    let (status, body) = get!(app, &format!("/v1/search?q={nft}"));
    assert_eq!(status, 200);
    assert_eq!(body["interpretedAs"], "address");
    assert_eq!(body["route"]["kind"], "nft");
    assert_eq!(body["route"]["id"], nft);
    assert!(body["wallet"].is_null());
    assert_eq!(body["groups"].as_array().unwrap().len(), 0);

    // A wallet with holdings beats nothing; a wallet with none is not a hit.
    let (_, body) = get!(app, &format!("/v1/search?q={owner}"));
    assert_eq!(body["route"]["kind"], "wallet");
    assert_eq!(body["wallet"]["totalCount"], 1);
    let (_, body) = get!(app, &format!("/v1/search?q={}", pk(250)));
    assert_eq!(body["interpretedAs"], "address");
    assert!(body["route"].is_null() && body["wallet"].is_null());
    assert_eq!(body["groups"].as_array().unwrap().len(), 0);

    // `#N` is the token number, grouped by collection, most hits first.
    let (_, body) = get!(app, "/v1/search?q=%231");
    assert_eq!(body["interpretedAs"], "number");
    assert!(body["route"].is_null());
    let groups = body["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 2);
    assert!(groups
        .iter()
        .all(|g| g["total"] == 1 && g["nfts"].as_array().unwrap().len() == 1));

    // Anything else is a name substring; an exact slug also routes.
    let (_, body) = get!(app, "/v1/search?q=Piggy");
    assert_eq!(body["interpretedAs"], "text");
    let groups = body["groups"].as_array().unwrap();
    assert_eq!(groups[0]["collection"]["slug"], "syn-search");
    assert_eq!(
        groups[0]["total"], 2,
        "the exact match count, not the preview"
    );
    let (_, body) = get!(app, "/v1/search?q=syn-other");
    assert_eq!(body["route"]["kind"], "collection");
    assert_eq!(body["route"]["id"], "syn-other");

    // Nothing indexed is a 200 with an empty result, never a 404.
    let (status, body) = get!(app, "/v1/search?q=nothing-matches-this");
    assert_eq!(status, 200);
    assert_eq!(body["groups"].as_array().unwrap().len(), 0);
    // `q` is the one required parameter in the document.
    let (status, body) = get!(app, "/v1/search");
    assert_eq!(status, 400);
    assert_eq!(body["details"]["parameter"], "q");
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_disabled_collection_is_invisible_to_every_detail_endpoint(pool: PgPool) {
    // `synth` builds its collections disabled — they are benchmark fixtures,
    // and this is exactly the leak the API must not have.
    let report = synth::seed_synthetic(
        &pool,
        &SyntheticSpec {
            slug: "bench-hidden".into(),
            name: "Bench".into(),
            assets: 40,
            unique_trait: false,
            seed: 0.21,
        },
    )
    .await
    .unwrap();
    let hidden: (String, Option<String>) = sqlx::query_as(
        "SELECT address, owner FROM assets WHERE collection_id = $1 AND owner IS NOT NULL LIMIT 1",
    )
    .bind(report.collection_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let (address, owner) = (hidden.0, hidden.1.unwrap());
    let app = app!(pool);

    for path in ["", "/activity", "/owners"] {
        let (status, _) = get!(app, &format!("/v1/nfts/{address}{path}"));
        assert_eq!(
            status, 404,
            "a disabled collection's asset must not be served"
        );
    }
    for path in ["/activity", "/holders"] {
        let (status, _) = get!(app, &format!("/v1/collections/bench-hidden{path}"));
        assert_eq!(status, 404);
    }
    let (status, portfolio) = get!(app, &format!("/v1/wallets/{owner}/nfts"));
    assert_eq!(status, 200);
    assert_eq!(portfolio["totalCount"], 0, "holdings there do not count");
    assert_eq!(portfolio["collections"].as_array().unwrap().len(), 0);

    let (_, search) = get!(app, &format!("/v1/search?q={address}"));
    assert!(search["route"].is_null(), "search must not route to it");
    let (_, search) = get!(app, "/v1/search?q=%231");
    assert_eq!(search["groups"].as_array().unwrap().len(), 0);
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn every_detail_endpoint_validates_with_an_etag(pool: PgPool) {
    let cid = collection(&pool, "syn-etag", 1).await;
    let (id, address) = asset(&pool, cid, 2, "#1").await;
    timeline(&pool, id, cid, 3).await;
    sqlx::query("UPDATE assets SET owner = $2, owner_slot = 1 WHERE id = $1")
        .bind(id)
        .bind(pk(103))
        .execute(&pool)
        .await
        .unwrap();
    let app = app!(pool);

    for uri in [
        format!("/v1/nfts/{address}"),
        format!("/v1/nfts/{address}/activity"),
        format!("/v1/nfts/{address}/owners"),
        format!("/v1/wallets/{}/nfts", pk(103)),
        "/v1/collections/syn-etag/activity".into(),
        "/v1/collections/syn-etag/holders".into(),
        "/v1/search?q=%231".into(),
    ] {
        let first = test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;
        assert_eq!(first.status().as_u16(), 200, "{uri}");
        let etag = first
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(etag.starts_with("W/\""), "{uri}: {etag}");

        let second = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&uri)
                .insert_header((header::IF_NONE_MATCH, etag.clone()))
                .to_request(),
        )
        .await;
        assert_eq!(second.status().as_u16(), 304, "{uri}");
        // The validators are repeated on the 304 so a client can refresh its
        // freshness window without a second round trip.
        assert_eq!(second.headers().get(header::ETAG).unwrap(), etag.as_str());
        assert!(second.headers().contains_key(header::CACHE_CONTROL));
        assert!(second.headers().contains_key(header::VARY));
        assert!(test::read_body(second).await.is_empty());
    }
}
