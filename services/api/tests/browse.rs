//! The browse endpoints against a real database (ALG-625).
//!
//! Prism's mock and proxy prove the *shape* of a response, but Prism ignores
//! the `trait` parameter entirely — the README says so — which means **filter
//! wiring is only ever testable here**. These tests cover what the contract
//! promises and a schema check cannot see: AND across trait types and OR
//! within one, the two different unknown-input behaviours, cursor validity,
//! and the rule that the grid and the sidebar can never disagree.
//!
//! Data comes from `synth`, the same generator the facet benchmark uses, so no
//! on-chain address appears here.

use actix_web::{http::header, test, web, App};
use indexer_data_model::synth::{self, SyntheticSpec};
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

/// Seeds a synthetic collection and enables it — `synth` creates them disabled
/// (they are benchmark fixtures) and the API only serves enabled rows.
async fn collection(pool: &PgPool, slug: &str, assets: i64) -> i32 {
    let report = synth::seed_synthetic(
        pool,
        &SyntheticSpec {
            slug: slug.to_string(),
            name: "Bench".into(),
            assets,
            unique_trait: true,
            seed: 0.21,
        },
    )
    .await
    .unwrap();
    // `synth` builds a Core collection with no address, and
    // `collections_enabled_resolvable` refuses to enable a row that cannot
    // resolve a membership rule — so enabling one means giving it the address
    // a real Core collection would have. Synthetic, per CLAUDE.md.
    sqlx::query("UPDATE collections SET enabled = true, address = $2 WHERE id = $1")
        .bind(report.collection_id)
        .bind(bs58::encode([7u8; 32]).into_string())
        .execute(pool)
        .await
        .unwrap();
    report.collection_id
}

/// `(trait type, its two most common values, another trait type)`.
async fn sample_values(pool: &PgPool, collection_id: i32) -> (String, String, String, String) {
    let counts = indexer_data_model::facets::facet_counts(pool, collection_id)
        .await
        .unwrap();
    let first_type = counts[0].trait_type.clone();
    let values: Vec<String> = counts
        .iter()
        .filter(|c| c.trait_type == first_type)
        .map(|c| c.value.clone())
        .take(2)
        .collect();
    let second = counts
        .iter()
        .find(|c| c.trait_type != first_type)
        .expect("a second trait type");
    (
        first_type,
        values[0].clone(),
        values[1].clone(),
        second.trait_type.clone(),
    )
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn values_or_within_a_type_and_types_and_across(pool: PgPool) {
    let id = collection(&pool, "bench-api-andor", 600).await;
    let app = app!(pool);
    let (a, v1, v2, b) = sample_values(&pool, id).await;
    let base = "/v1/collections/bench-api-andor/facets";

    let (_, one) = get!(app, &format!("{base}?trait[{a}]={v1}"));
    let (_, two) = get!(app, &format!("{base}?trait[{a}]={v1}&trait[{a}]={v2}"));
    assert!(
        two["total"].as_i64().unwrap() > one["total"].as_i64().unwrap(),
        "a second value of the same trait type must OR in more assets"
    );

    let (_, all) = get!(app, base);
    let b_value = all["facets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["traitType"] == b.as_str())
        .unwrap()["values"][0]["value"]
        .as_str()
        .unwrap()
        .to_string();
    let (_, crossed) = get!(app, &format!("{base}?trait[{a}]={v1}&trait[{b}]={b_value}"));
    assert!(
        crossed["total"].as_i64().unwrap() <= one["total"].as_i64().unwrap(),
        "a second trait type must AND, never OR"
    );
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn unknown_filter_input_is_never_an_error(pool: PgPool) {
    let id = collection(&pool, "bench-api-unknown", 400).await;
    let app = app!(pool);
    let (a, _, _, _) = sample_values(&pool, id).await;

    // An unknown trait TYPE: nothing can match and the sidebar has nothing to
    // show — but a bookmarked URL must still load.
    let (status, body) = get!(
        app,
        "/v1/collections/bench-api-unknown/facets?trait[Nope]=x"
    );
    assert_eq!(status, 200);
    assert_eq!(body["total"], 0);
    assert!(body["facets"].as_array().unwrap().is_empty());

    let (status, page) = get!(app, "/v1/collections/bench-api-unknown/nfts?trait[Nope]=x");
    assert_eq!(status, 200);
    assert!(page["data"].as_array().unwrap().is_empty());
    assert_eq!(page["hasMore"], false);

    // An unknown VALUE is different: the type still counts as selected, so it
    // matches nothing and every other type collapses — but its own values stay
    // visible, which is what lets the user correct the selection.
    let (status, body) = get!(
        app,
        &format!("/v1/collections/bench-api-unknown/facets?trait[{a}]=NoSuchValue")
    );
    assert_eq!(status, 200);
    assert_eq!(body["total"], 0);
    assert!(
        !body["facets"].as_array().unwrap().is_empty(),
        "an unknown value still leaves its own type's values visible"
    );
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn the_grid_and_the_sidebar_never_disagree(pool: PgPool) {
    let id = collection(&pool, "bench-api-total", 500).await;
    let app = app!(pool);
    let (a, v1, v2, _) = sample_values(&pool, id).await;
    let filter = format!("trait[{a}]={v1}&trait[{a}]={v2}");

    let (_, facets) = get!(
        app,
        &format!("/v1/collections/bench-api-total/facets?{filter}")
    );
    let total = facets["total"].as_i64().unwrap();

    // Page the grid to exhaustion under the same filters: keyset paging must
    // be exact, never skipping a row and never repeating one.
    let mut seen = std::collections::BTreeSet::new();
    let mut cursor: Option<String> = None;
    for _ in 0..50 {
        let uri = match &cursor {
            Some(c) => format!("/v1/collections/bench-api-total/nfts?{filter}&limit=25&cursor={c}"),
            None => format!("/v1/collections/bench-api-total/nfts?{filter}&limit=25"),
        };
        let (_, page) = get!(app, &uri);
        for card in page["data"].as_array().unwrap() {
            assert!(
                seen.insert(card["address"].as_str().unwrap().to_string()),
                "keyset paging must never return the same asset twice"
            );
        }
        assert_eq!(page["hasMore"], page["nextCursor"] != Value::Null);
        match page["nextCursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    assert_eq!(
        total,
        seen.len() as i64,
        "facets.total is exactly the number of cards the grid yields"
    );
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_cursor_is_valid_only_for_the_sort_and_filters_that_issued_it(pool: PgPool) {
    collection(&pool, "bench-api-cursor", 200).await;
    let app = app!(pool);

    let (_, page) = get!(
        app,
        "/v1/collections/bench-api-cursor/nfts?limit=5&sort=name"
    );
    let cursor = page["nextCursor"].as_str().unwrap().to_string();
    assert!(
        cursor
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            && (8..=512).contains(&cursor.len()),
        "the contract types a cursor as ^[A-Za-z0-9_-]{{8,512}}$: {cursor}"
    );

    let (status, _) = get!(
        app,
        &format!("/v1/collections/bench-api-cursor/nfts?limit=5&sort=name&cursor={cursor}")
    );
    assert_eq!(status, 200, "the same sort continues the scan");

    // A different sort must be refused rather than silently returning rows
    // from the wrong ordering.
    let (status, body) = get!(
        app,
        &format!("/v1/collections/bench-api-cursor/nfts?limit=5&sort=number&cursor={cursor}")
    );
    assert_eq!(status, 400);
    assert_eq!(body["error"], "invalid_cursor");
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn validators_and_the_error_shape(pool: PgPool) {
    collection(&pool, "bench-api-errors", 100).await;
    let app = app!(pool);
    let uri = "/v1/collections/bench-api-errors/facets";

    let resp = test::call_service(&app, test::TestRequest::get().uri(uri).to_request()).await;
    assert_eq!(resp.status(), 200);
    let etag = resp
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        etag.starts_with("W/\""),
        "the contract's ETag is weak: {etag}"
    );
    assert!(resp.headers().contains_key(header::CACHE_CONTROL));

    // 304 repeats the validators, so a client can refresh its freshness window
    // without a second round trip.
    let resp = test::call_service(
        &app,
        test::TestRequest::get()
            .uri(uri)
            .insert_header((header::IF_NONE_MATCH, etag.clone()))
            .to_request(),
    )
    .await;
    assert_eq!(resp.status(), 304);
    assert_eq!(resp.headers().get(header::ETAG).unwrap(), etag.as_str());
    assert!(resp.headers().contains_key(header::CACHE_CONTROL));

    // Reserved sorts are 422 with the supported list — not 400, and never a
    // silent fallback to the default.
    let (status, body) = get!(app, "/v1/collections/bench-api-errors/nfts?sort=rarity");
    assert_eq!(status, 422);
    assert_eq!(body["error"], "unsupported_sort");
    assert!(body["details"]["supported"].is_array());

    // An over-large limit is refused, never clamped.
    let (status, body) = get!(app, "/v1/collections/bench-api-errors/nfts?limit=101");
    assert_eq!(status, 422);
    assert_eq!(body["error"], "invalid_parameter");

    // Every error body is the contract's closed three-key shape.
    for uri in [
        "/v1/collections/bench-api-errors/nfts?sort=nope",
        "/v1/collections/does-not-exist",
    ] {
        let (_, body) = get!(app, uri);
        let object = body.as_object().unwrap();
        assert_eq!(object.len(), 3, "{uri}: {body}");
        for key in ["error", "message", "details"] {
            assert!(object.contains_key(key), "{uri} is missing {key}");
        }
    }
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_disabled_collection_is_not_served(pool: PgPool) {
    // `synth` leaves collections disabled, which is also how every `bench-*`
    // fixture sits in a production database — they must not be browsable.
    synth::seed_synthetic(
        &pool,
        &SyntheticSpec {
            slug: "bench-api-hidden".into(),
            name: "Hidden".into(),
            assets: 50,
            unique_trait: false,
            seed: 0.3,
        },
    )
    .await
    .unwrap();
    let app = app!(pool);

    for uri in [
        "/v1/collections/bench-api-hidden",
        "/v1/collections/bench-api-hidden/nfts",
        "/v1/collections/bench-api-hidden/facets",
    ] {
        let (status, body) = get!(app, uri);
        assert_eq!(status, 404, "{uri} must not serve a disabled collection");
        assert_eq!(body["error"], "not_found");
    }
    let (_, listing) = get!(app, "/v1/collections");
    assert!(listing["data"].as_array().unwrap().is_empty());
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn facets_are_ordered_and_exclude_what_the_registry_excludes(pool: PgPool) {
    collection(&pool, "bench-api-order", 300).await;
    let app = app!(pool);
    let (_, body) = get!(app, "/v1/collections/bench-api-order/facets");
    let facets = body["facets"].as_array().unwrap();

    let names: Vec<&str> = facets
        .iter()
        .map(|f| f["traitType"].as_str().unwrap())
        .collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "facets are ordered by trait type name");
    assert!(
        !names.contains(&synth::UNIQUE_TRAIT),
        "a per-asset-unique trait is excluded by facet_exclude, never faceted"
    );

    for facet in facets {
        let values = facet["values"].as_array().unwrap();
        let keys: Vec<(i64, &str)> = values
            .iter()
            .map(|v| (-v["count"].as_i64().unwrap(), v["value"].as_str().unwrap()))
            .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "values are ordered by count desc, then value");
        assert!(
            values.iter().all(|v| v["count"].as_i64().unwrap() > 0),
            "zero-count values are omitted"
        );
    }
}
