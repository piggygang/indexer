//! The webhook inbox's queue semantics, proven against Postgres with no
//! network. Every address is a synthetic base58 key (CLAUDE.md).
//! Ignored without a database: `cargo test --workspace -- --include-ignored`.

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use indexer_data_model::webhook_inbox::{self, Delivery};
use indexer_data_model::PgPool;
use serde_json::json;

fn sig(seed: u8) -> String {
    bs58::encode([seed; 64]).into_string()
}

fn ts(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
}

fn delivery(seed: u8, slot: i64) -> Delivery {
    Delivery {
        signature: sig(seed),
        slot,
        block_time: Some(ts(slot)),
        failed: false,
        body: json!({ "slot": slot, "meta": { "err": null } }),
    }
}

const LEASE: Duration = Duration::from_secs(60);
const NO_GRACE: Duration = Duration::from_secs(0);
const MAX_ATTEMPTS: i16 = 5;

async fn pending(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM webhook_inbox WHERE processed_at IS NULL")
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Helius warns that it redelivers, and its own FAQ says a duplicate is
/// expected rather than exceptional. One signature, one row — which is what
/// lets the receiver answer 200 to a retry without thinking about it.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_redelivered_signature_is_absorbed(pool: PgPool) {
    let first = webhook_inbox::enqueue(&pool, &[delivery(1, 100), delivery(2, 101)])
        .await
        .unwrap();
    assert_eq!(first, 2);

    // The same batch again, plus one genuinely new row.
    let second = webhook_inbox::enqueue(
        &pool,
        &[delivery(1, 100), delivery(2, 101), delivery(3, 102)],
    )
    .await
    .unwrap();
    assert_eq!(second, 1, "only the new signature counts");
    assert_eq!(pending(&pool).await, 3);
}

/// A claim is exclusive for the length of its lease, and expires so a process
/// killed mid-batch cannot strand its rows.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_claim_is_exclusive_until_its_lease_expires(pool: PgPool) {
    webhook_inbox::enqueue(&pool, &[delivery(1, 100), delivery(2, 101)])
        .await
        .unwrap();

    let claimed = webhook_inbox::claim(&pool, 10, LEASE, MAX_ATTEMPTS)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[0].attempts, 1);

    // A second drain (the overlapping container during a rolling deploy) sees
    // nothing while the lease holds.
    let concurrent = webhook_inbox::claim(&pool, 10, LEASE, MAX_ATTEMPTS)
        .await
        .unwrap();
    assert!(
        concurrent.is_empty(),
        "a live claim must not be handed out twice"
    );

    // A zero-length lease is the expired case: the rows come back, and the
    // attempt counter carries forward rather than resetting.
    let after_expiry = webhook_inbox::claim(&pool, 10, Duration::from_secs(0), MAX_ATTEMPTS)
        .await
        .unwrap();
    assert_eq!(after_expiry.len(), 2);
    assert_eq!(after_expiry[0].attempts, 2);
}

/// Claiming is oldest-first, so a backlog drains in arrival order rather than
/// starving its own tail.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn claiming_is_oldest_first_and_bounded(pool: PgPool) {
    let batch: Vec<Delivery> = (1..=5).map(|i| delivery(i, 100 + i as i64)).collect();
    webhook_inbox::enqueue(&pool, &batch).await.unwrap();

    let first = webhook_inbox::claim(&pool, 2, LEASE, MAX_ATTEMPTS)
        .await
        .unwrap();
    assert_eq!(first.len(), 2, "the limit is honoured");
    assert_eq!(first[0].signature, sig(1));
    assert_eq!(first[1].signature, sig(2));
}

/// A row `getTransaction` will never return must not be re-claimed forever —
/// it would also pin the watermark forever, freezing the cursor behind one bad
/// delivery.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_poison_row_stops_being_claimed_at_the_cap(pool: PgPool) {
    webhook_inbox::enqueue(&pool, &[delivery(1, 100)])
        .await
        .unwrap();

    for attempt in 1..=MAX_ATTEMPTS {
        let claimed = webhook_inbox::claim(&pool, 10, Duration::from_secs(0), MAX_ATTEMPTS)
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "attempt {attempt} should still be claimable"
        );
        webhook_inbox::release(&pool, claimed[0].id, "getTransaction returned null")
            .await
            .unwrap();
    }

    let over = webhook_inbox::claim(&pool, 10, Duration::from_secs(0), MAX_ATTEMPTS)
        .await
        .unwrap();
    assert!(over.is_empty(), "the cap stops the retry loop");

    // Retiring it is what actually releases the watermark; the cap alone
    // leaves the row pending forever.
    assert_eq!(pending(&pool).await, 1);
    let id: i64 = sqlx::query_scalar("SELECT id FROM webhook_inbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    webhook_inbox::mark_processed(&pool, id, Some("gave up"))
        .await
        .unwrap();
    assert_eq!(pending(&pool).await, 0);
}

/// The checkpoint invariant: "no events with a lower slot will follow".
///
/// The watermark must never pass a row we still owe, and must say nothing at
/// all rather than say `0` — a persisted `0` would make `seed_cursor` return
/// `Some(0)` and turn every candidate recovery into a full archival crawl.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn the_watermark_never_passes_a_pending_row(pool: PgPool) {
    // Nothing at all: no claim to make.
    assert_eq!(
        webhook_inbox::watermark(&pool, NO_GRACE).await.unwrap(),
        None,
        "an empty inbox says nothing rather than 0"
    );

    webhook_inbox::enqueue(
        &pool,
        &[delivery(1, 100), delivery(2, 200), delivery(3, 300)],
    )
    .await
    .unwrap();

    // All pending: the highest safe claim is below the oldest one we owe.
    assert_eq!(
        webhook_inbox::watermark(&pool, NO_GRACE).await.unwrap(),
        Some(99)
    );

    // Retire the middle one only. The oldest is still pending, so the
    // watermark must not move past it even though a *higher* slot is done.
    let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM webhook_inbox ORDER BY slot")
        .fetch_all(&pool)
        .await
        .unwrap();
    webhook_inbox::mark_processed(&pool, ids[1], None)
        .await
        .unwrap();
    assert_eq!(
        webhook_inbox::watermark(&pool, NO_GRACE).await.unwrap(),
        Some(99),
        "a processed higher slot cannot pull the cursor past a pending lower one"
    );

    // Once everything is retired, the watermark is the highest settled slot.
    for id in &ids {
        webhook_inbox::mark_processed(&pool, *id, None)
            .await
            .unwrap();
    }
    assert_eq!(
        webhook_inbox::watermark(&pool, NO_GRACE).await.unwrap(),
        Some(300)
    );
}

/// The grace window is the margin against a delivery that has not arrived at
/// all yet — the case `min(pending)` cannot see. Under a grace longer than the
/// rows' age, nothing counts as settled.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn the_grace_window_holds_the_watermark_back(pool: PgPool) {
    webhook_inbox::enqueue(&pool, &[delivery(1, 100)])
        .await
        .unwrap();
    let id: i64 = sqlx::query_scalar("SELECT id FROM webhook_inbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    webhook_inbox::mark_processed(&pool, id, None)
        .await
        .unwrap();

    assert_eq!(
        webhook_inbox::watermark(&pool, Duration::from_secs(3600))
            .await
            .unwrap(),
        None,
        "a just-processed row is not settled yet, so there is nothing to claim"
    );
    assert_eq!(
        webhook_inbox::watermark(&pool, NO_GRACE).await.unwrap(),
        Some(100),
        "and with no grace it is"
    );
}

/// Pruning drops retired rows and leaves the queue alone.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn pruning_only_touches_retired_rows(pool: PgPool) {
    webhook_inbox::enqueue(&pool, &[delivery(1, 100), delivery(2, 101)])
        .await
        .unwrap();
    let id: i64 = sqlx::query_scalar("SELECT min(id) FROM webhook_inbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    webhook_inbox::mark_processed(&pool, id, None)
        .await
        .unwrap();

    // Retention outlasting the row's age is the dual-run setting: the evidence
    // stays.
    assert_eq!(
        webhook_inbox::prune(&pool, Duration::from_secs(3600))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        webhook_inbox::prune(&pool, Duration::from_secs(0))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        pending(&pool).await,
        1,
        "the unprocessed row survives any retention"
    );
}

/// `activity.source` had to admit the new lane before it could write, and the
/// old members must still be accepted — the constraint is widened, not replaced.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn the_activity_source_check_admits_webhook_and_keeps_the_rest(pool: PgPool) {
    let collection_id: i32 = sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, enabled) \
         VALUES ('c', 'C', 'token_metadata', $1, 'SYN', true) RETURNING id",
    )
    .bind(bs58::encode([200u8; 32]).into_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let asset_id: i64 = sqlx::query_scalar(
        "INSERT INTO assets (address, collection_id, name) VALUES ($1, $2, '#1') RETURNING id",
    )
    .bind(bs58::encode([1u8; 32]).into_string())
    .bind(collection_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    for (seed, source) in [
        (10u8, "backfill"),
        (11, "live"),
        (12, "reconcile"),
        (13, "manual"),
        (14, "webhook"),
    ] {
        sqlx::query(
            "INSERT INTO activity \
               (asset_id, collection_id, signature, slot, block_time, kind, to_owner, source) \
             VALUES ($1, $2, $3, $4, now(), 'mint', $5, $6)",
        )
        .bind(asset_id)
        .bind(collection_id)
        .bind(sig(seed))
        .bind(i64::from(seed))
        .bind(bs58::encode([99u8; 32]).into_string())
        .bind(source)
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("source '{source}' must be accepted: {e}"));
    }

    let rejected = sqlx::query(
        "INSERT INTO activity \
           (asset_id, collection_id, signature, slot, block_time, kind, to_owner, source) \
         VALUES ($1, $2, $3, 99, now(), 'mint', $4, 'nonsense')",
    )
    .bind(asset_id)
    .bind(collection_id)
    .bind(sig(20))
    .bind(bs58::encode([99u8; 32]).into_string())
    .execute(&pool)
    .await;
    assert!(rejected.is_err(), "the set is still closed");
}

/// The retirement criterion.
///
/// The asymmetry is the point and it is easy to measure the wrong way round:
/// the WebSocket wins nearly every *race* (sub-second, against a poll plus a
/// re-fetch), so `written_by_webhook` stays small even when webhooks are
/// perfect. What decides the cutover is `missed_by_webhook` — events the
/// WebSocket recorded that the webhook never even delivered.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn coverage_counts_what_the_webhook_never_delivered(pool: PgPool) {
    let collection_id: i32 = sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, enabled) \
         VALUES ('c', 'C', 'token_metadata', $1, 'SYN', true) RETURNING id",
    )
    .bind(bs58::encode([200u8; 32]).into_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let asset_id: i64 = sqlx::query_scalar(
        "INSERT INTO assets (address, collection_id, name) VALUES ($1, $2, '#1') RETURNING id",
    )
    .bind(bs58::encode([1u8; 32]).into_string())
    .bind(collection_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    let write = |seed: u8, source: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO activity \
                   (asset_id, collection_id, signature, slot, block_time, kind, to_owner, source) \
                 VALUES ($1, $2, $3, $4, now(), 'mint', $5, $6)",
            )
            .bind(asset_id)
            .bind(collection_id)
            .bind(sig(seed))
            .bind(i64::from(seed))
            .bind(bs58::encode([99u8; 32]).into_string())
            .bind(source)
            .execute(&pool)
            .await
            .unwrap();
        }
    };

    // Two the WebSocket recorded; the webhook only ever delivered one of them.
    write(1, "live").await;
    write(2, "live").await;
    webhook_inbox::enqueue(&pool, &[delivery(1, 1)])
        .await
        .unwrap();
    // One the webhook lane wrote itself, and one delivery still queued.
    write(3, "webhook").await;
    webhook_inbox::enqueue(&pool, &[delivery(4, 4)])
        .await
        .unwrap();

    let window = Duration::from_secs(3600);
    let coverage = webhook_inbox::coverage(&pool, window).await.unwrap();
    assert_eq!(
        coverage.missed_by_webhook, 1,
        "signature 2 reached the WebSocket and never reached the inbox"
    );
    assert_eq!(coverage.written_by_webhook, 1);
    assert_eq!(coverage.pending, 2);

    // A window that predates everything must not accuse the webhook of misses
    // it was never given a chance at.
    let narrow = webhook_inbox::coverage(&pool, Duration::from_secs(0))
        .await
        .unwrap();
    assert_eq!(narrow.missed_by_webhook, 0);
    assert_eq!(narrow.written_by_webhook, 0);
}
