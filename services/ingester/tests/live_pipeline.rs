//! The live pipeline end to end: a decoded transaction becomes `activity`,
//! `ownership_history` and an updated owner.
//!
//! No network and no API key. Block times are seeded into the cache, which is
//! exactly what the recovery path does with `getSignaturesForAddress`'s
//! `blockTime`, so the DAS client is never called. Every address is a
//! synthetic base58 key (CLAUDE.md).

use chrono::{DateTime, TimeZone, Utc};
use indexer_das::DasClient;
use indexer_data_model::PgPool;
use indexer_ingest::decode::DecodeContext;
use indexer_ingest::{RawPayload, TransactionUpdate};
use indexer_ingester::pipeline::Pipeline;
use serde_json::{json, Value};

fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

fn sig(seed: u8) -> String {
    bs58::encode([seed; 64]).into_string()
}

fn at(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

async fn collection(pool: &PgPool) -> i32 {
    sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, enabled) \
         VALUES ('c', 'C', 'token_metadata', $1, 'SYN', true) RETURNING id",
    )
    .bind(pk(200))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn asset(pool: &PgPool, collection_id: i32, address: &str, owner: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO assets (address, collection_id, name, owner, owner_slot) \
         VALUES ($1, $2, '#1', $3, 1) RETURNING id",
    )
    .bind(address)
    .bind(collection_id)
    .bind(owner)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// A `jsonParsed` transfer in the shape the WebSocket delivers.
fn transfer(mint: &str, from: &str, to: &str, slot: u64, signature: &str) -> TransactionUpdate {
    let (source, destination) = (pk(80), pk(81));
    let payload: Value = json!({
        "signature": signature,
        "slot": slot,
        "transaction": {
            "transaction": {"message": {
                "accountKeys": [{"pubkey": from}, {"pubkey": source}, {"pubkey": destination}],
                "instructions": [{
                    "program": "spl-token",
                    "programId": pk(90),
                    "parsed": {"type": "transferChecked", "info": {
                        "source": source, "destination": destination, "mint": mint,
                    }},
                }],
            }},
            "meta": {
                "err": null,
                "preTokenBalances": [{
                    "accountIndex": 1, "mint": mint, "owner": from,
                    "uiTokenAmount": {"amount": "1", "decimals": 0},
                }],
                "postTokenBalances": [{
                    "accountIndex": 2, "mint": mint, "owner": to,
                    "uiTokenAmount": {"amount": "1", "decimals": 0},
                }],
                "innerInstructions": [],
            },
        }
    });

    TransactionUpdate {
        filters: vec!["tracked".into()],
        slot,
        signature: signature.to_string(),
        failed: false,
        account_keys: vec![from.to_string()],
        raw: RawPayload::Json(payload),
    }
}

/// A pipeline whose DAS endpoint is unroutable, proving nothing in these paths
/// reaches the network.
async fn pipeline(pool: &PgPool, slot: u64) -> Pipeline {
    let das = DasClient::with_endpoint("http://127.0.0.1:1", "").unwrap();
    let pipeline = Pipeline::new(pool.clone(), das, DecodeContext::default(), "live");
    pipeline
        .block_times()
        .insert(slot as i64, at(1_700_000_000))
        .await;
    pipeline
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_transfer_becomes_activity_ownership_and_an_owner(pool: PgPool) {
    let c = collection(&pool).await;
    let id = asset(&pool, c, &pk(1), &pk(50)).await;
    let pipeline = pipeline(&pool, 443_800_000).await;

    let outcome = pipeline
        .handle(&transfer(&pk(1), &pk(50), &pk(51), 443_800_000, &sig(1)))
        .await
        .unwrap();
    assert_eq!(outcome.recorded, 1);
    assert_eq!(outcome.untracked, 0);

    let (kind, source, from, to, block_time): (
        String,
        String,
        Option<String>,
        Option<String>,
        DateTime<Utc>,
    ) = sqlx::query_as(
        "SELECT kind, source, from_owner, to_owner, block_time FROM activity WHERE asset_id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(kind, "transfer");
    assert_eq!(source, "live");
    assert_eq!(from.as_deref(), Some(pk(50).as_str()));
    assert_eq!(to.as_deref(), Some(pk(51).as_str()));
    assert_eq!(block_time, at(1_700_000_000));

    let owner: Option<String> = sqlx::query_scalar("SELECT owner FROM assets WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(owner.as_deref(), Some(pk(51).as_str()));

    let open: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ownership_history WHERE asset_id = $1 AND to_slot IS NULL",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(open, 1);

    // Never a price, never a marketplace — that is ALG-622's job, and the
    // program ids are handed over in `details` so it needs no refetch.
    let details: Value = sqlx::query_scalar("SELECT details FROM activity WHERE asset_id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(details["programs"][0], pk(90));
    assert_eq!(details["instruction"], "transferChecked");
    let (price, marketplace): (Option<i64>, Option<String>) =
        sqlx::query_as("SELECT price_lamports, marketplace FROM activity WHERE asset_id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((price, marketplace), (None, None));
}

/// The transport is at-least-once, so the same notification arriving twice
/// must write nothing the second time.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_redelivered_notification_writes_nothing(pool: PgPool) {
    let c = collection(&pool).await;
    let id = asset(&pool, c, &pk(1), &pk(50)).await;
    let pipeline = pipeline(&pool, 443_800_000).await;
    let update = transfer(&pk(1), &pk(50), &pk(51), 443_800_000, &sig(1));

    let first = pipeline.handle(&update).await.unwrap();
    assert_eq!(first.recorded, 1);

    let before: DateTime<Utc> = sqlx::query_scalar("SELECT updated_at FROM assets WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();

    let second = pipeline.handle(&update).await.unwrap();
    assert_eq!(second.recorded, 0);
    assert_eq!(second.redelivered, 1);

    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM activity), (SELECT count(*) FROM ownership_history)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (1, 1));

    let after: DateTime<Utc> = sqlx::query_scalar("SELECT updated_at FROM assets WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(before, after, "a redelivery must not touch updated_at");
}

/// The filter matches whole transactions, which routinely move tokens we do
/// not track. Those are counted, never written, and never fatal.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn an_untracked_mint_is_ignored(pool: PgPool) {
    let c = collection(&pool).await;
    asset(&pool, c, &pk(1), &pk(50)).await;
    let pipeline = pipeline(&pool, 443_800_000).await;

    // pk(9) is not in `assets`.
    let outcome = pipeline
        .handle(&transfer(&pk(9), &pk(50), &pk(51), 443_800_000, &sig(2)))
        .await
        .unwrap();
    assert_eq!(outcome.untracked, 1);
    assert_eq!(outcome.recorded, 0);

    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM activity")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

/// `activity.block_time` is NOT NULL, so a slot we cannot date must park the
/// signature rather than have a timestamp invented for it — the migration's
/// "a signature whose block_time cannot be resolved stays unclassified".
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn an_unresolvable_block_time_parks_the_signature(pool: PgPool) {
    let c = collection(&pool).await;
    let id = asset(&pool, c, &pk(1), &pk(50)).await;
    // No block time seeded, and the DAS endpoint is unroutable.
    let das = DasClient::with_endpoint("http://127.0.0.1:1", "")
        .unwrap()
        .with_max_attempts(1);
    let pipeline = Pipeline::new(pool.clone(), das, DecodeContext::default(), "live");

    let outcome = pipeline
        .handle(&transfer(&pk(1), &pk(50), &pk(51), 443_800_000, &sig(1)))
        .await
        .unwrap();
    assert_eq!(outcome.parked, 1);
    assert_eq!(outcome.recorded, 0);

    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM asset_signatures WHERE asset_id = $1 AND classified_at IS NULL",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pending, 1, "handed to ALG-622's crawl, not dropped");

    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM activity")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0, "no row may be written without a block time");
}

/// The recovery path feeds `getTransaction`'s different nesting through the
/// same decoder and writer, tagged `reconcile` — which is what makes gap
/// recovery comparable to live behaviour.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_replayed_transaction_is_recorded_as_reconcile(pool: PgPool) {
    let c = collection(&pool).await;
    let id = asset(&pool, c, &pk(1), &pk(50)).await;
    let pipeline = pipeline(&pool, 443_800_000).await;

    // getTransaction hoists `transaction` and `meta` one level up.
    let live = transfer(&pk(1), &pk(50), &pk(51), 443_800_000, &sig(1));
    let RawPayload::Json(ws) = &live.raw else {
        unreachable!()
    };
    let rpc = json!({
        "slot": 443_800_000,
        "blockTime": 1_700_000_000,
        "transaction": ws["transaction"]["transaction"].clone(),
        "meta": ws["transaction"]["meta"].clone(),
    });

    let outcome = pipeline.replay(&sig(1), 443_800_000, &rpc).await.unwrap();
    assert_eq!(outcome.recorded, 1);
    // The recovery reports its whole outcome, not just the row count: ALG-624
    // counts a parked signature or a flagged asset as a correction too.
    assert_eq!((outcome.parked, outcome.dirty), (0, 0));

    let source: String = sqlx::query_scalar("SELECT source FROM activity WHERE asset_id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(source, "reconcile");

    // And the live delivery of the same signature is then a no-op.
    let outcome = pipeline.handle(&live).await.unwrap();
    assert_eq!(outcome.redelivered, 1);
    assert_eq!(outcome.recorded, 0);
}
