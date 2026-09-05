//! Reconciliation end to end (ALG-624), against a fake Helius on loopback.
//!
//! The acceptance criterion is "artificially drop an event → reconciliation
//! fixes it on the next run and logs it", so that is what the first test does:
//! the database is left believing a stale owner, the transfer that moved the
//! asset is reachable only from the archival RPC, and one sweep has to notice,
//! recover and record it. The second run of the same sweep must then correct
//! nothing — the "trends to ~0" half, proved the way `--expect-unchanged`
//! proves the backfills.
//!
//! No API key and no network. Every address is a synthetic base58 key
//! (CLAUDE.md).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use indexer_das::DasClient;
use indexer_data_model::PgPool;
use indexer_ingest::decode::DecodeContext;
use indexer_ingester::pipeline::Pipeline;
use indexer_ingester::reconcile;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

fn sig(seed: u8) -> String {
    bs58::encode([seed; 64]).into_string()
}

/// What the fake Helius should answer.
#[derive(Clone, Default)]
struct Script {
    slot: i64,
    /// DAS assets, by id, as `getAssetBatch` and `searchAssets` return them.
    assets: BTreeMap<String, Value>,
    /// Signatures per address, for `getSignaturesForAddress`.
    signatures: BTreeMap<String, Vec<Value>>,
    /// Transactions by signature, for `getTransaction`.
    transactions: BTreeMap<String, Value>,
}

fn das_asset(id: &str, owner: &str, burnt: bool) -> Value {
    json!({
        "id": id,
        "interface": "V1_NFT",
        "burnt": burnt,
        "content": {"metadata": {"name": "Syn #1", "symbol": "SYN"}},
        "ownership": {"owner": if burnt { "" } else { owner }},
    })
}

/// A `jsonParsed` transfer in the `getTransaction` nesting.
fn transfer_tx(mint: &str, from: &str, to: &str, slot: i64) -> Value {
    let (src, dst) = (pk(60), pk(61));
    let balance = |index: u64, owner: &str| {
        json!({
            "accountIndex": index, "mint": mint, "owner": owner,
            "uiTokenAmount": {"amount": "1", "decimals": 0},
        })
    };
    json!({
        "slot": slot,
        "transaction": {
            "message": {
                "accountKeys": [
                    {"pubkey": from}, {"pubkey": src}, {"pubkey": dst},
                ],
                "instructions": [{
                    "program": "spl-token",
                    "programId": pk(90),
                    "parsed": {"type": "transferChecked",
                               "info": {"source": src, "destination": dst, "mint": mint}},
                }],
            },
        },
        "meta": {
            "err": null,
            "fee": 5_000,
            "preTokenBalances": [balance(1, from)],
            "postTokenBalances": [balance(2, to)],
            "innerInstructions": [],
        },
    })
}

struct FakeHelius {
    base: String,
    listener: Option<TcpListener>,
    calls: Arc<AtomicUsize>,
}

impl FakeHelius {
    async fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        Self {
            base,
            listener: Some(listener),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn serve(&mut self, script: Script) {
        let listener = self.listener.take().unwrap();
        let calls = Arc::clone(&self.calls);
        tokio::spawn(async move {
            let script = Arc::new(script);
            while let Ok((stream, _)) = listener.accept().await {
                let (script, calls) = (Arc::clone(&script), Arc::clone(&calls));
                tokio::spawn(async move {
                    let _ = handle(stream, script, calls).await;
                });
            }
        });
    }

    fn client(&self) -> DasClient {
        DasClient::with_endpoint(&self.base, "").unwrap()
    }
}

async fn handle(
    mut stream: TcpStream,
    script: Arc<Script>,
    calls: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(start) = find(&buffer, b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buffer[..start]).to_lowercase();
            let length = head
                .split("content-length:")
                .nth(1)
                .and_then(|rest| rest.split("\r\n").next())
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if buffer.len() >= start + 4 + length {
                break;
            }
        }
    }
    let start = find(&buffer, b"\r\n\r\n").map(|i| i + 4).unwrap_or(0);
    let body: Value = serde_json::from_slice(&buffer[start..]).unwrap_or(Value::Null);
    calls.fetch_add(1, Ordering::Relaxed);

    let result = match body["method"].as_str() {
        Some("getSlot") => json!(script.slot),
        Some("getAssetBatch") => {
            let ids = body["params"]["ids"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            Value::Array(
                ids.iter()
                    .filter_map(|id| id.as_str())
                    .map(|id| script.assets.get(id).cloned().unwrap_or(Value::Null))
                    .collect(),
            )
        }
        Some("searchAssets") => json!({
            "items": script.assets.values().cloned().collect::<Vec<_>>(),
            "grand_total": script.assets.len(),
        }),
        Some("getSignaturesForAddress") => {
            let address = body["params"][0].as_str().unwrap_or_default();
            // `before` is honoured by returning nothing on the second page,
            // which is what a short page means to the caller.
            match body["params"][1].get("before") {
                Some(_) => json!([]),
                None => json!(script.signatures.get(address).cloned().unwrap_or_default()),
            }
        }
        Some("getTransaction") => {
            let signature = body["params"][0].as_str().unwrap_or_default();
            script
                .transactions
                .get(signature)
                .cloned()
                .unwrap_or(Value::Null)
        }
        _ => Value::Null,
    };

    let payload = json!({"jsonrpc": "2.0", "id": "indexer", "result": result}).to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

async fn allowlist_collection(pool: &PgPool, mints: &[String]) -> i32 {
    let id: i32 = sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, enabled) \
         VALUES ('syn-gang', 'Syn Gang', 'token_metadata', $1, 'SYN', true) RETURNING id",
    )
    .bind(pk(200))
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO collection_mints (mint, collection_id) \
         SELECT m, $2 FROM unnest($1::text[]) AS m",
    )
    .bind(mints)
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
    id
}

async fn core_collection(pool: &PgPool, address: &str) -> i32 {
    sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, address, enabled) \
         VALUES ('syn-core', 'Syn Core', 'core', $1, true) RETURNING id",
    )
    .bind(address)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn insert_asset(pool: &PgPool, collection_id: i32, address: &str, owner: &str, slot: i64) {
    // `name` and `symbol` match what the fake reports, so a sweep over
    // agreeing state really is a no-op: any field the fixture leaves unset is
    // a genuine correction the first sweep would make.
    sqlx::query(
        "INSERT INTO assets (address, collection_id, name, symbol, owner, owner_slot) \
         VALUES ($1, $2, 'Syn #1', 'SYN', $3, $4)",
    )
    .bind(address)
    .bind(collection_id)
    .bind(owner)
    .bind(slot)
    .execute(pool)
    .await
    .unwrap();
}

fn pipeline(pool: &PgPool, das: &DasClient) -> Pipeline {
    Pipeline::new(
        pool.clone(),
        das.clone(),
        DecodeContext::default(),
        "reconcile",
    )
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_dropped_transfer_is_healed_and_the_next_run_corrects_nothing(pool: PgPool) {
    let (mint, stale, current) = (pk(1), pk(10), pk(11));
    let collection_id = allowlist_collection(&pool, std::slice::from_ref(&mint)).await;
    // What the live pipeline believes, having missed the transfer.
    insert_asset(&pool, collection_id, &mint, &stale, 100).await;

    let mut fake = FakeHelius::bind().await;
    fake.serve(Script {
        slot: 1_000,
        assets: BTreeMap::from([(mint.clone(), das_asset(&mint, &current, false))]),
        signatures: BTreeMap::from([(
            mint.clone(),
            vec![
                json!({"signature": sig(1), "slot": 500, "blockTime": 1_700_000_000, "err": null}),
            ],
        )]),
        transactions: BTreeMap::from([(sig(1), transfer_tx(&mint, &stale, &current, 500))]),
    });
    let das = fake.client();
    let pipeline = pipeline(&pool, &das);

    let report = reconcile::run(&pool, &das, &pipeline, Some(1))
        .await
        .unwrap();
    report.log("test");

    assert_eq!(report.swept(), 1);
    assert_eq!(report.candidates(), 1, "the stale owner must be noticed");
    assert_eq!(
        report.recorded(),
        1,
        "the dropped transfer must be replayed"
    );
    assert!(report.corrections() > 0, "a correction happened");
    assert!(!report.overflowed);

    // The state is corrected …
    let owner: Option<String> = sqlx::query_scalar("SELECT owner FROM assets WHERE address = $1")
        .bind(&mint)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(owner.as_deref(), Some(current.as_str()));

    // … and the event is recorded as a reconciliation, not invented as live.
    let (source, from_owner, to_owner): (String, Option<String>, Option<String>) =
        sqlx::query_as("SELECT source, from_owner, to_owner FROM activity")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(source, "reconcile");
    assert_eq!(from_owner.as_deref(), Some(stale.as_str()));
    assert_eq!(to_owner.as_deref(), Some(current.as_str()));

    assert!(
        report.integrity.is_healthy(),
        "the integrity views must be empty afterwards: {:?}",
        report.integrity
    );

    // The drift metric is durable, not just logged.
    let progress: Value = sqlx::query_scalar(
        "SELECT progress FROM backfill_state WHERE collection_id = $1 AND kind = 'reconcile'",
    )
    .bind(collection_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(progress["candidates"], 1);
    assert_eq!(progress["recorded"], 1);
    assert_eq!(progress["owner_mismatch"], 0);

    // And the second run corrects nothing — the half that has to trend to 0.
    let again = reconcile::run(&pool, &das, &pipeline, Some(1))
        .await
        .unwrap();
    assert_eq!(again.candidates(), 0, "nothing disagrees any more");
    assert_eq!(
        again.corrections(),
        0,
        "a second sweep over unchanged state must correct nothing: {:?}",
        again.collections
    );
    assert!(again.is_noop());
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_core_asset_that_left_the_collection_is_flagged_not_deleted(pool: PgPool) {
    let (collection, stays, leaves) = (pk(5), pk(1), pk(2));
    let collection_id = core_collection(&pool, &collection).await;
    insert_asset(&pool, collection_id, &stays, &pk(10), 100).await;
    insert_asset(&pool, collection_id, &leaves, &pk(11), 100).await;

    // The enumeration no longer returns the second asset: its update authority
    // moved it out.
    let mut fake = FakeHelius::bind().await;
    fake.serve(Script {
        slot: 1_000,
        assets: BTreeMap::from([(stays.clone(), das_asset(&stays, &pk(10), false))]),
        ..Script::default()
    });
    let das = fake.client();
    let pipeline = pipeline(&pool, &das);

    let report = reconcile::run(&pool, &das, &pipeline, Some(1))
        .await
        .unwrap();
    assert_eq!(
        report
            .collections
            .iter()
            .map(|c| c.membership_removed)
            .sum::<u64>(),
        1
    );

    let statuses: Vec<(String, String)> =
        sqlx::query_as("SELECT address, membership_status FROM assets ORDER BY address")
            .fetch_all(&pool)
            .await
            .unwrap();
    let by_address: BTreeMap<_, _> = statuses.into_iter().collect();
    assert_eq!(by_address[&stays], "member");
    assert_eq!(
        by_address[&leaves], "removed",
        "the row survives so its activity and intervals do too"
    );
    let removed_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT removed_at FROM assets WHERE address = $1")
            .bind(&leaves)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(removed_at.is_some(), "assets_removed_pair demands the pair");

    // Idempotent: flipping a status that already agrees writes nothing.
    let again = reconcile::run(&pool, &das, &pipeline, Some(1))
        .await
        .unwrap();
    assert_eq!(
        again
            .collections
            .iter()
            .map(|c| c.membership_removed)
            .sum::<u64>(),
        0
    );
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_sweep_over_agreeing_state_writes_nothing(pool: PgPool) {
    let (mint, owner) = (pk(1), pk(10));
    let collection_id = allowlist_collection(&pool, std::slice::from_ref(&mint)).await;
    insert_asset(&pool, collection_id, &mint, &owner, 100).await;

    let mut fake = FakeHelius::bind().await;
    fake.serve(Script {
        slot: 1_000,
        assets: BTreeMap::from([(mint.clone(), das_asset(&mint, &owner, false))]),
        ..Script::default()
    });
    let das = fake.client();
    let pipeline = pipeline(&pool, &das);

    let before: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT updated_at FROM assets WHERE address = $1")
            .bind(&mint)
            .fetch_one(&pool)
            .await
            .unwrap();

    let report = reconcile::run(&pool, &das, &pipeline, Some(1))
        .await
        .unwrap();
    assert_eq!(report.candidates(), 0);
    assert!(report.is_noop(), "{:?}", report.collections);

    let after: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT updated_at FROM assets WHERE address = $1")
            .bind(&mint)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, after, "a no-op sweep must not touch updated_at");
    // Still recorded as a run, so the schedule knows when it last happened.
    let finished: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT finished_at FROM backfill_state WHERE collection_id = $1 AND kind = 'reconcile'",
    )
    .bind(collection_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(finished.is_some());
}

#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn an_asset_flagged_out_of_order_is_rebuilt_by_the_sweep(pool: PgPool) {
    use indexer_data_model::activity::{self, LiveEvent};
    use indexer_data_model::types::EventKind;

    let (mint, owner, later) = (pk(1), pk(10), pk(11));
    let collection_id = allowlist_collection(&pool, std::slice::from_ref(&mint)).await;
    insert_asset(&pool, collection_id, &mint, &owner, 100).await;
    let asset_id: i64 = sqlx::query_scalar("SELECT id FROM assets WHERE address = $1")
        .bind(&mint)
        .fetch_one(&pool)
        .await
        .unwrap();

    // Newest first, which is how a redelivery after a gap arrives: the second
    // event predates the interval the first one opened, so the writer stores
    // it and flags the asset instead of manufacturing a false history.
    let write = |signature: String, slot: i64, from: Option<String>, to: String| {
        let pool = pool.clone();
        async move {
            let mut tx = pool.begin().await.unwrap();
            let applied = activity::record(
                &mut tx,
                &LiveEvent {
                    asset_id,
                    collection_id,
                    signature: &signature,
                    seq: 0,
                    slot,
                    block_time: chrono::Utc::now(),
                    kind: EventKind::Transfer,
                    from_owner: from.as_deref(),
                    to_owner: Some(&to),
                    price_lamports: None,
                    marketplace: None,
                    details: None,
                    source: "live",
                },
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            applied
        }
    };
    write(sig(2), 300, Some(owner.clone()), later.clone()).await;
    let out_of_order = write(sig(1), 200, Some(pk(12)), owner.clone()).await;
    assert!(
        out_of_order.dirty,
        "the older event must be flagged, not applied"
    );
    assert_eq!(activity::dirty_count(&pool).await.unwrap(), 1);

    let mut fake = FakeHelius::bind().await;
    fake.serve(Script {
        slot: 1_000,
        assets: BTreeMap::from([(mint.clone(), das_asset(&mint, &later, false))]),
        ..Script::default()
    });
    let das = fake.client();
    let pipeline = pipeline(&pool, &das);

    let report = reconcile::run(&pool, &das, &pipeline, Some(400))
        .await
        .unwrap();
    assert_eq!(
        report.collections.iter().map(|c| c.rebuilt).sum::<u64>(),
        1,
        "the sweep re-derives the history the writer refused to guess at"
    );
    assert_eq!(
        activity::dirty_count(&pool).await.unwrap(),
        0,
        "and clears the flag"
    );
    assert!(report.corrections() > 0, "a rebuild is a correction");
    assert!(report.integrity.is_healthy(), "{:?}", report.integrity);
}
