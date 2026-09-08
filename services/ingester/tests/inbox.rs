//! The webhook drain as an `IngestSource`, against Postgres and a loopback RPC.
//!
//! No API key and no network beyond 127.0.0.1. Every address is a synthetic
//! base58 key (CLAUDE.md).
//!
//! Ignored without a database: `cargo test --workspace -- --include-ignored`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use indexer_config::{IngestConfig, Transport};
use indexer_das::DasClient;
use indexer_data_model::webhook_inbox::{self, Delivery};
use indexer_data_model::PgPool;
use indexer_ingest::{IngestEvent, IngestSource, ResumeFrom, StreamStatus, SubscriptionSpec};
use indexer_ingester::inbox::WebhookInbox;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

fn sig(seed: u8) -> String {
    bs58::encode([seed; 64]).into_string()
}

fn config() -> IngestConfig {
    IngestConfig {
        transports: vec![Transport::Webhook],
        // Tight, so the test is fast rather than patient.
        webhook_poll_ms: 20,
        webhook_batch: 10,
        webhook_lease_secs: 60,
        webhook_max_attempts: 2,
        // No settling window: the watermark should move as soon as a row is
        // retired, which is what these assertions are about.
        webhook_grace_secs: 0,
        webhook_retain_days: 30,
    }
}

fn delivery(seed: u8, slot: i64, failed: bool) -> Delivery {
    Delivery {
        signature: sig(seed),
        slot,
        block_time: None,
        failed,
        body: json!({ "slot": slot }),
    }
}

/// A loopback JSON-RPC that answers `getTransaction` from a script and counts
/// how many times it was asked — so "this path spent no credits" is assertable.
struct FakeRpc {
    base: String,
    calls: Arc<AtomicUsize>,
}

impl FakeRpc {
    async fn start(transactions: BTreeMap<String, Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let served = Arc::clone(&calls);
        let script = Arc::new(transactions);
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let (script, served) = (Arc::clone(&script), Arc::clone(&served));
                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    let mut chunk = [0u8; 8192];
                    // One request per connection (reqwest sends Connection:
                    // close is not guaranteed, but a single read of the head +
                    // body is enough for these small bodies).
                    if let Ok(read) = stream.read(&mut chunk).await {
                        buffer.extend_from_slice(&chunk[..read]);
                    }
                    let body = String::from_utf8_lossy(&buffer);
                    let request: Value = body
                        .split_once("\r\n\r\n")
                        .and_then(|(_, b)| serde_json::from_str(b).ok())
                        .unwrap_or(Value::Null);
                    let result = if request["method"] == "getTransaction" {
                        served.fetch_add(1, Ordering::Relaxed);
                        let signature = request["params"][0].as_str().unwrap_or_default();
                        script.get(signature).cloned().unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    };
                    let payload =
                        json!({"jsonrpc": "2.0", "id": "indexer", "result": result}).to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                        payload.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.flush().await;
                });
            }
        });
        Self { base, calls }
    }

    fn client(&self) -> DasClient {
        DasClient::with_endpoint(&self.base, "").unwrap()
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

/// A `getTransaction` response, in the jsonParsed shape the decoder wants —
/// which is the whole reason the drain re-fetches instead of reading the body.
fn transaction(slot: i64) -> Value {
    json!({
        "slot": slot,
        "blockTime": 1_700_000_000i64 + slot,
        "transaction": {"message": {"accountKeys": [], "instructions": []}},
        "meta": {"err": null, "preTokenBalances": [], "postTokenBalances": []},
    })
}

fn spec() -> watch::Receiver<SubscriptionSpec> {
    watch::channel(SubscriptionSpec::default()).1
}

/// Collects up to `want` events, giving up after a deadline so a hang is a
/// failure rather than a stuck suite.
async fn take(stream: &mut indexer_ingest::EventStream, want: usize) -> Vec<IngestEvent> {
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while events.len() < want {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(Ok(event))) => events.push(event),
            Ok(Some(Err(error))) => panic!("terminal stream error: {error}"),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    events
}

/// The happy path: a queued delivery becomes a transaction event, is retired,
/// and the checkpoint follows it.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_queued_delivery_is_fetched_and_emitted(pool: PgPool) {
    let rpc = FakeRpc::start(BTreeMap::from([(sig(1), transaction(100))])).await;
    webhook_inbox::enqueue(&pool, &[delivery(1, 100, false)])
        .await
        .unwrap();

    let source = WebhookInbox::new(pool.clone(), rpc.client(), config());
    let mut stream = source.subscribe(spec(), ResumeFrom::Latest);
    let events = take(&mut stream, 3).await;

    // `Connected` exactly once and first — it is the consumer's reconcile
    // trigger, never a heartbeat.
    assert!(
        matches!(events[0], IngestEvent::Status(StreamStatus::Connected)),
        "first event was {:?}",
        events[0]
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, IngestEvent::Status(StreamStatus::Connected)))
            .count(),
        1
    );

    let update = events
        .iter()
        .find_map(|e| match e {
            IngestEvent::Transaction(update) => Some(update),
            _ => None,
        })
        .expect("a transaction event");
    assert_eq!(update.signature, sig(1));
    assert_eq!(update.slot, 100);
    assert!(!update.failed);
    assert_eq!(rpc.calls(), 1, "one fetch per delivery");

    drop(stream);
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM webhook_inbox WHERE processed_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(pending, 0, "the row is retired once emitted");
}

/// A failed transaction is retired **without** a fetch. The decoder drops
/// failed transactions anyway, so fetching one spends a credit to learn
/// nothing.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_failed_delivery_is_retired_without_spending_a_fetch(pool: PgPool) {
    let rpc = FakeRpc::start(BTreeMap::new()).await;
    webhook_inbox::enqueue(&pool, &[delivery(7, 300, true)])
        .await
        .unwrap();

    let source = WebhookInbox::new(pool.clone(), rpc.client(), config());
    let mut stream = source.subscribe(spec(), ResumeFrom::Latest);
    let events = take(&mut stream, 2).await;
    drop(stream);

    assert_eq!(rpc.calls(), 0, "a failed transaction is never fetched");
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, IngestEvent::Transaction(_))),
        "and never emitted"
    );
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM webhook_inbox WHERE processed_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(pending, 0, "but it is retired, so it cannot pin the cursor");
}

/// A signature `getTransaction` never returns must retire at the attempt cap.
/// Left pending it would be re-claimed every lease forever *and* pin the
/// watermark, freezing the cursor behind one bad delivery.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_poison_delivery_retires_at_the_cap(pool: PgPool) {
    // The script is empty, so every fetch answers null.
    let rpc = FakeRpc::start(BTreeMap::new()).await;
    webhook_inbox::enqueue(&pool, &[delivery(3, 500, false)])
        .await
        .unwrap();

    let source = WebhookInbox::new(pool.clone(), rpc.client(), config());
    let mut stream = source.subscribe(spec(), ResumeFrom::Latest);
    // Long enough for both attempts (max_attempts = 2) at a 20 ms poll.
    let _ = take(&mut stream, 6).await;
    drop(stream);

    let (pending, attempts, last_error): (i64, i16, Option<String>) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE processed_at IS NULL), max(attempts), max(last_error) \
           FROM webhook_inbox",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pending, 0, "the poison row must not stay pending");
    assert_eq!(attempts, 2, "it is tried up to the cap, not once");
    assert!(
        last_error.is_some_and(|e| e.contains("null")),
        "and records why it was given up on"
    );
}

/// The checkpoint invariant, end to end: with a row still pending, the drain
/// must not claim a slot at or above it.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn the_checkpoint_never_passes_a_pending_delivery(pool: PgPool) {
    // 100 is fetchable; 200 is not scripted, so it stays pending after its
    // attempts — and must hold the cursor below itself the whole time.
    let rpc = FakeRpc::start(BTreeMap::from([(sig(1), transaction(100))])).await;
    webhook_inbox::enqueue(&pool, &[delivery(1, 100, false), delivery(2, 200, false)])
        .await
        .unwrap();

    // One attempt only, so signature 2 stays pending rather than retiring.
    let mut config = config();
    config.webhook_max_attempts = i16::MAX;
    let source = WebhookInbox::new(pool.clone(), rpc.client(), config);
    let mut stream = source.subscribe(spec(), ResumeFrom::Latest);
    let events = take(&mut stream, 4).await;
    drop(stream);

    let checkpoints: Vec<u64> = events
        .iter()
        .filter_map(|e| match e {
            IngestEvent::SlotCheckpoint(checkpoint) => Some(checkpoint.slot),
            _ => None,
        })
        .collect();
    assert!(!checkpoints.is_empty(), "the drain must checkpoint");
    for slot in &checkpoints {
        assert!(
            *slot < 200,
            "checkpointed {slot} while slot 200 was still pending"
        );
        assert!(*slot > 0, "0 would make seed_cursor walk all of history");
    }
}
