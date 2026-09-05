//! The Helius WebSocket adapter, driven by a loopback fake.
//!
//! No API key and no TLS: CI runs `--include-ignored` without a key, so
//! nothing here may reach the network. `ws://` also exercises the adapter's
//! non-TLS path. Every address is a synthetic base58 key (CLAUDE.md).
//!
//! The server half ships in `tokio-tungstenite`, which this crate already
//! depends on, so `accept_async` handles the handshake and the fake stays
//! small.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use indexer_ingest::ws::HeliusWs;
use indexer_ingest::{
    Commitment, IngestEvent, IngestSource, ResumeFrom, StreamStatus, SubscriptionSpec,
    TransactionFilter,
};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex};
use tokio_tungstenite::tungstenite::Message;

fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

/// Requests the fake saw, so a test can assert what was actually sent.
type Seen = Arc<Mutex<Vec<Value>>>;

/// Deterministic subscription ids so a test can address the transaction
/// subscription without depending on the order the adapter subscribes in.
const TX_SUBSCRIPTION: u64 = 101;
const ROOT_SUBSCRIPTION: u64 = 900;

fn subscription_for(request: &Value) -> u64 {
    match request["method"].as_str() {
        Some("rootSubscribe") => ROOT_SUBSCRIPTION,
        _ => TX_SUBSCRIPTION,
    }
}

fn spec(addresses: &[String]) -> SubscriptionSpec {
    let mut transactions = BTreeMap::new();
    transactions.insert(
        "tracked".to_string(),
        TransactionFilter {
            account_include: addresses.to_vec(),
            account_required: Vec::new(),
            include_failed: false,
        },
    );
    SubscriptionSpec {
        commitment: Commitment::Confirmed,
        accounts: BTreeMap::new(),
        transactions,
    }
}

fn notification(subscription: u64, slot: u64, signature: &str) -> String {
    json!({
        "jsonrpc": "2.0",
        "method": "transactionNotification",
        "params": {
            "subscription": subscription,
            "result": {
                "signature": signature,
                "slot": slot,
                "transaction": {
                    "transaction": {"message": {"accountKeys": [{"pubkey": pk(50)}]}},
                    "meta": {"err": null},
                }
            }
        }
    })
    .to_string()
}

/// Accepts one connection, acknowledges every request with an incrementing
/// subscription id, then runs `after` with the socket.
async fn serve_one<F, Fut>(listener: &TcpListener, seen: Seen, after: F)
where
    F: FnOnce(
        futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
            Message,
        >,
    ) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let (stream, _) = listener.accept().await.unwrap();
    let socket = tokio_tungstenite::accept_async(stream).await.unwrap();
    let (mut sink, mut source) = socket.split();

    // The adapter sends rootSubscribe plus one call per filter, then waits for
    // every ack before it reports Connected.
    while let Some(Ok(message)) = source.next().await {
        let Message::Text(text) = message else {
            continue;
        };
        let request: Value = serde_json::from_str(&text).unwrap();
        seen.lock().await.push(request.clone());
        let id = request["id"].as_u64().unwrap();
        sink.send(Message::Text(
            json!({"jsonrpc": "2.0", "result": subscription_for(&request), "id": id})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

        // Both subscriptions acknowledged.
        if seen.lock().await.len() >= 2 {
            break;
        }
    }

    after(sink).await;
}

async fn next_event(
    stream: &mut indexer_ingest::EventStream,
) -> Option<Result<IngestEvent, indexer_ingest::IngestError>> {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the adapter went quiet")
}

/// The happy path: the subscribe request carries what the decoder needs,
/// notifications become events tagged with their filter, and a root becomes a
/// checkpoint.
#[tokio::test]
async fn it_subscribes_and_delivers_events() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));

    let server_seen = seen.clone();
    tokio::spawn(async move {
        serve_one(&listener, server_seen, |mut sink| async move {
            sink.send(Message::Text(
                notification(TX_SUBSCRIPTION, 443_800_000, "SYNsig").into(),
            ))
            .await
            .unwrap();
            sink.send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "method": "rootNotification",
                    "params": {"result": 443_799_900_u64, "subscription": ROOT_SUBSCRIPTION},
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
        .await;
    });

    let source = HeliusWs::with_endpoint(&url);
    let (_tx, rx) = watch::channel(spec(&[pk(1), pk(2)]));
    let mut stream = source.subscribe(rx, ResumeFrom::Latest);

    let mut saw_connected = false;
    let mut transactions = Vec::new();
    let mut checkpoints = Vec::new();
    for _ in 0..4 {
        match next_event(&mut stream).await {
            Some(Ok(IngestEvent::Status(StreamStatus::Connected))) => saw_connected = true,
            Some(Ok(IngestEvent::Transaction(update))) => transactions.push(update),
            Some(Ok(IngestEvent::SlotCheckpoint(c))) => checkpoints.push(c.slot),
            Some(Ok(_)) => {}
            other => panic!("unexpected {other:?}"),
        }
        if saw_connected && !transactions.is_empty() && !checkpoints.is_empty() {
            break;
        }
    }

    assert!(saw_connected, "Connected means every subscription is acked");
    assert_eq!(transactions.len(), 1);
    assert_eq!(transactions[0].signature, "SYNsig");
    assert_eq!(transactions[0].slot, 443_800_000);
    assert_eq!(
        transactions[0].filters,
        vec!["tracked".to_string()],
        "the event is tagged with the filter that matched it"
    );
    assert_eq!(checkpoints, vec![443_799_900]);

    // The subscribe request must carry exactly what the decoder relies on.
    let requests = seen.lock().await.clone();
    let subscribe = requests
        .iter()
        .find(|r| r["method"] == "transactionSubscribe")
        .expect("a transactionSubscribe was sent");
    assert_eq!(subscribe["params"][0]["vote"], false);
    assert_eq!(subscribe["params"][0]["failed"], false);
    assert_eq!(subscribe["params"][0]["accountInclude"][0], pk(1));
    assert_eq!(subscribe["params"][1]["commitment"], "confirmed");
    assert_eq!(subscribe["params"][1]["encoding"], "jsonParsed");
    assert_eq!(subscribe["params"][1]["transactionDetails"], "full");
    assert!(
        requests.iter().any(|r| r["method"] == "rootSubscribe"),
        "checkpoints come from the finalized root"
    );
}

/// A dropped socket is transient, not terminal: the adapter reports
/// `Reconnecting`, opens a second connection and re-sends every subscription.
#[tokio::test]
async fn a_dropped_socket_reconnects_and_resubscribes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let first: Seen = Arc::new(Mutex::new(Vec::new()));
    let second: Seen = Arc::new(Mutex::new(Vec::new()));

    let (a, b) = (first.clone(), second.clone());
    tokio::spawn(async move {
        // First connection: ack, then hang up.
        serve_one(&listener, a, |mut sink| async move {
            sink.send(Message::Close(None)).await.ok();
        })
        .await;
        // Second connection: the adapter must subscribe again from scratch.
        serve_one(&listener, b, |mut sink| async move {
            sink.send(Message::Text(
                notification(TX_SUBSCRIPTION, 443_800_001, "SYNsig2").into(),
            ))
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
        .await;
    });

    let source = HeliusWs::with_endpoint(&url);
    let (_tx, rx) = watch::channel(spec(&[pk(1)]));
    let mut stream = source.subscribe(rx, ResumeFrom::Latest);

    let mut reconnecting = 0;
    let mut recovered = None;
    for _ in 0..8 {
        match next_event(&mut stream).await {
            Some(Ok(IngestEvent::Status(StreamStatus::Reconnecting { .. }))) => reconnecting += 1,
            Some(Ok(IngestEvent::Transaction(update))) => {
                recovered = Some(update.signature.clone());
                break;
            }
            Some(Ok(_)) => {}
            Some(Err(error)) => panic!("a dropped socket must not be terminal: {error}"),
            None => panic!("stream ended"),
        }
    }

    assert_eq!(reconnecting, 1);
    assert_eq!(recovered.as_deref(), Some("SYNsig2"));
    assert_eq!(
        second.lock().await.len(),
        2,
        "the new connection re-sent rootSubscribe and the filter"
    );
}

/// An unreachable endpoint exhausts the budget and ends the stream — the
/// service, not the adapter, decides what to do about it.
#[tokio::test]
async fn an_unreachable_endpoint_ends_the_stream() {
    // Port 1 is reserved and refuses connections immediately.
    let source = HeliusWs::with_endpoint("ws://127.0.0.1:1").with_max_attempts(2);
    let (_tx, rx) = watch::channel(spec(&[pk(1)]));
    let mut stream = source.subscribe(rx, ResumeFrom::Latest);

    let mut last = None;
    while let Some(item) = next_event(&mut stream).await {
        match item {
            Ok(_) => continue,
            Err(error) => {
                last = Some(error);
                break;
            }
        }
    }

    match last {
        Some(indexer_ingest::IngestError::Exhausted { attempts, .. }) => {
            assert_eq!(attempts, 2);
        }
        other => panic!("expected Exhausted, got {other:?}"),
    }
    assert!(
        next_event(&mut stream).await.is_none(),
        "an Err item is terminal: the stream ends after it"
    );
}

/// `ResumeFrom::Slot` is a floor on this transport, never a rewind — there is
/// no replay to rewind with. Anything below the cursor is dropped so the
/// consumer never re-processes what it already checkpointed.
#[tokio::test]
async fn resume_from_slot_filters_rather_than_replays() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));

    let server_seen = seen.clone();
    tokio::spawn(async move {
        serve_one(&listener, server_seen, |mut sink| async move {
            // Below the floor, then above it.
            sink.send(Message::Text(
                notification(TX_SUBSCRIPTION, 90, "SYNold").into(),
            ))
            .await
            .unwrap();
            sink.send(Message::Text(
                notification(TX_SUBSCRIPTION, 200, "SYNnew").into(),
            ))
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
        .await;
    });

    let source = HeliusWs::with_endpoint(&url);
    let (_tx, rx) = watch::channel(spec(&[pk(1)]));
    let mut stream = source.subscribe(rx, ResumeFrom::Slot(100));

    let mut delivered = Vec::new();
    for _ in 0..4 {
        if let Some(Ok(IngestEvent::Transaction(update))) = next_event(&mut stream).await {
            delivered.push(update.signature.clone());
            break;
        }
    }
    assert_eq!(
        delivered,
        vec!["SYNnew".to_string()],
        "the slot below the cursor was filtered out"
    );
}

/// A spec change is applied on the live connection: the new filter is
/// subscribed *before* the old one is dropped, so the overlap duplicates
/// rather than gaps.
#[tokio::test]
async fn a_spec_change_resubscribes_without_reconnecting() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));

    let server_seen = seen.clone();
    let accepted = Arc::new(Mutex::new(0usize));
    let counter = accepted.clone();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        *counter.lock().await += 1;
        let socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let (mut sink, mut source) = socket.split();
        while let Some(Ok(Message::Text(text))) = source.next().await {
            let request: Value = serde_json::from_str(&text).unwrap();
            server_seen.lock().await.push(request.clone());
            sink.send(Message::Text(
                json!({"jsonrpc": "2.0", "result": subscription_for(&request), "id": request["id"]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        }
    });

    let source = HeliusWs::with_endpoint(&url);
    let (tx, rx) = watch::channel(spec(&[pk(1)]));
    let mut stream = source.subscribe(rx, ResumeFrom::Latest);

    // Wait for the initial Connected before changing the spec.
    for _ in 0..4 {
        if let Some(Ok(IngestEvent::Status(StreamStatus::Connected))) =
            next_event(&mut stream).await
        {
            break;
        }
    }

    tx.send(spec(&[pk(1), pk(2)])).unwrap();

    let mut resubscribed = false;
    for _ in 0..4 {
        if let Some(Ok(IngestEvent::Status(StreamStatus::Resubscribed))) =
            next_event(&mut stream).await
        {
            resubscribed = true;
            break;
        }
    }
    assert!(resubscribed);
    assert_eq!(*accepted.lock().await, 1, "the same socket was reused");

    // `Resubscribed` means both frames were written, not that the server has
    // logged them yet.
    let requests = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let requests = seen.lock().await.clone();
            if requests
                .iter()
                .any(|r| r["method"] == "transactionUnsubscribe")
            {
                return requests;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the old filter was dropped");

    let subscribe_at = requests
        .iter()
        .rposition(|r| r["method"] == "transactionSubscribe")
        .expect("resubscribed");
    let unsubscribe_at = requests
        .iter()
        .position(|r| r["method"] == "transactionUnsubscribe")
        .unwrap();
    assert!(
        subscribe_at < unsubscribe_at,
        "make before break: subscribing after unsubscribing would open a gap"
    );
}
