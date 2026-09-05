//! Helius Enhanced WebSockets — the mainnet transport (ALG-618's decision,
//! Developer plan).
//!
//! The adapter owns reconnection and keepalive; the consumer owns the durable
//! cursor. Three properties of *this* transport shape the implementation:
//!
//! - **There is no replay.** `fromSlot` is a LaserStream gRPC feature; the
//!   WebSocket API exposes nothing equivalent. [`ResumeFrom::Slot`] is
//!   therefore honoured as a **floor** — events below it are dropped — and
//!   never as a rewind. Returning `UnsupportedSpec` instead would be wrong
//!   twice over: it is a terminal error, and `Slot` is what a consumer passes
//!   on every ordinary restart. Closing the gap is the consumer's job, via DAS
//!   reconciliation on [`StreamStatus::Connected`], and it already knows the
//!   slot because it passed it.
//! - **Checkpoints come from `rootSubscribe`.** The root is finalized, so it
//!   can never sit ahead of a confirmed transaction that has not been
//!   delivered — which, with no replay, would be permanent data loss.
//! - **A stalled consumer is a hole, not a delay.** The socket is drained by
//!   its own task into a bounded channel so pings keep flowing; if the channel
//!   stays full the adapter reports [`StreamStatus::Lagged`] and reconnects,
//!   because "dropped events" and "disconnected" need the same recovery.

mod wire;

pub use wire::{unsupported, Frame, SpecDiff, MAX_ADDRESSES, MAX_FILTERS};

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::event::{IngestEvent, Slot, SlotCheckpoint, StreamStatus};
use crate::source::{EventStream, IngestError, IngestSource, ResumeFrom};
use crate::spec::{FilterId, SubscriptionSpec};

/// Helius drops idle connections after 10 minutes and asks for a ping about
/// once a minute; half that leaves room for one lost ping.
const PING_INTERVAL: Duration = Duration::from_secs(30);

/// No pong and no root notification for this long means the socket is wedged
/// even though TCP has not noticed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

/// Bounded so a slow consumer applies backpressure to the channel rather than
/// to the socket, which would starve the keepalive.
const EVENT_BUFFER: usize = 4096;

/// How long a full channel is tolerated before the consumer is declared
/// behind rather than merely busy.
const LAG_TIMEOUT: Duration = Duration::from_secs(5);

const SUBSCRIBE_ACK_TIMEOUT: Duration = Duration::from_secs(20);

pub struct HeliusWs {
    url: String,
    max_attempts: u32,
}

impl HeliusWs {
    /// `api_key` is appended as a query parameter; it is never logged — see
    /// [`redact`].
    pub fn new(api_key: &str) -> Self {
        Self::with_endpoint(&format!("wss://mainnet.helius-rpc.com/?api-key={api_key}"))
    }

    /// Points the adapter at an arbitrary endpoint, so tests can drive a
    /// loopback `ws://` server with no key and no TLS.
    pub fn with_endpoint(url: &str) -> Self {
        Self {
            url: url.to_string(),
            max_attempts: 8,
        }
    }

    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }
}

impl IngestSource for HeliusWs {
    fn name(&self) -> &'static str {
        "helius-ws"
    }

    fn subscribe(
        &self,
        spec: watch::Receiver<SubscriptionSpec>,
        resume: ResumeFrom,
    ) -> EventStream {
        let (tx, mut rx) = mpsc::channel(EVENT_BUFFER);
        let url = self.url.clone();
        let max_attempts = self.max_attempts;

        let handle = tokio::spawn(async move {
            run_connection(url, max_attempts, spec, resume, tx).await;
        });

        Box::pin(async_stream::stream! {
            // Dropping the stream must kill the socket, or the connection slot
            // leaks — the Developer plan allows 150.
            let _guard = AbortOnDrop(handle);
            while let Some(item) = rx.recv().await {
                let terminal = item.is_err();
                yield item;
                if terminal {
                    break;
                }
            }
        })
    }
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

type Sender = mpsc::Sender<Result<IngestEvent, IngestError>>;

/// The reconnect loop. Every transient failure becomes a `Reconnecting`
/// status; only an exhausted budget is terminal.
async fn run_connection(
    url: String,
    max_attempts: u32,
    spec: watch::Receiver<SubscriptionSpec>,
    resume: ResumeFrom,
    tx: Sender,
) {
    install_crypto_provider();

    let floor = match resume {
        ResumeFrom::Slot(slot) => Some(slot),
        ResumeFrom::Latest => None,
    };

    let mut attempt = 0u32;
    loop {
        match session(&url, spec.clone(), floor, &tx).await {
            // A clean end means the consumer went away.
            Ok(()) => return,
            Err(reason) => {
                attempt += 1;
                if attempt >= max_attempts {
                    let _ = tx
                        .send(Err(IngestError::Exhausted {
                            attempts: attempt,
                            reason,
                        }))
                        .await;
                    return;
                }
                log::warn!(
                    "helius-ws reconnecting (attempt {attempt}/{max_attempts}) after: {reason}"
                );
                if tx
                    .send(Ok(IngestEvent::Status(StreamStatus::Reconnecting {
                        attempt,
                    })))
                    .await
                    .is_err()
                {
                    return;
                }
                tokio::time::sleep(backoff(attempt)).await;
            }
        }
    }
}

/// tokio-tungstenite's `rustls-tls-webpki-roots` enables rustls with **no**
/// crypto provider. A workspace build happens to unify `ring` in through
/// reqwest, but this crate must not depend on that accident, so the provider
/// is installed explicitly. Installing twice is not an error worth reporting.
fn install_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// One connection, from handshake to failure. `Ok(())` only when the consumer
/// has gone away.
async fn session(
    url: &str,
    mut spec: watch::Receiver<SubscriptionSpec>,
    floor: Option<Slot>,
    tx: &Sender,
) -> Result<(), String> {
    let mut current = spec.borrow_and_update().clone();
    if let Some(reason) = wire::unsupported(&current) {
        // A genuinely uncompilable spec is terminal — retrying cannot help.
        let _ = tx.send(Err(IngestError::UnsupportedSpec(reason))).await;
        return Ok(());
    }

    let (socket, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(|e| format!("connect to {}: {e}", redact(url)))?;
    let (mut sink, mut stream) = socket.split();

    let mut next_id = 1u64;
    let mut pending: BTreeMap<u64, Option<FilterId>> = BTreeMap::new();
    let mut subscriptions: BTreeMap<u64, FilterId> = BTreeMap::new();
    let mut by_filter: BTreeMap<FilterId, u64> = BTreeMap::new();

    // The checkpoint source first, so a connection that cannot checkpoint
    // fails before it starts delivering events it could not resume from.
    send(&mut sink, wire::root_subscribe_request(next_id)).await?;
    pending.insert(next_id, None);
    next_id += 1;

    for (id, filter) in &current.transactions {
        send(
            &mut sink,
            wire::subscribe_request(next_id, filter, current.commitment),
        )
        .await?;
        pending.insert(next_id, Some(id.clone()));
        next_id += 1;
    }

    let mut connected = false;
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await;
    let mut last_seen = tokio::time::Instant::now();
    let deadline = tokio::time::Instant::now() + SUBSCRIBE_ACK_TIMEOUT;

    loop {
        tokio::select! {
            biased;

            message = stream.next() => {
                let Some(message) = message else {
                    return Err("socket closed by peer".into());
                };
                let message = message.map_err(|e| format!("read: {e}"))?;
                last_seen = tokio::time::Instant::now();

                match message {
                    Message::Text(text) => {
                        match wire::parse_frame(&text) {
                            Frame::Ack { request_id, result } => {
                                if let Some(filter) = pending.remove(&request_id) {
                                    if let (Some(filter), Some(sub)) =
                                        (filter, result.as_u64())
                                    {
                                        subscriptions.insert(sub, filter.clone());
                                        by_filter.insert(filter, sub);
                                    }
                                }
                                // Connected means *subscribed*: a socket with
                                // no live filters would silently deliver
                                // nothing.
                                if !connected && pending.is_empty() {
                                    connected = true;
                                    log::info!(
                                        "helius-ws subscribed: {} filter(s)",
                                        by_filter.len()
                                    );
                                    forward(tx, IngestEvent::Status(StreamStatus::Connected))
                                        .await?;
                                }
                            }
                            Frame::Error { request_id, message } => {
                                if request_id.is_some_and(|id| pending.contains_key(&id)) {
                                    return Err(format!("subscribe rejected: {message}"));
                                }
                                log::warn!("helius-ws rpc error: {message}");
                            }
                            Frame::Transaction { subscription, mut update } => {
                                if floor.is_some_and(|floor| update.slot < floor) {
                                    continue;
                                }
                                // A notification for a subscription we just
                                // replaced still belongs to its filter;
                                // dropping it would be a real gap.
                                if let Some(filter) = subscriptions.get(&subscription) {
                                    update.filters = vec![filter.clone()];
                                }
                                forward(tx, IngestEvent::Transaction(*update)).await?;
                            }
                            Frame::Root { slot } => {
                                if floor.is_some_and(|floor| slot < floor) {
                                    continue;
                                }
                                forward(
                                    tx,
                                    IngestEvent::SlotCheckpoint(SlotCheckpoint { slot }),
                                )
                                .await?;
                            }
                            Frame::Other => {}
                        }
                    }
                    Message::Ping(payload) => {
                        sink.send(Message::Pong(payload))
                            .await
                            .map_err(|e| format!("pong: {e}"))?;
                    }
                    Message::Close(frame) => {
                        return Err(format!("peer closed: {frame:?}"));
                    }
                    _ => {}
                }
            }

            _ = ping.tick() => {
                if !connected && tokio::time::Instant::now() > deadline {
                    return Err("subscriptions were never acknowledged".into());
                }
                if last_seen.elapsed() > IDLE_TIMEOUT {
                    return Err(format!(
                        "no traffic for {}s", last_seen.elapsed().as_secs()
                    ));
                }
                sink.send(Message::Ping(Vec::new().into()))
                    .await
                    .map_err(|e| format!("ping: {e}"))?;
            }

            changed = spec.changed() => {
                if changed.is_err() {
                    // The consumer dropped the spec sender; nothing to serve.
                    return Ok(());
                }
                let next = spec.borrow_and_update().clone();
                if let Some(reason) = wire::unsupported(&next) {
                    let _ = tx.send(Err(IngestError::UnsupportedSpec(reason))).await;
                    return Ok(());
                }
                let d = wire::diff(&current, &next);
                if d.is_empty() {
                    continue;
                }

                // Make before break: the overlap delivers duplicates, which
                // signature-keyed idempotency absorbs. The reverse order would
                // drop everything in the window.
                for id in &d.added {
                    if let Some(filter) = next.transactions.get(id) {
                        send(
                            &mut sink,
                            wire::subscribe_request(next_id, filter, next.commitment),
                        )
                        .await?;
                        pending.insert(next_id, Some(id.clone()));
                        next_id += 1;
                    }
                }
                for id in &d.removed {
                    if let Some(sub) = by_filter.remove(id) {
                        subscriptions.remove(&sub);
                        send(&mut sink, wire::unsubscribe_request(next_id, sub)).await?;
                        pending.insert(next_id, None);
                        next_id += 1;
                    }
                }
                current = next;
                log::info!(
                    "helius-ws resubscribed: +{} -{}",
                    d.added.len(),
                    d.removed.len()
                );
                forward(tx, IngestEvent::Status(StreamStatus::Resubscribed)).await?;
            }
        }
    }
}

/// Hands an event to the consumer, treating a sustained full channel as a
/// hole rather than as backpressure to wait out.
async fn forward(tx: &Sender, event: IngestEvent) -> Result<(), String> {
    match tokio::time::timeout(LAG_TIMEOUT, tx.send(Ok(event))).await {
        Ok(Ok(())) => Ok(()),
        // The consumer went away; unwind quietly.
        Ok(Err(_)) => Err("consumer dropped".into()),
        Err(_) => {
            let _ = tx.try_send(Ok(IngestEvent::Status(StreamStatus::Lagged { dropped: 1 })));
            Err("consumer fell behind the event buffer".into())
        }
    }
}

async fn send<S>(sink: &mut S, request: serde_json::Value) -> Result<(), String>
where
    S: SinkExt<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::fmt::Display,
{
    sink.send(Message::Text(request.to_string().into()))
        .await
        .map_err(|e| format!("send: {e}"))
}

fn backoff(attempt: u32) -> Duration {
    Duration::from_millis(250u64.saturating_mul(1 << attempt.min(5)))
}

/// Strips the API key so a URL can appear in a log line.
pub fn redact(url: &str) -> String {
    match url.find("api-key=") {
        Some(index) => format!("{}api-key=***", &url[..index]),
        None => url.to_string(),
    }
}

/// Convenience for a consumer that wants one shared source.
pub fn shared(api_key: &str) -> Arc<dyn IngestSource> {
    Arc::new(HeliusWs::new(api_key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_api_key_never_reaches_a_log_line() {
        assert_eq!(
            redact("wss://mainnet.helius-rpc.com/?api-key=secret"),
            "wss://mainnet.helius-rpc.com/?api-key=***"
        );
        assert_eq!(redact("ws://127.0.0.1:9/"), "ws://127.0.0.1:9/");
    }

    #[test]
    fn the_endpoint_carries_the_key_as_a_query_parameter() {
        let source = HeliusWs::new("k");
        assert_eq!(source.url, "wss://mainnet.helius-rpc.com/?api-key=k");
        assert_eq!(source.name(), "helius-ws");
    }

    #[test]
    fn backoff_grows_and_stays_bounded() {
        assert!(backoff(1) < backoff(3));
        assert!(backoff(30) <= Duration::from_secs(8));
    }
}
