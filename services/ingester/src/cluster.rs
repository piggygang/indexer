//! The boot-time cluster check, and the gate it opens.
//!
//! [`indexer_das::cluster::verify`] asks the endpoint which chain it is on;
//! this decides what each answer means for a process that is meant to stay up
//! for weeks.
//!
//! * **It agrees** → the gate opens and ingestion starts.
//! * **It is another cluster** → fatal, on purpose. This is exactly the
//!   "configuration failure a restart cannot fix" the binary reserves a
//!   non-zero exit for, and the alternative is not a degraded ingester but a
//!   destructive one: the first sweep against the wrong chain finds none of a
//!   Core collection's assets and marks every one of them `removed`.
//! * **It does not answer** → wait, for as long as it takes, with a capped
//!   backoff. Never a pass — an unverified endpoint is the case the check
//!   exists for — and never an exit either: Railway's `ON_FAILURE` policy gives
//!   up after ten restarts, so exiting on an RPC outage would turn a Helius
//!   incident into an ingester that is still down after Helius is back. Nothing
//!   can be ingested without the RPC anyway (the live path needs
//!   `getBlockTime`, the drain needs `getTransaction`), so waiting costs
//!   nothing the outage had not already taken, and the reconnect reconcile
//!   closes the gap exactly as it does after any other downtime.
//!
//! **The gate is what makes the deploy tell those apart.** [`ClusterGate`]
//! is what `/health` answers from, and it stays shut — 503 — until the check
//! passes. So a new deployment that is on the wrong chain, or that cannot get
//! an answer at all (a mistyped host, a dead key), never goes green: Railway
//! keeps the previous one. A deployment that is *already* live and restarts
//! into a Helius incident is not gated by anything — the healthcheck only
//! gates deploys — and simply waits here, with the webhook receiver up and
//! filing, until the endpoint answers.
//!
//! The WebSocket endpoint is not checked here and cannot be: pubsub does not
//! answer `getGenesisHash`. It gets the config crate's offline check — a
//! hostname that names another cluster is refused at parse time — and that is
//! all it gets.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context;
use indexer_config::{Cluster, SolanaConfig};
use indexer_das::{cluster, ClusterError, DasClient};
use tokio::sync::watch;

/// Between attempts while the endpoint is not answering. The same shape as the
/// consumer supervisor's backoff, for the same reason: a persistent upstream
/// outage must not be a hot loop.
const BACKOFF: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(15),
    Duration::from_secs(30),
    Duration::from_secs(60),
];

/// How a wait for the cluster ended without an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The endpoint is on the configured cluster.
    Verified,
    /// Shutdown was requested before the endpoint was verified. Nothing was
    /// verified, so nothing may be started — including when the answer was
    /// already on its way.
    Interrupted,
}

/// Which cluster this deployment was configured for, and whether its RPC
/// endpoint has proved it yet. The receiver's `/health` answers from this.
///
/// Shut until [`ClusterGate::open`], and open for good after: the genesis hash
/// of an endpoint does not change under a running process, so there is nothing
/// that could shut it again and no reason for a probe to re-ask. That is what
/// keeps `/health` a liveness check — one fact, settled once at boot, the same
/// way "connected to Postgres and migrated" is — rather than a dependency ping
/// that could fail a later deploy on a Helius blip.
#[derive(Debug)]
pub struct ClusterGate {
    cluster: Cluster,
    verified: AtomicBool,
}

impl ClusterGate {
    pub fn new(cluster: Cluster) -> Self {
        Self {
            cluster,
            verified: AtomicBool::new(false),
        }
    }

    pub fn cluster(&self) -> Cluster {
        self.cluster
    }

    /// The endpoint has proved its cluster. One way only.
    pub fn open(&self) {
        self.verified.store(true, Ordering::Release);
    }

    pub fn is_open(&self) -> bool {
        self.verified.load(Ordering::Acquire)
    }
}

/// Waits until `SOLANA_RPC_URL` has proved it serves `SOLANA_CLUSTER`.
///
/// `Err` only for a mismatch, with a message that names both variables — it is
/// the last thing the process logs before it exits.
///
/// Give it a client that does not retry on its own (`with_max_attempts(1)`):
/// this loop owns the retrying and the backoff, and it is the half that can
/// hear a shutdown. The client's four attempts at a 30 s timeout each would
/// otherwise sit inside a single round.
pub async fn verify(
    das: &DasClient,
    solana: &SolanaConfig,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<Verdict> {
    let cluster = solana.cluster;
    let verdict = wait_for(das, cluster.genesis_hash(), &BACKOFF, shutdown)
        .await
        .with_context(|| {
            format!(
                "SOLANA_CLUSTER is {cluster}, but SOLANA_RPC_URL ({}) does not serve it — \
                 fix whichever of the two is wrong; refusing to ingest",
                solana
                    .rpc_url
                    .as_deref()
                    .map(indexer_config::origin)
                    .unwrap_or_else(|| "unset".to_string()),
            )
        })?;
    if verdict == Verdict::Verified {
        log::info!("cluster verified: the RPC endpoint's genesis hash is {cluster}'s");
    }
    Ok(verdict)
}

/// The loop itself, with the expected hash and the backoff as parameters so a
/// test can use synthetic hashes and millisecond waits.
///
/// Shutdown is raced against the request as well as against the backoff, and
/// it is the `biased` first arm of both: an endpoint that accepts a connection
/// and then says nothing must not hold SIGTERM for the length of its timeout,
/// and an answer that lands together with the request to stop must not come
/// back as `Verified` to a caller that has been told to stop.
async fn wait_for(
    das: &DasClient,
    expected_genesis_hash: &str,
    backoff: &[Duration],
    mut shutdown: watch::Receiver<bool>,
) -> Result<Verdict, ClusterError> {
    let mut attempt = 0usize;
    loop {
        let answer = tokio::select! {
            biased;
            _ = stopped(&mut shutdown) => return Ok(Verdict::Interrupted),
            answer = cluster::verify(das, expected_genesis_hash) => answer,
        };
        match answer {
            Ok(()) => return Ok(Verdict::Verified),
            Err(mismatch @ ClusterError::Mismatch { .. }) => return Err(mismatch),
            Err(ClusterError::Unverified(error)) => {
                let wait = backoff[attempt.min(backoff.len() - 1)];
                attempt += 1;
                log::warn!(
                    "cluster not verified ({error}); nothing is ingested until it is — \
                     retrying in {wait:?} (attempt {attempt})"
                );
                tokio::select! {
                    biased;
                    _ = stopped(&mut shutdown) => return Ok(Verdict::Interrupted),
                    _ = tokio::time::sleep(wait) => {}
                }
            }
        }
    }
}

/// Resolves once shutdown has been requested — and never, if the signal task
/// is gone without having asked. A closed channel has to lose every `select!`
/// it is in, so the request and the backoff run their course; resolving on it
/// would turn the backoff into a hot loop.
async fn stopped(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return std::future::pending().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // Synthetic, like every key in this repo's tests: the check compares
    // strings, so two that differ are all a test needs.
    const OURS: &str = "SYNgenesisOURS1111111111111111111111111111";
    const THEIRS: &str = "SYNgenesisTHEIRS11111111111111111111111111";

    const QUICK: [Duration; 1] = [Duration::from_millis(5)];

    /// How the loopback node behaves once it has read a request.
    #[derive(Clone, Copy)]
    enum Node {
        /// Cannot answer `getGenesisHash` for its first `failures` calls — the
        /// JSON-RPC error a node gives while it is still starting — and then
        /// states `hash`.
        Answers { failures: usize, hash: &'static str },
        /// States `hash`, but only after `delay`.
        Slow { delay: Duration, hash: &'static str },
        /// Accepts the connection and never says anything: a wedged proxy, a
        /// load balancer with no healthy backend.
        Silent,
    }

    async fn node(behaviour: Node) -> (DasClient, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let served = Arc::clone(&calls);
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let attempt = served.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let mut chunk = [0u8; 4096];
                    let _ = stream.read(&mut chunk).await;
                    let hash = match behaviour {
                        Node::Answers { failures, hash } => (attempt >= failures).then_some(hash),
                        Node::Slow { delay, hash } => {
                            tokio::time::sleep(delay).await;
                            Some(hash)
                        }
                        Node::Silent => return std::future::pending().await,
                    };
                    let payload = match hash {
                        Some(hash) => {
                            serde_json::json!({"jsonrpc": "2.0", "id": "indexer", "result": hash})
                        }
                        None => serde_json::json!({
                            "jsonrpc": "2.0", "id": "indexer",
                            "error": {"code": -32005, "message": "Node is unhealthy"},
                        }),
                    }
                    .to_string();
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
        // One attempt per round, as `main` builds it.
        let das = DasClient::with_endpoint(&endpoint, "")
            .unwrap()
            .with_max_attempts(1);
        (das, calls)
    }

    async fn rpc(failures: usize, hash: &'static str) -> (DasClient, Arc<AtomicUsize>) {
        node(Node::Answers { failures, hash }).await
    }

    fn never() -> (watch::Sender<bool>, watch::Receiver<bool>) {
        watch::channel(false)
    }

    /// A devnet configuration. `rpc_url` here is only what the error message
    /// prints: the client under test already dials the loopback fake it was
    /// built for, on a port nobody chose.
    fn devnet() -> SolanaConfig {
        SolanaConfig {
            cluster: Cluster::Devnet,
            rpc_url: Some("http://127.0.0.1:8899".into()),
            ws_url: None,
        }
    }

    /// The whole public path, with the real hashes read from the config crate
    /// rather than spelled here: a devnet deployment whose endpoint answers as
    /// devnet boots, and one whose endpoint answers as mainnet does not — with
    /// an error that names both variables, the endpoint's origin and both
    /// hashes, because it is the last line the process prints.
    #[tokio::test]
    async fn the_boot_check_passes_its_own_cluster_and_names_everything_on_a_mismatch() {
        let (das, _) = rpc(0, Cluster::Devnet.genesis_hash()).await;
        let (_keep, shutdown) = never();
        assert_eq!(
            verify(&das, &devnet(), shutdown).await.unwrap(),
            Verdict::Verified
        );

        let (das, _) = rpc(0, Cluster::MainnetBeta.genesis_hash()).await;
        let (_keep, shutdown) = never();
        let error = verify(&das, &devnet(), shutdown).await.unwrap_err();
        let message = format!("{error:#}");
        for expected in [
            "SOLANA_CLUSTER is devnet",
            "SOLANA_RPC_URL (http://127.0.0.1:8899)",
            Cluster::Devnet.genesis_hash(),
            Cluster::MainnetBeta.genesis_hash(),
        ] {
            assert!(
                message.contains(expected),
                "missing {expected:?} in: {message}"
            );
        }
    }

    #[tokio::test]
    async fn an_endpoint_on_the_configured_cluster_is_verified_at_once() {
        let (das, calls) = rpc(0, OURS).await;
        let (_keep, shutdown) = never();

        let verdict = wait_for(&das, OURS, &QUICK, shutdown).await.unwrap();
        assert_eq!(verdict, Verdict::Verified);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    /// The case the check exists for. It must end the wait rather than be
    /// retried — asking again cannot change which chain a node is on — and the
    /// error has to carry both hashes, because it is the only evidence the
    /// operator gets about which variable is the wrong one.
    #[tokio::test]
    async fn an_endpoint_on_another_cluster_is_fatal_and_never_retried() {
        let (das, calls) = rpc(0, THEIRS).await;
        let (_keep, shutdown) = never();

        let error = wait_for(&das, OURS, &QUICK, shutdown).await.unwrap_err();
        assert!(
            matches!(&error, ClusterError::Mismatch { expected, actual }
                if expected == OURS && actual == THEIRS),
            "unexpected error: {error}"
        );
        let message = error.to_string();
        assert!(message.contains(OURS) && message.contains(THEIRS));
        assert_eq!(calls.load(Ordering::Relaxed), 1, "a mismatch is an answer");
    }

    /// An endpoint that cannot say which chain it is on has not passed. It is
    /// waited out — however long that takes — and believed only once it
    /// answers.
    #[tokio::test]
    async fn an_endpoint_that_cannot_answer_is_waited_out_never_passed() {
        let (das, calls) = rpc(3, OURS).await;
        let (_keep, shutdown) = never();

        let verdict = wait_for(&das, OURS, &QUICK, shutdown).await.unwrap();
        assert_eq!(verdict, Verdict::Verified);
        assert_eq!(calls.load(Ordering::Relaxed), 4);

        // And the same outage in front of the wrong chain still ends fatally:
        // waiting must not turn into an eventual pass.
        let (das, _) = rpc(2, THEIRS).await;
        let (_keep, shutdown) = never();
        assert!(matches!(
            wait_for(&das, OURS, &QUICK, shutdown).await,
            Err(ClusterError::Mismatch { .. })
        ));
    }

    /// SIGTERM during an outage must not have to wait for the outage to end —
    /// and must not be mistaken for a verification. This is the backoff half:
    /// the node answers at once, with an error, and the wait is in the sleep.
    #[tokio::test]
    async fn a_shutdown_during_the_backoff_ends_the_wait() {
        let (das, _) = rpc(usize::MAX, OURS).await;
        let (stop, shutdown) = never();
        let long = [Duration::from_secs(3_600)];

        let waiting = tokio::spawn(async move { wait_for(&das, OURS, &long, shutdown).await });
        // Let it fail once and settle into the hour-long backoff.
        tokio::time::sleep(Duration::from_millis(100)).await;
        stop.send(true).unwrap();

        let verdict = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the wait must end promptly on shutdown")
            .unwrap()
            .unwrap();
        assert_eq!(verdict, Verdict::Interrupted);
    }

    /// The request half, which is the one that bites: an endpoint that accepts
    /// the connection and then says nothing holds the request open for the
    /// client's whole 30 s timeout. Without a race against shutdown the process
    /// neither serves nor exits for that long, and Railway ends it with SIGKILL.
    #[tokio::test]
    async fn a_shutdown_while_the_request_is_in_flight_ends_the_wait() {
        let (das, calls) = node(Node::Silent).await;
        let (stop, shutdown) = never();

        let waiting = tokio::spawn(async move { wait_for(&das, OURS, &QUICK, shutdown).await });
        // Long enough for the request to be on the wire and unanswered.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(calls.load(Ordering::Relaxed), 1, "the request is in flight");
        stop.send(true).unwrap();

        let verdict = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("an unanswered request must not hold the shutdown")
            .unwrap()
            .unwrap();
        assert_eq!(verdict, Verdict::Interrupted);
        assert_eq!(calls.load(Ordering::Relaxed), 1, "and it was not retried");
    }

    /// `Interrupted` means nothing may be started, so it has to hold even when
    /// the right answer is already on its way: shutdown is asked for while the
    /// node is still composing a reply that would have verified.
    #[tokio::test]
    async fn an_answer_that_arrives_after_the_shutdown_request_is_not_a_verification() {
        let (das, _) = node(Node::Slow {
            delay: Duration::from_millis(600),
            hash: OURS,
        })
        .await;
        let (stop, shutdown) = never();

        let waiting = tokio::spawn(async move { wait_for(&das, OURS, &QUICK, shutdown).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        stop.send(true).unwrap();

        let verdict = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(verdict, Verdict::Interrupted);

        // The same node, left alone, does verify — so the verdict above was
        // the shutdown's doing, not the node's.
        let (das, _) = node(Node::Slow {
            delay: Duration::from_millis(50),
            hash: OURS,
        })
        .await;
        let (_keep, shutdown) = never();
        assert_eq!(
            wait_for(&das, OURS, &QUICK, shutdown).await.unwrap(),
            Verdict::Verified
        );
    }

    /// With the signal task gone there is nothing left to wake the wait, and a
    /// closed channel must not turn the backoff into a hot loop.
    #[tokio::test]
    async fn a_closed_shutdown_channel_does_not_skip_the_backoff() {
        let (das, calls) = rpc(usize::MAX, OURS).await;
        let (stop, shutdown) = never();
        drop(stop);
        let slow = [Duration::from_millis(200)];

        let waited = tokio::time::timeout(
            Duration::from_millis(500),
            wait_for(&das, OURS, &slow, shutdown),
        )
        .await;
        assert!(waited.is_err(), "still waiting, as it should be");
        // A spin would make hundreds of calls in half a second; the backoff
        // allows three. Only the ceiling is asserted, so a slow runner that
        // fits fewer attempts into the window cannot fail this.
        let attempts = calls.load(Ordering::Relaxed);
        assert!(
            attempts <= 4,
            "one attempt per 200 ms backoff, not a spin: {attempts}"
        );

        // And it still verifies: a closed channel is "nobody will ask", not
        // "stop".
        let (das, _) = rpc(1, OURS).await;
        let (stop, shutdown) = never();
        drop(stop);
        assert_eq!(
            wait_for(&das, OURS, &QUICK, shutdown).await.unwrap(),
            Verdict::Verified
        );
    }

    /// Shut until the check passes, and open for good after.
    #[test]
    fn the_gate_is_shut_until_opened_and_remembers_its_cluster() {
        for cluster in Cluster::ALL {
            let gate = ClusterGate::new(cluster);
            assert!(!gate.is_open(), "a new gate must be shut");
            assert_eq!(gate.cluster(), cluster);
            gate.open();
            assert!(gate.is_open());
            // Idempotent: there is no state in which opening shuts it.
            gate.open();
            assert!(gate.is_open());
        }
    }
}
