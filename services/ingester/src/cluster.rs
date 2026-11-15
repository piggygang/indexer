//! The boot-time cluster check.
//!
//! [`indexer_das::cluster::verify`] asks the endpoint which chain it is on;
//! this decides what each answer means for a process that is meant to stay up
//! for weeks.
//!
//! * **It agrees** → ingest.
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
//! The WebSocket endpoint is not checked here and cannot be: pubsub does not
//! answer `getGenesisHash`. It gets the config crate's offline check — a
//! hostname that names another cluster is refused at parse time — and that is
//! all it gets.

use std::time::Duration;

use anyhow::Context;
use indexer_config::SolanaConfig;
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
    /// Shutdown was requested before the endpoint answered. Nothing was
    /// verified, so nothing may be started.
    Interrupted,
}

/// Waits until `SOLANA_RPC_URL` has proved it serves `SOLANA_CLUSTER`.
///
/// `Err` only for a mismatch, with a message that names both variables — it is
/// the last thing the process logs before it exits.
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

/// One bounded round of [`verify`], for the head of the boot sequence.
///
/// `Ok(true)` when the endpoint proved itself inside `budget`, `Ok(false)`
/// when it has not answered yet — which is *not yet*, not a pass: the caller
/// still owes an unbounded [`verify`] before it starts anything that writes.
/// `Err` for a mismatch, exactly as [`verify`].
///
/// It runs before the signal handler exists, so nothing interrupts it but the
/// budget; the process is still an ordinary one that SIGTERM simply ends.
pub async fn verify_within(
    das: &DasClient,
    solana: &SolanaConfig,
    budget: Duration,
) -> anyhow::Result<bool> {
    let (_idle, never) = watch::channel(false);
    match tokio::time::timeout(budget, verify(das, solana, never)).await {
        Ok(verdict) => Ok(verdict? == Verdict::Verified),
        Err(_) => {
            log::warn!(
                "cluster still not verified after {budget:?}; carrying on with the boot — \
                 the check resumes before anything is ingested"
            );
            Ok(false)
        }
    }
}

/// The loop itself, with the expected hash and the backoff as parameters so a
/// test can use synthetic hashes and millisecond waits.
async fn wait_for(
    das: &DasClient,
    expected_genesis_hash: &str,
    backoff: &[Duration],
    mut shutdown: watch::Receiver<bool>,
) -> Result<Verdict, ClusterError> {
    let mut attempt = 0usize;
    loop {
        if *shutdown.borrow() {
            return Ok(Verdict::Interrupted);
        }
        match cluster::verify(das, expected_genesis_hash).await {
            Ok(()) => return Ok(Verdict::Verified),
            Err(mismatch @ ClusterError::Mismatch { .. }) => return Err(mismatch),
            Err(ClusterError::Unverified(error)) => {
                let wait = backoff[attempt.min(backoff.len() - 1)];
                attempt += 1;
                log::warn!(
                    "cluster not verified ({error}); nothing is ingested until it is — \
                     retrying in {wait:?} (attempt {attempt})"
                );
                let sleep = tokio::time::sleep(wait);
                tokio::pin!(sleep);
                tokio::select! {
                    _ = &mut sleep => {}
                    changed = shutdown.changed() => {
                        // No signal source left to wake us: wait the backoff
                        // out rather than spin on a closed channel.
                        if changed.is_err() {
                            sleep.await;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // Synthetic, like every key in this repo's tests: the check compares
    // strings, so two that differ are all a test needs.
    const OURS: &str = "SYNgenesisOURS1111111111111111111111111111";
    const THEIRS: &str = "SYNgenesisTHEIRS11111111111111111111111111";

    const QUICK: [Duration; 1] = [Duration::from_millis(5)];

    /// A loopback RPC that cannot answer `getGenesisHash` for its first
    /// `failures` calls — the JSON-RPC error a node gives while it is still
    /// starting — and then states `hash`.
    async fn rpc(failures: usize, hash: &'static str) -> (DasClient, Arc<AtomicUsize>) {
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
                    let payload = if attempt < failures {
                        serde_json::json!({
                            "jsonrpc": "2.0", "id": "indexer",
                            "error": {"code": -32005, "message": "Node is unhealthy"},
                        })
                    } else {
                        serde_json::json!({"jsonrpc": "2.0", "id": "indexer", "result": hash})
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
        (DasClient::with_endpoint(&endpoint, "").unwrap(), calls)
    }

    fn never() -> (watch::Sender<bool>, watch::Receiver<bool>) {
        watch::channel(false)
    }

    /// A devnet configuration. `rpc_url` here is only what the error message
    /// prints: the client under test already dials the loopback fake it was
    /// built for, on a port nobody chose.
    fn devnet() -> SolanaConfig {
        SolanaConfig {
            cluster: indexer_config::Cluster::Devnet,
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
        use indexer_config::Cluster;
        let budget = Duration::from_secs(5);

        let (das, _) = rpc(0, Cluster::Devnet.genesis_hash()).await;
        assert!(verify_within(&das, &devnet(), budget).await.unwrap());

        let (das, _) = rpc(0, Cluster::MainnetBeta.genesis_hash()).await;
        let error = verify_within(&das, &devnet(), budget).await.unwrap_err();
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

    /// The head start is a budget, not a verdict: an endpoint that has not
    /// answered inside it is reported as *not yet*, so boot can bring the
    /// receiver up — and the caller still owes the unbounded check.
    #[tokio::test]
    async fn an_unanswered_head_start_is_not_yet_rather_than_a_pass_or_an_error() {
        let (das, _) = rpc(usize::MAX, OURS).await;
        let verified = verify_within(&das, &devnet(), Duration::from_millis(300))
            .await
            .unwrap();
        assert!(!verified, "no answer is never a pass");
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
    /// and must not be mistaken for a verification.
    #[tokio::test]
    async fn a_shutdown_ends_the_wait_without_verifying() {
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
    }
}
