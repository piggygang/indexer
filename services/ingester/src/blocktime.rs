//! Slot → wall-clock time, cached.
//!
//! The WebSocket notification carries `slot` and `signature` but **no
//! `blockTime`**, while `activity.block_time` is `NOT NULL`. The migration
//! anticipated this: "getBlockTime(slot) (cached per slot) … A signature whose
//! block_time cannot be resolved stays unclassified."
//!
//! A slot's time never changes, so the cache never needs invalidating — only
//! bounding.
//!
//! **A fresh slot is not a missing slot.** The stream delivers at `confirmed`,
//! and `getBlockTime` on a slot that new routinely answers *"Block not
//! available for slot N"* — the block simply has not landed on the node that
//! answered yet. Production lost a real transfer to exactly that on
//! 2026-09-07: the event decoded, the block time did not resolve, and the
//! signature was parked and never written. So a failure is retried briefly
//! before it is believed.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use indexer_das::DasClient;
use tokio::sync::Mutex;

/// Slots retained. ~2.5 slots/s, so this is roughly an hour of history at a
/// few tens of bytes each.
const CAPACITY: usize = 8192;

/// Waits before re-asking for a slot the cluster does not have yet.
///
/// Short and few: the caller is the live pipeline, so this blocks one event's
/// write, and a block that has not landed within a couple of seconds is a
/// different problem from one that landed a moment ago. Anything still
/// unresolved after this is genuinely parked, and the recovery walk — which
/// gets `blockTime` free from `getSignaturesForAddress` — will pick it up.
const RETRY_BACKOFF_MS: [u64; 3] = [400, 900, 2_000];

#[derive(Default)]
pub struct BlockTimes {
    resolved: Mutex<BTreeMap<i64, DateTime<Utc>>>,
}

impl BlockTimes {
    pub fn new() -> Self {
        Self::default()
    }

    /// `None` means the cluster could not tell us — the caller must park the
    /// signature rather than invent a timestamp.
    ///
    /// Retries before giving up: the common failure is a slot too fresh for
    /// the node that answered, which resolves within a second or two. Giving
    /// up on the first attempt is how a delivered, decoded transfer gets
    /// silently dropped.
    pub async fn get(&self, das: &DasClient, slot: i64) -> Option<DateTime<Utc>> {
        if let Some(hit) = self.resolved.lock().await.get(&slot) {
            return Some(*hit);
        }
        for (attempt, wait) in std::iter::once(&0)
            .chain(RETRY_BACKOFF_MS.iter())
            .enumerate()
        {
            if *wait > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(*wait)).await;
            }
            match das.get_block_time(slot).await {
                Ok(Some(time)) => {
                    if attempt > 0 {
                        log::info!("getBlockTime({slot}) resolved on attempt {}", attempt + 1);
                    }
                    self.insert(slot, time).await;
                    return Some(time);
                }
                // A `null` result and an RPC error mean the same thing here —
                // the node cannot answer *yet* — and both are worth retrying.
                Ok(None) => log::debug!("getBlockTime({slot}) not available yet"),
                Err(error) => log::debug!("getBlockTime({slot}) failed: {error}"),
            }
        }
        log::warn!(
            "getBlockTime({slot}) unresolved after {} attempts; the signature will be parked \
             and recovered by the next sweep",
            RETRY_BACKOFF_MS.len() + 1
        );
        None
    }

    /// Seeds a slot we already learned about elsewhere —
    /// `getSignaturesForAddress` returns `blockTime`, so the recovery path
    /// fills this in for free and never calls `getBlockTime` at all.
    pub async fn insert(&self, slot: i64, time: DateTime<Utc>) {
        let mut cache = self.resolved.lock().await;
        cache.insert(slot, time);
        // Slots arrive in roughly ascending order, so dropping the lowest keys
        // evicts the coldest entries.
        while cache.len() > CAPACITY {
            let Some(oldest) = cache.keys().next().copied() else {
                break;
            };
            cache.remove(&oldest);
        }
    }

    pub async fn len(&self) -> usize {
        self.resolved.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    /// A loopback RPC that answers `getBlockTime` the way a node does for a
    /// slot it has not seen yet — `failures` times — and then answers properly.
    ///
    /// The error text is the one production actually logged on 2026-09-07.
    async fn flaky_rpc(failures: usize, secs: i64) -> (DasClient, Arc<AtomicUsize>) {
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
                            "error": {"code": -32004,
                                      "message": "Block not available for slot 445026409"},
                        })
                    } else {
                        serde_json::json!({"jsonrpc": "2.0", "id": "indexer", "result": secs})
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

    /// The 2026-09-07 loss, reproduced: a decoded transfer whose slot was
    /// minutes old got *"Block not available for slot 445026409"*, was parked,
    /// and was never written. One attempt is not enough evidence that a block
    /// does not exist.
    #[tokio::test]
    async fn a_slot_the_node_has_not_caught_up_to_yet_is_retried_not_parked() {
        let (das, calls) = flaky_rpc(2, 1_700_000_500).await;
        let times = BlockTimes::new();

        assert_eq!(
            times.get(&das, 445_026_409).await,
            Some(at(1_700_000_500)),
            "the block landed on the third attempt and must be believed"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        // And it is cached, so the retry is paid once per slot, not per event.
        assert_eq!(times.get(&das, 445_026_409).await, Some(at(1_700_000_500)));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    /// The other half: a slot that genuinely has no block still parks, after
    /// the whole budget. Retrying must not turn "never" into a hang.
    #[tokio::test]
    async fn a_slot_that_never_resolves_is_parked_after_the_budget() {
        let (das, calls) = flaky_rpc(usize::MAX, 0).await;
        let times = BlockTimes::new();

        assert_eq!(times.get(&das, 1).await, None);
        assert_eq!(calls.load(Ordering::Relaxed), RETRY_BACKOFF_MS.len() + 1);
        assert!(times.is_empty().await, "nothing unresolved is ever cached");
    }

    #[tokio::test]
    async fn a_seeded_slot_is_served_from_the_cache() {
        let times = BlockTimes::new();
        assert!(times.is_empty().await);
        times.insert(100, at(1_700_000_000)).await;
        assert_eq!(times.len().await, 1);

        // A DasClient pointed at an unroutable endpoint proves the hit never
        // reaches the network: a miss here would error, not return.
        let das = DasClient::with_endpoint("http://127.0.0.1:1", "").unwrap();
        assert_eq!(times.get(&das, 100).await, Some(at(1_700_000_000)));
    }

    #[tokio::test]
    async fn the_cache_stays_bounded() {
        let times = BlockTimes::new();
        for slot in 0..(CAPACITY as i64 + 50) {
            times.insert(slot, at(1_700_000_000 + slot)).await;
        }
        assert_eq!(times.len().await, CAPACITY);
        // The coldest (lowest) slots were evicted, the newest retained.
        let cache = times.resolved.lock().await;
        assert!(!cache.contains_key(&0));
        assert!(cache.contains_key(&(CAPACITY as i64 + 49)));
    }
}
