//! Slot → wall-clock time, cached.
//!
//! The WebSocket notification carries `slot` and `signature` but **no
//! `blockTime`**, while `activity.block_time` is `NOT NULL`. The migration
//! anticipated this: "getBlockTime(slot) (cached per slot) … A signature whose
//! block_time cannot be resolved stays unclassified."
//!
//! A slot's time never changes, so the cache never needs invalidating — only
//! bounding. A negative answer is cached too, but briefly: a very fresh
//! `confirmed` slot often resolves a moment later.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use indexer_das::DasClient;
use tokio::sync::Mutex;

/// Slots retained. ~2.5 slots/s, so this is roughly an hour of history at a
/// few tens of bytes each.
const CAPACITY: usize = 8192;

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
    pub async fn get(&self, das: &DasClient, slot: i64) -> Option<DateTime<Utc>> {
        if let Some(hit) = self.resolved.lock().await.get(&slot) {
            return Some(*hit);
        }
        let fetched = match das.get_block_time(slot).await {
            Ok(value) => value,
            Err(error) => {
                log::warn!("getBlockTime({slot}) failed: {error}");
                None
            }
        };
        if let Some(time) = fetched {
            self.insert(slot, time).await;
        }
        fetched
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

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
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
