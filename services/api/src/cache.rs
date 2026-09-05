//! Short-TTL response cache and weak ETags (ALG-625).
//!
//! Two jobs, both cheap:
//!
//! * a **weak `ETag`** over the serialized body so a client that sends
//!   `If-None-Match` gets `304` with no body — the contract puts
//!   `If-None-Match` on every operation and `304` on every response set;
//! * a **short TTL** so a burst of identical requests costs one query. Latency
//!   is not the motive — the facet query measures under 30 ms p95 on the real
//!   collection — the shared five-connection pool is.
//!
//! Hand-rolled rather than a cache crate: the key space is four collections
//! times a handful of filter combinations, and this workspace curates its
//! dependency tree deliberately. Eviction is "clear when full", which for a
//! bounded key space and a seconds-long TTL is a bulk expiry, not thrashing.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

/// How long a rendered body stays fresh. Also the `max-age` clients are told.
/// The contract calls the exact value operational and changeable.
pub const TTL: Duration = Duration::from_secs(15);

/// Past this many entries the cache is cleared rather than evicted one by one.
const CAPACITY: usize = 512;

/// A rendered response: the JSON body and its validator.
#[derive(Debug, Clone)]
pub struct Cached {
    pub body: String,
    pub etag: String,
}

impl Cached {
    pub fn new(body: String) -> Self {
        // Weak, and 16 hex characters wide to match the contract's example
        // (`W/"1f0a2b3c4d5e6f70"`). Truncating a strong hash is fine for a
        // validator whose only job is equality.
        let digest = Sha256::digest(body.as_bytes());
        let etag = format!(
            "W/\"{}\"",
            digest[..8]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        Self { body, etag }
    }
}

#[derive(Default)]
pub struct ResponseCache {
    entries: RwLock<HashMap<String, (Instant, Cached)>>,
}

impl ResponseCache {
    pub fn get(&self, key: &str) -> Option<Cached> {
        let entries = self.entries.read().ok()?;
        let (stored, cached) = entries.get(key)?;
        (stored.elapsed() < TTL).then(|| cached.clone())
    }

    pub fn put(&self, key: String, cached: Cached) {
        let Ok(mut entries) = self.entries.write() else {
            return;
        };
        if entries.len() >= CAPACITY {
            entries.clear();
        }
        entries.insert(key, (Instant::now(), cached));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_etag_is_weak_stable_and_content_addressed() {
        let a = Cached::new("{\"a\":1}".into());
        assert_eq!(a.etag, Cached::new("{\"a\":1}".into()).etag);
        assert_ne!(a.etag, Cached::new("{\"a\":2}".into()).etag);
        assert!(a.etag.starts_with("W/\""), "{}", a.etag);
        // W/" + 16 hex + " — the width the contract's example uses.
        assert_eq!(a.etag.len(), 3 + 16 + 1);
    }

    #[test]
    fn a_stored_body_is_returned_and_a_full_cache_does_not_grow() {
        let cache = ResponseCache::default();
        cache.put("k".into(), Cached::new("body".into()));
        assert_eq!(cache.get("k").unwrap().body, "body");
        assert!(cache.get("absent").is_none());

        for i in 0..CAPACITY + 10 {
            cache.put(format!("k{i}"), Cached::new("x".into()));
        }
        assert!(cache.entries.read().unwrap().len() <= CAPACITY);
    }
}
