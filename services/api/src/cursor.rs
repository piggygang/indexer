//! Opaque keyset cursors (ALG-625).
//!
//! The contract types a cursor as `^[A-Za-z0-9_-]{8,512}$` — base64url,
//! unpadded — and tells clients to echo it verbatim and never decode it. The
//! *shape* inside is nonetheless pinned by the frozen examples, e.g.
//! `{"v":1,"s":"number","k":[3,"3"],"f":"9mKp2xQ1"}`, so this encodes exactly
//! that: version, sort, the `(key, id)` keyset tuple, and a fingerprint of the
//! filter set.
//!
//! `f` is what makes "valid only for the endpoint, sort and filter set that
//! issued it" enforceable rather than aspirational: change a filter and the
//! fingerprint stops matching, which is `400 invalid_cursor` — a condition the
//! contract explicitly calls normal and recoverable, not an outage.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use indexer_data_model::browse::{CursorKey, Sort};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::ApiError;

/// Bumped if the encoding ever changes; an older cursor then fails the version
/// check and the client restarts from page one.
const VERSION: u8 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct Payload {
    v: u8,
    s: String,
    /// `[sort key, id]`. The key is a JSON number for numeric sorts and a
    /// string for `name`; the id is always a string, matching the contract's
    /// `Int64String`.
    k: (serde_json::Value, String),
    f: String,
}

/// An 8-character fingerprint of everything a cursor is only valid under.
///
/// Collection and sort are included alongside the filters so a cursor cannot
/// be replayed across collections or against a different ordering, both of
/// which would silently return the wrong rows rather than erroring.
pub fn fingerprint(collection_id: i32, sort: Sort, filters: &str, q: Option<&str>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(collection_id.to_le_bytes());
    hasher.update(sort.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(filters.as_bytes());
    hasher.update([0]);
    hasher.update(q.unwrap_or_default().as_bytes());
    let digest = hasher.finalize();
    URL_SAFE_NO_PAD.encode(&digest[..6])
}

pub fn encode(sort: Sort, key: &CursorKey, fingerprint: &str) -> String {
    let payload = Payload {
        v: VERSION,
        s: sort.as_str().to_string(),
        k: (
            if matches!(sort, Sort::Name | Sort::NameDesc) {
                serde_json::Value::String(key.text.clone())
            } else {
                serde_json::Value::from(key.number)
            },
            key.id.to_string(),
        ),
        f: fingerprint.to_string(),
    };
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap_or_default())
}

/// Decodes a cursor, rejecting anything that was not issued for this exact
/// sort and filter set.
pub fn decode(raw: &str, sort: Sort, fingerprint: &str) -> Result<CursorKey, ApiError> {
    let stale = || ApiError::cursor("cursor was issued for a different sort or filter set");
    let bytes = URL_SAFE_NO_PAD.decode(raw).map_err(|_| stale())?;
    let payload: Payload = serde_json::from_slice(&bytes).map_err(|_| stale())?;
    if payload.v != VERSION || payload.s != sort.as_str() || payload.f != fingerprint {
        return Err(stale());
    }
    let id = payload.k.1.parse::<i64>().map_err(|_| stale())?;
    let (number, text) = match &payload.k.0 {
        serde_json::Value::Number(n) => (n.as_i64().ok_or_else(stale)?, String::new()),
        serde_json::Value::String(s) => (0, s.clone()),
        _ => return Err(stale()),
    };
    Ok(CursorKey { number, text, id })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(number: i64, text: &str, id: i64) -> CursorKey {
        CursorKey {
            number,
            text: text.to_string(),
            id,
        }
    }

    #[test]
    fn a_cursor_round_trips_and_matches_the_contract_alphabet() {
        let f = fingerprint(1, Sort::Number, "", None);
        let encoded = encode(Sort::Number, &key(3, "", 3), &f);
        assert!(
            encoded
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "must satisfy the contract's ^[A-Za-z0-9_-]+$: {encoded}"
        );
        assert!((8..=512).contains(&encoded.len()));
        assert_eq!(decode(&encoded, Sort::Number, &f).unwrap(), key(3, "", 3));
    }

    #[test]
    fn a_name_cursor_carries_the_text_key() {
        let f = fingerprint(1, Sort::Name, "", None);
        let encoded = encode(Sort::Name, &key(0, "#42", 7), &f);
        assert_eq!(decode(&encoded, Sort::Name, &f).unwrap(), key(0, "#42", 7));
    }

    #[test]
    fn a_cursor_is_refused_when_anything_it_was_issued_under_changes() {
        let f = fingerprint(1, Sort::Number, "Background=Pink", None);
        let encoded = encode(Sort::Number, &key(3, "", 3), &f);

        // A different filter set, sort, collection or search term all move the
        // fingerprint — the contract's "valid only for the endpoint, sort and
        // filter set it was issued for".
        for other in [
            fingerprint(1, Sort::Number, "Background=Blue", None),
            fingerprint(1, Sort::NumberDesc, "Background=Pink", None),
            fingerprint(2, Sort::Number, "Background=Pink", None),
            fingerprint(1, Sort::Number, "Background=Pink", Some("#12")),
        ] {
            assert!(decode(&encoded, Sort::Number, &other).is_err());
        }
        assert!(decode(&encoded, Sort::NumberDesc, &f).is_err());
        assert!(decode("not-base64!!", Sort::Number, &f).is_err());
    }
}
