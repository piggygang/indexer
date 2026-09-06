//! Opaque keyset cursors (ALG-625, generalized by ALG-626).
//!
//! The contract types a cursor as `^[A-Za-z0-9_-]{8,512}$` — base64url,
//! unpadded — and tells clients to echo it verbatim and never decode it. The
//! *shape* inside is nonetheless pinned by the frozen examples, e.g.
//! `{"v":1,"s":"number","k":[3,"3"],"f":"9mKp2xQ1"}`, so this encodes exactly
//! that: version, a mode discriminator, the `(key, id)` keyset tuple, and a
//! fingerprint of everything the cursor is only valid under.
//!
//! `f` is what makes "valid only for the endpoint, sort and filter set that
//! issued it" enforceable rather than aspirational: change a filter and the
//! fingerprint stops matching, which is `400 invalid_cursor` — a condition the
//! contract explicitly calls normal and recoverable, not an outage.
//!
//! **One deliberate divergence from the examples.** The frozen `-slot` and
//! `wallet` cursors carry no `f`. We emit one anyway. Without it a `?kind=sale`
//! cursor replayed against an unfiltered timeline would be accepted and would
//! silently skip rows — the exact failure the contract's own rule exists to
//! prevent. A longer cursor is still a conformant `^[A-Za-z0-9_-]{8,512}$`, and
//! clients never decode one, so nothing observable changes.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use indexer_data_model::browse::CursorKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::ApiError;

/// Bumped if the encoding ever changes; an older cursor then fails the version
/// check and the client restarts from page one.
const VERSION: u8 = 1;

/// The `s` discriminator of each paginated feed. Browse uses `Sort::as_str`.
pub const ACTIVITY: &str = "-slot";
pub const OWNERS: &str = "-from_slot";
pub const WALLET: &str = "wallet";

#[derive(Debug, Serialize, Deserialize)]
struct Payload {
    v: u8,
    s: String,
    /// `[sort key, id]`. The key is a JSON number for numeric orderings and a
    /// string for `name`; the id is always a string, matching the contract's
    /// `Int64String`.
    k: (serde_json::Value, String),
    f: String,
}

/// An 8-character fingerprint of everything a cursor is only valid under.
///
/// The parts are joined with a separator that cannot occur inside one, so no
/// two different scopes can hash to the same input. Every caller puts the
/// endpoint's identity first (a collection id, an asset address, a wallet),
/// which is what stops a cursor from one feed being replayed on another.
pub fn fingerprint(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    let digest = hasher.finalize();
    URL_SAFE_NO_PAD.encode(&digest[..6])
}

/// `text_key` selects which half of [`CursorKey`] is authoritative — the same
/// distinction `Sort::key_is_text` draws, passed explicitly so a mode that is
/// not a browse `Sort` can still be encoded.
pub fn encode(mode: &str, key: &CursorKey, text_key: bool, fingerprint: &str) -> String {
    let payload = Payload {
        v: VERSION,
        s: mode.to_string(),
        k: (
            if text_key {
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

/// A numeric-keyed cursor: the three ALG-626 feeds, whose keys are all slots or
/// collection ids.
pub fn encode_scalar(mode: &str, key: i64, id: i64, fingerprint: &str) -> String {
    encode(
        mode,
        &CursorKey {
            number: key,
            text: String::new(),
            id,
        },
        false,
        fingerprint,
    )
}

/// Decodes a cursor, rejecting anything that was not issued for this exact
/// mode and scope.
pub fn decode(raw: &str, mode: &str, fingerprint: &str) -> Result<CursorKey, ApiError> {
    let stale = || ApiError::cursor("cursor was issued for a different sort or filter set");
    let bytes = URL_SAFE_NO_PAD.decode(raw).map_err(|_| stale())?;
    let payload: Payload = serde_json::from_slice(&bytes).map_err(|_| stale())?;
    if payload.v != VERSION || payload.s != mode || payload.f != fingerprint {
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

/// The `(key, id)` position of a numeric-keyed cursor.
pub fn decode_scalar(raw: &str, mode: &str, fingerprint: &str) -> Result<(i64, i64), ApiError> {
    let key = decode(raw, mode, fingerprint)?;
    Ok((key.number, key.id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer_data_model::browse::Sort;

    fn key(number: i64, text: &str, id: i64) -> CursorKey {
        CursorKey {
            number,
            text: text.to_string(),
            id,
        }
    }

    fn browse_fingerprint(
        collection_id: i32,
        sort: Sort,
        filters: &str,
        q: Option<&str>,
    ) -> String {
        fingerprint(&[
            &collection_id.to_string(),
            sort.as_str(),
            filters,
            q.unwrap_or_default(),
        ])
    }

    #[test]
    fn a_cursor_round_trips_and_matches_the_contract_alphabet() {
        let f = browse_fingerprint(1, Sort::Number, "", None);
        let encoded = encode(Sort::Number.as_str(), &key(3, "", 3), false, &f);
        assert!(
            encoded
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "must satisfy the contract's ^[A-Za-z0-9_-]+$: {encoded}"
        );
        assert!((8..=512).contains(&encoded.len()));
        assert_eq!(
            decode(&encoded, Sort::Number.as_str(), &f).unwrap(),
            key(3, "", 3)
        );
    }

    #[test]
    fn a_name_cursor_carries_the_text_key() {
        let f = browse_fingerprint(1, Sort::Name, "", None);
        let encoded = encode(Sort::Name.as_str(), &key(0, "#42", 7), true, &f);
        assert_eq!(
            decode(&encoded, Sort::Name.as_str(), &f).unwrap(),
            key(0, "#42", 7)
        );
    }

    #[test]
    fn a_cursor_is_refused_when_anything_it_was_issued_under_changes() {
        let f = browse_fingerprint(1, Sort::Number, "Background=Pink", None);
        let encoded = encode(Sort::Number.as_str(), &key(3, "", 3), false, &f);

        // A different filter set, sort, collection or search term all move the
        // fingerprint — the contract's "valid only for the endpoint, sort and
        // filter set it was issued for".
        for other in [
            browse_fingerprint(1, Sort::Number, "Background=Blue", None),
            browse_fingerprint(1, Sort::NumberDesc, "Background=Pink", None),
            browse_fingerprint(2, Sort::Number, "Background=Pink", None),
            browse_fingerprint(1, Sort::Number, "Background=Pink", Some("#12")),
        ] {
            assert!(decode(&encoded, Sort::Number.as_str(), &other).is_err());
        }
        assert!(decode(&encoded, Sort::NumberDesc.as_str(), &f).is_err());
        assert!(decode("not-base64!!", Sort::Number.as_str(), &f).is_err());
    }

    #[test]
    fn the_feeds_do_not_share_a_scope() {
        // One asset's timeline, one collection's feed and a portfolio all key
        // on a bigint pair; only the fingerprint keeps them apart.
        let timeline = fingerprint(&["nft-activity", "SYNa", "mint,transfer,sale,burn"]);
        let filtered = fingerprint(&["nft-activity", "SYNa", "sale"]);
        let other_asset = fingerprint(&["nft-activity", "SYNb", "mint,transfer,sale,burn"]);
        let feed = fingerprint(&["collection-activity", "1", "mint,transfer,sale,burn"]);

        let cursor = encode_scalar(ACTIVITY, 318_441_077, 904_399, &timeline);
        assert_eq!(
            decode_scalar(&cursor, ACTIVITY, &timeline).unwrap(),
            (318_441_077, 904_399)
        );
        for other in [&filtered, &other_asset, &feed] {
            assert!(decode_scalar(&cursor, ACTIVITY, other).is_err());
        }
        // …and the mode alone rejects a same-scope cursor from another feed.
        assert!(decode_scalar(&cursor, OWNERS, &timeline).is_err());
        assert!(decode_scalar(&cursor, WALLET, &timeline).is_err());
    }

    #[test]
    fn a_fingerprints_parts_cannot_be_confused_by_concatenation() {
        // "ab" + "c" must not hash like "a" + "bc"; the separator is what
        // guarantees it.
        assert_ne!(fingerprint(&["ab", "c"]), fingerprint(&["a", "bc"]));
    }
}
