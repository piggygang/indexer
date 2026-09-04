//! The JSON layer of the Helius Enhanced WebSocket protocol, kept free of
//! sockets so every request shape and every parse rule is unit-testable.

use serde_json::{json, Value};

use crate::event::{RawPayload, Slot, TransactionUpdate};
use crate::spec::{Commitment, FilterId, SubscriptionSpec, TransactionFilter};

/// Helius caps each of `accountInclude`/`accountExclude`/`accountRequired` at
/// 50 000 entries. All 17 073 tracked mints plus the Core collection address
/// fit comfortably; the limit exists so an oversized spec fails loudly at
/// compile time rather than as an opaque server error.
pub const MAX_ADDRESSES: usize = 50_000;

/// Subscriptions per connection, per the Developer plan.
pub const MAX_FILTERS: usize = 1_000;

const fn commitment_str(commitment: Commitment) -> &'static str {
    match commitment {
        Commitment::Processed => "processed",
        Commitment::Confirmed => "confirmed",
        Commitment::Finalized => "finalized",
    }
}

/// One `transactionSubscribe` call.
///
/// `vote: false` is hard-coded because [`SubscriptionSpec`] states it as an
/// invariant with no field ("Vote transactions are always excluded").
/// `jsonParsed` + `full` are what the decoder needs: parsed instruction names
/// keep on-chain program addresses out of Rust, and the token balances that
/// establish ownership live in `meta`.
pub fn subscribe_request(
    request_id: u64,
    filter: &TransactionFilter,
    commitment: Commitment,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "transactionSubscribe",
        "params": [
            {
                "vote": false,
                "failed": filter.include_failed,
                "accountInclude": filter.account_include,
                "accountRequired": filter.account_required,
            },
            {
                "commitment": commitment_str(commitment),
                "encoding": "jsonParsed",
                "transactionDetails": "full",
                "showRewards": false,
                "maxSupportedTransactionVersion": 0,
            }
        ]
    })
}

/// The checkpoint source. `SubscriptionSpec` carries no slot filter by design
/// — "adapters always track slots themselves and emit `SlotCheckpoint`".
///
/// `rootSubscribe` rather than `slotSubscribe`: the root is finalized, so it
/// can never sit ahead of a confirmed transaction that was not delivered,
/// which on a transport with no replay would be permanent loss.
pub fn root_subscribe_request(request_id: u64) -> Value {
    json!({"jsonrpc": "2.0", "id": request_id, "method": "rootSubscribe"})
}

pub fn unsubscribe_request(request_id: u64, subscription: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "transactionUnsubscribe",
        "params": [subscription],
    })
}

/// What arrived on the socket.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// A subscribe/unsubscribe reply: `id` → subscription id (or `true`).
    Ack { request_id: u64, result: Value },
    Error {
        request_id: Option<u64>,
        message: String,
    },
    Transaction {
        subscription: u64,
        update: Box<TransactionUpdate>,
    },
    /// A finalized root, from `rootSubscribe`.
    Root { slot: Slot },
    /// Anything else, including notifications we do not act on.
    Other,
}

pub fn parse_frame(text: &str) -> Frame {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return Frame::Other;
    };

    if let Some(error) = value.get("error") {
        return Frame::Error {
            request_id: value.get("id").and_then(Value::as_u64),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error")
                .to_string(),
        };
    }

    if let (Some(request_id), Some(result)) =
        (value.get("id").and_then(Value::as_u64), value.get("result"))
    {
        return Frame::Ack {
            request_id,
            result: result.clone(),
        };
    }

    match value.get("method").and_then(Value::as_str) {
        Some("transactionNotification") => transaction_frame(&value),
        Some("rootNotification") => value
            .pointer("/params/result")
            .and_then(Value::as_u64)
            .map(|slot| Frame::Root { slot })
            .unwrap_or(Frame::Other),
        _ => Frame::Other,
    }
}

fn transaction_frame(value: &Value) -> Frame {
    let Some(subscription) = value
        .pointer("/params/subscription")
        .and_then(Value::as_u64)
    else {
        return Frame::Other;
    };
    let Some(result) = value.pointer("/params/result") else {
        return Frame::Other;
    };
    let Some(signature) = result.get("signature").and_then(Value::as_str) else {
        return Frame::Other;
    };
    let Some(slot) = result.get("slot").and_then(Value::as_u64) else {
        return Frame::Other;
    };

    let failed = result
        .pointer("/transaction/meta/err")
        .is_some_and(|e| !e.is_null());

    Frame::Transaction {
        subscription,
        update: Box::new(TransactionUpdate {
            // Filled in by the adapter, which owns the subscription→filter map.
            filters: Vec::new(),
            slot,
            signature: signature.to_string(),
            failed,
            account_keys: account_keys(result),
            raw: RawPayload::Json(result.clone()),
        }),
    }
}

/// Static keys plus lookup-table entries — "enough to route the transaction to
/// a collection/mint without opening `raw`", per `TransactionUpdate`.
fn account_keys(result: &Value) -> Vec<String> {
    let mut keys: Vec<String> = result
        .pointer("/transaction/transaction/message/accountKeys")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|k| match k {
                    Value::String(s) => Some(s.clone()),
                    other => other
                        .get("pubkey")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                })
                .collect()
        })
        .unwrap_or_default();

    for section in ["writable", "readonly"] {
        if let Some(list) = result
            .pointer(&format!("/transaction/meta/loadedAddresses/{section}"))
            .and_then(Value::as_array)
        {
            keys.extend(list.iter().filter_map(Value::as_str).map(str::to_string));
        }
    }
    keys
}

/// Filters to drop and filters to (re)subscribe, so a spec change costs a
/// diff rather than a reconnect.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SpecDiff {
    pub removed: Vec<FilterId>,
    pub added: Vec<FilterId>,
}

impl SpecDiff {
    pub fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.added.is_empty()
    }
}

/// A filter whose contents changed appears in **both** lists; the caller must
/// subscribe the new one before unsubscribing the old (make-before-break), or
/// the overlap becomes a gap.
pub fn diff(old: &SubscriptionSpec, new: &SubscriptionSpec) -> SpecDiff {
    let mut out = SpecDiff::default();

    // A commitment change invalidates every subscription.
    if old.commitment != new.commitment {
        out.removed = old.transactions.keys().cloned().collect();
        out.added = new.transactions.keys().cloned().collect();
        return out;
    }

    for (id, filter) in &new.transactions {
        match old.transactions.get(id) {
            Some(existing) if existing == filter => {}
            Some(_) => {
                out.removed.push(id.clone());
                out.added.push(id.clone());
            }
            None => out.added.push(id.clone()),
        }
    }
    for id in old.transactions.keys() {
        if !new.transactions.contains_key(id) {
            out.removed.push(id.clone());
        }
    }
    out.removed.sort();
    out.removed.dedup();
    out.added.sort();
    out
}

/// Why a spec cannot be compiled for this transport.
pub fn unsupported(spec: &SubscriptionSpec) -> Option<String> {
    if spec.transactions.len() > MAX_FILTERS {
        return Some(format!(
            "{} transaction filters exceeds the {MAX_FILTERS} subscriptions a \
             connection allows",
            spec.transactions.len()
        ));
    }
    for (id, filter) in &spec.transactions {
        let widest = filter
            .account_include
            .len()
            .max(filter.account_required.len());
        if widest > MAX_ADDRESSES {
            return Some(format!(
                "filter {id:?} carries {widest} addresses, over the {MAX_ADDRESSES} limit"
            ));
        }
    }
    // `accounts` entries would need accountSubscribe/programSubscribe; the
    // pipeline deliberately uses none, so a spec carrying them is a mistake
    // worth surfacing rather than silently ignoring.
    if !spec.accounts.is_empty() {
        return Some(format!(
            "{} account filters are not compiled by this adapter",
            spec.accounts.len()
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn pk(seed: u8) -> String {
        bs58::encode([seed; 32]).into_string()
    }

    fn filter(addresses: &[String]) -> TransactionFilter {
        TransactionFilter {
            account_include: addresses.to_vec(),
            account_required: Vec::new(),
            include_failed: false,
        }
    }

    fn spec(entries: &[(&str, TransactionFilter)]) -> SubscriptionSpec {
        SubscriptionSpec {
            commitment: Commitment::Confirmed,
            accounts: BTreeMap::new(),
            transactions: entries
                .iter()
                .map(|(id, f)| ((*id).to_string(), f.clone()))
                .collect(),
        }
    }

    #[test]
    fn the_subscribe_request_carries_the_options_the_decoder_needs() {
        let request = subscribe_request(7, &filter(&[pk(1)]), Commitment::Confirmed);
        assert_eq!(request["method"], "transactionSubscribe");
        assert_eq!(request["id"], 7);

        let f = &request["params"][0];
        assert_eq!(f["vote"], false, "vote transactions are always excluded");
        assert_eq!(f["failed"], false);
        assert_eq!(f["accountInclude"][0], pk(1));

        let o = &request["params"][1];
        assert_eq!(o["commitment"], "confirmed");
        assert_eq!(o["encoding"], "jsonParsed");
        assert_eq!(o["transactionDetails"], "full");
        assert_eq!(o["maxSupportedTransactionVersion"], 0);
    }

    #[test]
    fn an_ack_maps_a_request_to_a_subscription() {
        let frame = parse_frame(r#"{"jsonrpc":"2.0","result":4743323479349712,"id":7}"#);
        assert_eq!(
            frame,
            Frame::Ack {
                request_id: 7,
                result: serde_json::json!(4_743_323_479_349_712_i64),
            }
        );
    }

    #[test]
    fn an_rpc_error_is_not_mistaken_for_an_ack() {
        let frame = parse_frame(
            r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"too many addresses"},"id":7}"#,
        );
        assert_eq!(
            frame,
            Frame::Error {
                request_id: Some(7),
                message: "too many addresses".into(),
            }
        );
    }

    #[test]
    fn a_transaction_notification_becomes_an_update() {
        let text = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "transactionNotification",
            "params": {
                "subscription": 42,
                "result": {
                    "signature": "SYNsig",
                    "slot": 443_800_000_u64,
                    "transaction": {
                        "transaction": {"message": {"accountKeys": [{"pubkey": pk(1)}]}},
                        "meta": {"err": null, "loadedAddresses": {"writable": [pk(2)], "readonly": []}},
                    }
                }
            }
        })
        .to_string();

        let Frame::Transaction {
            subscription,
            update,
        } = parse_frame(&text)
        else {
            panic!("expected a transaction frame");
        };
        assert_eq!(subscription, 42);
        assert_eq!(update.slot, 443_800_000);
        assert_eq!(update.signature, "SYNsig");
        assert!(!update.failed);
        assert_eq!(
            update.account_keys,
            vec![pk(1), pk(2)],
            "lookup-table addresses are part of the routing keys"
        );
        assert!(matches!(update.raw, RawPayload::Json(_)));
    }

    #[test]
    fn a_failed_transaction_is_marked() {
        let text = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "transactionNotification",
            "params": {"subscription": 1, "result": {
                "signature": "SYNsig", "slot": 1,
                "transaction": {"meta": {"err": {"InstructionError": [0, "Custom"]}}},
            }}
        })
        .to_string();
        let Frame::Transaction { update, .. } = parse_frame(&text) else {
            panic!("expected a transaction frame");
        };
        assert!(update.failed);
    }

    #[test]
    fn a_root_notification_is_the_checkpoint_source() {
        let frame = parse_frame(
            r#"{"jsonrpc":"2.0","method":"rootNotification","params":{"result":443800000,"subscription":1}}"#,
        );
        assert_eq!(frame, Frame::Root { slot: 443_800_000 });
    }

    #[test]
    fn junk_is_tolerated_rather_than_fatal() {
        assert_eq!(parse_frame("not json"), Frame::Other);
        assert_eq!(parse_frame("{}"), Frame::Other);
        assert_eq!(parse_frame(r#"{"method":"somethingNew"}"#), Frame::Other);
    }

    #[test]
    fn an_unchanged_spec_produces_no_diff() {
        let a = spec(&[("tracked", filter(&[pk(1)]))]);
        assert!(diff(&a, &a.clone()).is_empty());
    }

    /// A changed filter must appear in both lists so the caller can subscribe
    /// before unsubscribing — the reverse order would open a real gap.
    #[test]
    fn a_changed_filter_is_both_removed_and_added() {
        let a = spec(&[("tracked", filter(&[pk(1)]))]);
        let b = spec(&[("tracked", filter(&[pk(1), pk(2)]))]);
        let d = diff(&a, &b);
        assert_eq!(d.removed, vec!["tracked".to_string()]);
        assert_eq!(d.added, vec!["tracked".to_string()]);
    }

    #[test]
    fn added_and_removed_filters_are_detected() {
        let a = spec(&[("one", filter(&[pk(1)]))]);
        let b = spec(&[("two", filter(&[pk(2)]))]);
        let d = diff(&a, &b);
        assert_eq!(d.removed, vec!["one".to_string()]);
        assert_eq!(d.added, vec!["two".to_string()]);
    }

    #[test]
    fn a_commitment_change_invalidates_every_subscription() {
        let a = spec(&[("tracked", filter(&[pk(1)]))]);
        let mut b = a.clone();
        b.commitment = Commitment::Finalized;
        let d = diff(&a, &b);
        assert_eq!(d.removed, vec!["tracked".to_string()]);
        assert_eq!(d.added, vec!["tracked".to_string()]);
    }

    #[test]
    fn an_oversized_spec_is_refused_before_connecting() {
        let wide = spec(&[("tracked", filter(&vec![pk(1); MAX_ADDRESSES + 1]))]);
        assert!(unsupported(&wide).unwrap().contains("over the"));

        let ok = spec(&[("tracked", filter(&[pk(1)]))]);
        assert_eq!(unsupported(&ok), None);
    }

    #[test]
    fn account_filters_are_refused_rather_than_silently_dropped() {
        let mut s = spec(&[("tracked", filter(&[pk(1)]))]);
        s.accounts
            .insert("accts".into(), crate::spec::AccountFilter::default());
        assert!(unsupported(&s).unwrap().contains("account filters"));
    }
}
