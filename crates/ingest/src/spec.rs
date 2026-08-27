use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Key of a [`SubscriptionSpec`] entry; echoed on every matching event.
pub type FilterId = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Commitment {
    Processed,
    #[default]
    Confirmed,
    Finalized,
}

/// The transport-neutral description of what to subscribe to. Each adapter
/// compiles it to its native form: one Yellowstone `SubscribeRequest` on
/// gRPC; one `accountSubscribe`/`programSubscribe`/`transactionSubscribe`
/// call per entry on Enhanced WebSockets (diffing old vs. new spec on
/// updates).
///
/// `BTreeMap` keeps the spec deterministic, so `PartialEq` and diffs are
/// cheap. There are no slot filters here — adapters always track slots
/// themselves and emit [`crate::IngestEvent::SlotCheckpoint`]. Vote
/// transactions are always excluded.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SubscriptionSpec {
    pub commitment: Commitment,
    pub accounts: BTreeMap<FilterId, AccountFilter>,
    pub transactions: BTreeMap<FilterId, TransactionFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AccountFilter {
    /// Explicit pubkeys (base58) — `accountSubscribe` / Yellowstone `account`.
    pub accounts: Vec<String>,
    /// Owner programs (base58) — `programSubscribe` / Yellowstone `owner`.
    pub owners: Vec<String>,
    /// Narrowing filters on account data — WS `filters` / Yellowstone
    /// `filters`. E.g. memcmp on the collection pubkey inside Core assets.
    pub data_filters: Vec<DataFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataFilter {
    DataSize(u64),
    Memcmp { offset: u64, bytes: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TransactionFilter {
    /// EWS `accountInclude` / Yellowstone `account_include`.
    pub account_include: Vec<String>,
    /// EWS `accountRequired` / Yellowstone `account_required`.
    pub account_required: Vec<String>,
    /// Default false.
    pub include_failed: bool,
}
