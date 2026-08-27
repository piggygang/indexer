use crate::spec::FilterId;

pub type Slot = u64;

/// Transport-native payload of a transaction, kept for the classification
/// stage (ALG-623). Pipeline code MUST NOT match on the variants — only the
/// ingest crate's own decode module may. That containment is the abstraction
/// boundary.
#[derive(Debug, Clone)]
pub enum RawPayload {
    /// Enhanced WebSockets notification (jsonParsed transaction + meta).
    Json(serde_json::Value),
    /// Protobuf-encoded Yellowstone `SubscribeUpdateTransactionInfo`.
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
pub enum IngestEvent {
    Account(AccountUpdate),
    Transaction(TransactionUpdate),
    /// No events with a lower slot will follow on this stream. Persist the
    /// durable cursor on this event, and only on this event.
    SlotCheckpoint(SlotCheckpoint),
    /// Telemetry, never control flow.
    Status(StreamStatus),
}

impl IngestEvent {
    /// Slot for cursor tracking (`None` for status events).
    pub fn slot(&self) -> Option<Slot> {
        match self {
            IngestEvent::Account(a) => Some(a.slot),
            IngestEvent::Transaction(t) => Some(t.slot),
            IngestEvent::SlotCheckpoint(c) => Some(c.slot),
            IngestEvent::Status(_) => None,
        }
    }
}

/// An account state change. `data` is the complete decoded payload, so no raw
/// escape hatch is needed here.
#[derive(Debug, Clone)]
pub struct AccountUpdate {
    /// Which [`crate::SubscriptionSpec`] entries matched this event.
    pub filters: Vec<FilterId>,
    pub slot: Slot,
    /// Base58. Plain strings throughout keep the solana-sdk dependency tree
    /// out of this crate and match the DB's text columns.
    pub pubkey: String,
    /// Base58 owner program.
    pub owner: String,
    pub lamports: u64,
    pub executable: bool,
    /// Decoded account data (adapters decode base64 at the edge).
    pub data: Vec<u8>,
    /// gRPC-only bonus field; never load-bearing.
    pub write_version: Option<u64>,
    /// gRPC-only bonus field; never load-bearing.
    pub txn_signature: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    /// Which [`crate::SubscriptionSpec`] entries matched this event.
    pub filters: Vec<FilterId>,
    pub slot: Slot,
    /// Base58. The idempotency key downstream.
    pub signature: String,
    /// Error detail lives in `raw`.
    pub failed: bool,
    /// Static and address-lookup-table-loaded keys — enough to route the
    /// transaction to a collection/mint without opening `raw`.
    pub account_keys: Vec<String>,
    /// Full transaction + meta for classification. See [`RawPayload`].
    pub raw: RawPayload,
}

#[derive(Debug, Clone, Copy)]
pub struct SlotCheckpoint {
    pub slot: Slot,
}

#[derive(Debug, Clone)]
pub enum StreamStatus {
    Connected,
    Reconnecting {
        attempt: u32,
    },
    /// The consumer outpaced the transport and events were dropped. The
    /// resume machinery makes this safe (redelivery + idempotent handlers).
    Lagged {
        dropped: u64,
    },
    /// A live [`crate::SubscriptionSpec`] change was applied.
    Resubscribed,
}
