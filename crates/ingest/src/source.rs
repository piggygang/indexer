use std::pin::Pin;

use futures_core::Stream;
use tokio::sync::watch;

use crate::event::{IngestEvent, Slot};
use crate::spec::SubscriptionSpec;

pub type EventStream =
    Pin<Box<dyn Stream<Item = Result<IngestEvent, IngestError>> + Send + 'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeFrom {
    /// Live tip only.
    Latest,
    /// Deliver events with slot >= this (INCLUSIVE, matching Yellowstone
    /// `from_slot`). Passing back the persisted cursor deliberately overlaps
    /// the boundary slot; signature-keyed idempotent upserts absorb the
    /// duplicates.
    Slot(Slot),
}

/// An `Err` item is TERMINAL: the stream ends after yielding it. Transient
/// trouble is not an error — it surfaces as [`crate::StreamStatus`] events
/// while the adapter reconnects internally.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IngestError {
    #[error("subscription spec not supported by this transport: {0}")]
    UnsupportedSpec(String),
    #[error("gave up reconnecting after {attempts} attempts: {reason}")]
    Exhausted { attempts: u32, reason: String },
    #[error("transport error: {0}")]
    Transport(String),
}

/// One upstream of Solana events. Transports: mock (ALG-618), Helius Enhanced
/// WebSockets and LaserStream gRPC (ALG-623).
///
/// Contract:
/// - Implementations own reconnection and in-session replay; the CONSUMER
///   owns the durable cursor (`ingest_state.last_processed_slot`), persisting
///   it only on [`crate::IngestEvent::SlotCheckpoint`] and passing it back as
///   [`ResumeFrom::Slot`] on restart.
/// - Delivery is at-least-once; downstream handlers must be idempotent
///   (keyed by signature).
/// - Three channels, one meaning each: `Ok(event)` is data,
///   [`crate::StreamStatus`] is telemetry, `Err` is a dead stream. The
///   restart policy is the service's decision, not the adapter's.
pub trait IngestSource: Send + Sync {
    /// `"mock"` | `"helius-ws"` | `"laserstream-grpc"`.
    fn name(&self) -> &'static str;

    /// Lazy: connects on first poll. `spec` is a live view of the desired
    /// subscriptions — send a new [`SubscriptionSpec`] on the watch channel
    /// to update without restart (full-replacement semantics; the adapter
    /// emits [`crate::StreamStatus::Resubscribed`] when applied).
    fn subscribe(&self, spec: watch::Receiver<SubscriptionSpec>, resume: ResumeFrom)
        -> EventStream;
}
