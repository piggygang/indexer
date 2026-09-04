//! Transport-agnostic ingest interface (ALG-618).
//!
//! Everything downstream — the pipeline, the data model, the API — is written
//! against [`IngestSource`] and [`IngestEvent`]. Which wire the events arrive
//! on is an adapter detail:
//!
//! - `mock` (this crate, always compiled): scripted events for tests.
//! - [`ws`] (feature `ws`, on by default): Helius Enhanced WebSockets, the
//!   mainnet transport. Note this transport has **no replay** — `fromSlot` is
//!   a LaserStream gRPC feature — so [`ResumeFrom::Slot`] is a floor, and the
//!   consumer closes a gap by reconciling against DAS on
//!   [`StreamStatus::Connected`].
//! - LaserStream gRPC — the Business-plan upgrade path, still unbuilt. It
//!   arrives as `#[cfg(feature = "grpc")] pub mod grpc;` with
//!   `grpc = ["dep:helius-laserstream"]`.
//!
//! Swapping transports is a config change plus an adapter; pipeline code does
//! not change. The one escape hatch is [`RawPayload`] on transaction events —
//! and only this crate's [`decode`] module may look inside it. Pipeline code
//! matching on `RawPayload` variants re-couples the pipeline to a transport
//! and is a review error.

pub mod decode;
mod event;
mod mock;
mod source;
mod spec;
#[cfg(feature = "ws")]
pub mod ws;

pub use event::{
    AccountUpdate, IngestEvent, RawPayload, Slot, SlotCheckpoint, StreamStatus, TransactionUpdate,
};
pub use mock::MockSource;
pub use source::{EventStream, IngestError, IngestSource, ResumeFrom};
pub use spec::{
    AccountFilter, Commitment, DataFilter, FilterId, SubscriptionSpec, TransactionFilter,
};
