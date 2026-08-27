//! Transport-agnostic ingest interface (ALG-618).
//!
//! Everything downstream — the pipeline, the data model, the API — is written
//! against [`IngestSource`] and [`IngestEvent`]. Which wire the events arrive
//! on is an adapter detail:
//!
//! - `mock` (this crate, always compiled): scripted events for tests.
//! - Helius Enhanced WebSockets — the chosen mainnet transport (Developer
//!   plan). Added in ALG-623 as `#[cfg(feature = "ws")] pub mod ws;` with
//!   `ws = ["dep:tokio-tungstenite", ...]`.
//! - LaserStream gRPC — the Business-plan upgrade path. Added in ALG-623 as
//!   `#[cfg(feature = "grpc")] pub mod grpc;` with
//!   `grpc = ["dep:helius-laserstream"]`.
//!
//! Swapping transports is a config change plus an adapter; pipeline code does
//! not change. The one escape hatch is [`RawPayload`] on transaction events —
//! and only a (future, ALG-623) `decode` module inside this crate may look
//! inside it. Pipeline code matching on `RawPayload` variants re-couples the
//! pipeline to a transport and is a review error.

mod event;
mod mock;
mod source;
mod spec;

pub use event::{
    AccountUpdate, IngestEvent, RawPayload, Slot, SlotCheckpoint, StreamStatus, TransactionUpdate,
};
pub use mock::MockSource;
pub use source::{EventStream, IngestError, IngestSource, ResumeFrom};
pub use spec::{
    AccountFilter, Commitment, DataFilter, FilterId, SubscriptionSpec, TransactionFilter,
};
