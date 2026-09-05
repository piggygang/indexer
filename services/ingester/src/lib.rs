//! The live pipeline (ALG-623): Helius Enhanced WebSockets in, `activity` and
//! `ownership_history` out.
//!
//! The service layer is the only place that needs all four crates at once —
//! the registry and writers (`data-model`), the transport (`ingest`), DAS for
//! Core hydration and gap recovery (`das`), and env config. A `lib` target
//! exists so the integration test drives this code rather than a copy of it.

pub mod blocktime;
pub mod consumer;
pub mod pipeline;
pub mod reconcile;
pub mod schedule;
pub mod spec;
