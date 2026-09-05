//! Helius DAS client and the asset backfill (ALG-621).
//!
//! **Request/response only.** This crate deliberately does not implement
//! [`indexer_ingest::IngestSource`] and never will: that trait is the
//! *streaming* interface, with a durable slot cursor and at-least-once
//! redelivery. A backfill is a batch job whose cursor lives in
//! `backfill_state`, and conflating the two would put transport concerns into
//! a crate that has none.
//!
//! The split inside is deliberate too: every database statement lives in
//! `indexer_data_model::assets`, so the writer invariants are provable
//! against Postgres with no network, and this crate only decides *what* to
//! write. Reconciliation (ALG-624) reuses both halves.

pub mod asset;
pub mod backfill;
pub mod client;

pub use asset::Asset;
pub use backfill::{BackfillOptions, BackfillReport, BatchProgress, CollectionReport};
pub use client::{ArchivedTx, DasClient, DasError, Reachability, SignatureInfo, TxPage};
