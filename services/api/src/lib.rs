//! `indexer-api` — the public read-only REST API over the indexer.
//!
//! A `lib` target exists so the integration tests drive this code rather than a
//! copy of it: `handlers::configure` builds the identical route table the
//! binary serves, which is the only way a contract test can be about the real
//! API and not about a re-implementation of it.

pub mod cache;
pub mod cursor;
pub mod dto;
pub mod error;
pub mod handlers;
pub mod query;
