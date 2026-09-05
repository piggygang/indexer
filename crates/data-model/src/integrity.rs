//! The integrity views, counted.
//!
//! `migrations/20260829000400_activity_ownership.sql` introduces them with
//! *"Integrity views: what reconciliation (ALG-624) diffs and logs. Empty =
//! healthy."* This module is the "and logs" half: one scalar per view, so a
//! reconciliation run can record how healthy the database was when it
//! finished, and an operator can ask the same question by hand.
//!
//! Counting through one function rather than inlining the SQL at each caller
//! is the same discipline [`crate::activity::owner_agrees`] states: the
//! metric and the acceptance query must not be able to drift apart.

use serde::Serialize;
use sqlx::PgExecutor;

/// How far the database disagrees with itself. All zeroes is healthy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Integrity {
    /// Observed owner (`assets.owner`) vs the open ownership interval.
    pub owner_mismatch: i64,
    /// Asset filed under an allowlist collection but absent from its allowlist.
    pub allowlist_violation: i64,
    /// Allowlist asset whose on-chain symbol disagrees with the registry.
    pub symbol_mismatch: i64,
    /// Assets an out-of-order event flagged for the ownership rebuild.
    pub ownership_dirty: i64,
}

impl Integrity {
    /// Nothing disagrees. This is the acceptance criterion, in one place.
    pub fn is_healthy(&self) -> bool {
        *self == Self::default()
    }
}

/// Counts every integrity view in one round trip.
pub async fn snapshot<'e>(exec: impl PgExecutor<'e>) -> sqlx::Result<Integrity> {
    let (owner_mismatch, allowlist_violation, symbol_mismatch, ownership_dirty) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM integrity_owner_mismatch)::bigint, \
                    (SELECT count(*) FROM integrity_allowlist_violation)::bigint, \
                    (SELECT count(*) FROM integrity_symbol_mismatch)::bigint, \
                    (SELECT count(*) FROM assets WHERE ownership_dirty)::bigint",
    )
    .fetch_one(exec)
    .await?;
    Ok(Integrity {
        owner_mismatch,
        allowlist_violation,
        symbol_mismatch,
        ownership_dirty,
    })
}
