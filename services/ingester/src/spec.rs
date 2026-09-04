//! Registry → [`SubscriptionSpec`].
//!
//! Membership is decided by `match`ing on [`MembershipRule`], one arm per
//! rule and never on a slug, so onboarding a collection stays a TOML change.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use indexer_data_model::registry::CollectionRow;
use indexer_data_model::types::MembershipRule;
use indexer_data_model::{assets, registry, PgPool};
use indexer_ingest::{Commitment, SubscriptionSpec, TransactionFilter};

/// The single filter id. One filter rather than one per collection: a swap
/// burns a SOL Gang pig and mints a Core asset in the *same signature*, so
/// per-collection filters would deliver — and bill for — that transaction
/// twice, while the database lookup by address is authoritative either way.
pub const TRACKED: &str = "tracked";

/// Helius's per-array limit; chunking above it keeps the spec compilable.
const MAX_ADDRESSES: usize = indexer_ingest::ws::MAX_ADDRESSES;

/// Every address the pipeline wants transactions for.
pub async fn tracked_addresses(pool: &PgPool) -> anyhow::Result<Vec<String>> {
    let mut addresses = BTreeSet::new();
    for collection in registry::list_enabled(pool).await? {
        addresses.extend(addresses_for(pool, &collection).await?);
    }
    Ok(addresses.into_iter().collect())
}

async fn addresses_for(pool: &PgPool, c: &CollectionRow) -> anyhow::Result<Vec<String>> {
    let Some(rule) = c.membership_rule else {
        return Ok(Vec::new());
    };
    match rule {
        // The committed mint list, so the filter is correct even before the
        // backfill has run.
        MembershipRule::TmAllowlist => Ok(registry::allowlist(pool, c.id).await?),
        // A certified collection mint never appears in a member's transfer, so
        // the members themselves are the filter. New members are picked up by
        // the next registry poll after a backfill adds them.
        MembershipRule::TmCollection => Ok(assets::member_addresses(pool, c.id).await?),
        // Metaplex Core passes the collection account on every member
        // instruction, so this one address catches transfers *and mints of
        // assets that do not exist yet* — which is why individual Core asset
        // addresses never enter the filter, and why the address list only
        // changes when the registry does.
        MembershipRule::CoreCollection => {
            let address = c
                .address
                .clone()
                .with_context(|| format!("{} has rule {rule:?} but no address", c.slug))?;
            Ok(vec![address])
        }
    }
}

/// Core collection addresses, for the decoder's structural recognition.
pub async fn core_collections(pool: &PgPool) -> anyhow::Result<BTreeSet<String>> {
    Ok(registry::list_enabled(pool)
        .await?
        .into_iter()
        .filter(|c| c.membership_rule == Some(MembershipRule::CoreCollection))
        .filter_map(|c| c.address)
        .collect())
}

/// Pure: addresses → a spec, chunked so no single filter exceeds the limit.
pub fn compile(addresses: Vec<String>) -> SubscriptionSpec {
    let mut transactions = BTreeMap::new();
    if addresses.is_empty() {
        return SubscriptionSpec {
            commitment: Commitment::Confirmed,
            accounts: BTreeMap::new(),
            transactions,
        };
    }

    let chunks: Vec<&[String]> = addresses.chunks(MAX_ADDRESSES).collect();
    for (index, chunk) in chunks.iter().enumerate() {
        let id = if chunks.len() == 1 {
            TRACKED.to_string()
        } else {
            format!("{TRACKED}-{index}")
        };
        transactions.insert(
            id,
            TransactionFilter {
                account_include: chunk.to_vec(),
                account_required: Vec::new(),
                // A failed transaction moved nothing; the subscription also
                // filters them server-side.
                include_failed: false,
            },
        );
    }

    SubscriptionSpec {
        // `processed` can be rolled back and there is no un-write path for an
        // activity row; `finalized` would cost ~13 s of latency for nothing.
        commitment: Commitment::Confirmed,
        accounts: BTreeMap::new(),
        transactions,
    }
}

pub async fn build(pool: &PgPool) -> anyhow::Result<SubscriptionSpec> {
    Ok(compile(tracked_addresses(pool).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(seed: u8) -> String {
        bs58::encode([seed; 32]).into_string()
    }

    #[test]
    fn one_filter_holds_everything_that_fits() {
        let spec = compile(vec![pk(1), pk(2)]);
        assert_eq!(spec.transactions.len(), 1);
        let filter = &spec.transactions[TRACKED];
        assert_eq!(filter.account_include, vec![pk(1), pk(2)]);
        assert!(!filter.include_failed);
        assert_eq!(spec.commitment, Commitment::Confirmed);
        assert!(spec.accounts.is_empty(), "no accountSubscribe entries");
    }

    #[test]
    fn an_oversized_list_is_chunked_into_compilable_filters() {
        let spec = compile((0..MAX_ADDRESSES + 5).map(|i| format!("SYN{i}")).collect());
        assert_eq!(spec.transactions.len(), 2);
        assert!(spec.transactions.contains_key("tracked-0"));
        assert!(spec.transactions.contains_key("tracked-1"));
        assert_eq!(indexer_ingest::ws::unsupported(&spec), None);
    }

    #[test]
    fn an_empty_registry_compiles_to_an_empty_spec() {
        assert!(compile(Vec::new()).transactions.is_empty());
    }
}
