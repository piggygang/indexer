//! Registry → [`SubscriptionSpec`].
//!
//! Only the transport shaping lives here. *Which* addresses are tracked is
//! `registry::tracked_addresses`, in `data-model`, because the Helius webhook's
//! `accountAddresses` and the assertion that compares the two must derive the
//! same set from the same code — a second copy is how address-list drift
//! becomes real.

use std::collections::{BTreeMap, BTreeSet};

use indexer_data_model::types::MembershipRule;
use indexer_data_model::{registry, PgPool};
use indexer_ingest::{Commitment, SubscriptionSpec, TransactionFilter};

/// The single filter id. One filter rather than one per collection: a swap
/// burns a SOL Gang pig and mints a Core asset in the *same signature*, so
/// per-collection filters would deliver — and bill for — that transaction
/// twice, while the database lookup by address is authoritative either way.
pub const TRACKED: &str = "tracked";

/// Helius's per-array limit; chunking above it keeps the spec compilable.
const MAX_ADDRESSES: usize = indexer_ingest::ws::MAX_ADDRESSES;

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
    Ok(compile(registry::tracked_addresses(pool).await?))
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
