//! Ownership-chain analysis: is the timeline we crawled actually complete?
//!
//! The crawl cannot know in advance whether querying an asset's address found
//! every transaction that moved it — a plain `spl-token` `transfer` names
//! neither the mint nor the wallets, so it is invisible there. What it *can*
//! do is check the timeline against itself and against DAS:
//!
//! - **Contiguity.** Each hop's sender must be whoever the previous hop handed
//!   the asset to. A disagreement means a move happened that we did not see.
//! - **Agreement with DAS.** The last owner the chain arrives at must be the
//!   owner DAS reports. That is the issue's own acceptance criterion, and
//!   using it as the stop condition is what makes the crawl adaptive.
//!
//! Either failure triggers expansion to the asset's token accounts, where the
//! invisible transfers are visible.

use indexer_data_model::activity::AssetRef;
use indexer_data_model::types::EventKind;

use crate::classify::TimelineEvent;

/// What an asset's classified events add up to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Chain {
    /// Hops whose sender disagreed with the previous hop's receiver.
    pub gaps: usize,
    /// Who holds it at the end of the timeline.
    pub final_owner: Option<String>,
    /// The timeline ends in a burn.
    pub burned: bool,
    pub hops: usize,
}

/// Whether a chain may be trusted as complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Contiguous, and it ends where DAS says the asset is.
    Complete,
    /// A hop's sender disagreed with the previous receiver.
    Gapped { gaps: usize },
    /// Contiguous but it ends somewhere else.
    Mismatch {
        derived: Option<String>,
        observed: Option<String>,
    },
    /// Nothing to check against: DAS has no owner for a live asset, or the
    /// crawl produced no ownership events at all (a Metaplex Core asset, whose
    /// instructions the RPC does not parse).
    Unverifiable,
}

impl Default for Verdict {
    /// Nothing crawled yet is nothing to disprove.
    fn default() -> Self {
        Self::Unverifiable
    }
}

impl Verdict {
    /// Should the crawl stop here? Only a complete chain — or one there is no
    /// way to check — is worth spending no more calls on.
    pub const fn is_settled(&self) -> bool {
        matches!(self, Self::Complete | Self::Unverifiable)
    }
}

/// Walks classified events, which the caller has ordered by `(slot, seq)`.
pub fn derive(events: &[TimelineEvent]) -> Chain {
    let mut chain = Chain::default();
    let mut current: Option<String> = None;

    for event in events {
        if !matches!(
            event.kind,
            EventKind::Mint | EventKind::Transfer | EventKind::Sale | EventKind::Burn
        ) {
            continue;
        }
        chain.hops += 1;
        // A sender we know about that is not who we thought held it. The mint
        // has no sender, and an unrecorded sender (pre-2022 balances the owner
        // map could not resolve) is unknown rather than contradictory.
        if let (Some(from), Some(held)) = (event.from_owner.as_deref(), current.as_deref()) {
            if from != held {
                chain.gaps += 1;
            }
        }
        match event.kind {
            EventKind::Burn => {
                chain.burned = true;
                current = None;
            }
            _ => {
                chain.burned = false;
                if event.to_owner.is_some() {
                    current = event.to_owner.clone();
                }
            }
        }
    }

    chain.final_owner = current;
    chain
}

/// Cross-validates a chain against the observed (DAS) owner.
pub fn verify(chain: &Chain, asset: &AssetRef) -> Verdict {
    if chain.hops == 0 {
        return Verdict::Unverifiable;
    }
    if chain.gaps > 0 {
        return Verdict::Gapped { gaps: chain.gaps };
    }
    if asset.burned {
        return if chain.burned {
            Verdict::Complete
        } else {
            Verdict::Mismatch {
                derived: chain.final_owner.clone(),
                observed: None,
            }
        };
    }
    // DAS not knowing an owner is not evidence against the chain — the
    // backfill treats an absent owner as "unobserved", exactly as
    // `assets::upsert_batch` does.
    let Some(observed) = asset.owner.as_deref() else {
        return Verdict::Unverifiable;
    };
    if chain.final_owner.as_deref() == Some(observed) {
        Verdict::Complete
    } else {
        Verdict::Mismatch {
            derived: chain.final_owner.clone(),
            observed: Some(observed.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn pk(seed: u8) -> String {
        bs58::encode([seed; 32]).into_string()
    }

    fn event(slot: i64, kind: EventKind, from: Option<&str>, to: Option<&str>) -> TimelineEvent {
        TimelineEvent {
            signature: bs58::encode([slot as u8; 64]).into_string(),
            slot,
            block_time: chrono::Utc.timestamp_opt(1_700_000_000 + slot, 0).unwrap(),
            seq: 0,
            kind,
            from_owner: from.map(str::to_string),
            to_owner: to.map(str::to_string),
            price_lamports: None,
            marketplace: None,
            details: json!({}),
        }
    }

    fn asset(owner: Option<&str>, burned: bool) -> AssetRef {
        AssetRef {
            id: 1,
            address: pk(1),
            collection_id: 1,
            owner: owner.map(str::to_string),
            owner_slot: owner.map(|_| 100),
            burned,
        }
    }

    #[test]
    fn a_contiguous_chain_that_lands_on_the_das_owner_is_complete() {
        let (a, b, c) = (pk(10), pk(11), pk(12));
        let events = vec![
            event(1, EventKind::Mint, None, Some(&a)),
            event(2, EventKind::Transfer, Some(&a), Some(&b)),
            event(3, EventKind::Sale, Some(&b), Some(&c)),
        ];
        let chain = derive(&events);
        assert_eq!((chain.gaps, chain.hops), (0, 3));
        assert_eq!(chain.final_owner.as_deref(), Some(c.as_str()));
        assert_eq!(verify(&chain, &asset(Some(&c), false)), Verdict::Complete);
    }

    #[test]
    fn a_sender_who_never_received_it_is_a_gap() {
        // Exactly what a missed escrow move looks like: the pig turns up in
        // someone else's hands with no transfer that put it there.
        let (a, b, stranger) = (pk(10), pk(11), pk(12));
        let events = vec![
            event(1, EventKind::Mint, None, Some(&a)),
            event(2, EventKind::Transfer, Some(&stranger), Some(&b)),
        ];
        let chain = derive(&events);
        assert_eq!(chain.gaps, 1);
        assert_eq!(
            verify(&chain, &asset(Some(&b), false)),
            Verdict::Gapped { gaps: 1 }
        );
        assert!(
            !verify(&chain, &asset(Some(&b), false)).is_settled(),
            "a gap must send the crawl to the token accounts even though the owner matches"
        );
    }

    #[test]
    fn an_unknown_sender_is_not_a_contradiction() {
        // A pre-2022 balance the owner map could not resolve leaves
        // `from_owner` null. That is missing information, not disagreement.
        let (a, b) = (pk(10), pk(11));
        let events = vec![
            event(1, EventKind::Mint, None, Some(&a)),
            event(2, EventKind::Transfer, None, Some(&b)),
        ];
        assert_eq!(derive(&events).gaps, 0);
    }

    #[test]
    fn a_chain_ending_somewhere_else_is_a_mismatch() {
        let (a, elsewhere) = (pk(10), pk(13));
        let events = vec![event(1, EventKind::Mint, None, Some(&a))];
        let chain = derive(&events);
        assert_eq!(
            verify(&chain, &asset(Some(&elsewhere), false)),
            Verdict::Mismatch {
                derived: Some(a),
                observed: Some(elsewhere)
            }
        );
    }

    #[test]
    fn a_burn_ends_the_chain_and_matches_a_burned_asset() {
        let a = pk(10);
        let events = vec![
            event(1, EventKind::Mint, None, Some(&a)),
            event(2, EventKind::Burn, Some(&a), None),
        ];
        let chain = derive(&events);
        assert!(chain.burned && chain.final_owner.is_none());
        assert_eq!(verify(&chain, &asset(None, true)), Verdict::Complete);
    }

    #[test]
    fn nothing_to_check_against_is_unverifiable_not_wrong() {
        // Two ways to get here, and neither is evidence of a bad crawl: a
        // Metaplex Core asset yields no token events at all, and DAS not
        // knowing an owner is "unobserved", not "no owner".
        assert_eq!(
            verify(&Chain::default(), &asset(Some(&pk(10)), false)),
            Verdict::Unverifiable
        );
        let events = vec![event(1, EventKind::Mint, None, Some(&pk(10)))];
        assert_eq!(
            verify(&derive(&events), &asset(None, false)),
            Verdict::Unverifiable
        );
    }
}
