//! Turning decoded ownership changes into `activity` rows — the part the live
//! pipeline deliberately leaves undone.
//!
//! `decode` establishes *that* an NFT moved and between whom; it stops short
//! of `sale` because a sale needs a price, and a price is not in any
//! instruction argument — it is the SOL that changed hands. This module reads
//! the lamport deltas `decode` now reports, decides whether a marketplace was
//! involved, and produces the priced event.
//!
//! Two schema constraints shape every decision here.
//! `activity_sale_has_price` forbids a `sale` without a price, and
//! `activity_price_only_sale` forbids a price or a venue on anything else. So
//! there is no "sale we could not price": that is a `transfer`, recorded
//! honestly, with the program ids still in `details` so a later run can
//! reclassify it once the venue is known.

use chrono::{DateTime, Utc};
use indexer_data_model::types::EventKind;
use indexer_ingest::decode::{Decoded, DecodedKind, TokenEvent};
use serde_json::{json, Value};

use crate::marketplaces::Venues;

/// Below this, a "price" is fee and rent noise rather than a trade.
///
/// One token account's rent-exempt minimum is 2 039 280 lamports and a
/// signature costs 5 000, so anything under 0.005 SOL is change left over from
/// bookkeeping. A trade genuinely worth less than that is recorded as an
/// honest transfer, which the contract allows and a wrong price does not.
const MIN_PRICE_LAMPORTS: i64 = 5_000_000;

/// One classified event, ready for `activity::record`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineEvent {
    pub signature: String,
    pub slot: i64,
    pub block_time: DateTime<Utc>,
    pub seq: i16,
    pub kind: EventKind,
    pub from_owner: Option<String>,
    pub to_owner: Option<String>,
    pub price_lamports: Option<i64>,
    pub marketplace: Option<String>,
    pub details: Value,
}

/// Where a price came from, recorded in `details` so a surprising number can
/// be traced back to the side of the trade it was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PriceSource {
    /// Everything that reached a wallet: seller proceeds plus royalty plus
    /// marketplace commission. Preferred, and the only form that is neither
    /// short a royalty (the seller's side) nor long the rent and fee the
    /// buyer also paid (the buyer's side).
    Credited,
    /// What the buyer paid, net of their fee. Fallback for a flow where the
    /// money lands somewhere that is not a plain account credit.
    Buyer,
    /// What the seller received. Last resort — a lower bound, because
    /// royalties and commission come out before the seller sees it.
    Seller,
}

impl PriceSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Credited => "credited",
            Self::Buyer => "buyer",
            Self::Seller => "seller",
        }
    }
}

/// Classifies one decoded ownership change.
///
/// `block_time` is required because `activity.block_time` is NOT NULL — a
/// signature whose time is unknown is parked, never guessed at, so the caller
/// resolves it before calling.
pub fn classify(
    signature: &str,
    slot: i64,
    block_time: DateTime<Utc>,
    decoded: &Decoded,
    event: &TokenEvent,
    venues: &Venues,
) -> TimelineEvent {
    let mut details = json!({
        "programs": decoded.programs,
        "fee_payer": decoded.fee_payer,
        "instruction": event.instruction,
    });

    let (kind, price_lamports, marketplace) = match event.kind {
        DecodedKind::Mint => (EventKind::Mint, None, None),
        DecodedKind::Burn => (EventKind::Burn, None, None),
        DecodedKind::Transfer => {
            let candidate = price_of(
                decoded,
                event.from_owner.as_deref(),
                event.to_owner.as_deref(),
            );
            // Recorded whether or not this becomes a sale, and this is what
            // makes the migration's promise — "reclassification never
            // re-fetches" — actually true. Only signatures are stored, not
            // transaction bodies, so without the amount here, teaching the
            // registry a new venue later would mean crawling the chain again.
            // With it, `reclassify` is a database-only pass.
            if let Some((lamports, source)) = candidate {
                details["price_candidate"] =
                    json!({"lamports": lamports, "source": source.as_str()});
            }
            match (venues.find(&decoded.programs), candidate) {
                (Some(venue), Some((price, _))) => {
                    (EventKind::Sale, Some(price), Some(venue.to_string()))
                }
                // A venue with no derivable price, or no venue at all. Either
                // way this is a transfer — see the module docs.
                _ => (EventKind::Transfer, None, None),
            }
        }
    };

    TimelineEvent {
        signature: signature.to_string(),
        slot,
        block_time,
        seq: event.seq,
        kind,
        from_owner: event.from_owner.clone(),
        to_owner: event.to_owner.clone(),
        price_lamports,
        marketplace,
        details,
    }
}

/// The sale price in lamports.
///
/// Preferred form: everything credited to an account that is not a token
/// account. That is the seller's proceeds plus the royalty plus the
/// marketplace's cut — the price — and excluding token accounts is what keeps
/// the rent for the buyer's freshly created ATA out of it. Verified against a
/// Solanart sale of a SOL Gang pig: Helius reports 1 950 000 000 lamports, the
/// buyer's own balance moved 1 952 039 279, and the difference is exactly the
/// account rent.
///
/// Returns `None` rather than zero or a negative: `price_lamports >= 0` is a
/// CHECK, and a zero-SOL "sale" is a transfer with a marketplace program in
/// it, not a sale.
fn price_of(
    decoded: &Decoded,
    from_owner: Option<&str>,
    to_owner: Option<&str>,
) -> Option<(i64, PriceSource)> {
    let delta = |wallet: &str| -> Option<i64> {
        decoded
            .native_deltas
            .iter()
            .find(|(account, _)| account == wallet)
            .map(|(_, amount)| *amount)
    };

    // Rent moving into and out of the token accounts this transaction
    // touched. Neither direction is part of the price.
    let (rent_funded, rent_released): (i64, i64) = decoded
        .native_deltas
        .iter()
        .filter(|(account, _)| decoded.token_accounts.contains(account))
        .fold((0, 0), |(funded, released), (_, amount)| {
            if *amount > 0 {
                (funded + amount, released)
            } else {
                (funded, released - amount)
            }
        });

    let buyer_side = to_owner.and_then(|buyer| {
        let spent = delta(buyer).filter(|d| *d < 0)?;
        let mut paid = spent.saturating_neg() - rent_funded;
        if decoded.fee_payer.as_deref() == Some(buyer) {
            paid -= i64::try_from(decoded.fee).unwrap_or(0);
        }
        Some((paid, PriceSource::Buyer))
    });

    let credited = decoded
        .native_deltas
        .iter()
        .filter(|(account, amount)| *amount > 0 && !decoded.token_accounts.contains(account))
        .map(|(_, amount)| *amount)
        .sum::<i64>()
        - rent_released;

    // Strict preference, not the larger of the two: the buyer's side is the
    // exact one, and the credited side runs over by whatever rent the trade
    // refunds. Credited is for the flows where the buyer's own wallet never
    // moves — a bid accepted out of an escrow — which the floor detects,
    // because what is left in the buyer's wallet there is dust.
    [buyer_side, Some((credited, PriceSource::Credited))]
        .into_iter()
        .flatten()
        .find(|(price, _)| *price >= MIN_PRICE_LAMPORTS)
        .or_else(|| {
            from_owner
                .and_then(delta)
                .filter(|received| *received >= MIN_PRICE_LAMPORTS)
                .map(|received| (received, PriceSource::Seller))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use indexer_ingest::decode::TokenEvent;

    fn pk(seed: u8) -> String {
        bs58::encode([seed; 32]).into_string()
    }

    fn ts() -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000, 0).unwrap()
    }

    fn venues(program: &str) -> Venues {
        [(program.to_string(), "Synthetic Market".to_string())]
            .into_iter()
            .collect()
    }

    fn transfer(from: &str, to: &str) -> TokenEvent {
        TokenEvent {
            address: pk(1),
            kind: DecodedKind::Transfer,
            from_owner: Some(from.to_string()),
            to_owner: Some(to.to_string()),
            seq: 0,
            instruction: "transferChecked".into(),
        }
    }

    /// A marketplace sale as the chain records it: the buyer's lamports go
    /// down by the price plus their fee, the seller's go up by the price less
    /// royalties and commission.
    fn sale_payload(program: &str, buyer: &str, seller: &str) -> Decoded {
        Decoded {
            programs: vec![program.to_string(), pk(90)],
            fee_payer: Some(buyer.to_string()),
            fee: 5_000,
            native_deltas: vec![
                (buyer.to_string(), -580_005_000),
                (seller.to_string(), 545_200_000),
                (pk(80), 34_800_000),
            ],
            ..Decoded::default()
        }
    }

    #[test]
    fn a_marketplace_transfer_with_a_price_becomes_a_sale() {
        let (buyer, seller, program) = (pk(10), pk(11), pk(92));
        let decoded = sale_payload(&program, &buyer, &seller);
        let out = classify(
            "sig",
            100,
            ts(),
            &decoded,
            &transfer(&seller, &buyer),
            &venues(&program),
        );
        assert_eq!(out.kind, EventKind::Sale);
        assert_eq!(
            out.price_lamports,
            Some(580_000_000),
            "the buyer's side, net of the fee they paid — not the seller's, \
             which is already short the royalty"
        );
        assert_eq!(out.marketplace.as_deref(), Some("Synthetic Market"));
        assert_eq!(out.details["price_candidate"]["source"], "buyer");
    }

    #[test]
    fn an_unknown_venue_stays_a_transfer_but_keeps_the_price() {
        let (buyer, seller, program) = (pk(10), pk(11), pk(92));
        let decoded = sale_payload(&program, &buyer, &seller);
        let out = classify(
            "sig",
            100,
            ts(),
            &decoded,
            &transfer(&seller, &buyer),
            &Venues::default(),
        );
        assert_eq!(out.kind, EventKind::Transfer);
        assert_eq!((out.price_lamports, out.marketplace), (None, None));
        // The whole point: adding this program to the registry later is a
        // database-only reprice, never another crawl.
        assert_eq!(out.details["price_candidate"]["lamports"], 580_000_000);
    }

    #[test]
    fn a_venue_with_no_money_moving_is_not_a_sale() {
        // A listing, a cancel or a delist runs the marketplace program and
        // moves the token, but nobody pays. `activity_sale_has_price` would
        // reject a priceless sale, so it stays a transfer.
        let (holder, escrow, program) = (pk(10), pk(11), pk(92));
        let decoded = Decoded {
            programs: vec![program.clone()],
            fee_payer: Some(holder.clone()),
            fee: 5_000,
            native_deltas: vec![(holder.clone(), -5_000)],
            ..Decoded::default()
        };
        let out = classify(
            "sig",
            100,
            ts(),
            &decoded,
            &transfer(&holder, &escrow),
            &venues(&program),
        );
        assert_eq!(out.kind, EventKind::Transfer);
        assert_eq!(out.price_lamports, None);
    }

    #[test]
    fn the_buyers_account_rent_is_not_part_of_the_price() {
        // The regression this exists for: a Solanart sale of a SOL Gang pig
        // that Helius prices at 1 950 000 000 moved 1 952 039 279 out of the
        // buyer's wallet. The difference is the rent that funded the token
        // account the sale created, and counting it made every priced sale
        // wrong by ~0.002 SOL.
        let (buyer, seller, ata, program) = (pk(10), pk(11), pk(20), pk(92));
        let decoded = Decoded {
            programs: vec![program.clone()],
            fee_payer: Some(buyer.clone()),
            fee: 5_000,
            native_deltas: vec![
                (buyer.clone(), -1_952_044_279),
                (seller.clone(), 1_950_000_000),
                (ata.clone(), 2_039_279),
            ],
            token_accounts: vec![ata],
            ..Decoded::default()
        };
        let out = classify(
            "sig",
            100,
            ts(),
            &decoded,
            &transfer(&seller, &buyer),
            &venues(&program),
        );
        assert_eq!(out.price_lamports, Some(1_950_000_000));
    }

    #[test]
    fn an_escrow_sale_reads_the_seller_side_credit() {
        // Escrow-era flows pay a PDA, so the buyer's wallet may not appear in
        // the deltas at all. The seller's receipt is a lower bound and the
        // honest number to record.
        let (seller, escrow_buyer, program) = (pk(10), pk(11), pk(92));
        let decoded = Decoded {
            programs: vec![program.clone()],
            fee_payer: Some(pk(70)),
            fee: 5_000,
            native_deltas: vec![(seller.clone(), 8_000_000_000)],
            ..Decoded::default()
        };
        let out = classify(
            "sig",
            100,
            ts(),
            &decoded,
            &transfer(&seller, &escrow_buyer),
            &venues(&program),
        );
        assert_eq!(out.kind, EventKind::Sale);
        assert_eq!(out.price_lamports, Some(8_000_000_000));
        assert_eq!(out.details["price_candidate"]["source"], "credited");
    }

    #[test]
    fn mints_and_burns_are_never_priced() {
        for kind in [DecodedKind::Mint, DecodedKind::Burn] {
            let event = TokenEvent {
                address: pk(1),
                kind,
                from_owner: None,
                to_owner: Some(pk(10)),
                seq: 0,
                instruction: "mintTo".into(),
            };
            let decoded = Decoded {
                programs: vec![pk(92)],
                native_deltas: vec![(pk(10), -1_000_000)],
                ..Decoded::default()
            };
            let out = classify("sig", 1, ts(), &decoded, &event, &venues(&pk(92)));
            assert_eq!((out.price_lamports, out.marketplace), (None, None));
        }
    }
}
