//! The per-asset archival crawl.
//!
//! One asset at a time, oldest transaction first, expanding only when the
//! timeline proves incomplete:
//!
//! 1. Page `getTransactionsForAddress` over the asset's own address. One call
//!    covers most assets, and every row carries `blockTime`, so the crawl
//!    never needs `getBlockTime`.
//! 2. Harvest a token-account → wallet map from those transactions, because
//!    pre-2022 token balances carry no `owner` and would otherwise decode to
//!    nothing.
//! 3. Decode, classify, and check the resulting ownership chain against
//!    itself and against DAS.
//! 4. Only if that check fails, expand to the asset's token accounts and go
//!    round again. That is where escrow-era marketplace moves hide: a plain
//!    `spl-token` `transfer` names neither the mint nor the wallets, so it is
//!    invisible on the mint address and visible on the token accounts.
//!
//! Measured on 2021-era pigs the fixpoint closes in two rounds, and the
//! adaptive stop keeps the common case at a single call.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use indexer_das::{ArchivedTx, DasClient};
use indexer_data_model::activity::{AssetRef, CrawledSignature};
use indexer_ingest::decode::{
    self, harvest_token_account_owners, token_accounts_for_mint, DecodeContext, TokenEvent,
};

use crate::chain::{self, Chain, Verdict};
use crate::classify::{self, TimelineEvent};
use crate::marketplaces::Venues;

/// Transactions per archival page. Helius caps `transactionDetails: "full"` at
/// 1 000; even the busiest pig is far below one page.
const PAGE: u32 = 1_000;

/// Expansion rounds before giving up. Round 1 is the asset address, round 2
/// its token accounts, round 3 anything they revealed — measured convergence
/// is 2, and a bound keeps a pathological asset from paging forever.
const MAX_ROUNDS: usize = 3;

/// What one asset's crawl produced.
#[derive(Debug, Clone, Default)]
pub struct AssetCrawl {
    /// Every signature seen, for `asset_signatures`.
    pub signatures: Vec<CrawledSignature>,
    /// Classified events in `(slot, seq)` order.
    pub events: Vec<TimelineEvent>,
    pub chain: Chain,
    pub verdict: Verdict,
    /// Addresses queried — 1 when the asset address sufficed.
    pub queried: usize,
    pub rounds: usize,
    /// Events dropped because the archival response carried no `blockTime`.
    /// `activity.block_time` is NOT NULL, so these are parked, never guessed.
    pub undated: usize,
}

/// Crawls one asset's full history.
pub async fn crawl_asset(
    das: &DasClient,
    venues: &Venues,
    core_collections: &BTreeSet<String>,
    asset: &AssetRef,
) -> Result<AssetCrawl> {
    let mut transactions: BTreeMap<String, ArchivedTx> = BTreeMap::new();
    let mut queued: BTreeSet<String> = BTreeSet::from([asset.address.clone()]);
    let mut queried: BTreeSet<String> = BTreeSet::new();
    let mut outcome = AssetCrawl::default();

    for round in 1..=MAX_ROUNDS {
        let todo: Vec<String> = queued.difference(&queried).cloned().collect();
        if todo.is_empty() {
            break;
        }
        outcome.rounds = round;
        for address in todo {
            for tx in page_address(das, &address).await? {
                transactions.insert(tx.signature.clone(), tx);
            }
            queried.insert(address);
        }

        outcome = analyze(
            venues,
            core_collections,
            asset,
            &transactions,
            outcome.rounds,
        );
        outcome.queried = queried.len();
        if outcome.verdict.is_settled() {
            return Ok(outcome);
        }

        // Expand. Only token accounts of *this* mint: a transaction may touch
        // many tokens, and crawling a stranger's account teaches us nothing.
        for tx in transactions.values() {
            queued.extend(token_accounts_for_mint(&tx.value, &asset.address));
        }
    }

    outcome.queried = queried.len();
    Ok(outcome)
}

/// Pages one address to exhaustion.
///
/// Terminates on an empty page, never on a null token: Helius returns a
/// `paginationToken` even on the last non-empty page, so stopping when it goes
/// away truncates some addresses and never terminates on others.
async fn page_address(das: &DasClient, address: &str) -> Result<Vec<ArchivedTx>> {
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = das
            .get_transactions_for_address(address, token.as_deref(), PAGE)
            .await
            .with_context(|| format!("getTransactionsForAddress {address}"))?;
        let empty = page.data.is_empty();
        out.extend(page.data);
        token = page.pagination_token;
        if empty || token.is_none() {
            return Ok(out);
        }
    }
}

/// Decodes and classifies everything crawled so far, then judges it.
///
/// The owner map is harvested across *all* transactions before any of them is
/// decoded: a token account created in 2021 is named by the `create`
/// instruction of that year's transaction, and needed by every transfer after.
fn analyze(
    venues: &Venues,
    core_collections: &BTreeSet<String>,
    asset: &AssetRef,
    transactions: &BTreeMap<String, ArchivedTx>,
    rounds: usize,
) -> AssetCrawl {
    let mut token_account_owners = BTreeMap::new();
    for tx in transactions.values() {
        for (account, owner) in harvest_token_account_owners(&tx.value) {
            token_account_owners.entry(account).or_insert(owner);
        }
    }
    let ctx = DecodeContext {
        core_collections: core_collections.clone(),
        token_account_owners,
    };

    let mut ordered: Vec<&ArchivedTx> = transactions.values().collect();
    ordered.sort_by(|a, b| {
        a.slot
            .cmp(&b.slot)
            .then_with(|| a.signature.cmp(&b.signature))
    });

    let mut out = AssetCrawl {
        rounds,
        ..AssetCrawl::default()
    };
    for tx in ordered {
        out.signatures.push(CrawledSignature {
            signature: tx.signature.clone(),
            slot: tx.slot,
            block_time: tx.block_time,
            // A failed transaction never reaches the decoder (`decode_json`
            // stops on a non-null `meta.err`), so the row is stored and left
            // unclassified rather than silently dropped.
            failed: tx
                .value
                .pointer("/meta/err")
                .is_some_and(|err| !err.is_null()),
        });

        let decoded = decode::decode_json(&tx.value, &ctx);
        let mine: Vec<&TokenEvent> = decoded
            .events
            .iter()
            .filter(|event| event.address == asset.address)
            .collect();
        if mine.is_empty() {
            continue;
        }
        let Some(block_time) = tx.block_time else {
            out.undated += mine.len();
            continue;
        };
        for event in mine {
            out.events.push(classify::classify(
                &tx.signature,
                tx.slot,
                block_time,
                &decoded,
                event,
                venues,
            ));
        }
    }

    out.events
        .sort_by(|a, b| a.slot.cmp(&b.slot).then_with(|| a.seq.cmp(&b.seq)));
    out.chain = chain::derive(&out.events);
    out.verdict = chain::verify(&out.chain, asset);
    out
}
