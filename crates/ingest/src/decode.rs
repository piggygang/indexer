//! Turns a transport-native transaction payload into classified per-asset
//! events.
//!
//! **This is the module the containment rule reserves.** It is the only place
//! in the tree that may `match` on [`RawPayload`]; pipeline code that does so
//! re-couples the pipeline to a transport and is a review error. Keeping the
//! quarantine here is also why it is always compiled rather than gated behind
//! the `ws` feature — a boundary that disappears from default builds is not a
//! boundary.
//!
//! Two deliberate choices shape the implementation:
//!
//! - **Ownership comes from `meta.preTokenBalances`/`postTokenBalances`, not
//!   from instruction arguments.** Balances are what actually moved, so the
//!   same code is correct for `transfer` (which carries no mint), for
//!   `transferChecked`, for a CPI transfer inside a marketplace instruction,
//!   and for token-2022.
//! - **No on-chain address appears in this file.** `jsonParsed` labels SPL
//!   instructions by *name*, so matching is on strings. Metaplex Core is not
//!   parsed by the RPC, so it is recognised structurally against the
//!   collection address the registry supplies at runtime. Every invoked
//!   program id is *captured as data* for `activity.details`, which is what
//!   lets ALG-622 reclassify a marketplace transfer into a priced sale later.
//!
//! The payload is accepted in both shapes the two callers produce: the
//! WebSocket notification (`result.transaction.{transaction,meta}`) and
//! `getTransaction` (`result.{transaction,meta}`), so the live path and the
//! gap-recovery path share one decoder — which is what makes their results
//! comparable.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::event::{RawPayload, TransactionUpdate};

/// What the registry knows that the decoder needs.
#[derive(Debug, Clone, Default)]
pub struct DecodeContext {
    /// Metaplex Core collection addresses. Core passes the collection account
    /// on every member instruction, which is what lets one address identify
    /// transfers *and* mints of assets that do not exist yet.
    pub core_collections: BTreeSet<String>,
}

/// The ownership-bearing kinds this decoder can establish from a payload.
/// Deliberately not `EventKind`: `sale` needs a price, which is ALG-622's job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodedKind {
    Mint,
    Transfer,
    Burn,
}

/// One classified ownership change of one token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenEvent {
    /// The mint — `assets.address` for a Token Metadata asset.
    pub address: String,
    pub kind: DecodedKind,
    pub from_owner: Option<String>,
    pub to_owner: Option<String>,
    /// Ordinal of this asset's events within the transaction, in instruction
    /// order. 0 normally.
    pub seq: i16,
    /// The parsed instruction name, for `activity.details`.
    pub instruction: String,
}

/// A Metaplex Core instruction touching one of our collections. The kind is
/// resolved one layer up against DAS, because doing it here would mean
/// hardcoding Core's Borsh discriminators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreTouch {
    pub asset: String,
    pub collection: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Decoded {
    /// Every invoked program id, in execution order. Carries the marketplace
    /// id to ALG-622 without this crate ever naming one.
    pub programs: Vec<String>,
    pub fee_payer: Option<String>,
    pub events: Vec<TokenEvent>,
    pub core: Vec<CoreTouch>,
}

impl Decoded {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty() && self.core.is_empty()
    }
}

/// One token account's state on one side of the transaction.
#[derive(Debug, Clone)]
struct Balance {
    mint: String,
    owner: Option<String>,
    amount: String,
    decimals: u64,
}

impl Balance {
    /// An NFT leg: exactly one indivisible unit. Without this gate a SOL,
    /// USDC or `$PIGGY` transfer riding in the same transaction would be read
    /// as an ownership change.
    fn is_nft_unit(&self) -> bool {
        self.decimals == 0 && self.amount == "1"
    }
}

pub fn decode_transaction(update: &TransactionUpdate, ctx: &DecodeContext) -> Decoded {
    // The one permitted match on RawPayload.
    let payload = match &update.raw {
        RawPayload::Json(value) => value,
        // Yellowstone protobuf arrives with the gRPC adapter (not this issue);
        // returning empty keeps the pipeline honest rather than guessing.
        RawPayload::Bytes(_) => return Decoded::default(),
    };
    if update.failed {
        return Decoded::default();
    }
    decode_json(payload, ctx)
}

/// Decodes a `jsonParsed` transaction from either caller's nesting.
pub fn decode_json(payload: &Value, ctx: &DecodeContext) -> Decoded {
    let Some(meta) = pick(payload, &["/transaction/meta", "/meta"]) else {
        return Decoded::default();
    };
    if !meta.get("err").is_none_or(Value::is_null) {
        return Decoded::default();
    }
    let Some(message) = pick(
        payload,
        &["/transaction/transaction/message", "/transaction/message"],
    ) else {
        return Decoded::default();
    };

    let keys = account_keys(message, meta);
    let pre = balances(meta, "preTokenBalances", &keys);
    let post = balances(meta, "postTokenBalances", &keys);

    let mut decoded = Decoded {
        fee_payer: keys.first().cloned(),
        ..Decoded::default()
    };
    let mut seen_programs = BTreeSet::new();
    let mut seq_by_address: BTreeMap<String, i16> = BTreeMap::new();
    let mut core_seen = BTreeSet::new();

    for instruction in flatten_instructions(message, meta) {
        if let Some(program_id) = instruction.get("programId").and_then(Value::as_str) {
            if seen_programs.insert(program_id.to_string()) {
                decoded.programs.push(program_id.to_string());
            }
        }

        // Metaplex Core: unparsed, so recognised by the collection account it
        // carries. Account 0 is the asset for every Core asset instruction.
        if let Some(accounts) = instruction.get("accounts").and_then(Value::as_array) {
            let accounts: Vec<&str> = accounts.iter().filter_map(Value::as_str).collect();
            if let Some(collection) = accounts
                .iter()
                .find(|a| ctx.core_collections.contains(**a))
                .map(|a| a.to_string())
            {
                if let Some(asset) = accounts.first().filter(|a| **a != collection) {
                    if core_seen.insert(asset.to_string()) {
                        decoded.core.push(CoreTouch {
                            asset: asset.to_string(),
                            collection,
                        });
                    }
                }
            }
        }

        let Some(parsed) = instruction.get("parsed") else {
            continue;
        };
        let Some(name) = parsed.get("type").and_then(Value::as_str) else {
            continue;
        };
        let Some(info) = parsed.get("info") else {
            continue;
        };

        let event = match name {
            "transfer" | "transferChecked" => token_transfer(info, &pre, &post),
            "burn" | "burnChecked" => token_burn(info, &pre),
            "mintTo" | "mintToChecked" => token_mint(info, &post),
            _ => None,
        };

        if let Some((address, kind, from_owner, to_owner)) = event {
            let seq = seq_by_address.entry(address.clone()).or_insert(0);
            decoded.events.push(TokenEvent {
                address,
                kind,
                from_owner,
                to_owner,
                seq: *seq,
                instruction: name.to_string(),
            });
            *seq += 1;
        }
    }

    decoded
}

type Classified = (String, DecodedKind, Option<String>, Option<String>);

fn token_transfer(
    info: &Value,
    pre: &BTreeMap<String, Balance>,
    post: &BTreeMap<String, Balance>,
) -> Option<Classified> {
    let source = info.get("source").and_then(Value::as_str)?;
    let destination = info.get("destination").and_then(Value::as_str)?;
    // The source's *pre* state names the mint and the sender; the
    // destination's *post* state names the receiver.
    let sent = pre.get(source)?;
    if !sent.is_nft_unit() {
        return None;
    }
    let received = post.get(destination);
    let to_owner = received.and_then(|b| b.owner.clone());
    let from_owner = sent.owner.clone();

    // Moving between two token accounts of the same wallet is an ATA
    // migration, not a change of ownership.
    if from_owner.is_some() && from_owner == to_owner {
        return None;
    }
    // `activity_transfer_shape` requires a receiver; without one this is not
    // something we may record as a transfer.
    to_owner.as_ref()?;

    Some((
        sent.mint.clone(),
        DecodedKind::Transfer,
        from_owner,
        to_owner,
    ))
}

fn token_burn(info: &Value, pre: &BTreeMap<String, Balance>) -> Option<Classified> {
    let account = info.get("account").and_then(Value::as_str)?;
    let held = pre.get(account)?;
    if !held.is_nft_unit() {
        return None;
    }
    Some((
        held.mint.clone(),
        DecodedKind::Burn,
        held.owner.clone(),
        None,
    ))
}

fn token_mint(info: &Value, post: &BTreeMap<String, Balance>) -> Option<Classified> {
    let account = info.get("account").and_then(Value::as_str)?;
    let minted = post.get(account)?;
    if !minted.is_nft_unit() {
        return None;
    }
    let to_owner = minted.owner.clone();
    // `activity_mint_shape` requires a receiver and forbids a sender.
    to_owner.as_ref()?;
    Some((minted.mint.clone(), DecodedKind::Mint, None, to_owner))
}

/// Static keys plus the address-lookup-table entries, in the index order the
/// token balances refer to.
fn account_keys(message: &Value, meta: &Value) -> Vec<String> {
    let mut keys: Vec<String> = message
        .get("accountKeys")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|k| match k {
                    Value::String(s) => Some(s.clone()),
                    other => other
                        .get("pubkey")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                })
                .collect()
        })
        .unwrap_or_default();

    for section in ["writable", "readonly"] {
        if let Some(list) = meta
            .pointer(&format!("/loadedAddresses/{section}"))
            .and_then(Value::as_array)
        {
            keys.extend(list.iter().filter_map(Value::as_str).map(str::to_string));
        }
    }
    keys
}

/// Token balances re-keyed from `accountIndex` to the token account address,
/// which is what the parsed instructions actually name.
fn balances(meta: &Value, field: &str, keys: &[String]) -> BTreeMap<String, Balance> {
    let mut out = BTreeMap::new();
    let Some(list) = meta.get(field).and_then(Value::as_array) else {
        return out;
    };
    for entry in list {
        let Some(index) = entry.get("accountIndex").and_then(Value::as_u64) else {
            continue;
        };
        let Some(address) = keys.get(index as usize) else {
            continue;
        };
        let Some(mint) = entry.get("mint").and_then(Value::as_str) else {
            continue;
        };
        out.insert(
            address.clone(),
            Balance {
                mint: mint.to_string(),
                // "Omitted if the validator did not record it."
                owner: entry
                    .get("owner")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                amount: entry
                    .pointer("/uiTokenAmount/amount")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                decimals: entry
                    .pointer("/uiTokenAmount/decimals")
                    .and_then(Value::as_u64)
                    .unwrap_or(u64::MAX),
            },
        );
    }
    out
}

/// Outer instruction *i*, then its inner instructions, then outer *i+1*.
/// That flattening is the definition of "instruction order" `seq` uses.
fn flatten_instructions<'a>(message: &'a Value, meta: &'a Value) -> Vec<&'a Value> {
    let outer = message
        .get("instructions")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let inner = meta
        .get("innerInstructions")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();

    let mut flat = Vec::with_capacity(outer.len());
    for (index, instruction) in outer.iter().enumerate() {
        flat.push(instruction);
        for group in inner {
            if group.get("index").and_then(Value::as_u64) == Some(index as u64) {
                if let Some(list) = group.get("instructions").and_then(Value::as_array) {
                    flat.extend(list.iter());
                }
            }
        }
    }
    flat
}

fn pick<'a>(value: &'a Value, paths: &[&str]) -> Option<&'a Value> {
    paths.iter().find_map(|path| value.pointer(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pk(seed: u8) -> String {
        bs58::encode([seed; 32]).into_string()
    }

    /// Builds a websocket-shaped payload. `pre`/`post` are
    /// `(account_index, mint, owner, amount, decimals)`.
    fn payload(
        keys: &[String],
        instructions: Vec<Value>,
        pre: Vec<Value>,
        post: Vec<Value>,
    ) -> Value {
        json!({
            "signature": "SYNsig",
            "slot": 100,
            "transaction": {
                "transaction": {
                    "message": {
                        "accountKeys": keys.iter().map(|k| json!({"pubkey": k})).collect::<Vec<_>>(),
                        "instructions": instructions,
                    }
                },
                "meta": {
                    "err": null,
                    "preTokenBalances": pre,
                    "postTokenBalances": post,
                    "innerInstructions": [],
                }
            }
        })
    }

    fn balance(index: u64, mint: &str, owner: &str, amount: &str, decimals: u64) -> Value {
        json!({
            "accountIndex": index, "mint": mint, "owner": owner,
            "uiTokenAmount": {"amount": amount, "decimals": decimals},
        })
    }

    fn spl(name: &str, info: Value) -> Value {
        json!({"program": "spl-token", "programId": pk(90), "parsed": {"type": name, "info": info}})
    }

    fn decode(payload: &Value) -> Decoded {
        decode_json(payload, &DecodeContext::default())
    }

    #[test]
    fn transfer_checked_reads_owners_from_balances() {
        let (src, dst, mint) = (pk(1), pk(2), pk(3));
        let keys = vec![pk(50), src.clone(), dst.clone(), mint.clone()];
        let p = payload(
            &keys,
            vec![spl(
                "transferChecked",
                json!({"source": src, "destination": dst, "mint": mint}),
            )],
            vec![balance(1, &mint, &pk(50), "1", 0)],
            vec![balance(2, &mint, &pk(51), "1", 0)],
        );
        let out = decode(&p);
        assert_eq!(out.events.len(), 1);
        let e = &out.events[0];
        assert_eq!(e.address, mint);
        assert_eq!(e.kind, DecodedKind::Transfer);
        assert_eq!(e.from_owner.as_deref(), Some(pk(50).as_str()));
        assert_eq!(e.to_owner.as_deref(), Some(pk(51).as_str()));
        assert_eq!(e.seq, 0);
        assert_eq!(out.fee_payer.as_deref(), Some(pk(50).as_str()));
        assert_eq!(out.programs, vec![pk(90)]);
    }

    /// The plain `transfer` instruction carries no mint, which is exactly why
    /// the decoder reads balances instead of instruction arguments.
    #[test]
    fn plain_transfer_without_a_mint_argument_still_decodes() {
        let (src, dst, mint) = (pk(1), pk(2), pk(3));
        let keys = vec![pk(50), src.clone(), dst.clone()];
        let p = payload(
            &keys,
            vec![spl(
                "transfer",
                json!({"source": src, "destination": dst, "amount": "1"}),
            )],
            vec![balance(1, &mint, &pk(50), "1", 0)],
            vec![balance(2, &mint, &pk(51), "1", 0)],
        );
        let out = decode(&p);
        assert_eq!(out.events.len(), 1);
        assert_eq!(out.events[0].address, mint);
        assert_eq!(out.events[0].to_owner.as_deref(), Some(pk(51).as_str()));
    }

    /// A fungible leg riding along in the same transaction must never be read
    /// as an ownership change.
    #[test]
    fn a_fungible_leg_is_ignored() {
        let (src, dst, mint) = (pk(1), pk(2), pk(3));
        let keys = vec![pk(50), src.clone(), dst.clone()];
        let p = payload(
            &keys,
            vec![spl(
                "transferChecked",
                json!({"source": src, "destination": dst, "mint": mint}),
            )],
            vec![balance(1, &mint, &pk(50), "1500000000", 9)],
            vec![balance(2, &mint, &pk(51), "1500000000", 9)],
        );
        assert!(decode(&p).events.is_empty());
    }

    #[test]
    fn moving_between_two_accounts_of_one_wallet_is_not_a_transfer() {
        let (src, dst, mint) = (pk(1), pk(2), pk(3));
        let keys = vec![pk(50), src.clone(), dst.clone()];
        let p = payload(
            &keys,
            vec![spl(
                "transferChecked",
                json!({"source": src, "destination": dst, "mint": mint}),
            )],
            vec![balance(1, &mint, &pk(50), "1", 0)],
            vec![balance(2, &mint, &pk(50), "1", 0)],
        );
        assert!(decode(&p).events.is_empty());
    }

    #[test]
    fn burn_and_mint_are_classified() {
        let (acct, mint) = (pk(1), pk(3));
        let keys = vec![pk(50), acct.clone()];

        let burn = payload(
            &keys,
            vec![spl("burn", json!({"account": acct, "mint": mint}))],
            vec![balance(1, &mint, &pk(50), "1", 0)],
            vec![],
        );
        let out = decode(&burn);
        assert_eq!(out.events[0].kind, DecodedKind::Burn);
        assert_eq!(out.events[0].from_owner.as_deref(), Some(pk(50).as_str()));
        assert_eq!(out.events[0].to_owner, None);

        let mint_tx = payload(
            &keys,
            vec![spl("mintTo", json!({"account": acct, "mint": mint}))],
            vec![],
            vec![balance(1, &mint, &pk(50), "1", 0)],
        );
        let out = decode(&mint_tx);
        assert_eq!(out.events[0].kind, DecodedKind::Mint);
        assert_eq!(out.events[0].from_owner, None);
        assert_eq!(out.events[0].to_owner.as_deref(), Some(pk(50).as_str()));
    }

    /// One signature, two assets — the swap shape. Each asset's own counter
    /// starts at 0.
    #[test]
    fn two_assets_in_one_transaction_each_start_at_seq_zero() {
        let (a1, a2, m1, m2) = (pk(1), pk(2), pk(3), pk(4));
        let keys = vec![pk(50), a1.clone(), a2.clone()];
        let p = payload(
            &keys,
            vec![
                spl("burn", json!({"account": a1, "mint": m1})),
                spl("mintTo", json!({"account": a2, "mint": m2})),
            ],
            vec![balance(1, &m1, &pk(50), "1", 0)],
            vec![balance(2, &m2, &pk(50), "1", 0)],
        );
        let out = decode(&p);
        assert_eq!(out.events.len(), 2);
        assert!(out.events.iter().all(|e| e.seq == 0));
    }

    /// The same asset twice in one transaction is what `seq` exists for.
    #[test]
    fn the_same_asset_twice_gets_increasing_seq() {
        let (a1, a2, a3, mint) = (pk(1), pk(2), pk(4), pk(3));
        let keys = vec![pk(50), a1.clone(), a2.clone(), a3.clone()];
        let p = payload(
            &keys,
            vec![
                spl(
                    "transferChecked",
                    json!({"source": a1, "destination": a2, "mint": mint}),
                ),
                spl(
                    "transferChecked",
                    json!({"source": a2, "destination": a3, "mint": mint}),
                ),
            ],
            vec![
                balance(1, &mint, &pk(50), "1", 0),
                balance(2, &mint, &pk(51), "1", 0),
            ],
            vec![
                balance(2, &mint, &pk(51), "1", 0),
                balance(3, &mint, &pk(52), "1", 0),
            ],
        );
        let out = decode(&p);
        assert_eq!(out.events.len(), 2);
        assert_eq!(out.events[0].seq, 0);
        assert_eq!(out.events[1].seq, 1);
    }

    #[test]
    fn a_failed_transaction_decodes_to_nothing() {
        let (src, dst, mint) = (pk(1), pk(2), pk(3));
        let keys = vec![pk(50), src.clone(), dst.clone()];
        let mut p = payload(
            &keys,
            vec![spl(
                "transferChecked",
                json!({"source": src, "destination": dst, "mint": mint}),
            )],
            vec![balance(1, &mint, &pk(50), "1", 0)],
            vec![balance(2, &mint, &pk(51), "1", 0)],
        );
        p["transaction"]["meta"]["err"] = json!({"InstructionError": [0, "Custom"]});
        assert!(decode(&p).is_empty());
    }

    #[test]
    fn a_core_instruction_is_found_by_its_collection_account() {
        let (asset, collection) = (pk(10), pk(11));
        let ctx = DecodeContext {
            core_collections: [collection.clone()].into_iter().collect(),
        };
        let keys = vec![pk(50), asset.clone(), collection.clone()];
        let p = payload(
            &keys,
            vec![json!({
                "programId": pk(91),
                "accounts": [asset, collection.clone()],
                "data": "SYNdata",
            })],
            vec![],
            vec![],
        );
        let out = decode_json(&p, &ctx);
        assert_eq!(
            out.core,
            vec![CoreTouch {
                asset: pk(10),
                collection
            }]
        );
    }

    /// A collection-level instruction (the collection itself at account 0) is
    /// not an asset touch.
    #[test]
    fn a_collection_level_instruction_is_not_an_asset_touch() {
        let collection = pk(11);
        let ctx = DecodeContext {
            core_collections: [collection.clone()].into_iter().collect(),
        };
        let keys = vec![pk(50), collection.clone()];
        let p = payload(
            &keys,
            vec![json!({
                "programId": pk(91), "accounts": [collection.clone(), pk(60)], "data": "SYNdata",
            })],
            vec![],
            vec![],
        );
        assert!(decode_json(&p, &ctx).core.is_empty());
    }

    /// The gap-recovery path feeds `getTransaction`'s nesting into the same
    /// decoder, so both paths must produce identical results.
    #[test]
    fn the_get_transaction_shape_decodes_identically() {
        let (src, dst, mint) = (pk(1), pk(2), pk(3));
        let keys = vec![pk(50), src.clone(), dst.clone()];
        let ws = payload(
            &keys,
            vec![spl(
                "transferChecked",
                json!({"source": src, "destination": dst, "mint": mint}),
            )],
            vec![balance(1, &mint, &pk(50), "1", 0)],
            vec![balance(2, &mint, &pk(51), "1", 0)],
        );
        // getTransaction hoists `transaction` and `meta` one level up.
        let rpc = json!({
            "slot": 100,
            "blockTime": 1_700_000_000,
            "transaction": ws["transaction"]["transaction"].clone(),
            "meta": ws["transaction"]["meta"].clone(),
        });
        assert_eq!(decode(&ws).events, decode(&rpc).events);
        assert!(!decode(&rpc).events.is_empty());
    }

    #[test]
    fn inner_instructions_are_walked_in_execution_order() {
        let (src, dst, mint) = (pk(1), pk(2), pk(3));
        let keys = vec![pk(50), src.clone(), dst.clone()];
        let mut p = payload(
            &keys,
            vec![json!({"programId": pk(92), "accounts": [], "data": "SYNouter"})],
            vec![balance(1, &mint, &pk(50), "1", 0)],
            vec![balance(2, &mint, &pk(51), "1", 0)],
        );
        // A marketplace program CPIs into spl-token: the transfer is inner.
        p["transaction"]["meta"]["innerInstructions"] = json!([{
            "index": 0,
            "instructions": [spl("transferChecked", json!({
                "source": src, "destination": dst, "mint": mint
            }))],
        }]);
        let out = decode(&p);
        assert_eq!(out.events.len(), 1, "a CPI transfer is still a transfer");
        assert_eq!(
            out.programs,
            vec![pk(92), pk(90)],
            "programs captured in execution order for ALG-622"
        );
    }

    #[test]
    fn a_protobuf_payload_decodes_to_nothing_for_now() {
        let update = TransactionUpdate {
            filters: vec!["tracked".into()],
            slot: 1,
            signature: "SYNsig".into(),
            failed: false,
            account_keys: vec![],
            raw: RawPayload::Bytes(vec![1, 2, 3]),
        };
        assert!(decode_transaction(&update, &DecodeContext::default()).is_empty());
    }

    #[test]
    fn a_failed_update_is_skipped_before_parsing() {
        let update = TransactionUpdate {
            filters: vec!["tracked".into()],
            slot: 1,
            signature: "SYNsig".into(),
            failed: true,
            account_keys: vec![],
            raw: RawPayload::Json(json!({})),
        };
        assert!(decode_transaction(&update, &DecodeContext::default()).is_empty());
    }

    #[test]
    fn lookup_table_addresses_extend_the_key_list() {
        let (src, dst, mint) = (pk(1), pk(2), pk(3));
        // `dst` is only reachable through the address lookup table.
        let keys = vec![pk(50), src.clone()];
        let mut p = payload(
            &keys,
            vec![spl(
                "transferChecked",
                json!({"source": src, "destination": dst, "mint": mint}),
            )],
            vec![balance(1, &mint, &pk(50), "1", 0)],
            vec![balance(2, &mint, &pk(51), "1", 0)],
        );
        p["transaction"]["meta"]["loadedAddresses"] = json!({"writable": [dst], "readonly": []});
        let out = decode(&p);
        assert_eq!(out.events.len(), 1);
        assert_eq!(out.events[0].to_owner.as_deref(), Some(pk(51).as_str()));
    }
}
