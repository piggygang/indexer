//! The archival crawl end to end, against a fake Helius served from loopback.
//!
//! No `HELIUS_API_KEY` and no network: CI runs `--include-ignored` without a
//! key, so nothing here may depend on one. Every address is a synthetic base58
//! key (CLAUDE.md), including the marketplace program — the venue registry is
//! built in memory rather than read from `config/`.
//!
//! The two scenarios are the two halves of the design: an asset whose history
//! is entirely visible on its own address (one call, no expansion), and one
//! whose sale is invisible there and only shows up on its token account —
//! which is what a pre-`transferChecked` escrow move actually looks like.

use std::collections::BTreeMap;
use std::sync::Arc;

use indexer_activity::{Options, Venues};
use indexer_das::DasClient;
use indexer_data_model::PgPool;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

fn sig(seed: u8) -> String {
    bs58::encode([seed; 64]).into_string()
}

/// A `getTransaction`-shaped transaction, which is exactly what
/// `getTransactionsForAddress` returns for `transactionDetails: "full"`.
struct Tx {
    signature: String,
    slot: i64,
    keys: Vec<String>,
    instructions: Vec<Value>,
    pre: Vec<Value>,
    post: Vec<Value>,
    lamports: Vec<(u64, u64)>,
}

impl Tx {
    fn value(&self) -> Value {
        let (pre_balances, post_balances): (Vec<u64>, Vec<u64>) =
            self.lamports.iter().copied().unzip();
        json!({
            "slot": self.slot,
            "blockTime": 1_700_000_000i64 + self.slot,
            "transaction": {
                "signatures": [self.signature],
                "message": {
                    "accountKeys": self.keys.iter().map(|k| json!({"pubkey": k})).collect::<Vec<_>>(),
                    "instructions": self.instructions,
                },
            },
            "meta": {
                "err": null,
                "fee": 5_000,
                "preBalances": pre_balances,
                "postBalances": post_balances,
                "preTokenBalances": self.pre,
                "postTokenBalances": self.post,
                "innerInstructions": [],
            },
        })
    }
}

fn balance(index: u64, mint: &str, owner: Option<&str>, amount: &str) -> Value {
    let mut entry = json!({
        "accountIndex": index, "mint": mint,
        "uiTokenAmount": {"amount": amount, "decimals": 0},
    });
    // Absent on pre-2022 transactions — the case the owner map exists for.
    if let Some(owner) = owner {
        entry["owner"] = json!(owner);
    }
    entry
}

fn spl(name: &str, info: Value) -> Value {
    json!({"program": "spl-token", "programId": pk(90), "parsed": {"type": name, "info": info}})
}

/// A minimal HTTP/1.1 server that answers `getTransactionsForAddress` from a
/// fixed per-address script. Hand-rolled for the same reason the DAS test's
/// fake is: one test does not justify a mock-server dependency.
struct FakeHelius {
    base: String,
    listener: Option<TcpListener>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl FakeHelius {
    async fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        Self {
            base,
            listener: Some(listener),
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// `script` maps an address to the transactions a query on it returns.
    fn serve(&mut self, script: BTreeMap<String, Vec<Value>>) {
        let listener = self.listener.take().unwrap();
        let calls = Arc::clone(&self.calls);
        tokio::spawn(async move {
            let script = Arc::new(script);
            while let Ok((stream, _)) = listener.accept().await {
                let (script, calls) = (Arc::clone(&script), Arc::clone(&calls));
                tokio::spawn(async move {
                    let _ = handle(stream, script, calls).await;
                });
            }
        });
    }

    /// How many archival queries were issued — the adaptive-crawl assertion.
    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::Relaxed)
    }
}

async fn handle(
    mut stream: TcpStream,
    script: Arc<BTreeMap<String, Vec<Value>>>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
) -> std::io::Result<()> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(start) = find(&buffer, b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buffer[..start]).to_lowercase();
            let length = head
                .split("content-length:")
                .nth(1)
                .and_then(|rest| rest.split("\r\n").next())
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if buffer.len() >= start + 4 + length {
                break;
            }
        }
    }
    let start = find(&buffer, b"\r\n\r\n").map(|i| i + 4).unwrap_or(0);
    let body: Value = serde_json::from_slice(&buffer[start..]).unwrap_or(Value::Null);

    let result = match body["method"].as_str() {
        Some("getTransactionsForAddress") => {
            calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let address = body["params"][0].as_str().unwrap_or_default();
            let token = body["params"][1]["paginationToken"].as_str();
            let rows = script.get(address).cloned().unwrap_or_default();
            // Page 1 returns everything and still hands back a token — the
            // real endpoint does, which is why the crawl pages until empty.
            match token {
                None => json!({"data": rows, "paginationToken": "1:0"}),
                Some(_) => json!({"data": [], "paginationToken": null}),
            }
        }
        _ => Value::Null,
    };
    let payload = json!({"jsonrpc": "2.0", "id": "indexer", "result": result}).to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

/// `(kind, from_owner, to_owner, price_lamports, marketplace)` — the five
/// columns the contract promises and the classifier decides.
type ActivityRow = (
    String,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
);

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

async fn seed(pool: &PgPool, mint: &str, owner: &str) -> i32 {
    let collection_id: i32 = sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, enabled) \
         VALUES ('syn-gang', 'Syn Gang', 'token_metadata', $1, 'SYN', true) RETURNING id",
    )
    .bind(pk(200))
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (address, collection_id, name, owner, owner_slot) \
         VALUES ($1, $2, 'Syn #1', $3, 1)",
    )
    .bind(mint)
    .bind(collection_id)
    .bind(owner)
    .execute(pool)
    .await
    .unwrap();
    collection_id
}

/// mint -> A, then A sells to B through a marketplace program.
fn history(mint: &str, ata_a: &str, ata_b: &str, a: &str, b: &str, venue: &str) -> (Value, Value) {
    let mint_tx = Tx {
        signature: sig(1),
        slot: 100,
        keys: vec![a.to_string(), ata_a.to_string(), mint.to_string()],
        instructions: vec![
            json!({
                "program": "spl-associated-token-account",
                "programId": pk(91),
                "parsed": {"type": "create", "info": {"account": ata_a, "mint": mint, "wallet": a}},
            }),
            spl("mintTo", json!({"account": ata_a, "mint": mint})),
        ],
        pre: vec![],
        // No `owner`: a 2021 validator did not record it, so the receiver can
        // only come from the `create` instruction above.
        post: vec![balance(1, mint, None, "1")],
        lamports: vec![(2_000_000_000, 1_997_955_720), (0, 2_039_280), (0, 0)],
    };
    let sale_tx = Tx {
        signature: sig(2),
        slot: 200,
        keys: vec![
            b.to_string(),
            a.to_string(),
            ata_a.to_string(),
            ata_b.to_string(),
            venue.to_string(),
        ],
        instructions: vec![
            json!({"programId": venue, "accounts": [ata_a, ata_b]}),
            // The buyer's token account is created in the sale itself, which
            // is the only thing that names them: the balances below carry no
            // `owner`, and a plain `transfer` names no wallet.
            json!({
                "program": "spl-associated-token-account",
                "programId": pk(91),
                "parsed": {"type": "create", "info": {"account": ata_b, "mint": mint, "wallet": b}},
            }),
            spl(
                "transfer",
                json!({"source": ata_a, "destination": ata_b, "authority": a}),
            ),
        ],
        pre: vec![balance(2, mint, None, "1")],
        post: vec![balance(3, mint, None, "1")],
        // The buyer pays 0.58 SOL plus the fee; the seller nets it less a
        // royalty that goes nowhere we model.
        lamports: vec![
            (3_000_000_000, 2_419_995_000),
            (0, 545_200_000),
            (0, 0),
            (0, 0),
            (0, 0),
        ],
    };
    (mint_tx.value(), sale_tx.value())
}

async fn run(pool: &PgPool, das: &DasClient, venues: &Venues) -> indexer_activity::Report {
    indexer_activity::run(pool, das, venues, &Options::default(), |_| {})
        .await
        .unwrap()
}

#[sqlx::test(migrations = "../data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_visible_history_is_crawled_in_one_call_and_is_idempotent(pool: PgPool) {
    let (mint, ata_a, ata_b) = (pk(1), pk(2), pk(3));
    let (a, b, venue) = (pk(10), pk(11), pk(92));
    seed(&pool, &mint, &b).await;

    let (mint_tx, sale_tx) = history(&mint, &ata_a, &ata_b, &a, &b, &venue);
    let mut fake = FakeHelius::bind().await;
    fake.serve(BTreeMap::from([(mint.clone(), vec![mint_tx, sale_tx])]));
    let das = DasClient::with_endpoint(&fake.base, "").unwrap();
    let venues: Venues = [(venue.clone(), "Synthetic Market".to_string())]
        .into_iter()
        .collect();

    let report = run(&pool, &das, &venues).await;
    let totals = report.totals();
    assert_eq!((totals.assets, totals.signatures, totals.events), (1, 2, 2));
    assert_eq!(totals.sales, 1);
    assert_eq!(totals.expanded, 0, "the asset's own address sufficed");
    assert_eq!(totals.mismatched, 0, "the derived owner agrees with DAS");
    assert_eq!(fake.calls(), 2, "one page plus the empty page that ends it");

    // The 2021 mint had no `owner` on its balance; only the map from the
    // `create` instruction makes it a mint at all.
    let rows: Vec<ActivityRow> = sqlx::query_as(
        "SELECT kind, from_owner, to_owner, price_lamports, marketplace \
               FROM activity ORDER BY slot",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows[0].0, "mint");
    assert_eq!(rows[0].2.as_deref(), Some(a.as_str()));
    assert_eq!(rows[1].0, "sale");
    assert_eq!(
        rows[1].3,
        Some(580_000_000),
        "the buyer's side, net of the fee"
    );
    assert_eq!(rows[1].4.as_deref(), Some("Synthetic Market"));

    let intervals: Vec<(String, i64, Option<i64>)> = sqlx::query_as(
        "SELECT owner, from_slot, to_slot FROM ownership_history ORDER BY from_slot",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        intervals,
        vec![(a, 100, Some(200)), (b, 200, None)],
        "ownership derived from the transfer chain, one open interval"
    );

    let before = fake.calls();
    let again = run(&pool, &das, &venues).await;
    assert!(
        again.is_noop(),
        "re-running a crawled collection must change nothing: {:?}",
        again.totals()
    );
    assert!(
        fake.calls() > before,
        "it re-reads the chain but writes nothing"
    );
}

#[sqlx::test(migrations = "../data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_sale_invisible_on_the_mint_is_found_on_the_token_account(pool: PgPool) {
    let (mint, ata_a, ata_b) = (pk(1), pk(2), pk(3));
    let (a, b, venue) = (pk(10), pk(11), pk(92));
    seed(&pool, &mint, &b).await;

    // The escrow-era shape: a plain `transfer` names neither the mint nor the
    // wallets, so a query on the mint address never sees the sale.
    let (mint_tx, sale_tx) = history(&mint, &ata_a, &ata_b, &a, &b, &venue);
    let mut fake = FakeHelius::bind().await;
    fake.serve(BTreeMap::from([
        (mint.clone(), vec![mint_tx]),
        (ata_a.clone(), vec![sale_tx]),
    ]));
    let das = DasClient::with_endpoint(&fake.base, "").unwrap();
    let venues: Venues = [(venue.clone(), "Synthetic Market".to_string())]
        .into_iter()
        .collect();

    let totals = run(&pool, &das, &venues).await.totals();
    assert_eq!(
        totals.expanded, 1,
        "round 1 derived owner A while DAS says B, so the crawl expanded"
    );
    assert_eq!(
        totals.events, 2,
        "the mint and the sale the mint query missed"
    );
    assert_eq!(totals.sales, 1);
    assert_eq!(totals.mismatched, 0, "expansion closed the gap");

    let owner: Option<String> =
        sqlx::query_scalar("SELECT owner FROM ownership_history WHERE to_slot IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(owner.as_deref(), Some(b.as_str()));
}
