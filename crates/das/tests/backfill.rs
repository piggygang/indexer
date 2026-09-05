//! End-to-end backfill against a fake Helius served from loopback.
//!
//! No `HELIUS_API_KEY` and no network: CI runs `--include-ignored` without a
//! key, so nothing here may depend on one. Every address is a synthetic
//! base58 key — no on-chain address appears in a test (CLAUDE.md).
//!
//! Note the deliberate shape: the registry's `metadata_uri_template` carries a
//! `LIKE 'https://%'` CHECK, so a loopback template cannot be stored. This
//! exercises the **no-template** branch instead — the fake DAS reports a
//! `content.json_uri` pointing at the same server and
//! `CollectionRow::metadata_source_uri` falls through to it. The template
//! branch is covered by the pure `merge` tests.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use indexer_das::backfill::{self, BackfillOptions};
use indexer_das::DasClient;
use indexer_data_model::PgPool;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

/// A minimal HTTP/1.1 server that speaks just enough to be Helius and a
/// metadata host. Hand-rolled rather than pulling in a mock-server crate for
/// one test.
///
/// Bind and serve are separate on purpose: the asset fixtures have to embed
/// the server's own URL in `content.json_uri`, so the address must be known
/// before they are built.
struct FakeHelius {
    base: String,
    listener: Option<TcpListener>,
    calls: Arc<AtomicUsize>,
}

impl FakeHelius {
    async fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        Self {
            base,
            listener: Some(listener),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn serve(&mut self, assets: HashMap<String, Value>, unknown: Vec<String>, slot: i64) {
        let listener = self.listener.take().expect("already serving");
        let counter = self.calls.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let assets = assets.clone();
                let unknown = unknown.clone();
                let counter = counter.clone();
                tokio::spawn(async move {
                    let _ = handle(stream, assets, unknown, slot, counter).await;
                });
            }
        });
    }

    /// How many metadata documents were actually requested — the proof that a
    /// second pass refetches nothing.
    fn document_calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

async fn handle(
    mut stream: TcpStream,
    assets: HashMap<String, Value>,
    unknown: Vec<String>,
    slot: i64,
    document_calls: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];

    // Headers, then exactly Content-Length bytes of body.
    let header_end = loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(index) = find(&buffer, b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8_lossy(&buffer[..header_end]).to_lowercase();
    let length: usize = headers
        .split("content-length:")
        .nth(1)
        .and_then(|rest| rest.split("\r\n").next())
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0);
    while buffer.len() < header_end + length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }

    let request_line = String::from_utf8_lossy(&buffer[..header_end])
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();
    let body: Value = serde_json::from_slice(&buffer[header_end..]).unwrap_or(Value::Null);

    // A GET is a metadata document fetch; a POST is JSON-RPC.
    if request_line.starts_with("GET ") {
        document_calls.fetch_add(1, Ordering::SeqCst);
        let path = request_line.split_whitespace().nth(1).unwrap_or("/");
        let id = path.trim_start_matches("/doc/").trim_end_matches(".json");
        return match assets.get(id) {
            Some(_) => {
                let document = json!({
                    "name": format!("#{}", &id[..4]),
                    "symbol": "SYN",
                    "image": format!("https://rehost.invalid/{id}.png"),
                    "attributes": [
                        {"trait_type": "Background", "value": "Pink"},
                        {"trait_type": "Name", "value": format!("#{}", &id[..4])},
                    ],
                });
                respond(&mut stream, 200, &document.to_string()).await
            }
            None => respond(&mut stream, 404, "not found").await,
        };
    }

    let method = body.get("method").and_then(Value::as_str).unwrap_or("");
    match method {
        "getSlot" => respond_rpc(&mut stream, json!(slot)).await,
        "getAssetBatch" => {
            let ids: Vec<String> = body
                .pointer("/params/ids")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            // The whole-batch 404 shape: any unknown id poisons the chunk,
            // which is exactly what forces the client to bisect.
            if ids.iter().any(|id| unknown.contains(id)) {
                return respond(&mut stream, 404, "one or more assets not found").await;
            }
            let items: Vec<Value> = ids
                .iter()
                .filter_map(|id| assets.get(id).cloned())
                .collect();
            respond_rpc(&mut stream, Value::Array(items)).await
        }
        _ => respond_rpc(&mut stream, Value::Null).await,
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

async fn respond_rpc(stream: &mut TcpStream, result: Value) -> std::io::Result<()> {
    let body = json!({"jsonrpc": "2.0", "id": "indexer", "result": result}).to_string();
    respond(stream, 200, &body).await
}

async fn respond(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

fn das_asset(base: &str, id: &str, owner: u8, burnt: bool) -> Value {
    json!({
        "id": id,
        "interface": "V1_NFT",
        "burnt": burnt,
        "ownership": {"owner": pk(owner), "ownership_model": "single"},
        "content": {
            "json_uri": format!("{base}/doc/{id}.json"),
            "metadata": {"name": "das name", "symbol": "SYN"},
            "links": {"image": "https://das.invalid/fallback.png"},
        },
    })
}

async fn seed_collection(pool: &PgPool, mints: &[String]) -> i32 {
    let id: i32 = sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, \
                                  facet_exclude, enabled) \
         VALUES ('syn-gang', 'Syn Gang', 'token_metadata', $1, 'SYN', ARRAY['Name'], true) \
         RETURNING id",
    )
    .bind(pk(200))
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO collection_mints (mint, collection_id) \
         SELECT m, $2 FROM unnest($1::text[]) AS m",
    )
    .bind(mints)
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
    id
}

async fn updated_ats(pool: &PgPool) -> Vec<(String, DateTime<Utc>)> {
    sqlx::query_as("SELECT address, updated_at FROM assets ORDER BY address")
        .fetch_all(pool)
        .await
        .unwrap()
}

/// The whole pipeline: enumerate the allowlist, ask DAS, bisect around an id
/// DAS does not know, fetch the documents, write assets/attributes/documents,
/// and land a `done` cursor — then prove a second pass changes nothing.
#[sqlx::test(migrations = "../data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn backfills_an_allowlist_collection_and_is_idempotent(pool: PgPool) {
    let mints: Vec<String> = (1..=3u8).map(pk).collect();
    let collection_id = seed_collection(&pool, &mints).await;

    let mut fake = FakeHelius::bind().await;
    // DAS knows the first two; the third is unknown, which makes the fake
    // reject any chunk containing it and forces the client to bisect.
    let assets: HashMap<String, Value> = mints
        .iter()
        .take(2)
        .enumerate()
        .map(|(i, m)| (m.clone(), das_asset(&fake.base, m, 50 + i as u8, false)))
        .collect();
    fake.serve(assets, vec![mints[2].clone()], 500);

    let das = DasClient::with_endpoint(&fake.base, "").unwrap();
    let options = BackfillOptions {
        slug: Some("syn-gang".into()),
        batch: 3,
        fetch_concurrency: 4,
        ..Default::default()
    };

    let report = backfill::run(&pool, &das, &options, |_| {}).await.unwrap();
    let collection = &report.collections[0];

    assert_eq!(collection.counts.inserted, 2, "the two DAS knows");
    assert_eq!(
        collection.missing_total, 1,
        "the third is reported, not invented"
    );
    assert_eq!(collection.missing, vec![pk(3)]);
    assert_eq!(collection.members, 2);
    assert_eq!(collection.status, "done");
    assert_eq!(collection.counts.documents, 2);
    // Two traits each, and `Name` is excluded from facets but still stored.
    assert_eq!(collection.counts.attributes_written, 4);

    let facets: Vec<(String, bool)> = sqlx::query_as(
        "SELECT name, is_facet FROM trait_types WHERE collection_id = $1 ORDER BY name",
    )
    .bind(collection_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        facets,
        vec![
            ("Background".to_string(), true),
            ("Name".to_string(), false)
        ]
    );

    // The document wins over DAS's cached name and image.
    let (name, image, source): (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT name, image_uri, metadata_source_uri FROM assets WHERE address = $1",
    )
    .bind(pk(1))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(name.starts_with('#'), "document name, not {name:?}");
    assert!(image.unwrap().starts_with("https://rehost.invalid/"));
    assert!(source.unwrap().starts_with(&fake.base));

    let state =
        indexer_data_model::ingest_state::backfill_state(&pool, collection_id, backfill::KIND)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(state.status, "done");
    assert_eq!(state.progress["missing"], json!(1));

    // ---- second pass: nothing may change ----
    let stamps = updated_ats(&pool).await;
    let documents_before = fake.document_calls();

    let again = backfill::run(&pool, &das, &options, |_| {}).await.unwrap();
    assert!(
        again.is_noop(),
        "a re-run must change nothing, got {:?}",
        again.totals()
    );
    assert_eq!(again.collections[0].counts.unchanged, 2);
    assert_eq!(updated_ats(&pool).await, stamps, "updated_at must not move");
    assert_eq!(
        fake.document_calls(),
        documents_before,
        "an unchanged collection must not refetch a single document"
    );
}

/// `--limit` is a smoke run: it must not mark the collection backfilled, or
/// the next full run would be skipped and the supply check would silently
/// pass against a partial collection.
#[sqlx::test(migrations = "../data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_limited_run_never_reports_done(pool: PgPool) {
    let mints: Vec<String> = (1..=3u8).map(pk).collect();
    let collection_id = seed_collection(&pool, &mints).await;

    let mut fake = FakeHelius::bind().await;
    let assets: HashMap<String, Value> = mints
        .iter()
        .enumerate()
        .map(|(i, m)| (m.clone(), das_asset(&fake.base, m, 50 + i as u8, false)))
        .collect();
    fake.serve(assets, Vec::new(), 500);

    let das = DasClient::with_endpoint(&fake.base, "").unwrap();
    let options = BackfillOptions {
        slug: Some("syn-gang".into()),
        batch: 2,
        limit: Some(2),
        ..Default::default()
    };

    let report = backfill::run(&pool, &das, &options, |_| {}).await.unwrap();
    assert_eq!(report.collections[0].status, "running");
    assert_eq!(report.collections[0].members, 2, "only the limited slice");

    let state =
        indexer_data_model::ingest_state::backfill_state(&pool, collection_id, backfill::KIND)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(state.status, "running");
    assert_eq!(state.cursor["next_index"], json!(2));
}

/// A burned asset keeps its row in the browse population but loses its owner,
/// which is what `assets_burned_has_no_owner` requires and what lets the UI
/// grey the card.
#[sqlx::test(migrations = "../data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_burned_asset_lands_without_an_owner(pool: PgPool) {
    let mints: Vec<String> = (1..=1u8).map(pk).collect();
    seed_collection(&pool, &mints).await;

    let mut fake = FakeHelius::bind().await;
    let mut assets = HashMap::new();
    assets.insert(mints[0].clone(), das_asset(&fake.base, &mints[0], 50, true));
    fake.serve(assets, Vec::new(), 500);

    let das = DasClient::with_endpoint(&fake.base, "").unwrap();
    let options = BackfillOptions {
        slug: Some("syn-gang".into()),
        batch: 10,
        ..Default::default()
    };
    let report = backfill::run(&pool, &das, &options, |_| {}).await.unwrap();
    assert_eq!(report.collections[0].counts.inserted, 1);

    let (burned, owner): (bool, Option<String>) =
        sqlx::query_as("SELECT burned, owner FROM assets WHERE address = $1")
            .bind(pk(1))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(burned);
    assert_eq!(owner, None);
}

/// The dead-metadata-host case (Pig Mud): DAS knows the assets, every
/// document 404s. Assets and owners still land; attributes stay empty; the
/// report says so rather than the run failing.
#[sqlx::test(migrations = "../data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_dead_metadata_host_still_yields_assets_and_owners(pool: PgPool) {
    let mints: Vec<String> = (1..=2u8).map(pk).collect();
    seed_collection(&pool, &mints).await;

    // The fake serves the RPC but has no documents: its GET handler only
    // knows ids present in its asset map, and we point json_uri elsewhere.
    let mut fake = FakeHelius::bind().await;
    let assets: HashMap<String, Value> = mints
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let mut asset = das_asset(&fake.base, m, 50 + i as u8, false);
            asset["content"]["json_uri"] = json!(format!("{}/doc/missing.json", fake.base));
            (m.clone(), asset)
        })
        .collect();
    fake.serve(assets, Vec::new(), 500);

    let das = DasClient::with_endpoint(&fake.base, "").unwrap();
    let options = BackfillOptions {
        slug: Some("syn-gang".into()),
        batch: 10,
        ..Default::default()
    };
    let report = backfill::run(&pool, &das, &options, |_| {}).await.unwrap();
    let collection = &report.collections[0];

    assert_eq!(collection.counts.inserted, 2, "assets still land");
    assert_eq!(collection.status, "done");
    assert_eq!(collection.documents_failed, 2);
    assert_eq!(collection.counts.documents, 0);
    assert!(collection
        .warnings
        .iter()
        .any(|w| w.contains("unreachable")));

    let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM assets WHERE owner IS NOT NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(owners, 2, "owners come from DAS, not the document");

    let attributes: i64 = sqlx::query_scalar("SELECT count(*) FROM asset_attributes")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        attributes, 0,
        "no metadata means no attributes, not a failure"
    );
}
