//! The Helius webhook endpoint, driven through the real route table.
//!
//! Every address is a synthetic base58 key (CLAUDE.md), and the payloads below
//! are shaped like Helius's **documented raw webhook** — a JSON array of
//! `getTransaction`-shaped objects with `slot`, `blockTime`, `meta` and
//! `transaction.signatures`.
//!
//! Ignored without a database: `cargo test --workspace -- --include-ignored`.

use actix_web::{http::header, test, web, App};
use indexer_data_model::PgPool;
use indexer_ingester::receiver::{self, WebhookSecret};
use serde_json::{json, Value};

const SECRET: &str = "Bearer test-secret";

fn sig(seed: u8) -> String {
    bs58::encode([seed; 64]).into_string()
}

fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

/// One element of a raw delivery, in the shape Helius documents.
fn delivery(seed: u8, slot: i64, failed: bool) -> Value {
    json!({
        "blockTime": 1_700_000_000i64 + slot,
        "indexWithinBlock": 7,
        "slot": slot,
        "meta": {
            "err": if failed { json!({"InstructionError": [0, "Custom"]}) } else { Value::Null },
            "fee": 5000,
            "preTokenBalances": [],
            "postTokenBalances": [],
            "innerInstructions": [],
        },
        "transaction": {
            "signatures": [sig(seed)],
            "message": {
                "accountKeys": [pk(1), pk(2)],
                // Index form — what a raw webhook actually delivers, and what
                // the decoder cannot read. The drain re-fetches; the receiver
                // does not care, which is the point of storing the body whole.
                "instructions": [{
                    "accounts": [0, 1],
                    "data": "3GyWrkssW12wSfxjTynBnbif",
                    "programIdIndex": 1,
                }],
            },
        },
    })
}

/// Builds the identical route table the binary serves, with the app data the
/// caller wants present — `None` for either models "not configured".
macro_rules! app {
    ($writer:expr, $secret:expr) => {{
        let mut app = App::new().configure(indexer_ingester::receiver::configure);
        if let Some(writer) = $writer {
            app = app.app_data(web::Data::new(writer));
        }
        if let Some(secret) = $secret {
            app = app.app_data(web::Data::new(WebhookSecret::new(secret)));
        }
        test::init_service(app).await
    }};
}

macro_rules! post {
    ($app:expr, $auth:expr, $body:expr) => {{
        let mut request = test::TestRequest::post().uri("/webhooks/helius");
        if let Some(auth) = $auth {
            request = request.insert_header((header::AUTHORIZATION, auth));
        }
        let resp = test::call_service(&$app, request.set_json($body).to_request()).await;
        let status = resp.status().as_u16();
        let body = test::read_body(resp).await;
        let json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        (status, json)
    }};
}

/// Spawns the writer task and hands back the sender the handler holds.
fn writer(pool: &PgPool) -> receiver::InboxWriter {
    let (writer, task) = receiver::writer(pool.clone(), 8);
    tokio::spawn(task);
    writer
}

async fn rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM webhook_inbox")
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The happy path, and the projection it depends on.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_signed_delivery_is_filed(pool: PgPool) {
    let app = app!(Some(writer(&pool)), Some(SECRET));
    let (status, body) = post!(app, Some(SECRET), &json!([delivery(1, 100, false)]));

    assert_eq!(status, 200);
    assert_eq!(body["filed"], 1);
    assert_eq!(rows(&pool).await, 1);

    let (signature, slot, block_time, failed): (
        String,
        i64,
        Option<chrono::DateTime<chrono::Utc>>,
        bool,
    ) = sqlx::query_as("SELECT signature, slot, block_time, failed FROM webhook_inbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(signature, sig(1));
    assert_eq!(slot, 100);
    assert!(block_time.is_some(), "blockTime must survive projection");
    assert!(!failed);

    // The raw body is kept whole — the forensic record of what Helius sent,
    // and the input a future index-form decoder would read.
    let stored: Value = sqlx::query_scalar("SELECT body FROM webhook_inbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored["slot"], 100);
    assert!(stored
        .pointer("/transaction/message/instructions/0/programIdIndex")
        .is_some());
}

/// The secret is the entire authenticity check — Helius offers no HMAC — so
/// every way of getting it wrong must be a 401 that writes nothing.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn an_unsigned_or_wrongly_signed_delivery_is_refused(pool: PgPool) {
    let app = app!(Some(writer(&pool)), Some(SECRET));

    for auth in [None, Some(""), Some("Bearer wrong"), Some("test-secret")] {
        let (status, _) = post!(app, auth, &json!([delivery(1, 100, false)]));
        assert_eq!(status, 401, "auth {auth:?} must be refused");
    }
    assert_eq!(rows(&pool).await, 0, "nothing may be written");
}

/// A route table with no secret must not authenticate anyone — and 503 rather
/// than 401, because it is our misconfiguration and a retry might survive it.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_missing_secret_refuses_everything(pool: PgPool) {
    let app = app!(Some(writer(&pool)), None::<&str>);
    let (status, _) = post!(app, Some(SECRET), &json!([delivery(1, 100, false)]));
    assert_eq!(status, 503);
    assert_eq!(rows(&pool).await, 0);
}

/// Helius warns that it redelivers. A duplicate is still a 200, and still one
/// row — that is what makes its retry budget usable.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_redelivery_is_absorbed(pool: PgPool) {
    let app = app!(Some(writer(&pool)), Some(SECRET));
    let payload = json!([delivery(1, 100, false), delivery(2, 101, false)]);

    let (first, first_body) = post!(app, Some(SECRET), &payload);
    let (second, second_body) = post!(app, Some(SECRET), &payload);

    assert_eq!((first, second), (200, 200));
    assert_eq!(first_body["filed"], 2);
    assert_eq!(second_body["filed"], 0, "the redelivery files nothing new");
    assert_eq!(second_body["received"], 2, "but is still acknowledged");
    assert_eq!(rows(&pool).await, 2);
}

/// A partial batch is filed, not refused. Rejecting would lose the good rows
/// too: Helius would retry the same bad payload three times and then drop it.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn an_unusable_element_is_skipped_and_the_rest_filed(pool: PgPool) {
    let app = app!(Some(writer(&pool)), Some(SECRET));
    let payload = json!([
        delivery(1, 100, false),
        json!({"slot": 101}),                             // no signature
        json!({"transaction": {"signatures": [sig(3)]}}), // no slot
    ]);

    let (status, body) = post!(app, Some(SECRET), &payload);
    assert_eq!(status, 200);
    assert_eq!(body["filed"], 1);
    assert_eq!(body["skipped"], 2);
    assert_eq!(rows(&pool).await, 1);
}

/// A failed transaction is recorded rather than discarded: the drain skips it
/// without spending a `getTransaction`, and the row still proves the delivery
/// happened.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_failed_transaction_is_filed_and_flagged(pool: PgPool) {
    let app = app!(Some(writer(&pool)), Some(SECRET));
    let (status, _) = post!(app, Some(SECRET), &json!([delivery(9, 200, true)]));

    assert_eq!(status, 200);
    let failed: bool = sqlx::query_scalar("SELECT failed FROM webhook_inbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(failed);
}

/// An empty batch is a legitimate delivery, not an error.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn an_empty_batch_is_acknowledged(pool: PgPool) {
    let app = app!(Some(writer(&pool)), Some(SECRET));
    let (status, body) = post!(app, Some(SECRET), &json!([]));
    assert_eq!(status, 200);
    assert_eq!(body["filed"], 0);
    assert_eq!(rows(&pool).await, 0);
}

/// A body that is not the shape we registered for is a 400 — retrying cannot
/// fix it, and the error log plus Helius's own failure metric are the alarm.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn an_unreadable_body_is_rejected(pool: PgPool) {
    let app = app!(Some(writer(&pool)), Some(SECRET));
    let resp = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/webhooks/helius")
            .insert_header((header::AUTHORIZATION, SECRET))
            .insert_header((header::CONTENT_TYPE, "application/json"))
            .set_payload("this is not json")
            .to_request(),
    )
    .await;
    assert_eq!(resp.status().as_u16(), 400);
    assert_eq!(rows(&pool).await, 0);
}

/// Helius documents an array, but tolerating a bare object costs one line and
/// removes a whole class of outage.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_single_object_is_treated_as_one_element(pool: PgPool) {
    let app = app!(Some(writer(&pool)), Some(SECRET));
    let (status, body) = post!(app, Some(SECRET), &delivery(4, 400, false));
    assert_eq!(status, 200);
    assert_eq!(body["filed"], 1);
    assert_eq!(rows(&pool).await, 1);
}

/// Liveness only, and deliberately no database: the Railway healthcheck gates
/// the deploy, and gating it on Postgres would let a blip block a cutover.
#[actix_web::test]
async fn health_answers_without_a_pool() {
    let app = app!(None::<receiver::InboxWriter>, None::<&str>);
    let resp = test::call_service(&app, test::TestRequest::get().uri("/health").to_request()).await;
    assert!(resp.status().is_success());
}
