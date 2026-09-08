//! The Helius webhook endpoint.
//!
//! This service already owns the transport's other two thirds — the drain
//! ([`crate::inbox`]) and the pipeline — so it owns receipt too. It runs on the
//! binary's existing multi-thread tokio runtime, which actix-web explicitly
//! supports, and is spawned independently of the consumer supervisor so a
//! consumer restart never stops accepting deliveries.
//!
//! **The handler never touches Postgres.** sqlx pools are not runtime-affine,
//! but a connection *opened on an actix worker's runtime* and later checked out
//! by a background task breaks when that worker's runtime is dropped — and
//! actix restarts faulted workers and drops their runtimes on shutdown. So the
//! handler projects the payload and hands it to a writer task on the main
//! runtime, which is the only thing here that speaks to the database. That also
//! means there is no `/ready`: a DB ping from a worker would reintroduce
//! exactly the connection this design keeps out, and the ingester's readiness
//! is read from `ingest_state`, not from an HTTP probe (see the README
//! runbook).
//!
//! **A 200 means the row is committed.** The handler waits for the writer's
//! reply before answering, because Helius retries roughly three times and then
//! drops the event for good — an optimistic ACK we could not honour would
//! reintroduce the loss window the inbox exists to close. The round trip is
//! milliseconds against a one-second budget.

use std::time::Duration;

use actix_web::{get, http::header, post, web, HttpRequest, HttpResponse};
use indexer_data_model::webhook_inbox::{self, Delivery};
use indexer_data_model::PgPool;
use serde_json::Value;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, oneshot};

/// Generous on purpose: a raw transaction with full `meta` and logs runs to
/// tens of KB and Helius batches them into one array. Going over answers 413,
/// which costs three retries and then the delivery — so the failure mode of
/// being too tight is data loss, and of being too generous is bounded memory
/// on an endpoint taking a few hundred requests a day.
pub const MAX_BODY: usize = 8 * 1024 * 1024;

/// How long the handler waits for the writer task.
///
/// Deliberately longer than Helius's ~1 s budget. If we commit and answer late,
/// Helius has already retried and the retry deduplicates on the signature — so
/// a late success is free, while giving up early on a write that is about to
/// succeed is not.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// One batch, and somewhere to say whether it landed.
type WriteRequest = (Vec<Delivery>, oneshot::Sender<Result<u64, String>>);

/// The handle the HTTP workers hold. Not a pool — see the module doc.
#[derive(Clone)]
pub struct InboxWriter(mpsc::Sender<WriteRequest>);

/// The shared secret, stored as its digest.
///
/// Helius offers no HMAC and no signature scheme: it echoes the configured
/// `authHeader` verbatim, so this value is the entire authenticity check.
pub struct WebhookSecret {
    digest: [u8; 32],
}

impl std::fmt::Debug for WebhookSecret {
    /// Redacted: `Config` derives `Debug` throughout, and a secret that can be
    /// printed will eventually be printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WebhookSecret(<redacted>)")
    }
}

impl WebhookSecret {
    pub fn new(secret: &str) -> Self {
        Self {
            digest: Sha256::digest(secret.as_bytes()).into(),
        }
    }

    /// Constant-time, over digests rather than the raw bytes.
    ///
    /// Comparing the raw strings with `subtle` would still leak the *presented*
    /// token's length, because `ct_eq` short-circuits on a length mismatch.
    /// Hashing first makes both sides exactly 32 bytes, so nothing about the
    /// candidate is observable from timing.
    pub fn matches(&self, presented: &str) -> bool {
        let candidate: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        candidate.ct_eq(&self.digest).into()
    }
}

/// Builds the channel and the task that owns the database side.
///
/// The task runs on the caller's runtime — the binary's main multi-thread one —
/// and ends when every [`InboxWriter`] is dropped.
pub fn writer(
    pool: PgPool,
    capacity: usize,
) -> (InboxWriter, impl std::future::Future<Output = ()>) {
    let (tx, mut rx) = mpsc::channel::<WriteRequest>(capacity);
    let task = async move {
        while let Some((deliveries, reply)) = rx.recv().await {
            let result = webhook_inbox::enqueue(&pool, &deliveries)
                .await
                .map_err(|e| e.to_string());
            if let Err(error) = &result {
                log::error!("webhook inbox write failed: {error}");
            }
            // A dropped receiver means the request timed out and gave up; the
            // rows are still committed, and Helius's retry will deduplicate.
            let _ = reply.send(result);
        }
        log::info!("webhook inbox writer stopped");
    };
    (InboxWriter(tx), task)
}

/// Registers the route table, including the body limit.
///
/// The limit lives here rather than on the `App` in `main` for the reason the
/// api's `handlers::configure` exists: tests build this same table, so a limit
/// set elsewhere would be one no test exercises. It is attached to a scope
/// because the `#[post]` attribute macro has no hook for `app_data` — and it
/// must be `PayloadConfig`, not `JsonConfig`, because `web::Bytes` reads the
/// former and `web::Json` the latter.
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(health).service(
        web::scope("/webhooks")
            .app_data(web::PayloadConfig::new(MAX_BODY))
            .service(helius),
    );
}

/// Liveness only — no database.
///
/// The Railway healthcheck gates the deploy, and gating it on Postgres would
/// let a database blip block a cutover. Same rule the api follows.
#[get("/health")]
pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "service": env!("CARGO_PKG_NAME"),
        "version": env!("CARGO_PKG_VERSION"),
        "commit": match option_env!("GIT_SHA") {
            Some(sha) if !sha.is_empty() => sha,
            _ => "unknown",
        },
    }))
}

/// Status codes are chosen for how Helius reacts to each:
///
/// * **401** — a wrong secret will never become right, so its retries are
///   wasted either way; 401 is what a human debugging needs to see.
/// * **503** — the only class Helius retries usefully, so it is reserved for
///   "we might have this next time": no secret configured, and a failed write.
/// * **400** — the body is not the shape we registered for. Retrying cannot fix
///   it, but the error log is the alarm and Helius's own failure metric is the
///   backstop.
/// * **200** — filed. Including when *some* elements were unusable; see below.
#[post("/helius")]
pub async fn helius(
    writer: Option<web::Data<InboxWriter>>,
    secret: Option<web::Data<WebhookSecret>>,
    req: HttpRequest,
    body: web::Bytes,
) -> HttpResponse {
    let Some(secret) = secret else {
        log::error!("webhook delivery refused: HELIUS_WEBHOOK_SECRET is not configured");
        return refuse(
            HttpResponse::ServiceUnavailable(),
            "no webhook secret configured",
        );
    };
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !secret.matches(presented) {
        log::warn!("webhook delivery refused: bad Authorization header");
        // Empty body: a prober learns nothing about why.
        return HttpResponse::Unauthorized().finish();
    }
    let Some(writer) = writer else {
        return refuse(HttpResponse::ServiceUnavailable(), "no inbox writer");
    };

    let (deliveries, skipped) = match project(&body) {
        Ok(projected) => projected,
        Err(error) => {
            log::error!("webhook payload is not a JSON array of transactions: {error}");
            return refuse(HttpResponse::BadRequest(), "unreadable payload");
        }
    };
    if skipped > 0 {
        let level = if deliveries.is_empty() {
            // Nothing at all projected: the payload is not what we registered
            // for — an `enhanced` webhook where we asked for `raw`, or a
            // changed schema. Silence here is how a transport dies unnoticed.
            log::Level::Error
        } else {
            log::Level::Warn
        };
        log::log!(
            level,
            "webhook: {skipped} of {} element(s) had no usable signature/slot",
            skipped + deliveries.len()
        );
    }
    if deliveries.is_empty() {
        return HttpResponse::Ok().json(serde_json::json!({ "filed": 0, "skipped": skipped }));
    }

    let total = deliveries.len();
    let (reply, answer) = oneshot::channel();
    if writer.0.send((deliveries, reply)).await.is_err() {
        return refuse(HttpResponse::ServiceUnavailable(), "inbox writer stopped");
    }
    match tokio::time::timeout(WRITE_TIMEOUT, answer).await {
        Ok(Ok(Ok(filed))) => HttpResponse::Ok().json(serde_json::json!({
            // `filed` counts rows that were new; the difference is Helius's
            // own redelivery, absorbed by the signature unique index.
            "filed": filed,
            "received": total,
            "skipped": skipped,
        })),
        Ok(Ok(Err(error))) => refuse(HttpResponse::ServiceUnavailable(), &error),
        Ok(Err(_)) => refuse(HttpResponse::ServiceUnavailable(), "inbox writer dropped"),
        Err(_) => refuse(HttpResponse::ServiceUnavailable(), "inbox write timed out"),
    }
}

fn refuse(mut builder: actix_web::HttpResponseBuilder, reason: &str) -> HttpResponse {
    builder.json(serde_json::json!({ "status": "refused", "reason": reason }))
}

/// Projects a raw Helius delivery into rows, returning what could not be used.
///
/// The envelope is `getTransaction`-shaped, so `signature`, `slot`, `blockTime`
/// and `meta.err` all sit where the recovery path already expects them. A
/// single object is accepted as a one-element array: the docs say array, and
/// tolerating the other shape costs one line against a whole class of outage.
///
/// **Elements that do not project are skipped, not rejected.** A malformed
/// signature would fail the table's `CHECK (is_signature(...))`, turning the
/// whole batch into a 503 that Helius retries three times and then drops — so
/// refusing loses the good rows too. The caller logs the skip loudly instead.
pub fn project(body: &[u8]) -> Result<(Vec<Delivery>, usize), serde_json::Error> {
    let parsed: Value = serde_json::from_slice(body)?;
    let elements = match parsed {
        Value::Array(elements) => elements,
        object @ Value::Object(_) => vec![object],
        other => {
            // Provoke a typed error rather than inventing one.
            return serde_json::from_value::<Vec<Value>>(other).map(|v| (Vec::new(), v.len()));
        }
    };

    let mut deliveries = Vec::with_capacity(elements.len());
    let mut skipped = 0usize;
    for element in elements {
        let signature = element
            .pointer("/transaction/signatures/0")
            .and_then(Value::as_str);
        let slot = element.get("slot").and_then(Value::as_i64);
        let (Some(signature), Some(slot)) = (signature, slot) else {
            skipped += 1;
            continue;
        };
        deliveries.push(Delivery {
            signature: signature.to_string(),
            slot,
            block_time: element
                .get("blockTime")
                .and_then(Value::as_i64)
                .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0)),
            // Recorded rather than discarded: the drain skips these without
            // spending a `getTransaction`, and the row is still the forensic
            // record that the delivery happened.
            failed: element
                .pointer("/meta/err")
                .is_some_and(|err| !err.is_null()),
            body: element,
        });
    }
    Ok((deliveries, skipped))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_secret_compares_whole_header_values() {
        let secret = WebhookSecret::new("Bearer s3cr3t");
        assert!(secret.matches("Bearer s3cr3t"));
        // Helius echoes the configured value verbatim, so there is no prefix
        // to strip and a partial match is not a match.
        assert!(!secret.matches("s3cr3t"));
        assert!(!secret.matches("Bearer s3cr3T"));
        assert!(!secret.matches(""));
    }

    #[test]
    fn debug_never_prints_the_secret() {
        let rendered = format!("{:?}", WebhookSecret::new("hunter2"));
        assert!(!rendered.contains("hunter2"), "leaked: {rendered}");
    }
}
