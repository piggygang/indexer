//! The Helius webhook transport's durable buffer.
//!
//! Two processes share this table and neither knows about the other: the api
//! service appends (it is the only one with a public domain and TLS, and
//! Helius gives an endpoint about a second to answer), and the ingester's
//! `WebhookInbox` drains it as an `IngestSource`.
//!
//! Contract (see `crates/ingest`): the consumer persists `last_processed_slot`
//! ONLY on `SlotCheckpoint`, and that checkpoint means "no events with a lower
//! slot will follow". [`watermark`] is what makes that claim true for a queue
//! whose deliveries arrive unordered — see its doc.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgExecutor};

/// One delivery, as the receiver files it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub signature: String,
    pub slot: i64,
    /// The payload's own `blockTime`, when it carried one. Saves the live path
    /// a `getBlockTime` — the call that silently dropped a real transfer on
    /// 2026-09-07 when a `confirmed`-fresh slot answered "Block not available".
    pub block_time: Option<DateTime<Utc>>,
    /// `meta.err` was set. Kept rather than discarded at the door so the
    /// forensic record is complete, but never fetched: the decoder drops
    /// failed transactions anyway.
    pub failed: bool,
    pub body: serde_json::Value,
}

/// A claimed row, ready to fetch and replay.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct Claimed {
    pub id: i64,
    pub signature: String,
    pub slot: i64,
    pub block_time: Option<DateTime<Utc>>,
    pub failed: bool,
    pub attempts: i16,
}

/// Files a batch of deliveries. Returns how many were new.
///
/// `ON CONFLICT DO NOTHING` on the signature is what makes Helius's redelivery
/// free — it warns that duplicates are expected, and this absorbs them the same
/// way `activity::record` absorbs them one layer down. A caller that needs to
/// know whether it saw something new reads the count; the receiver does not,
/// because a duplicate is still a 200.
pub async fn enqueue<'e>(exec: impl PgExecutor<'e>, deliveries: &[Delivery]) -> sqlx::Result<u64> {
    if deliveries.is_empty() {
        return Ok(0);
    }
    let signatures: Vec<&str> = deliveries.iter().map(|d| d.signature.as_str()).collect();
    let slots: Vec<i64> = deliveries.iter().map(|d| d.slot).collect();
    let block_times: Vec<Option<DateTime<Utc>>> = deliveries.iter().map(|d| d.block_time).collect();
    let failed: Vec<bool> = deliveries.iter().map(|d| d.failed).collect();
    let bodies: Vec<serde_json::Value> = deliveries.iter().map(|d| d.body.clone()).collect();

    let done = sqlx::query(
        "INSERT INTO webhook_inbox (signature, slot, block_time, failed, body) \
         SELECT * FROM unnest($1::text[], $2::bigint[], $3::timestamptz[], $4::boolean[], $5::jsonb[]) \
         ON CONFLICT (signature) DO NOTHING",
    )
    .bind(&signatures)
    .bind(&slots)
    .bind(&block_times)
    .bind(&failed)
    .bind(&bodies)
    .execute(exec)
    .await?;
    Ok(done.rows_affected())
}

/// Claims up to `limit` rows for processing.
///
/// Three guards, for three different failures:
///
/// * **`FOR UPDATE SKIP LOCKED`** — not for a second worker you plan to run,
///   but for the ~30 s during a Railway rolling deploy when the old and new
///   containers overlap. Without it both drain the same rows and one spends a
///   `getTransaction` per row producing nothing but redeliveries.
/// * **The `claimed_at` lease** — a process killed mid-batch leaves rows
///   claimed but unprocessed. They become claimable again after `lease`.
/// * **`attempts < max_attempts`** — a signature `getTransaction` never
///   returns would otherwise be re-claimed every lease forever *and* pin
///   [`watermark`] forever, freezing the cursor behind one bad row.
///
/// Claimed in arrival order (`id`), because slot order is not knowable until
/// the backlog is drained; the caller sorts the batch by slot before
/// processing, which is what removes most within-burst reordering.
pub async fn claim<'e>(
    exec: impl PgExecutor<'e>,
    limit: i64,
    lease: std::time::Duration,
    max_attempts: i16,
) -> sqlx::Result<Vec<Claimed>> {
    sqlx::query_as::<_, Claimed>(
        "UPDATE webhook_inbox SET claimed_at = now(), attempts = attempts + 1 \
          WHERE id IN ( \
            SELECT id FROM webhook_inbox \
             WHERE processed_at IS NULL \
               AND attempts < $3 \
               AND (claimed_at IS NULL OR claimed_at < now() - $2::interval) \
             ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED) \
        RETURNING id, signature, slot, block_time, failed, attempts",
    )
    .bind(limit)
    .bind(lease)
    .bind(max_attempts)
    .fetch_all(exec)
    .await
}

/// Retires a row. `error` retires it *unsuccessfully* — the row is done either
/// way, because a row that is never done pins the watermark.
pub async fn mark_processed<'e>(
    exec: impl PgExecutor<'e>,
    id: i64,
    error: Option<&str>,
) -> sqlx::Result<()> {
    sqlx::query("UPDATE webhook_inbox SET processed_at = now(), last_error = $2 WHERE id = $1")
        .bind(id)
        .bind(error)
        .execute(exec)
        .await?;
    Ok(())
}

/// Releases a claim without retiring the row, so the next poll re-claims it
/// immediately instead of waiting out the lease.
///
/// For the transient case — DAS was unreachable, the pool was exhausted. The
/// `attempts` increment from [`claim`] stands, so a row that only ever fails
/// still reaches the cap and retires.
pub async fn release<'e>(exec: impl PgExecutor<'e>, id: i64, error: &str) -> sqlx::Result<()> {
    sqlx::query("UPDATE webhook_inbox SET claimed_at = NULL, last_error = $2 WHERE id = $1")
        .bind(id)
        .bind(error)
        .execute(exec)
        .await?;
    Ok(())
}

/// Releases a claim *and refunds the attempt*, for a failure that says nothing
/// about the row.
///
/// [`release`] is for "this delivery failed": the `attempts` increment stands,
/// so a row that only ever fails reaches the cap and retires. A rate limit is
/// not that. Helius stops retrying a delivery after about three tries, so a
/// row retired here is an event lost for good — and being throttled is the one
/// failure guaranteed to hit every row in the batch equally, which would retire
/// the whole backlog in `WEBHOOK_MAX_ATTEMPTS` polls. Refunding keeps the cap
/// meaning "this signature is poison" rather than "Helius was busy".
pub async fn defer<'e>(exec: impl PgExecutor<'e>, id: i64, error: &str) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE webhook_inbox \
         SET claimed_at = NULL, attempts = greatest(attempts - 1, 0), last_error = $2 \
         WHERE id = $1",
    )
    .bind(id)
    .bind(error)
    .execute(exec)
    .await?;
    Ok(())
}

/// The highest slot below which nothing is still pending, held back by a grace
/// window to cover deliveries that have not arrived yet.
///
/// This is the whole adaptation that lets an unordered queue satisfy
/// `SlotCheckpoint`'s "no events with a lower slot will follow". Two terms:
///
/// * `min(pending) - 1` — never claim past a row we know we still owe. This is
///   why a stuck row *pins* the cursor, which is correct and is a monitorable
///   condition: `ingest_state.updated_at` advances while `last_processed_slot`
///   does not.
/// * `max(processed older than grace)` — the margin against the delivery we
///   cannot see at all, for a slot below the current maximum. Helius neither
///   orders webhooks nor documents when it gives up retrying.
///
/// `LEAST` ignores NULLs, so all four states fall out of one expression:
/// nothing pending yields the second term, nothing settled yields the first,
/// neither yields `NULL` — meaning *say nothing*, and the previous checkpoint
/// stands.
///
/// **Never the chain tip.** The drain holds a `DasClient` and `getSlot` is
/// right there; it is the one value guaranteed to be a lie about this stream.
pub async fn watermark<'e>(
    exec: impl PgExecutor<'e>,
    grace: std::time::Duration,
) -> sqlx::Result<Option<i64>> {
    sqlx::query_scalar(
        "SELECT least( \
            (SELECT min(slot) - 1 FROM webhook_inbox WHERE processed_at IS NULL), \
            (SELECT max(slot) FROM webhook_inbox \
              WHERE processed_at IS NOT NULL AND processed_at < now() - $1::interval))",
    )
    .bind(grace)
    .fetch_one(exec)
    .await
}

/// Drops retired rows older than `retain`.
///
/// Retention has to outlast the dual-run evaluation window: [`coverage`] reads
/// this table, so pruning early deletes the evidence the retirement decision
/// rests on.
pub async fn prune<'e>(
    exec: impl PgExecutor<'e>,
    retain: std::time::Duration,
) -> sqlx::Result<u64> {
    let done = sqlx::query(
        "DELETE FROM webhook_inbox \
          WHERE processed_at IS NOT NULL AND processed_at < now() - $1::interval",
    )
    .bind(retain)
    .execute(exec)
    .await?;
    Ok(done.rows_affected())
}

/// What the dual run is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Coverage {
    /// Events the WebSocket recorded that the webhook never even delivered.
    /// **This is the number that must reach zero before the WebSocket is
    /// retired.**
    pub missed_by_webhook: i64,
    /// Events the webhook lane wrote first.
    ///
    /// Deliberately *not* the criterion. The WebSocket wins nearly every race
    /// — it is sub-second, while this path is a poll plus a re-fetch — so this
    /// stays small even if webhooks are perfect. Coverage is the test; wins are
    /// a curiosity.
    pub written_by_webhook: i64,
    /// Deliveries still unprocessed. A number that only grows means the drain
    /// is not keeping up, or is stuck behind a poison row.
    pub pending: i64,
}

/// Compares the two transports over a window.
pub async fn coverage<'e>(
    exec: impl PgExecutor<'e>,
    window: std::time::Duration,
) -> sqlx::Result<Coverage> {
    let row: (i64, i64, i64) = sqlx::query_as(
        "SELECT \
           (SELECT count(*) FROM activity a \
             WHERE a.source = 'live' AND a.block_time > now() - $1::interval \
               AND NOT EXISTS (SELECT 1 FROM webhook_inbox w WHERE w.signature = a.signature)), \
           (SELECT count(*) FROM activity \
             WHERE source = 'webhook' AND block_time > now() - $1::interval), \
           (SELECT count(*) FROM webhook_inbox WHERE processed_at IS NULL)",
    )
    .bind(window)
    .fetch_one(exec)
    .await?;
    Ok(Coverage {
        missed_by_webhook: row.0,
        written_by_webhook: row.1,
        pending: row.2,
    })
}
