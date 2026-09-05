//! Durable cursors: the live-stream slot checkpoint (`ingest_state`) and the
//! per-collection backfill cursors (`backfill_state`).
//!
//! Contract (see `crates/ingest`): the consumer persists `last_processed_slot`
//! ONLY on `SlotCheckpoint` and resumes with the inclusive `ResumeFrom::Slot`,
//! so delivery is at-least-once and every write path must be idempotent.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgExecutor};

fn slot_to_db(slot: u64) -> sqlx::Result<i64> {
    i64::try_from(slot).map_err(|e| sqlx::Error::Encode(Box::new(e)))
}

pub async fn last_processed_slot<'e>(
    exec: impl PgExecutor<'e>,
    stream: &str,
) -> sqlx::Result<Option<u64>> {
    let slot: Option<i64> =
        sqlx::query_scalar("SELECT last_processed_slot FROM ingest_state WHERE stream = $1")
            .bind(stream)
            .fetch_optional(exec)
            .await?;
    Ok(slot.map(|s| s as u64))
}

/// Monotonic checkpoint: a stale writer can never move the cursor backwards.
pub async fn checkpoint<'e>(
    exec: impl PgExecutor<'e>,
    stream: &str,
    slot: u64,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO ingest_state (stream, last_processed_slot) VALUES ($1, $2) \
         ON CONFLICT (stream) DO UPDATE \
            SET last_processed_slot = GREATEST(ingest_state.last_processed_slot, EXCLUDED.last_processed_slot), \
                updated_at = now()",
    )
    .bind(stream)
    .bind(slot_to_db(slot)?)
    .execute(exec)
    .await?;
    Ok(())
}

/// Deliberate rewind (targeted re-backfill after an outage beyond the replay
/// window). The only way the cursor moves backwards.
pub async fn reset<'e>(exec: impl PgExecutor<'e>, stream: &str, slot: u64) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO ingest_state (stream, last_processed_slot) VALUES ($1, $2) \
         ON CONFLICT (stream) DO UPDATE \
            SET last_processed_slot = EXCLUDED.last_processed_slot, updated_at = now()",
    )
    .bind(stream)
    .bind(slot_to_db(slot)?)
    .execute(exec)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct BackfillState {
    pub collection_id: i32,
    pub kind: String,
    pub status: String,
    pub cursor: serde_json::Value,
    pub progress: serde_json::Value,
    pub last_error: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

pub async fn backfill_state<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
    kind: &str,
) -> sqlx::Result<Option<BackfillState>> {
    sqlx::query_as::<_, BackfillState>(
        "SELECT collection_id, kind, status, cursor, progress, last_error, started_at, finished_at, updated_at \
           FROM backfill_state WHERE collection_id = $1 AND kind = $2",
    )
    .bind(collection_id)
    .bind(kind)
    .fetch_optional(exec)
    .await
}

/// Upserts every column except `updated_at` (always `now()`).
pub async fn put_backfill_state<'e>(
    exec: impl PgExecutor<'e>,
    state: &BackfillState,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO backfill_state \
            (collection_id, kind, status, cursor, progress, last_error, started_at, finished_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (collection_id, kind) DO UPDATE \
            SET status = EXCLUDED.status, cursor = EXCLUDED.cursor, progress = EXCLUDED.progress, \
                last_error = EXCLUDED.last_error, started_at = EXCLUDED.started_at, \
                finished_at = EXCLUDED.finished_at, updated_at = now()",
    )
    .bind(state.collection_id)
    .bind(&state.kind)
    .bind(&state.status)
    .bind(&state.cursor)
    .bind(&state.progress)
    .bind(&state.last_error)
    .bind(state.started_at)
    .bind(state.finished_at)
    .execute(exec)
    .await?;
    Ok(())
}

/// When every collection last finished a pass of this `kind`.
///
/// `None` means at least one enabled collection has never finished one — a
/// fresh database, a newly added collection, or a run that failed — and the
/// caller should treat the job as due. Taking the **minimum** rather than the
/// maximum is what makes a single lagging collection re-trigger the job
/// instead of hiding behind its siblings.
pub async fn last_finished<'e>(
    exec: impl PgExecutor<'e>,
    kind: &str,
) -> sqlx::Result<Option<DateTime<Utc>>> {
    let finished: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT min(s.finished_at) FROM collections c \
           LEFT JOIN backfill_state s ON s.collection_id = c.id AND s.kind = $1 \
          WHERE c.enabled",
    )
    .bind(kind)
    .fetch_one(exec)
    .await?;
    Ok(finished)
}

/// Marks a periodic job as "last finished now" for every enabled collection
/// that has no record of it, and reports how many rows it seeded.
///
/// Without this a job whose row does not exist yet is due immediately, so the
/// first tick after a fresh deploy runs it regardless of its configured
/// interval — which for the weekly deep pass means a full document re-fetch a
/// minute after boot. Seeding makes the first run land one interval out, which
/// is what the interval says. Existing rows are never touched, so this cannot
/// delay a job that is genuinely overdue.
pub async fn seed_schedule<'e>(exec: impl PgExecutor<'e>, kind: &str) -> sqlx::Result<u64> {
    let done = sqlx::query(
        "INSERT INTO backfill_state (collection_id, kind, status, finished_at) \
         SELECT c.id, $1, 'idle', now() FROM collections c WHERE c.enabled \
         ON CONFLICT (collection_id, kind) DO NOTHING",
    )
    .bind(kind)
    .execute(exec)
    .await?;
    Ok(done.rows_affected())
}

/// The highest slot any DAS backfill recorded, used to seed a live cursor on a
/// database that has never checkpointed — so the first reconciliation covers
/// "since the backfill ran" rather than all of history.
pub async fn backfilled_slot<'e>(exec: impl PgExecutor<'e>) -> sqlx::Result<Option<i64>> {
    sqlx::query_scalar(
        "SELECT max((progress->>'slot')::bigint) FROM backfill_state WHERE kind = 'das_assets'",
    )
    .fetch_one(exec)
    .await
}
