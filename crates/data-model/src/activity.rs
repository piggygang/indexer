//! Activity and ownership-interval writer — the live pipeline's whole database
//! surface (ALG-623), and the same entry point ALG-622 (`source = 'backfill'`)
//! and ALG-624 (`'reconcile'`) will call.
//!
//! This is an implementation of the writer contract stated once in
//! `migrations/20260829000400_activity_ownership.sql`: lock the asset, insert
//! the activity row with `ON CONFLICT DO NOTHING … RETURNING`, and mutate
//! owner/history **only when a row came back** — so at-least-once redelivery
//! does nothing twice. An event older than the asset's frontier is stored but
//! not applied; it sets `assets.ownership_dirty` for the per-asset rebuild.
//!
//! Three things the SQL deliberately does not do:
//!
//! - **It never writes `assets.last_activity_slot`/`last_activity_at`.** The
//!   `activity_touch_assets` statement trigger owns them. Note the trigger
//!   still fires when `ON CONFLICT DO NOTHING` inserts nothing, but with an
//!   empty transition table — which is what makes a redelivery a true no-op
//!   all the way down.
//! - **It never opens a synthetic interval for an asset that has none.**
//!   `assets.owner_slot` records when the backfill *looked*, not when the
//!   holder acquired the asset, and `ownership_history.from_ts` is NOT NULL
//!   and unobtainable for a 2021 slot. Opening one would make
//!   `/nfts/{id}/owners` claim a pig held for years was acquired on backfill
//!   day. `opened_by IS NULL` stays reserved for ALG-622's archival crawl,
//!   which has the real first signature and can date it honestly. Live history
//!   therefore starts at the first live event, and the API's `heldSince`
//!   is null until then — exactly what the frozen contract promises.
//! - **It never writes `price_lamports` or `marketplace`.** Detecting a sale's
//!   price is ALG-622's job; `activity_price_only_sale` forbids them on every
//!   kind this writer emits, and inventing a price to satisfy
//!   `activity_sale_has_price` would be worse than recording an honest
//!   transfer. The invoked program ids go in `details` so ALG-622 can
//!   reclassify without re-fetching.
//!
//! One operational note: `ownership_no_overlap` is `DEFERRABLE INITIALLY
//! DEFERRED`, so an overlap surfaces as SQLSTATE 23P01 **at `commit()`**, not
//! at the offending statement. Callers must handle a failing commit; see
//! [`is_retryable_conflict`].

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgExecutor, Postgres, Transaction};

use crate::types::EventKind;

/// Just enough of an asset to route an event to it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct AssetRef {
    pub id: i64,
    pub address: String,
    pub collection_id: i32,
    pub owner: Option<String>,
    pub owner_slot: Option<i64>,
    pub burned: bool,
}

/// Resolves addresses seen on chain to assets we track. Addresses we do not
/// know are simply absent — an `activity` row for an unknown asset is
/// impossible anyway (`asset_id` is a FK), and inventing the asset would
/// bypass the registry's membership rules.
///
/// Restricted to enabled collections so a disabled collection stops producing
/// activity without any pipeline change.
pub async fn assets_by_address<'e>(
    exec: impl PgExecutor<'e>,
    addresses: &[String],
) -> sqlx::Result<Vec<AssetRef>> {
    sqlx::query_as(
        "SELECT a.id, a.address, a.collection_id, a.owner, a.owner_slot, a.burned \
           FROM assets a \
           JOIN collections c ON c.id = a.collection_id \
          WHERE a.address = ANY($1::text[]) AND c.enabled",
    )
    .bind(addresses)
    .fetch_all(exec)
    .await
}

/// One classified on-chain event, ready to write.
#[derive(Debug, Clone)]
pub struct LiveEvent<'a> {
    pub asset_id: i64,
    pub collection_id: i32,
    pub signature: &'a str,
    /// Ordinal of this asset's events within the transaction, in instruction
    /// order. 0 normally.
    pub seq: i16,
    pub slot: i64,
    pub block_time: DateTime<Utc>,
    pub kind: EventKind,
    pub from_owner: Option<&'a str>,
    pub to_owner: Option<&'a str>,
    /// Classifier extras — program ids, instruction name. Never load-bearing.
    pub details: Option<&'a Value>,
    /// Sale price. `activity_sale_has_price` requires it on every `sale` and
    /// `activity_price_only_sale` forbids it on every other kind, so a
    /// marketplace transfer nobody could price stays an honest `transfer`.
    pub price_lamports: Option<i64>,
    /// Free-text venue, and only ever on a priced sale. `None` on a sale we
    /// could not attribute — which the frozen contract explicitly permits.
    pub marketplace: Option<&'a str>,
    /// `'live' | 'backfill' | 'reconcile' | 'manual'`.
    pub source: &'a str,
}

/// What [`record`] actually did. `activity_id: None` means the event was
/// already stored — a redelivery, and nothing else ran.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Applied {
    pub activity_id: Option<i64>,
    pub opened: bool,
    pub closed: bool,
    pub owner_moved: bool,
    /// The event was stored but not applied: it predates the asset's frontier,
    /// or its `from_owner` disagrees with the open interval. Queued for
    /// [`rebuild_ownership`].
    pub dirty: bool,
}

impl Applied {
    /// Was this a redelivery? Backs the idempotency assertions.
    pub fn is_redelivery(&self) -> bool {
        self.activity_id.is_none()
    }
}

#[derive(sqlx::FromRow)]
struct Frontier {
    burned: bool,
    open_id: Option<i64>,
    open_from: Option<i64>,
    open_owner: Option<String>,
}

/// Records one event for one asset, applying the ownership change when the
/// event is at or after the asset's frontier.
///
/// Runs inside the caller's transaction and locks exactly one asset, so two
/// events for different assets can never deadlock against each other.
pub async fn record(
    tx: &mut Transaction<'_, Postgres>,
    event: &LiveEvent<'_>,
) -> sqlx::Result<Applied> {
    // Lock the asset and read its ownership frontier in one round trip.
    // `FOR UPDATE OF a` rather than a bare `FOR UPDATE`: Postgres refuses to
    // lock the nullable side of an outer join, and only `assets` should be
    // locked anyway. `ownership_no_overlap` guarantees at most one open
    // interval, so this stays single-row.
    let frontier: Frontier = sqlx::query_as(
        "SELECT a.burned, \
                h.id AS open_id, h.from_slot AS open_from, h.owner AS open_owner \
           FROM assets a \
           LEFT JOIN ownership_history h ON h.asset_id = a.id AND h.to_slot IS NULL \
          WHERE a.id = $1 \
            FOR UPDATE OF a",
    )
    .bind(event.asset_id)
    .fetch_one(&mut **tx)
    .await?;

    // The idempotency gate. Everything below runs only when this inserted.
    let activity_id: Option<i64> = sqlx::query_scalar(
        "INSERT INTO activity \
            (asset_id, collection_id, signature, seq, slot, block_time, kind, \
             from_owner, to_owner, price_lamports, marketplace, details, source) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
         ON CONFLICT (asset_id, signature, seq) DO NOTHING \
         RETURNING id",
    )
    .bind(event.asset_id)
    .bind(event.collection_id)
    .bind(event.signature)
    .bind(event.seq)
    .bind(event.slot)
    .bind(event.block_time)
    .bind(event.kind)
    .bind(event.from_owner)
    .bind(event.to_owner)
    .bind(event.price_lamports)
    .bind(event.marketplace)
    .bind(event.details)
    .bind(event.source)
    .fetch_optional(&mut **tx)
    .await?;

    let Some(activity_id) = activity_id else {
        return Ok(Applied::default());
    };

    let mut applied = Applied {
        activity_id: Some(activity_id),
        ..Applied::default()
    };

    // Kinds that carry no ownership meaning are stored and nothing else.
    if !matches!(
        event.kind,
        EventKind::Mint | EventKind::Transfer | EventKind::Sale | EventKind::Burn
    ) {
        return Ok(applied);
    }

    // An asset with no history is never "out of order": opening [slot, ∞)
    // where nothing exists cannot overlap. Only an event that predates an
    // existing open interval is.
    //
    // A burn is terminal, so anything but another burn arriving afterwards is
    // an event we are seeing late — applying it would leave an open interval
    // on a burned asset, which `assets_burned_has_no_owner` exists to prevent.
    let in_order = frontier.open_from.is_none_or(|from| event.slot >= from)
        && (!frontier.burned || event.kind == EventKind::Burn);

    // A disagreement here means we missed an intermediate event — applying
    // anyway would manufacture a false interval. This is the check that turns
    // a silent gap into a flagged one at write time.
    let sender_matches = match (event.from_owner, frontier.open_owner.as_deref()) {
        (Some(from), Some(open)) => from == open,
        _ => true,
    };

    if !in_order || !sender_matches {
        let changed = sqlx::query(
            "UPDATE assets SET ownership_dirty = true WHERE id = $1 AND NOT ownership_dirty",
        )
        .bind(event.asset_id)
        .execute(&mut **tx)
        .await?;
        let _ = changed;
        applied.dirty = true;
        return Ok(applied);
    }

    // Close the open interval. Ordering against the insert below is free
    // precisely because the exclusion constraint is deferred to commit.
    if frontier.open_id.is_some() {
        let closed = sqlx::query(
            "UPDATE ownership_history \
                SET to_slot = $2, to_ts = $3, closed_by = $4 \
              WHERE asset_id = $1 AND to_slot IS NULL AND from_slot <= $2",
        )
        .bind(event.asset_id)
        .bind(event.slot)
        .bind(event.block_time)
        .bind(activity_id)
        .execute(&mut **tx)
        .await?;
        applied.closed = closed.rows_affected() > 0;
    }

    if let Some(to_owner) = event.to_owner {
        // A same-slot hand-off yields the empty range [slot, slot), which the
        // schema explicitly blesses.
        sqlx::query(
            "INSERT INTO ownership_history \
                (asset_id, owner, from_slot, from_ts, opened_by, source) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(event.asset_id)
        .bind(to_owner)
        .bind(event.slot)
        .bind(event.block_time)
        .bind(activity_id)
        .bind(event.source)
        .execute(&mut **tx)
        .await?;
        applied.opened = true;
    }

    if event.kind == EventKind::Burn {
        // Both columns move together, satisfying assets_burned_has_no_owner
        // and assets_owner_has_slot. Burning is irreversible, so no slot guard.
        let moved = sqlx::query(
            "UPDATE assets \
                SET burned = true, owner = NULL, owner_slot = NULL \
              WHERE id = $1 \
                AND (burned, owner, owner_slot) \
                 IS DISTINCT FROM (true, NULL::text, NULL::bigint)",
        )
        .bind(event.asset_id)
        .execute(&mut **tx)
        .await?;
        applied.owner_moved = moved.rows_affected() > 0;
    } else if let Some(to_owner) = event.to_owner {
        // Forward-only, and compares the OWNER not the slot — the same choice
        // `assets::UPSERT_ASSETS` makes, so a re-observation of an unchanged
        // owner never fires `assets_updated_at`.
        let moved = sqlx::query(
            "UPDATE assets \
                SET owner = $2, owner_slot = $3 \
              WHERE id = $1 AND NOT burned \
                AND (owner_slot IS NULL OR $3 > owner_slot) \
                AND owner IS DISTINCT FROM $2",
        )
        .bind(event.asset_id)
        .bind(to_owner)
        .bind(event.slot)
        .execute(&mut **tx)
        .await?;
        applied.owner_moved = moved.rows_affected() > 0;
    }

    Ok(applied)
}

/// Stores a signature we could not classify — the migration's rule that "a
/// signature whose block_time cannot be resolved stays unclassified". The row
/// has `classified_at IS NULL`, which is exactly how `asset_signatures`
/// documents "pending", so ALG-622's crawl picks it up instead of the event
/// being dropped.
pub async fn park_signature<'e>(
    exec: impl PgExecutor<'e>,
    asset_id: i64,
    signature: &str,
    slot: i64,
    failed: bool,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO asset_signatures (asset_id, signature, slot, failed) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (asset_id, signature) DO NOTHING",
    )
    .bind(asset_id)
    .bind(signature)
    .bind(slot)
    .bind(failed)
    .execute(exec)
    .await?;
    Ok(())
}

/// One row for `asset_signatures`, as the archival crawl produces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrawledSignature {
    pub signature: String,
    pub slot: i64,
    /// The archival response carries `blockTime`, so unlike the live path
    /// this is almost always known.
    pub block_time: Option<DateTime<Utc>>,
    pub failed: bool,
}

/// Stores the raw signatures a crawl saw for one asset.
///
/// This is what `asset_signatures` is for: the crawl's durable output, so
/// reclassification never re-fetches. Re-crawling is a no-op except that a
/// row parked by the live pipeline (which has no `blockTime` to give) gains
/// one — that single `WHERE` is what keeps "re-running changes nothing" true.
pub async fn record_signatures<'e>(
    exec: impl PgExecutor<'e>,
    asset_id: i64,
    rows: &[CrawledSignature],
) -> sqlx::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let signatures: Vec<&str> = rows.iter().map(|r| r.signature.as_str()).collect();
    let slots: Vec<i64> = rows.iter().map(|r| r.slot).collect();
    let block_times: Vec<Option<DateTime<Utc>>> = rows.iter().map(|r| r.block_time).collect();
    let failed: Vec<bool> = rows.iter().map(|r| r.failed).collect();

    let done = sqlx::query(
        "INSERT INTO asset_signatures (asset_id, signature, slot, block_time, failed) \
         SELECT $1, s, l, t, f \
           FROM unnest($2::text[], $3::bigint[], $4::timestamptz[], $5::boolean[]) \
                AS batch(s, l, t, f) \
         ON CONFLICT (asset_id, signature) DO UPDATE \
            SET block_time = EXCLUDED.block_time \
          WHERE asset_signatures.block_time IS NULL \
            AND EXCLUDED.block_time IS NOT NULL",
    )
    .bind(asset_id)
    .bind(&signatures)
    .bind(&slots)
    .bind(&block_times)
    .bind(&failed)
    .execute(exec)
    .await?;
    Ok(done.rows_affected())
}

/// Marks signatures as classified. `classified_at IS NULL` is how
/// `asset_signatures` documents "pending", and until now nothing ever cleared
/// it — the partial index had no consumer.
pub async fn mark_classified<'e>(
    exec: impl PgExecutor<'e>,
    asset_id: i64,
    signatures: &[String],
) -> sqlx::Result<u64> {
    if signatures.is_empty() {
        return Ok(0);
    }
    let done = sqlx::query(
        "UPDATE asset_signatures SET classified_at = now() \
          WHERE asset_id = $1 AND signature = ANY($2) AND classified_at IS NULL",
    )
    .bind(asset_id)
    .bind(signatures)
    .execute(exec)
    .await?;
    Ok(done.rows_affected())
}

/// Signatures this asset has stored but never classified, oldest first.
pub async fn pending_signatures<'e>(
    exec: impl PgExecutor<'e>,
    asset_id: i64,
) -> sqlx::Result<Vec<CrawledSignature>> {
    sqlx::query_as(
        "SELECT signature, slot, block_time, failed FROM asset_signatures \
          WHERE asset_id = $1 AND classified_at IS NULL ORDER BY slot, signature",
    )
    .bind(asset_id)
    .fetch_all(exec)
    .await
    .map(|rows: Vec<(String, i64, Option<DateTime<Utc>>, bool)>| {
        rows.into_iter()
            .map(|(signature, slot, block_time, failed)| CrawledSignature {
                signature,
                slot,
                block_time,
                failed,
            })
            .collect()
    })
}

/// Throws away everything derived for one asset so it can be re-derived from
/// the stored signatures.
///
/// The migration describes reclassification as "DELETE the (asset, signature)
/// rows + re-insert, then rebuild". Doing it per signature leaves the asset's
/// intervals half-derived from the old classification, so the whole-asset form
/// is the honest one: intervals, activity, the activity-derived `assets`
/// columns and the `classified_at` marks all go together, and the next crawl
/// pass rebuilds them in slot order. The raw `asset_signatures` rows survive —
/// that is the point of storing them.
pub async fn reset_for_reclassify(
    tx: &mut Transaction<'_, Postgres>,
    asset_id: i64,
) -> sqlx::Result<u64> {
    sqlx::query("DELETE FROM ownership_history WHERE asset_id = $1")
        .bind(asset_id)
        .execute(&mut **tx)
        .await?;
    let removed = sqlx::query("DELETE FROM activity WHERE asset_id = $1")
        .bind(asset_id)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    // `activity_touch_assets` is an INSERT trigger, so a delete never moves
    // these back; clearing them lets the re-insert set them afresh.
    sqlx::query(
        "UPDATE assets \
            SET last_activity_slot = NULL, last_activity_at = NULL, ownership_dirty = false \
          WHERE id = $1",
    )
    .bind(asset_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query("UPDATE asset_signatures SET classified_at = NULL WHERE asset_id = $1")
        .bind(asset_id)
        .execute(&mut **tx)
        .await?;
    Ok(removed)
}

/// Recomputes `assets.last_activity_*` from the surviving activity rows.
///
/// The `activity_touch_assets` trigger is monotonic on inserts only, which the
/// migration flags: "a reclassification that deletes rows recomputes
/// explicitly (ALG-622)". This is that recompute, for a caller that deleted
/// some of an asset's events but not all of them.
pub async fn recompute_last_activity<'e>(
    exec: impl PgExecutor<'e>,
    asset_id: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE assets a \
            SET last_activity_slot = n.slot, last_activity_at = n.block_time \
           FROM (SELECT max(slot) AS slot, max(block_time) AS block_time \
                   FROM activity WHERE asset_id = $1) n \
          WHERE a.id = $1 \
            AND (a.last_activity_slot, a.last_activity_at) \
                IS DISTINCT FROM (n.slot, n.block_time)",
    )
    .bind(asset_id)
    .execute(exec)
    .await?;
    Ok(())
}

/// A stored `transfer` that carries everything needed to become a `sale`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepriceCandidate {
    pub id: i64,
    pub asset_id: i64,
    /// `details`, which carries the invoked program ids and the price the
    /// classifier derived at crawl time.
    pub details: Value,
}

/// Stored transfers a new venue could turn into sales, keyset-paged.
///
/// This is what makes reclassification a database-only pass: only signatures
/// are stored, never transaction bodies, so without the price the classifier
/// already computed, teaching the registry a new marketplace would mean
/// crawling the chain a second time.
pub async fn reprice_candidates<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
    after_id: i64,
    limit: i64,
) -> sqlx::Result<Vec<RepriceCandidate>> {
    sqlx::query_as(
        "SELECT id, asset_id, details FROM activity \
          WHERE collection_id = $1 AND id > $2 AND kind = 'transfer' \
            AND details ? 'price_candidate' \
          ORDER BY id LIMIT $3",
    )
    .bind(collection_id)
    .bind(after_id)
    .bind(limit)
    .fetch_all(exec)
    .await
    .map(|rows: Vec<(i64, i64, Value)>| {
        rows.into_iter()
            .map(|(id, asset_id, details)| RepriceCandidate {
                id,
                asset_id,
                details,
            })
            .collect()
    })
}

/// Promotes one stored transfer to a priced sale.
///
/// Ownership is untouched: a `sale` and a `transfer` carry the same sender and
/// receiver and are treated identically by `rebuild_ownership`, so re-labelling
/// one never needs the intervals rebuilt. The guard keeps a re-run a true
/// no-op.
pub async fn promote_to_sale<'e>(
    exec: impl PgExecutor<'e>,
    activity_id: i64,
    price_lamports: i64,
    marketplace: Option<&str>,
) -> sqlx::Result<bool> {
    let done = sqlx::query(
        "UPDATE activity \
            SET kind = 'sale', price_lamports = $2, marketplace = $3 \
          WHERE id = $1 AND kind = 'transfer' \
            AND (price_lamports, marketplace) IS DISTINCT FROM ($2, $3)",
    )
    .bind(activity_id)
    .bind(price_lamports)
    .bind(marketplace)
    .execute(exec)
    .await?;
    Ok(done.rows_affected() > 0)
}

/// Does the asset's derived history agree with its observed owner?
///
/// Asks `integrity_owner_mismatch` about one asset, so a backfill's own
/// self-check and the acceptance query (`SELECT count(*) FROM
/// integrity_owner_mismatch`) can never drift apart. False only when the asset
/// has history *and* its open interval disagrees — an asset with no history
/// yet is not a disagreement.
pub async fn owner_agrees<'e>(exec: impl PgExecutor<'e>, asset_id: i64) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT NOT EXISTS (SELECT 1 FROM integrity_owner_mismatch WHERE asset_id = $1)",
    )
    .bind(asset_id)
    .fetch_one(exec)
    .await
}

/// One keyset page of a collection's assets, for a per-asset pass.
///
/// `assets_in_collection` loads the whole collection at once, which is right
/// for a reconnect sweep and wrong for a crawl that commits a cursor every
/// batch.
pub async fn assets_after<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
    after_id: i64,
    limit: i64,
) -> sqlx::Result<Vec<AssetRef>> {
    sqlx::query_as(
        "SELECT id, address, collection_id, owner, owner_slot, burned FROM assets \
          WHERE collection_id = $1 AND id > $2 AND membership_status = 'member' \
          ORDER BY id LIMIT $3",
    )
    .bind(collection_id)
    .bind(after_id)
    .bind(limit)
    .fetch_all(exec)
    .await
}

/// Assets whose history could not be applied in order, oldest flag first.
pub async fn dirty_assets<'e>(
    exec: impl PgExecutor<'e>,
    limit: i64,
) -> sqlx::Result<Vec<AssetRef>> {
    sqlx::query_as(
        "SELECT a.id, a.address, a.collection_id, a.owner, a.owner_slot, a.burned \
           FROM assets a WHERE a.ownership_dirty ORDER BY a.id LIMIT $1",
    )
    .bind(limit)
    .fetch_all(exec)
    .await
}

/// Flags an asset for the ownership rebuild without recording an event —
/// used when a commit-time conflict persists past a retry.
pub async fn mark_dirty<'e>(exec: impl PgExecutor<'e>, asset_id: i64) -> sqlx::Result<()> {
    sqlx::query("UPDATE assets SET ownership_dirty = true WHERE id = $1 AND NOT ownership_dirty")
        .bind(asset_id)
        .execute(exec)
        .await?;
    Ok(())
}

/// Every asset of one collection, for the reconciliation sweep's diff.
pub async fn assets_in_collection<'e>(
    exec: impl PgExecutor<'e>,
    collection_id: i32,
) -> sqlx::Result<Vec<AssetRef>> {
    sqlx::query_as(
        "SELECT id, address, collection_id, owner, owner_slot, burned \
           FROM assets WHERE collection_id = $1",
    )
    .bind(collection_id)
    .fetch_all(exec)
    .await
}

pub async fn dirty_count<'e>(exec: impl PgExecutor<'e>) -> sqlx::Result<i64> {
    sqlx::query_scalar("SELECT count(*)::bigint FROM assets WHERE ownership_dirty")
        .fetch_one(exec)
        .await
}

#[derive(sqlx::FromRow)]
struct HistoryEvent {
    id: i64,
    slot: i64,
    block_time: DateTime<Utc>,
    kind: String,
    to_owner: Option<String>,
}

/// What one [`rebuild_ownership`] produced.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Rebuilt {
    pub events: u64,
    pub intervals: u64,
    pub was_dirty: bool,
}

/// Re-derives an asset's ownership intervals from its stored activity, in
/// `(slot, seq)` order, and clears `ownership_dirty`.
///
/// This is the repair half of the writer contract — the migration describes it
/// as "delete the asset's intervals, re-derive from activity ordered by slot,
/// seq". ALG-622 still owns *classification* of historical signatures; this
/// only rebuilds from what is already classified, which is why it is safe to
/// run before that issue exists. Without it `ownership_dirty` would be a
/// write-only flag with no consumer.
pub async fn rebuild_ownership(
    tx: &mut Transaction<'_, Postgres>,
    asset_id: i64,
) -> sqlx::Result<Rebuilt> {
    let was_dirty: bool =
        sqlx::query_scalar("SELECT ownership_dirty FROM assets WHERE id = $1 FOR UPDATE")
            .bind(asset_id)
            .fetch_one(&mut **tx)
            .await?;

    sqlx::query("DELETE FROM ownership_history WHERE asset_id = $1")
        .bind(asset_id)
        .execute(&mut **tx)
        .await?;

    let events: Vec<HistoryEvent> = sqlx::query_as(
        "SELECT id, slot, block_time, kind, to_owner \
           FROM activity \
          WHERE asset_id = $1 AND kind IN ('mint', 'transfer', 'sale', 'burn') \
          ORDER BY slot, seq, id",
    )
    .bind(asset_id)
    .fetch_all(&mut **tx)
    .await?;

    // Walk the timeline, closing the previous interval at each ownership
    // change. `open` carries the row we have not written yet, so a burn or the
    // next transfer can close it before it is inserted.
    let mut open: Option<(String, i64, DateTime<Utc>, i64)> = None;
    let mut intervals = 0u64;
    let mut burned = false;

    for event in &events {
        let closes_at = (event.slot, event.block_time, event.id);
        if let Some((owner, from_slot, from_ts, opened_by)) = open.take() {
            sqlx::query(
                "INSERT INTO ownership_history \
                    (asset_id, owner, from_slot, from_ts, to_slot, to_ts, opened_by, closed_by, source) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'reconcile')",
            )
            .bind(asset_id)
            .bind(&owner)
            .bind(from_slot)
            .bind(from_ts)
            .bind(closes_at.0)
            .bind(closes_at.1)
            .bind(opened_by)
            .bind(closes_at.2)
            .execute(&mut **tx)
            .await?;
            intervals += 1;
        }

        burned = event.kind == "burn";
        if let Some(to_owner) = event.to_owner.clone() {
            open = Some((to_owner, event.slot, event.block_time, event.id));
        }
    }

    if let Some((owner, from_slot, from_ts, opened_by)) = open {
        sqlx::query(
            "INSERT INTO ownership_history \
                (asset_id, owner, from_slot, from_ts, opened_by, source) \
             VALUES ($1, $2, $3, $4, $5, 'reconcile')",
        )
        .bind(asset_id)
        .bind(&owner)
        .bind(from_slot)
        .bind(from_ts)
        .bind(opened_by)
        .execute(&mut **tx)
        .await?;
        intervals += 1;

        if !burned {
            sqlx::query(
                "UPDATE assets SET owner = $2, owner_slot = $3 \
                  WHERE id = $1 AND NOT burned AND owner IS DISTINCT FROM $2",
            )
            .bind(asset_id)
            .bind(&owner)
            .bind(from_slot)
            .execute(&mut **tx)
            .await?;
        }
    }

    sqlx::query("UPDATE assets SET ownership_dirty = false WHERE id = $1 AND ownership_dirty")
        .bind(asset_id)
        .execute(&mut **tx)
        .await?;

    Ok(Rebuilt {
        events: events.len() as u64,
        intervals,
        was_dirty,
    })
}

/// Is this a commit-time conflict the caller should retry once before giving
/// up and flagging the asset?
///
/// The deferred exclusion constraint means a genuine overlap does not surface
/// until `commit()`, where it is indistinguishable from a transport fault
/// unless the caller looks at the SQLSTATE. `23P01` is the exclusion
/// violation; `23505` covers the `UNIQUE(opened_by)`/`UNIQUE(closed_by)` races.
pub fn is_retryable_conflict(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|e| e.code())
        .is_some_and(|code| code == "23P01" || code == "23505")
}
