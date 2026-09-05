//! The ALG-623 writer contract, proven against Postgres with no network.
//! Every address is a synthetic base58 key (CLAUDE.md).
//! Ignored without a database: `cargo test --workspace -- --include-ignored`.

use chrono::{DateTime, TimeZone, Utc};
use indexer_data_model::activity::{self, Applied, CrawledSignature, LiveEvent};
use indexer_data_model::types::EventKind;
use indexer_data_model::PgPool;

const EXCLUSION_VIOLATION: &str = "23P01";

fn pk(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

fn sig(seed: u8) -> String {
    bs58::encode([seed; 64]).into_string()
}

fn ts(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
}

async fn collection(pool: &PgPool) -> i32 {
    sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, enabled) \
         VALUES ('c', 'C', 'token_metadata', $1, 'SYN', true) RETURNING id",
    )
    .bind(pk(200))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn asset(pool: &PgPool, collection_id: i32, seed: u8) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO assets (address, collection_id, name) VALUES ($1, $2, '#1') RETURNING id",
    )
    .bind(pk(seed))
    .bind(collection_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Writes one event in its own transaction, the way the live loop does.
#[allow(clippy::too_many_arguments)]
async fn write(
    pool: &PgPool,
    asset_id: i64,
    collection_id: i32,
    signature: &str,
    slot: i64,
    kind: EventKind,
    from: Option<&str>,
    to: Option<&str>,
) -> Applied {
    let mut tx = pool.begin().await.unwrap();
    let applied = activity::record(
        &mut tx,
        &LiveEvent {
            asset_id,
            collection_id,
            signature,
            seq: 0,
            slot,
            block_time: ts(slot),
            kind,
            from_owner: from,
            to_owner: to,
            price_lamports: None,
            marketplace: None,
            details: None,
            source: "live",
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    applied
}

async fn intervals(pool: &PgPool, asset_id: i64) -> Vec<(String, i64, Option<i64>)> {
    sqlx::query_as(
        "SELECT owner, from_slot, to_slot FROM ownership_history \
          WHERE asset_id = $1 ORDER BY from_slot",
    )
    .bind(asset_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn owner_of(pool: &PgPool, asset_id: i64) -> (Option<String>, Option<i64>, bool) {
    sqlx::query_as("SELECT owner, owner_slot, burned FROM assets WHERE id = $1")
        .bind(asset_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn is_dirty(pool: &PgPool, asset_id: i64) -> bool {
    sqlx::query_scalar("SELECT ownership_dirty FROM assets WHERE id = $1")
        .bind(asset_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn transfer_writes_activity_interval_and_owner(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;

    let applied = write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Transfer,
        None,
        Some(&pk(50)),
    )
    .await;
    assert!(applied.activity_id.is_some());
    assert!(applied.opened && applied.owner_moved);
    assert!(!applied.closed, "nothing to close on a first event");

    assert_eq!(intervals(&pool, a).await, vec![(pk(50), 100, None)]);
    assert_eq!(owner_of(&pool, a).await, (Some(pk(50)), Some(100), false));
}

/// At-least-once redelivery is the transport's contract, so the second write
/// must change nothing at all — not the activity row, not the interval, not
/// `assets.updated_at`.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn redelivery_is_a_true_no_op(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;
    write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Transfer,
        None,
        Some(&pk(50)),
    )
    .await;

    let before: DateTime<Utc> = sqlx::query_scalar("SELECT updated_at FROM assets WHERE id = $1")
        .bind(a)
        .fetch_one(&pool)
        .await
        .unwrap();

    let again = write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Transfer,
        None,
        Some(&pk(50)),
    )
    .await;
    assert!(again.is_redelivery());
    assert_eq!(again, Applied::default());

    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM activity WHERE asset_id = $1), \
                (SELECT count(*) FROM ownership_history WHERE asset_id = $1)",
    )
    .bind(a)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (1, 1));

    let after: DateTime<Utc> = sqlx::query_scalar("SELECT updated_at FROM assets WHERE id = $1")
        .bind(a)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(before, after, "a redelivery must not touch updated_at");
}

/// Three transfers open and close cleanly, and — the point of the test — the
/// transaction COMMITS, proving the deferred exclusion constraint tolerates
/// closing and opening in one unit.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn chained_transfers_leave_exactly_one_open_interval(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;

    write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Transfer,
        None,
        Some(&pk(50)),
    )
    .await;
    write(
        &pool,
        a,
        c,
        &sig(2),
        200,
        EventKind::Transfer,
        Some(&pk(50)),
        Some(&pk(51)),
    )
    .await;
    write(
        &pool,
        a,
        c,
        &sig(3),
        300,
        EventKind::Transfer,
        Some(&pk(51)),
        Some(&pk(52)),
    )
    .await;

    assert_eq!(
        intervals(&pool, a).await,
        vec![
            (pk(50), 100, Some(200)),
            (pk(51), 200, Some(300)),
            (pk(52), 300, None),
        ]
    );
    assert_eq!(owner_of(&pool, a).await.0, Some(pk(52)));

    let mismatches: i64 = sqlx::query_scalar("SELECT count(*) FROM integrity_owner_mismatch")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(mismatches, 0);
}

/// An event that predates the open interval is stored but never applied —
/// applying it would manufacture a false interval.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn an_out_of_order_event_is_stored_but_flagged(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;
    write(
        &pool,
        a,
        c,
        &sig(1),
        200,
        EventKind::Transfer,
        None,
        Some(&pk(50)),
    )
    .await;

    let late = write(
        &pool,
        a,
        c,
        &sig(2),
        100,
        EventKind::Transfer,
        Some(&pk(60)),
        Some(&pk(61)),
    )
    .await;
    assert!(late.activity_id.is_some(), "the event is still recorded");
    assert!(late.dirty);
    assert!(!late.opened && !late.closed && !late.owner_moved);

    assert_eq!(intervals(&pool, a).await, vec![(pk(50), 200, None)]);
    assert_eq!(owner_of(&pool, a).await.0, Some(pk(50)));
    assert!(is_dirty(&pool, a).await);
}

/// A sender that disagrees with the open interval means we missed an event.
/// Detecting that at write time is worth more than a plausible-looking chain.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn a_sender_mismatch_is_flagged_not_applied(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;
    write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Transfer,
        None,
        Some(&pk(50)),
    )
    .await;

    // Owner is pk(50), but this transfer claims to come from pk(99).
    let odd = write(
        &pool,
        a,
        c,
        &sig(2),
        200,
        EventKind::Transfer,
        Some(&pk(99)),
        Some(&pk(51)),
    )
    .await;
    assert!(odd.dirty);
    assert_eq!(intervals(&pool, a).await, vec![(pk(50), 100, None)]);
    assert!(is_dirty(&pool, a).await);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn burn_closes_the_interval_and_clears_the_owner(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;
    write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Transfer,
        None,
        Some(&pk(50)),
    )
    .await;

    let burn = write(
        &pool,
        a,
        c,
        &sig(2),
        200,
        EventKind::Burn,
        Some(&pk(50)),
        None,
    )
    .await;
    assert!(burn.closed && !burn.opened && burn.owner_moved);

    assert_eq!(intervals(&pool, a).await, vec![(pk(50), 100, Some(200))]);
    assert_eq!(owner_of(&pool, a).await, (None, None, true));
}

/// Burning is irreversible on chain, so a transfer arriving after a burn is an
/// event we are seeing late — never a resurrection.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn a_transfer_after_a_burn_never_resurrects_the_asset(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;
    write(&pool, a, c, &sig(1), 100, EventKind::Burn, None, None).await;

    let late = write(
        &pool,
        a,
        c,
        &sig(2),
        300,
        EventKind::Transfer,
        None,
        Some(&pk(51)),
    )
    .await;
    assert!(late.dirty, "stored, flagged, not applied");
    assert_eq!(owner_of(&pool, a).await, (None, None, true));
    assert!(intervals(&pool, a).await.is_empty());
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn a_mint_opens_without_closing(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;

    let mint = write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Mint,
        None,
        Some(&pk(50)),
    )
    .await;
    assert!(mint.opened && !mint.closed);
    assert_eq!(intervals(&pool, a).await, vec![(pk(50), 100, None)]);
}

/// The schema blesses `[slot, slot)` — an empty range — for a hand-off inside
/// one slot, which is what a marketplace sale routed through an escrow does.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn a_same_slot_handoff_is_legal(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;
    write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Transfer,
        None,
        Some(&pk(50)),
    )
    .await;
    write(
        &pool,
        a,
        c,
        &sig(2),
        100,
        EventKind::Transfer,
        Some(&pk(50)),
        Some(&pk(51)),
    )
    .await;

    assert_eq!(
        intervals(&pool, a).await,
        vec![(pk(50), 100, Some(100)), (pk(51), 100, None)]
    );
}

/// Kinds with no ownership meaning are stored and nothing else.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn a_non_ownership_kind_only_stores_the_row(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;
    write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Transfer,
        None,
        Some(&pk(50)),
    )
    .await;

    let other = write(&pool, a, c, &sig(2), 200, EventKind::Other, None, None).await;
    assert!(other.activity_id.is_some());
    assert!(!other.opened);
    assert!(!other.closed);
    assert!(!other.dirty);
    assert_eq!(intervals(&pool, a).await, vec![(pk(50), 100, None)]);
}

/// The writer must never set `last_activity_*` — the statement trigger owns
/// them — and yet they must end up correct.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn the_trigger_owns_last_activity(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;
    write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Transfer,
        None,
        Some(&pk(50)),
    )
    .await;
    write(
        &pool,
        a,
        c,
        &sig(2),
        500,
        EventKind::Transfer,
        Some(&pk(50)),
        Some(&pk(51)),
    )
    .await;
    // An older event must not move it backwards.
    write(&pool, a, c, &sig(3), 50, EventKind::Other, None, None).await;

    let (slot, at): (Option<i64>, Option<DateTime<Utc>>) =
        sqlx::query_as("SELECT last_activity_slot, last_activity_at FROM assets WHERE id = $1")
            .bind(a)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(slot, Some(500));
    assert!(at.is_some());
}

/// Proof the exclusion constraint is deferred: a genuine overlap surfaces at
/// COMMIT, not at the statement. The pipeline must therefore handle a failing
/// commit, which is what `is_retryable_conflict` exists for.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn an_overlap_fails_at_commit_not_at_the_statement(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;

    let mut tx = pool.begin().await.unwrap();
    for (slot, owner) in [(100i64, pk(50)), (200, pk(51))] {
        sqlx::query(
            "INSERT INTO ownership_history (asset_id, owner, from_slot, from_ts) \
             VALUES ($1, $2, $3, now())",
        )
        .bind(a)
        .bind(&owner)
        .bind(slot)
        .execute(&mut *tx)
        .await
        .expect("each statement succeeds; the overlap is only checked at commit");
    }

    let error = tx.commit().await.unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(|e| e.code())
            .map(|c| c.to_string())
            .unwrap_or_default(),
        EXCLUSION_VIOLATION
    );
    assert!(activity::is_retryable_conflict(&error));
}

/// The repair path: feed events out of order so the asset goes dirty, then
/// rebuild from stored activity and check the history is correct and the flag
/// cleared. Without this, `ownership_dirty` would be write-only.
#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn rebuild_repairs_an_out_of_order_asset(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;

    // Arrive newest-first: the 300 transfer lands, then the earlier two are
    // stored but refused.
    write(
        &pool,
        a,
        c,
        &sig(3),
        300,
        EventKind::Transfer,
        Some(&pk(51)),
        Some(&pk(52)),
    )
    .await;
    write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Mint,
        None,
        Some(&pk(50)),
    )
    .await;
    write(
        &pool,
        a,
        c,
        &sig(2),
        200,
        EventKind::Transfer,
        Some(&pk(50)),
        Some(&pk(51)),
    )
    .await;
    assert!(is_dirty(&pool, a).await);
    assert_eq!(intervals(&pool, a).await, vec![(pk(52), 300, None)]);

    let mut tx = pool.begin().await.unwrap();
    let rebuilt = activity::rebuild_ownership(&mut tx, a).await.unwrap();
    tx.commit().await.unwrap();

    assert!(rebuilt.was_dirty);
    assert_eq!(rebuilt.events, 3);
    assert_eq!(rebuilt.intervals, 3);
    assert_eq!(
        intervals(&pool, a).await,
        vec![
            (pk(50), 100, Some(200)),
            (pk(51), 200, Some(300)),
            (pk(52), 300, None),
        ],
        "history re-derived in slot order"
    );
    assert!(!is_dirty(&pool, a).await, "flag cleared");
    assert_eq!(owner_of(&pool, a).await.0, Some(pk(52)));

    let mismatches: i64 = sqlx::query_scalar("SELECT count(*) FROM integrity_owner_mismatch")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(mismatches, 0);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn rebuild_ends_a_burned_asset_without_an_open_interval(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;
    write(
        &pool,
        a,
        c,
        &sig(1),
        100,
        EventKind::Mint,
        None,
        Some(&pk(50)),
    )
    .await;
    write(
        &pool,
        a,
        c,
        &sig(2),
        200,
        EventKind::Burn,
        Some(&pk(50)),
        None,
    )
    .await;

    let mut tx = pool.begin().await.unwrap();
    activity::rebuild_ownership(&mut tx, a).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(intervals(&pool, a).await, vec![(pk(50), 100, Some(200))]);
    let open: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ownership_history WHERE asset_id = $1 AND to_slot IS NULL",
    )
    .bind(a)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(open, 0, "a burned asset holds no open interval");
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn addresses_resolve_only_within_enabled_collections(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;
    asset(&pool, c, 2).await;

    let found = activity::assets_by_address(&pool, &[pk(1), pk(2), pk(3)])
        .await
        .unwrap();
    assert_eq!(found.len(), 2, "pk(3) is not tracked");
    assert!(found.iter().any(|r| r.id == a));

    sqlx::query("UPDATE collections SET enabled = false WHERE id = $1")
        .bind(c)
        .execute(&pool)
        .await
        .unwrap();
    let found = activity::assets_by_address(&pool, &[pk(1)]).await.unwrap();
    assert!(
        found.is_empty(),
        "a disabled collection stops producing activity"
    );
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn an_unclassifiable_signature_is_parked(pool: PgPool) {
    let c = collection(&pool).await;
    let a = asset(&pool, c, 1).await;

    activity::park_signature(&pool, a, &sig(1), 100, false)
        .await
        .unwrap();
    activity::park_signature(&pool, a, &sig(1), 100, false)
        .await
        .unwrap();

    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM asset_signatures WHERE asset_id = $1 AND classified_at IS NULL",
    )
    .bind(a)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pending, 1, "parking is idempotent");

    let activity_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM activity")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(activity_rows, 0, "parked, never guessed at");
}

// --- ALG-622: the archival crawl's writer surface -------------------------

/// The crawl's own signature rows, as `record_signatures` takes them.
fn crawled(seed: u8, slot: i64, block_time: Option<i64>, failed: bool) -> CrawledSignature {
    CrawledSignature {
        signature: sig(seed),
        slot,
        block_time: block_time.map(ts),
        failed,
    }
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn a_sale_carries_its_price_and_venue(pool: PgPool) {
    let collection_id = collection(&pool).await;
    let asset_id = asset(&pool, collection_id, 1).await;
    let (seller, buyer) = (pk(50), pk(51));

    let mut tx = pool.begin().await.unwrap();
    activity::record(
        &mut tx,
        &LiveEvent {
            asset_id,
            collection_id,
            signature: &sig(1),
            seq: 0,
            slot: 100,
            block_time: ts(100),
            kind: EventKind::Sale,
            from_owner: Some(&seller),
            to_owner: Some(&buyer),
            price_lamports: Some(580_000_000),
            marketplace: Some("Magic Eden"),
            details: None,
            source: "backfill",
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let (price, marketplace, source): (Option<i64>, Option<String>, String) = sqlx::query_as(
        "SELECT price_lamports, marketplace, source FROM activity WHERE asset_id = $1",
    )
    .bind(asset_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(price, Some(580_000_000));
    assert_eq!(marketplace.as_deref(), Some("Magic Eden"));
    assert_eq!(source, "backfill");
    // A sale still moves ownership like any other transfer.
    assert_eq!(
        owner_of(&pool, asset_id).await.0.as_deref(),
        Some(buyer.as_str())
    );
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn crawled_signatures_are_stored_and_marked(pool: PgPool) {
    let collection_id = collection(&pool).await;
    let asset_id = asset(&pool, collection_id, 1).await;

    let rows = vec![
        crawled(1, 100, Some(100), false),
        crawled(2, 200, Some(200), true),
    ];
    assert_eq!(
        activity::record_signatures(&pool, asset_id, &rows)
            .await
            .unwrap(),
        2
    );
    // Re-crawling is a true no-op: same rows, nothing written.
    assert_eq!(
        activity::record_signatures(&pool, asset_id, &rows)
            .await
            .unwrap(),
        0,
        "a second crawl of an unchanged asset must write nothing"
    );

    let pending = activity::pending_signatures(&pool, asset_id).await.unwrap();
    assert_eq!(pending.len(), 2, "nothing is classified yet");
    assert_eq!(pending[0], rows[0], "ordered by slot, block times intact");

    assert_eq!(
        activity::mark_classified(&pool, asset_id, &[sig(1)])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        activity::mark_classified(&pool, asset_id, &[sig(1)])
            .await
            .unwrap(),
        0,
        "marking twice changes nothing"
    );
    let pending = activity::pending_signatures(&pool, asset_id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].signature, sig(2));
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn a_parked_signature_gains_its_block_time_from_the_crawl(pool: PgPool) {
    let collection_id = collection(&pool).await;
    let asset_id = asset(&pool, collection_id, 1).await;

    // The live pipeline parks what it cannot date; the archival response
    // carries `blockTime`, so the crawl repairs the row rather than colliding
    // with it.
    activity::park_signature(&pool, asset_id, &sig(1), 100, false)
        .await
        .unwrap();
    let before: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT block_time FROM asset_signatures WHERE asset_id = $1")
            .bind(asset_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(before.is_none());

    activity::record_signatures(&pool, asset_id, &[crawled(1, 100, Some(100), false)])
        .await
        .unwrap();
    let after = activity::pending_signatures(&pool, asset_id).await.unwrap();
    assert_eq!(after[0].block_time, Some(ts(100)));
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn reclassifying_clears_everything_derived_but_keeps_the_signatures(pool: PgPool) {
    let collection_id = collection(&pool).await;
    let asset_id = asset(&pool, collection_id, 1).await;
    let (a, b) = (pk(50), pk(51));

    activity::record_signatures(&pool, asset_id, &[crawled(1, 100, Some(100), false)])
        .await
        .unwrap();
    write(
        &pool,
        asset_id,
        collection_id,
        &sig(1),
        100,
        EventKind::Mint,
        None,
        Some(&a),
    )
    .await;
    write(
        &pool,
        asset_id,
        collection_id,
        &sig(2),
        200,
        EventKind::Transfer,
        Some(&a),
        Some(&b),
    )
    .await;
    activity::mark_classified(&pool, asset_id, &[sig(1)])
        .await
        .unwrap();
    assert_eq!(intervals(&pool, asset_id).await.len(), 2);

    let mut tx = pool.begin().await.unwrap();
    let removed = activity::reset_for_reclassify(&mut tx, asset_id)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(removed, 2);

    assert!(intervals(&pool, asset_id).await.is_empty());
    let (owner, _, _) = owner_of(&pool, asset_id).await;
    assert_eq!(
        owner.as_deref(),
        Some(b.as_str()),
        "assets.owner is DAS's, not ours to clear"
    );
    let last: Option<i64> =
        sqlx::query_scalar("SELECT last_activity_slot FROM assets WHERE id = $1")
            .bind(asset_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(last.is_none(), "the INSERT-only trigger cannot undo itself");
    // The raw crawl output survives — that is the point of storing it.
    let pending = activity::pending_signatures(&pool, asset_id).await.unwrap();
    assert_eq!(
        pending.len(),
        1,
        "the signature is back to pending, not gone"
    );
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn a_transfer_is_repriced_into_a_sale_without_the_network(pool: PgPool) {
    let collection_id = collection(&pool).await;
    let asset_id = asset(&pool, collection_id, 1).await;
    let (a, b) = (pk(50), pk(51));

    let mut tx = pool.begin().await.unwrap();
    let details = serde_json::json!({
        "programs": [pk(92)],
        "price_candidate": {"lamports": 580_000_000, "source": "buyer"},
    });
    activity::record(
        &mut tx,
        &LiveEvent {
            asset_id,
            collection_id,
            signature: &sig(1),
            seq: 0,
            slot: 100,
            block_time: ts(100),
            kind: EventKind::Transfer,
            from_owner: Some(&a),
            to_owner: Some(&b),
            price_lamports: None,
            marketplace: None,
            details: Some(&details),
            source: "backfill",
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let candidates = activity::reprice_candidates(&pool, collection_id, 0, 10)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].details["price_candidate"]["lamports"],
        580_000_000
    );

    assert!(
        activity::promote_to_sale(&pool, candidates[0].id, 580_000_000, Some("Magic Eden"))
            .await
            .unwrap()
    );
    assert!(
        !activity::promote_to_sale(&pool, candidates[0].id, 580_000_000, Some("Magic Eden"))
            .await
            .unwrap(),
        "repricing twice changes nothing"
    );

    let (kind, price): (String, Option<i64>) =
        sqlx::query_as("SELECT kind, price_lamports FROM activity WHERE id = $1")
            .bind(candidates[0].id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((kind.as_str(), price), ("sale", Some(580_000_000)));
    assert!(
        activity::reprice_candidates(&pool, collection_id, 0, 10)
            .await
            .unwrap()
            .is_empty(),
        "a promoted row is no longer a candidate"
    );
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn last_activity_is_recomputed_after_a_partial_delete(pool: PgPool) {
    let collection_id = collection(&pool).await;
    let asset_id = asset(&pool, collection_id, 1).await;
    let (a, b) = (pk(50), pk(51));
    write(
        &pool,
        asset_id,
        collection_id,
        &sig(1),
        100,
        EventKind::Mint,
        None,
        Some(&a),
    )
    .await;
    write(
        &pool,
        asset_id,
        collection_id,
        &sig(2),
        200,
        EventKind::Transfer,
        Some(&a),
        Some(&b),
    )
    .await;

    sqlx::query("DELETE FROM activity WHERE asset_id = $1 AND signature = $2")
        .bind(asset_id)
        .bind(sig(2))
        .execute(&pool)
        .await
        .unwrap();
    let stale: Option<i64> =
        sqlx::query_scalar("SELECT last_activity_slot FROM assets WHERE id = $1")
            .bind(asset_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        stale,
        Some(200),
        "the trigger is INSERT-only, so the delete left it stale"
    );

    activity::recompute_last_activity(&pool, asset_id)
        .await
        .unwrap();
    let fixed: Option<i64> =
        sqlx::query_scalar("SELECT last_activity_slot FROM assets WHERE id = $1")
            .bind(asset_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(fixed, Some(100));
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn assets_are_paged_by_keyset_for_the_crawl(pool: PgPool) {
    let collection_id = collection(&pool).await;
    let ids: Vec<i64> = futures_lite_join(&pool, collection_id).await;

    let first = activity::assets_after(&pool, collection_id, 0, 2)
        .await
        .unwrap();
    assert_eq!(first.iter().map(|a| a.id).collect::<Vec<_>>(), ids[..2]);
    let next = activity::assets_after(&pool, collection_id, ids[1], 2)
        .await
        .unwrap();
    assert_eq!(next.iter().map(|a| a.id).collect::<Vec<_>>(), ids[2..]);
    assert!(
        activity::assets_after(&pool, collection_id, *ids.last().unwrap(), 2)
            .await
            .unwrap()
            .is_empty()
    );
}

async fn futures_lite_join(pool: &PgPool, collection_id: i32) -> Vec<i64> {
    let mut ids = Vec::new();
    for seed in 1..=3u8 {
        ids.push(asset(pool, collection_id, seed).await);
    }
    ids.sort_unstable();
    ids
}

// --- ALG-624: the integrity snapshot and membership -----------------------

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn the_integrity_snapshot_counts_a_real_disagreement(pool: PgPool) {
    use indexer_data_model::integrity;

    let collection_id = collection(&pool).await;
    let asset_id = asset(&pool, collection_id, 1).await;
    let (a, b) = (pk(50), pk(51));

    // The fixture asset is filed under an allowlist collection but is not on
    // its allowlist, which is exactly what `integrity_allowlist_violation`
    // exists to catch — so the snapshot is wired to all four views, not just
    // the owner one.
    let seeded = integrity::snapshot(&pool).await.unwrap();
    assert_eq!(seeded.allowlist_violation, 1);
    assert!(!seeded.is_healthy());

    let address: String = sqlx::query_scalar("SELECT address FROM assets WHERE id = $1")
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO collection_mints (mint, collection_id) VALUES ($1, $2)")
        .bind(&address)
        .bind(collection_id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        integrity::snapshot(&pool).await.unwrap().is_healthy(),
        "on the allowlist, nothing disagrees"
    );

    write(
        &pool,
        asset_id,
        collection_id,
        &sig(1),
        100,
        EventKind::Mint,
        None,
        Some(&a),
    )
    .await;
    assert!(integrity::snapshot(&pool).await.unwrap().is_healthy());

    // Move the observed owner behind the derived one's back — exactly the
    // shape a dropped event leaves, and what the sweep exists to catch.
    sqlx::query("UPDATE assets SET owner = $2 WHERE id = $1")
        .bind(asset_id)
        .bind(&b)
        .execute(&pool)
        .await
        .unwrap();

    let drifted = integrity::snapshot(&pool).await.unwrap();
    assert_eq!(drifted.owner_mismatch, 1);
    assert!(
        !drifted.is_healthy(),
        "the metric must not be vacuously green"
    );

    // And `ownership_dirty` is counted too — the overflow path's queue.
    activity::mark_dirty(&pool, asset_id).await.unwrap();
    assert_eq!(integrity::snapshot(&pool).await.unwrap().ownership_dirty, 1);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn membership_flips_without_losing_the_asset(pool: PgPool) {
    use indexer_data_model::assets;

    let collection_id = collection(&pool).await;
    let asset_id = asset(&pool, collection_id, 1).await;
    let address: String = sqlx::query_scalar("SELECT address FROM assets WHERE id = $1")
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    write(
        &pool,
        asset_id,
        collection_id,
        &sig(1),
        100,
        EventKind::Mint,
        None,
        Some(&pk(50)),
    )
    .await;

    let removed =
        assets::set_membership(&pool, collection_id, std::slice::from_ref(&address), true)
            .await
            .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(
        assets::set_membership(&pool, collection_id, std::slice::from_ref(&address), true)
            .await
            .unwrap(),
        0,
        "flipping a status that already agrees writes nothing"
    );

    // The row and its history survive — the migration's reason for a status
    // column rather than a delete.
    let (status, has_activity): (String, i64) = sqlx::query_as(
        "SELECT a.membership_status, (SELECT count(*) FROM activity x WHERE x.asset_id = a.id) \
           FROM assets a WHERE a.id = $1",
    )
    .bind(asset_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((status.as_str(), has_activity), ("removed", 1));

    assert_eq!(
        assets::set_membership(&pool, collection_id, &[address], false)
            .await
            .unwrap(),
        1,
        "an asset that comes back is a member again"
    );
    let removed_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT removed_at FROM assets WHERE id = $1")
            .bind(asset_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(removed_at.is_none(), "assets_removed_pair demands the pair");
}
