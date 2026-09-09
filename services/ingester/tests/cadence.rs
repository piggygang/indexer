//! The cadence gate — the thing that stands between a transient failure and a
//! runaway Helius bill.
//!
//! Two amplifiers shared one root cause: a job whose last *attempt* was not
//! recorded looks perpetually due. The scheduler then re-ran it on its next
//! tick (10 s, not the configured interval), and the consumer spawned a full
//! ~480-credit sweep on every single `Connected`. A dead API key turned that
//! into 746 requests in 42 minutes; a flapping socket turned it into a sweep
//! per reconnect.
//!
//! Both paths now ask the same question — `schedule::due` — against the same
//! durable record, so this is where that behaviour is pinned.
//!
//! Ignored without a database: `cargo test --workspace -- --include-ignored`.

use std::time::Duration;

use indexer_data_model::{ingest_state, PgPool};
use indexer_ingester::{probe, reconcile, schedule};

async fn collection(pool: &PgPool) -> i32 {
    sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, enabled) \
         VALUES ('c', 'C', 'token_metadata', $1, 'SYN', true) RETURNING id",
    )
    .bind(bs58::encode([7u8; 32]).into_string())
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn succeeded(pool: &PgPool, collection_id: i32, kind: &str) {
    ingest_state::put_backfill_state(
        pool,
        &ingest_state::BackfillState {
            collection_id,
            kind: kind.to_string(),
            status: "done".to_string(),
            cursor: serde_json::json!({}),
            progress: serde_json::json!({"corrections": 0}),
            last_error: None,
            started_at: Some(chrono::Utc::now()),
            finished_at: Some(chrono::Utc::now()),
            updated_at: chrono::Utc::now(),
        },
    )
    .await
    .unwrap();
}

/// A job that has just run is not due again, whether it succeeded or failed.
///
/// The failure half is the one that matters: before `record_failure` existed, a
/// failing job never wrote `finished_at`, so this predicate stayed true and the
/// job ran again seconds later — forever, and against a metered API.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn a_recent_attempt_suppresses_the_next_one(pool: PgPool) {
    let id = collection(&pool).await;
    let hour = Duration::from_secs(3_600);

    // Never run: `due` seeds the schedule and reports not-due, so a fresh
    // deploy does not immediately fire every job it has ever been configured
    // with. The seeding is what makes the second call meaningful.
    assert!(!schedule::due(&pool, reconcile::KIND, hour).await);

    // A success suppresses it for the interval.
    succeeded(&pool, id, reconcile::KIND).await;
    assert!(
        !schedule::due(&pool, reconcile::KIND, hour).await,
        "a job that just succeeded must not run again this hour"
    );

    // And so does a failure. This is the amplifier fix: without it the next
    // tick — ten seconds later — would run the job again.
    ingest_state::record_failure(&pool, reconcile::KIND, "getSlot: HTTP 401")
        .await
        .unwrap();
    assert!(
        !schedule::due(&pool, reconcile::KIND, hour).await,
        "a job that just FAILED must also back off, or a dead key becomes a \
         request storm"
    );

    // Age the record past the interval and it is due again — which is what
    // proves the suppression above was the interval doing its job and not the
    // record being unreadable.
    //
    // Backdated rather than asserted with a zero interval: `finished_at` is the
    // *database's* `now()` and `due` compares it against the *process's* clock,
    // so a sub-millisecond skew makes "zero" a coin flip. `due` treats a finish
    // in the future as not-due, which is the conservative reading and the one
    // that costs nothing.
    sqlx::query(
        "UPDATE backfill_state SET finished_at = now() - interval '2 hours' WHERE kind = $1",
    )
    .bind(reconcile::KIND)
    .execute(&pool)
    .await
    .unwrap();
    assert!(schedule::due(&pool, reconcile::KIND, hour).await);
}

/// Each job keeps its own cadence: a sweep does not suppress the probe.
///
/// They share one table, so a keying mistake would silently couple them — and
/// coupling the 5-minute probe to the hourly sweep would quietly stop the
/// freshness path.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn each_job_has_its_own_cadence(pool: PgPool) {
    let id = collection(&pool).await;
    let hour = Duration::from_secs(3_600);

    for kind in [reconcile::KIND, probe::KIND, reconcile::DEEP_KIND] {
        assert!(!schedule::due(&pool, kind, hour).await);
    }
    succeeded(&pool, id, reconcile::KIND).await;

    assert!(
        !schedule::due(&pool, reconcile::KIND, hour).await,
        "the swept job is suppressed"
    );
    // Backdate the other two jobs' seeded rows: if the sweep's success had
    // leaked across kinds, these would stay suppressed.
    sqlx::query(
        "UPDATE backfill_state SET finished_at = now() - interval '2 hours' WHERE kind <> $1",
    )
    .bind(reconcile::KIND)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        schedule::due(&pool, probe::KIND, hour).await,
        "the probe is untouched by it"
    );
    assert!(
        schedule::due(&pool, reconcile::DEEP_KIND, hour).await,
        "and so is the deep pass"
    );
}

/// A lagging collection re-triggers the job; a brand-new one does not.
///
/// `last_finished` takes the **minimum** finish across enabled collections, so
/// a collection whose record is old drags the whole job back into due-ness
/// rather than hiding behind fresher siblings. But SQL's `min()` skips NULLs,
/// so a collection with *no* record at all is invisible to it — it waits out
/// the interval instead. That is fine, and worth pinning so nobody "fixes" it
/// into a per-collection trigger: every job iterates all enabled collections
/// when it runs, so the newcomer is swept by the next ordinary pass.
#[sqlx::test(migrations = "../../crates/data-model/migrations")]
#[ignore = "needs DATABASE_URL"]
async fn the_oldest_collection_sets_the_cadence(pool: PgPool) {
    let first = collection(&pool).await;
    let hour = Duration::from_secs(3_600);
    succeeded(&pool, first, reconcile::KIND).await;
    assert!(!schedule::due(&pool, reconcile::KIND, hour).await);

    let second: i32 = sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, verified_creator, symbol, enabled) \
         VALUES ('d', 'D', 'token_metadata', $1, 'SYN2', true) RETURNING id",
    )
    .bind(bs58::encode([8u8; 32]).into_string())
    .fetch_one(&pool)
    .await
    .unwrap();

    // No record at all: invisible to `min()`, so the job stays suppressed.
    assert!(
        !schedule::due(&pool, reconcile::KIND, hour).await,
        "a brand-new collection does not force an off-cadence run"
    );

    // Give it a record, then age it. Now it is the minimum, and it drags the
    // job back into due-ness even though its sibling finished moments ago.
    succeeded(&pool, second, reconcile::KIND).await;
    sqlx::query(
        "UPDATE backfill_state SET finished_at = now() - interval '2 hours' \
          WHERE collection_id = $1 AND kind = $2",
    )
    .bind(second)
    .bind(reconcile::KIND)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        schedule::due(&pool, reconcile::KIND, hour).await,
        "one lagging collection must re-trigger the job for all of them"
    );
}
