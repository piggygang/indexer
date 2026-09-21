//! `webhook_inbox` → [`IngestSource`].
//!
//! The third transport, and the first one whose upstream is a table rather than
//! a socket. It lives in this service rather than `crates/ingest` for the same
//! reason `crates/das` may not implement the trait: `crates/ingest` is the
//! transport *interface* and knows nothing about a database, a feature-gated
//! module there would go unlinted by `cargo clippy --workspace --all-targets`
//! (the argument CLAUDE.md gives for why `ws` is a *default* feature), and the
//! drain needs ingest, data-model, das and config at once — which is this
//! service's documented job.
//!
//! **It re-fetches every transaction with `getTransaction` rather than decoding
//! the stored body.** Helius raw webhooks deliver `encoding: "json"`
//! instructions — `{"accounts":[0,1],"data":"…","programIdIndex":10}` — while
//! `decode.rs` dispatches on `instruction.parsed.type` and reads `programId`
//! and `accounts` as base58 strings. Fed index-form input it returns
//! `Decoded::default()`: no error, no log, just `recorded=0` forever. The
//! re-fetch costs one credit and reuses the exact path `reconcile::recover_asset`
//! already runs in production, and leaves the decoder — the component that must
//! not be destabilised — untouched.

use std::time::{Duration, Instant};

use indexer_config::IngestConfig;
use indexer_das::DasClient;
use indexer_data_model::webhook_inbox;
use indexer_data_model::PgPool;
use indexer_ingest::{
    EventStream, IngestEvent, IngestSource, RawPayload, ResumeFrom, SlotCheckpoint, StreamStatus,
    SubscriptionSpec, TransactionUpdate,
};
use serde_json::Value;
use tokio::sync::watch;

/// `IngestSource::name()`; `ingest_state.stream` is `<name>:<label>`.
pub const NAME: &str = "helius-webhook";

/// Helius's cap on one webhook's `accountAddresses`.
pub const MAX_ADDRESSES: usize = indexer_das::webhooks::MAX_ADDRESSES;

/// How often a checkpoint is emitted when nothing is arriving.
///
/// The consumer's watchdog resets only on `SlotCheckpoint` and `Connected` and
/// trips at 300 s, so a quiet inbox that emitted neither would restart the
/// process every five minutes forever. 60 s leaves five heartbeats of margin.
const HEARTBEAT: Duration = Duration::from_secs(60);

/// How often retired rows are dropped.
const PRUNE_EVERY: Duration = Duration::from_secs(3_600);

/// Backoff before re-probing the table at startup. Short: this is a local
/// query, and a failure here is almost always Postgres still coming up.
const PROBE_BACKOFF_MS: [u64; 4] = [250, 1_000, 3_000, 8_000];

pub struct WebhookInbox {
    pool: PgPool,
    /// Its **own** rate-limited client, never the shared reconcile one: a burst
    /// of deliveries would otherwise consume the reconcile's whole budget.
    das: DasClient,
    config: IngestConfig,
}

impl WebhookInbox {
    pub fn new(pool: PgPool, das: DasClient, config: IngestConfig) -> Self {
        Self { pool, das, config }
    }
}

/// Why this spec cannot be served by a Helius webhook, if it cannot.
///
/// Mirrors `ws::unsupported`, and for the same reason: an uncompilable spec is
/// a configuration error, and the trait says that is one of the few terminal
/// failures rather than something to retry forever.
pub fn unsupported(spec: &SubscriptionSpec) -> Option<String> {
    if !spec.accounts.is_empty() {
        return Some("webhooks have no accountSubscribe equivalent".into());
    }
    let addresses: usize = spec
        .transactions
        .values()
        .map(|f| f.account_include.len())
        .sum();
    if addresses > MAX_ADDRESSES {
        return Some(format!(
            "{addresses} addresses exceeds the webhook limit of {MAX_ADDRESSES}"
        ));
    }
    None
}

impl IngestSource for WebhookInbox {
    fn name(&self) -> &'static str {
        NAME
    }

    fn subscribe(
        &self,
        mut spec: watch::Receiver<SubscriptionSpec>,
        resume: ResumeFrom,
    ) -> EventStream {
        let pool = self.pool.clone();
        let das = self.das.clone();
        let config = self.config.clone();

        Box::pin(async_stream::stream! {
            let grace = Duration::from_secs(config.webhook_grace_secs);
            let lease = Duration::from_secs(config.webhook_lease_secs);
            let poll = Duration::from_millis(config.webhook_poll_ms);
            let retain = Duration::from_secs(u64::from(config.webhook_retain_days) * 86_400);

            // Bound before the `if let`: an `if let` scrutinee's temporaries
            // live to the end of its block, and a `watch::Ref` held across the
            // `yield` below would make the whole stream `!Send`.
            let unsupported_reason = unsupported(&spec.borrow_and_update().clone());
            if let Some(reason) = unsupported_reason {
                yield Err(indexer_ingest::IngestError::UnsupportedSpec(reason));
                return;
            }

            // Probe the table before claiming to be connected. Retried, because
            // "Postgres is still starting" is not a dead transport.
            for (attempt, wait) in std::iter::once(&0).chain(PROBE_BACKOFF_MS.iter()).enumerate() {
                if *wait > 0 {
                    tokio::time::sleep(Duration::from_millis(*wait)).await;
                }
                match webhook_inbox::watermark(&pool, grace).await {
                    Ok(_) => break,
                    Err(error) => {
                        log::warn!("webhook inbox not readable yet: {error}");
                        yield Ok(IngestEvent::Status(StreamStatus::Reconnecting {
                            attempt: attempt as u32 + 1,
                        }));
                    }
                }
            }

            // Exactly once, ever. NOT a heartbeat: `Connected` is the consumer's
            // reconcile trigger, so re-emitting it would fire a full DAS sweep
            // every minute the day this lane inherits `reconcile_das`.
            yield Ok(IngestEvent::Status(StreamStatus::Connected));

            // The resume floor seeds the first checkpoint and nothing else.
            // Unlike the WebSocket adapter this is deliberately *not* a delivery
            // filter: `processed_at` is a strictly stronger dedup key than a slot
            // floor, and a below-floor row is a *late webhook* — the only copy of
            // an event nothing else delivered, which is precisely what this
            // transport exists to catch.
            let floor = match resume {
                ResumeFrom::Slot(slot) => i64::try_from(slot).unwrap_or(i64::MAX),
                ResumeFrom::Latest => 0,
            };
            let mut last_checkpoint: Option<i64> = None;
            let mut last_beat = Instant::now();
            let mut last_prune = Instant::now();

            loop {
                if spec.has_changed().unwrap_or(false) {
                    spec.borrow_and_update();
                    // The address list is server-side state at Helius; this
                    // process cannot change it. `indexer-admin webhook` does.
                    log::info!(
                        "tracked addresses changed — run `indexer-admin webhook` to re-register"
                    );
                    yield Ok(IngestEvent::Status(StreamStatus::Resubscribed));
                }

                let claimed = match webhook_inbox::claim(
                    &pool,
                    config.webhook_batch,
                    lease,
                    config.webhook_max_attempts,
                )
                .await
                {
                    Ok(rows) => rows,
                    Err(error) => {
                        // Transient by assumption. `Err` on the stream is
                        // TERMINAL by contract and would tear down a healthy
                        // pipeline for a blip; a persistent outage still trips
                        // the consumer's 300 s watchdog, which is the correct
                        // escalation and already exists.
                        log::warn!("claiming webhook deliveries: {error}");
                        tokio::time::sleep(poll).await;
                        continue;
                    }
                };

                let full = claimed.len() as i64 >= config.webhook_batch;
                let mut rows = claimed;
                // Claimed in arrival order because slot order is not knowable
                // until the backlog is drained; sorted here so a burst is
                // replayed in slot order and does not flag `ownership_dirty`
                // for reordering the writer would otherwise have to repair.
                rows.sort_by_key(|row| (row.slot, row.id));
                let drained = !rows.is_empty();

                // Set by the first 429 of the batch. Being throttled says
                // nothing about any individual signature, so the rest of the
                // batch is handed back untouched rather than each row spending
                // one of its `WEBHOOK_MAX_ATTEMPTS` on a shared condition.
                let mut throttled = false;
                let mut deferred = 0usize;

                for row in rows {
                    if throttled {
                        if let Err(error) =
                            webhook_inbox::defer(&pool, row.id, "helius rate limited").await
                        {
                            log::warn!("deferring {}: {error}", row.signature);
                        }
                        deferred += 1;
                        continue;
                    }
                    if row.failed {
                        // The decoder drops failed transactions, so fetching one
                        // would spend a credit to learn nothing.
                        if let Err(error) =
                            webhook_inbox::mark_processed(&pool, row.id, None).await
                        {
                            log::warn!("retiring failed {}: {error}", row.signature);
                        }
                        continue;
                    }
                    match das.get_transaction(&row.signature).await {
                        Ok(Some(transaction)) => {
                            let slot = transaction
                                .get("slot")
                                .and_then(Value::as_i64)
                                .unwrap_or(row.slot);
                            yield Ok(IngestEvent::Transaction(TransactionUpdate {
                                // A webhook names a webhook, not a spec entry.
                                filters: Vec::new(),
                                slot: slot as u64,
                                signature: row.signature.clone(),
                                failed: false,
                                // The pipeline decodes from `raw`; `replay` does
                                // the same on the recovery path.
                                account_keys: Vec::new(),
                                raw: RawPayload::Json(transaction),
                            }));
                            if let Err(error) =
                                webhook_inbox::mark_processed(&pool, row.id, None).await
                            {
                                log::warn!("retiring {}: {error}", row.signature);
                            }
                        }
                        Ok(None) => {
                            retire_or_retry(
                                &pool,
                                &row,
                                config.webhook_max_attempts,
                                "getTransaction returned null",
                            )
                            .await;
                        }
                        Err(error) if error.is_rate_limited() => {
                            // Stop the batch here. Draining on through a rate
                            // limit at one poll per second is how a throttled
                            // drain retires a whole backlog of real events:
                            // Helius gives up redelivering after about three
                            // tries, so a row retired for this is lost.
                            throttled = true;
                            if let Err(e) =
                                webhook_inbox::defer(&pool, row.id, &error.to_string()).await
                            {
                                log::warn!("deferring {}: {e}", row.signature);
                            }
                            deferred += 1;
                        }
                        Err(error) => {
                            retire_or_retry(
                                &pool,
                                &row,
                                config.webhook_max_attempts,
                                &error.to_string(),
                            )
                            .await;
                        }
                    }
                }

                if throttled {
                    log::warn!(
                        "helius rate limited the webhook drain; deferred {deferred} \
                         delivery(ies) with their attempt counts intact"
                    );
                }

                if drained || last_beat.elapsed() >= HEARTBEAT {
                    match checkpoint(&pool, grace, last_checkpoint, floor).await {
                        Some(slot) => {
                            last_checkpoint = Some(slot);
                            yield Ok(IngestEvent::SlotCheckpoint(SlotCheckpoint {
                                slot: slot as u64,
                            }));
                        }
                        // Nothing this stream can honestly claim yet: an empty
                        // inbox on a database with no backfilled cursor, so the
                        // floor is 0 and 0 is the one value this checkpoint may
                        // never emit. The poll still succeeded, so the transport
                        // is alive — say so, or the consumer's watchdog trips at
                        // 300 s and the supervisor restarts a healthy lane every
                        // five minutes forever.
                        None => yield Ok(IngestEvent::Status(StreamStatus::Idle)),
                    }
                    last_beat = Instant::now();
                }

                if last_prune.elapsed() >= PRUNE_EVERY {
                    match webhook_inbox::prune(&pool, retain).await {
                        Ok(dropped) if dropped > 0 => {
                            log::info!("pruned {dropped} retired webhook deliveries")
                        }
                        Ok(_) => {}
                        Err(error) => log::warn!("pruning the webhook inbox: {error}"),
                    }
                    last_prune = Instant::now();
                }

                // A full batch means there is more waiting: keep draining —
                // unless we were throttled, where the whole point is to stop
                // asking for a moment.
                if !full || throttled {
                    tokio::time::sleep(poll).await;
                }
            }
        })
    }
}

/// Retires a row at the attempt cap, or releases it for the next poll.
///
/// Retiring an unrecoverable row matters as much as recovering a good one: a
/// row that is never processed pins [`webhook_inbox::watermark`] forever, and
/// the cursor would freeze behind one bad delivery.
async fn retire_or_retry(
    pool: &PgPool,
    row: &webhook_inbox::Claimed,
    max_attempts: i16,
    error: &str,
) {
    if row.attempts >= max_attempts {
        log::error!(
            "giving up on {} after {} attempt(s): {error}",
            row.signature,
            row.attempts
        );
        if let Err(e) = webhook_inbox::mark_processed(pool, row.id, Some(error)).await {
            log::warn!("retiring {}: {e}", row.signature);
        }
    } else {
        log::warn!(
            "fetching {} (attempt {}): {error}",
            row.signature,
            row.attempts
        );
        if let Err(e) = webhook_inbox::release(pool, row.id, error).await {
            log::warn!("releasing {}: {e}", row.signature);
        }
    }
}

/// The slot to claim, or `None` for "say nothing and let the last one stand".
///
/// Never `0`: a persisted zero would make `reconcile::seed_cursor` return
/// `Some(0)`, and the sweep's fallback floor for never-recorded assets would
/// become "walk all of history" — turning an hourly sweep into an archival
/// crawl. Never the chain tip either; `getSlot` is right there and it is the one
/// value guaranteed to be a lie about this stream.
async fn checkpoint(pool: &PgPool, grace: Duration, last: Option<i64>, floor: i64) -> Option<i64> {
    let watermark = match webhook_inbox::watermark(pool, grace).await {
        Ok(watermark) => watermark,
        Err(error) => {
            log::warn!("reading the inbox watermark: {error}");
            None
        }
    };
    // `GREATEST` in the writer makes a lower value a no-op rather than a
    // rewind, but emitting one would still be a false claim about this stream —
    // so a watermark *below* the last claim is dropped.
    //
    // An *equal* one is deliberately kept. The watermark is
    // `least(min(pending slot) - 1, max(settled slot))`, which is a constant
    // while nothing arrives, so suppressing it meant a quiet inbox emitted
    // exactly one checkpoint ever. That starved the consumer's watchdog — which
    // resets only on `SlotCheckpoint` and `Connected` — into restarting the
    // process every five minutes, and every restart fires a full DAS sweep on
    // `Connected`. Re-emitting is free: the writer's `GREATEST` makes it a
    // no-op on `last_processed_slot`, and it keeps `ingest_state.updated_at`
    // moving, which is the runbook's liveness signal.
    watermark
        .or(last)
        .or(Some(floor))
        .filter(|slot| *slot > 0 && *slot >= last.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer_data_model::webhook_inbox::Delivery;

    /// In-module rather than in `tests/`, because the property worth pinning
    /// is `checkpoint`'s own filter and that is private. Driving it through
    /// the stream instead would mean waiting out a 60 s [`HEARTBEAT`] against
    /// a 300 s watchdog — and simulated time is not available here, since
    /// pausing the clock stalls the sqlx pool's own timers.
    const NO_GRACE: Duration = Duration::ZERO;

    fn sig(seed: u8) -> String {
        bs58::encode([seed; 64]).into_string()
    }

    /// The regression, stated as a table.
    ///
    /// `watermark` is `least(min(pending slot) - 1, max(settled slot))`, which
    /// is a **constant** while nothing arrives. The filter used to be
    /// `Some(*slot) != last`, so a quiet inbox emitted exactly one checkpoint
    /// ever, the consumer's watchdog — which resets only on `SlotCheckpoint`
    /// and `Connected` — tripped at 300 s, and the restart's `Connected` fired
    /// a full DAS sweep. Every five minutes, on a perfectly healthy stream.
    #[sqlx::test(migrations = "../../crates/data-model/migrations")]
    #[ignore = "needs DATABASE_URL"]
    async fn a_quiet_inbox_re_emits_its_checkpoint(pool: PgPool) {
        // First heartbeat: nothing stored, so the resume floor stands.
        assert_eq!(checkpoint(&pool, NO_GRACE, None, 500).await, Some(500));
        // Every heartbeat after it. This is the one that used to return None.
        assert_eq!(
            checkpoint(&pool, NO_GRACE, Some(500), 500).await,
            Some(500),
            "an unchanged watermark is still a true claim, and it is what \
             feeds the watchdog"
        );
    }

    /// Equal is fine; *lower* is a false claim about this stream.
    ///
    /// The writer's `GREATEST` would discard it silently, which is precisely
    /// why emitting it is worth refusing rather than tolerating.
    #[sqlx::test(migrations = "../../crates/data-model/migrations")]
    #[ignore = "needs DATABASE_URL"]
    async fn the_checkpoint_never_goes_backwards(pool: PgPool) {
        // A late delivery lands below everything already claimed, dragging
        // `min(pending) - 1` under the last emitted slot.
        webhook_inbox::enqueue(
            &pool,
            &[Delivery {
                signature: sig(7),
                slot: 100,
                block_time: None,
                failed: false,
                body: serde_json::json!({}),
            }],
        )
        .await
        .unwrap();

        assert_eq!(
            checkpoint(&pool, NO_GRACE, Some(600), 500).await,
            None,
            "watermark 99 is below the last claim of 600"
        );
        assert_eq!(
            checkpoint(&pool, NO_GRACE, Some(50), 0).await,
            Some(99),
            "and it is emitted when it genuinely advances"
        );
    }

    /// A cold start against a database the backfill has never seeded.
    ///
    /// There is no slot to claim: the floor is 0, and 0 is the one value this
    /// may never emit — a persisted zero makes `reconcile::seed_cursor` return
    /// `Some(0)` and turns the sweep's fallback floor into a full archival
    /// crawl. `None` is the caller's cue to emit `StreamStatus::Idle` instead,
    /// so the transport still says it is alive.
    #[sqlx::test(migrations = "../../crates/data-model/migrations")]
    #[ignore = "needs DATABASE_URL"]
    async fn a_floorless_cold_start_claims_nothing(pool: PgPool) {
        assert_eq!(checkpoint(&pool, NO_GRACE, None, 0).await, None);
    }
}
