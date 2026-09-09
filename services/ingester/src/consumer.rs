//! The consumer loop.
//!
//! It owns the durable cursor — `crates/ingest`'s contract is explicit that
//! `last_processed_slot` is persisted **only** on `SlotCheckpoint` — and it
//! owns gap recovery, because this transport cannot replay. Everything else
//! (reconnecting, resubscribing, keepalive) belongs to the adapter.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use indexer_das::DasClient;
use indexer_data_model::{ingest_state, PgPool};
use indexer_ingest::{IngestEvent, IngestSource, ResumeFrom, StreamStatus, SubscriptionSpec};
use tokio::sync::watch;

use crate::pipeline::{Outcome, Pipeline};
use crate::{reconcile, schedule, spec};

/// The durable identity of one transport.
///
/// `stream` is the `ingest_state.stream` key the migration specifies
/// (`'<IngestSource::name()>:<label>'`); `source` is the `activity.source` tag
/// this lane's writes carry, so a dual run is measurable one row at a time
/// rather than only in aggregate.
///
/// One `Copy` struct rather than two fields, because the two must never
/// disagree: a consumer that checkpoints one lane and tags its rows with
/// another is the bug this shape forecloses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lane {
    pub stream: &'static str,
    pub source: &'static str,
}

/// Helius Enhanced WebSockets. No replay — `ResumeFrom::Slot` is a floor.
pub const WS: Lane = Lane {
    stream: "helius-ws:mainnet",
    source: "live",
};

/// Helius webhooks, received by this service and drained from `webhook_inbox`.
pub const WEBHOOK: Lane = Lane {
    stream: "helius-webhook:mainnet",
    source: "webhook",
};

/// Checkpoints are coalesced: roots arrive ~2.5/s and each is an upsert.
/// `GREATEST` makes throttling safe, and the contract only requires that a
/// persisted slot means "nothing lower will follow".
const CHECKPOINT_EVERY: Duration = Duration::from_secs(10);

/// How often the registry is re-read for new collections or Core assets.
const REGISTRY_POLL: Duration = Duration::from_secs(300);

/// A heartbeat this stale means the consumer is wedged. Railway healthchecks
/// gate deploys only and never restart a running container, so the process has
/// to notice its own death and exit into the restart policy.
const WATCHDOG: Duration = Duration::from_secs(300);

pub struct Consumer {
    pub pool: PgPool,
    pub das: DasClient,
    /// The client the spawned reconcile uses — rate-limited, and the **same
    /// instance** `schedule::run` holds so the two share one budget rather than
    /// two. The live `das` above stays unthrottled: it is the half that cannot
    /// wait.
    ///
    /// `None` on every lane but one. An `Option` rather than a flag because a
    /// flag and a client can disagree and an absent client cannot — and which
    /// lane holds it is *derived* (`IngestConfig::reconciler`), so retiring a
    /// transport moves the reconcile with no second variable to forget. Two
    /// lanes reconciling would ask Helius for twice `RECONCILE_RPS`.
    pub reconcile_das: Option<DasClient>,
    /// The floor between two on-`Connected` reconciles.
    ///
    /// Without it a reconnect storm is a sweep storm: `Connected` fires per
    /// reconnect, each sweep is ~480 Helius credits, and nothing else throttles
    /// it — "one at a time" only serialises them. A socket that connects,
    /// delivers nothing and times out can bill more than the entire scheduled
    /// workload. Gated on the same `backfill_state` cadence every scheduled job
    /// uses, so a connect that arrives inside the window is a no-op.
    pub reconcile_every: Duration,
    pub source: Arc<dyn IngestSource>,
    pub lane: Lane,
}

/// The on-`Connected` reconcile, owned by the consumer's run so it cannot
/// outlive it.
///
/// Abandoning a sweep in flight is safe for the same reason `schedule::run_job`
/// abandons one: every step commits its own transaction, and `finished_at` is
/// written only on completion, so the next run picks up from the same state.
/// What is *not* safe is leaving one running while the supervisor starts a
/// fresh consumer — hence the `Drop`.
#[derive(Default)]
struct Reconciling(Option<tokio::task::JoinHandle<()>>);

impl Reconciling {
    fn in_flight(&self) -> bool {
        self.0.as_ref().is_some_and(|handle| !handle.is_finished())
    }
}

impl Drop for Reconciling {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    pub events: u64,
    pub outcome: Outcome,
    pub reconnects: u64,
    pub reconciles: u64,
}

impl Consumer {
    /// Runs until the stream ends, the shutdown signal fires, or the watchdog
    /// trips. Returns the accumulated stats.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> anyhow::Result<Stats> {
        let mut pipeline = Pipeline::new(
            self.pool.clone(),
            self.das.clone(),
            reconcile::context(&self.pool).await?,
            self.lane.source,
        );

        let resume = reconcile::seed_cursor(&self.pool, self.lane.stream).await?;
        let (spec_tx, spec_rx) = watch::channel(spec::build(&self.pool).await?);
        log::info!(
            "resuming {} from {} with {} filter(s)",
            self.lane.stream,
            resume
                .map(|s| s.to_string())
                .unwrap_or_else(|| "the live tip".into()),
            spec_rx.borrow().transactions.len()
        );

        let mut stream = self.source.subscribe(
            spec_rx.clone(),
            resume.map(ResumeFrom::Slot).unwrap_or(ResumeFrom::Latest),
        );

        let mut stats = Stats::default();
        let mut reconciling = Reconciling::default();
        let mut pending_checkpoint: Option<u64> = None;
        let mut last_checkpoint_write = Instant::now();
        let mut last_progress = Instant::now();
        let mut poll = tokio::time::interval(REGISTRY_POLL);
        poll.tick().await;

        loop {
            tokio::select! {
                biased;

                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        log::info!("shutdown requested; flushing");
                        break;
                    }
                }

                _ = poll.tick() => {
                    // A registry change (a new collection, or Core assets the
                    // backfill added) reaches the socket without a restart.
                    match spec::build(&self.pool).await {
                        Ok(next) => {
                            spec_tx.send_if_modified(|current| {
                                let changed = *current != next;
                                if changed {
                                    *current = next;
                                }
                                changed
                            });
                        }
                        Err(error) => log::warn!("rebuilding the subscription spec: {error:#}"),
                    }
                    if let Ok(context) = reconcile::context(&self.pool).await {
                        *pipeline.context_mut() = context;
                    }
                    if last_progress.elapsed() > WATCHDOG {
                        anyhow::bail!(
                            "no checkpoint in {}s — exiting so the restart policy reconnects \
                             and reconciles",
                            last_progress.elapsed().as_secs()
                        );
                    }
                }

                item = stream.next() => {
                    let Some(item) = item else {
                        log::warn!("stream ended");
                        break;
                    };
                    // A terminal error is the adapter giving up; the service
                    // decides the restart policy, not the adapter.
                    let event = item?;

                    match event {
                        IngestEvent::Transaction(update) => {
                            stats.events += 1;
                            match pipeline.handle(&update).await {
                                Ok(outcome) => stats.outcome.add(outcome),
                                Err(error) => {
                                    log::error!("{}: {error:#}", update.signature);
                                    return Err(error);
                                }
                            }
                        }
                        IngestEvent::SlotCheckpoint(checkpoint) => {
                            last_progress = Instant::now();
                            pending_checkpoint = Some(checkpoint.slot);
                            if last_checkpoint_write.elapsed() >= CHECKPOINT_EVERY {
                                self.checkpoint(&mut pending_checkpoint).await?;
                                last_checkpoint_write = Instant::now();
                            }
                        }
                        IngestEvent::Status(StreamStatus::Connected) => {
                            // Reconcile on EVERY connect, so cold start, crash
                            // restart and mid-run reconnect are one path.
                            //
                            // Spawned, not awaited. A sweep takes tens of
                            // seconds, and awaiting it here meant the consumer
                            // stopped reading the socket for exactly as long —
                            // on a transport with no replay, every event that
                            // arrived in that window was lost, which is the
                            // failure this reconcile exists to repair. A
                            // reconnect storm is the worst case and the one
                            // that made it self-defeating.
                            last_progress = Instant::now();
                            // Only the reconciling lane, so a dual run does not
                            // double the DAS budget.
                            if let Some(das) = &self.reconcile_das {
                                if reconciling.in_flight() {
                                    log::info!("reconcile already in flight; this connect rides it");
                                } else if !schedule::due(
                                    &self.pool,
                                    reconcile::KIND,
                                    self.reconcile_every,
                                )
                                .await
                                {
                                    log::debug!("reconciled recently; this connect skips it");
                                } else {
                                    stats.reconciles += 1;
                                    reconciling.0 = Some(tokio::spawn(reconcile_once(
                                        self.pool.clone(),
                                        das.clone(),
                                        self.lane.stream,
                                    )));
                                }
                            }
                        }
                        IngestEvent::Status(StreamStatus::Reconnecting { attempt }) => {
                            stats.reconnects += 1;
                            log::warn!("transport reconnecting (attempt {attempt})");
                        }
                        IngestEvent::Status(StreamStatus::Lagged { dropped }) => {
                            log::warn!("dropped {dropped} event(s); the reconnect will reconcile");
                        }
                        IngestEvent::Status(StreamStatus::Resubscribed) => {
                            log::info!("subscriptions updated without a reconnect");
                        }
                        IngestEvent::Account(_) => {}
                    }
                }
            }
        }

        self.checkpoint(&mut pending_checkpoint).await?;
        Ok(stats)
    }

    async fn checkpoint(&self, pending: &mut Option<u64>) -> anyhow::Result<()> {
        if let Some(slot) = pending.take() {
            ingest_state::checkpoint(&self.pool, self.lane.stream, slot).await?;
        }
        Ok(())
    }
}

/// One reconcile, with its own pipeline and its own error handling.
///
/// A failure is logged rather than returned: this no longer runs on the
/// consumer's path, so it has no consumer to take down, and every connect and
/// every scheduled tick is another attempt. It builds a fresh `DecodeContext`
/// for the same reason `schedule::sweep` does — the consumer owns its own
/// mutably, and a fresh one picks up registry changes without a restart.
async fn reconcile_once(pool: PgPool, das: DasClient, stream: &'static str) {
    let context = match reconcile::context(&pool).await {
        Ok(context) => context,
        Err(error) => return log::error!("reconcile could not read the venue registry: {error:#}"),
    };
    let pipeline = Pipeline::new(pool.clone(), das.clone(), context, "reconcile");
    let from = match ingest_state::last_processed_slot(&pool, stream).await {
        Ok(from) => from,
        Err(error) => return log::error!("reconcile could not read the cursor: {error:#}"),
    };
    match reconcile::run(&pool, &das, &pipeline, from).await {
        Ok(report) => report.log("reconcile"),
        Err(error) => log::error!("reconcile failed: {error:#}"),
    }
}

/// Runs one consumer until shutdown, restarting it with a backoff.
///
/// Lifted out of `main` so two lanes can be supervised independently: a failure
/// on one transport must not tear down the other, and `Consumer::run` returns
/// `Err` on a pipeline error. Merged, an inbox hiccup would force a reconnect
/// on the transport that has no replay — precisely the event that loses data.
pub async fn supervise(consumer: Consumer, mut shutdown: watch::Receiver<bool>) {
    /// Between restarts, so a persistent upstream outage is not a hot loop.
    const BACKOFF: [u64; 5] = [1, 5, 15, 30, 60];
    let lane = consumer.lane;
    let mut restarts = 0usize;

    loop {
        if *shutdown.borrow() {
            return;
        }
        match consumer.run(shutdown.clone()).await {
            Ok(stats) => {
                log::info!(
                    "{} stopped: events={} recorded={} redelivered={} dirty={} \
                     parked={} reconnects={} reconciles={}",
                    lane.stream,
                    stats.events,
                    stats.outcome.recorded,
                    stats.outcome.redelivered,
                    stats.outcome.dirty,
                    stats.outcome.parked,
                    stats.reconnects,
                    stats.reconciles,
                );
            }
            Err(error) => log::error!("{} failed: {error:#}", lane.stream),
        }
        if *shutdown.borrow() {
            return;
        }
        let wait = BACKOFF[restarts.min(BACKOFF.len() - 1)];
        restarts += 1;
        log::warn!("restarting {} in {wait}s (restart {restarts})", lane.stream);
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(wait)) => {}
            _ = shutdown.changed() => {}
        }
    }
}

/// A shutdown signal wired to SIGTERM and SIGINT.
///
/// New to this repo, and not optional: the Dockerfile `exec`s this binary so
/// it becomes PID 1, and PID 1 ignores SIGTERM unless it installs a handler
/// (`pid_namespaces(7)`) — every redeploy would otherwise wait out Railway's
/// full grace period before being killed.
pub fn shutdown_signal() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(error) => {
                    log::error!("cannot listen for SIGTERM: {error}");
                    return;
                }
            };
            tokio::select! {
                _ = term.recv() => log::info!("SIGTERM"),
                _ = tokio::signal::ctrl_c() => log::info!("SIGINT"),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        let _ = tx.send(true);
    });
    rx
}

/// Builds the spec once, for callers that want it without a consumer.
pub async fn current_spec(pool: &PgPool) -> anyhow::Result<SubscriptionSpec> {
    spec::build(pool).await
}
