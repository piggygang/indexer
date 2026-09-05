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
use crate::{reconcile, spec};

/// `ingest_state.stream` — `'<IngestSource::name()>:<label>'`, per the
/// migration.
pub const STREAM: &str = "helius-ws:mainnet";

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
    pub source: Arc<dyn IngestSource>,
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
            "live",
        );

        let resume = reconcile::seed_cursor(&self.pool, STREAM).await?;
        let (spec_tx, spec_rx) = watch::channel(spec::build(&self.pool).await?);
        log::info!(
            "resuming {STREAM} from {} with {} filter(s)",
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
                                        stats.reconciles += 1;
                                        let from = ingest_state::last_processed_slot(&self.pool, STREAM).await?;
                                        let report = reconcile::run(&self.pool, &self.das, &pipeline, from)
                                            .await?;
            report.log("reconcile");
                                        last_progress = Instant::now();
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
            ingest_state::checkpoint(&self.pool, STREAM, slot).await?;
        }
        Ok(())
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
