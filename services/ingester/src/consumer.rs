//! The consumer loop.
//!
//! It owns the durable cursor — `crates/ingest`'s contract is explicit that
//! `last_processed_slot` is persisted **only** on `SlotCheckpoint` — and it
//! owns gap recovery, because this transport cannot replay. Everything else
//! (reconnecting, resubscribing, keepalive) belongs to the adapter.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use indexer_config::Cluster;
use indexer_das::DasClient;
use indexer_data_model::{ingest_state, PgPool};
use indexer_ingest::{IngestEvent, IngestSource, ResumeFrom, StreamStatus, SubscriptionSpec};
use tokio::sync::watch;

use crate::pipeline::{Outcome, Pipeline};
use crate::{reconcile, spec};

/// The durable identity of one transport on one cluster.
///
/// `stream` is the `ingest_state.stream` key the migration specifies
/// (`'<IngestSource::name()>:<label>'`); `source` is the `activity.source` tag
/// this lane's writes carry, so a dual run is measurable one row at a time
/// rather than only in aggregate.
///
/// One `Copy` struct rather than two fields, because the two must never
/// disagree: a consumer that checkpoints one lane and tags its rows with
/// another is the bug this shape forecloses.
///
/// **The cluster is in the key, and slots are why.** A cursor is a slot
/// number, and slot numbers are only comparable on the chain that produced
/// them — devnet's run tens of millions ahead of mainnet's. Checkpoints are
/// written with `GREATEST`, so a cursor that once took the other cluster's
/// slot would sit above this cluster's tip for years, and on a transport with
/// no replay everything below the cursor is dropped. Each cluster has its own
/// Postgres, so the keys never meet in practice; a key that names its cluster
/// is what keeps that true of the data rather than only of the deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lane {
    pub stream: &'static str,
    pub source: &'static str,
}

impl Lane {
    /// Helius Enhanced WebSockets. No replay — `ResumeFrom::Slot` is a floor.
    ///
    /// `mainnet`, not `mainnet-beta`: these are the keys production has
    /// checkpointed under since ALG-623, and the label is not cosmetic. A
    /// renamed key is a cursor nobody reads — the consumer would re-seed from
    /// the DAS backfill's slot and re-walk everything since.
    pub const fn ws(cluster: Cluster) -> Self {
        Self {
            stream: match cluster {
                Cluster::MainnetBeta => "helius-ws:mainnet",
                Cluster::Devnet => "helius-ws:devnet",
            },
            source: "live",
        }
    }

    /// Helius webhooks, received by this service and drained from
    /// `webhook_inbox`.
    pub const fn webhook(cluster: Cluster) -> Self {
        Self {
            stream: match cluster {
                Cluster::MainnetBeta => "helius-webhook:mainnet",
                Cluster::Devnet => "helius-webhook:devnet",
            },
            source: "webhook",
        }
    }
}

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
    /// Told, on every `Connected`, that a transport just (re)connected and a
    /// gap may exist.
    ///
    /// This consumer no longer *runs* a reconcile. It used to spawn one on
    /// every connect, gated by nothing but "one at a time", so a socket that
    /// connects, delivers nothing and times out on the 90 s idle timeout billed
    /// a full catalogue sweep per lap. The scheduler owns the sweep now and
    /// answers the same due-ness question every other job answers, which means
    /// a reconnect storm costs one sweep per interval instead of one per
    /// connect.
    ///
    /// Every lane nudges; there is one scheduler per process, so there is no
    /// double-budget risk to derive around.
    pub nudge: Arc<tokio::sync::Notify>,
    pub source: Arc<dyn IngestSource>,
    pub lane: Lane,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    pub events: u64,
    pub outcome: Outcome,
    pub reconnects: u64,
    /// Connects that asked the scheduler to look at its work, not sweeps run.
    /// A high number here with a flat `reconcile` cadence is the healthy shape:
    /// it means the gate held.
    pub nudges: u64,
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
                             and the reconnect nudges the schedule",
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
                            // Tell the scheduler a transport connected; do not
                            // reconcile here. Cold start, crash restart and
                            // mid-run reconnect are still one path — it is just
                            // a path that now asks the same due-ness question
                            // every scheduled job asks, so a socket that
                            // connects, delivers nothing and times out every
                            // 90 s no longer bills a full catalogue sweep per
                            // lap. What a connect buys is a *lower* interval,
                            // not an exemption: see `schedule::RECONNECT_FLOOR`.
                            last_progress = Instant::now();
                            stats.nudges += 1;
                            self.nudge.notify_one();
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
                        IngestEvent::Status(StreamStatus::Idle) => {
                            // A poll that succeeded and found nothing. Proof of
                            // life, not progress: it feeds the watchdog and
                            // touches no cursor.
                            last_progress = Instant::now();
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
                     parked={} reconnects={} nudges={}",
                    lane.stream,
                    stats.events,
                    stats.outcome.recorded,
                    stats.outcome.redelivered,
                    stats.outcome.dirty,
                    stats.outcome.parked,
                    stats.reconnects,
                    stats.nudges,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Production's two cursors live under exactly these keys. Changing either
    /// string orphans a cursor: the consumer finds no row, re-seeds from the
    /// backfill's slot, and the first sweep replays weeks of activity.
    #[test]
    fn the_mainnet_keys_are_the_ones_production_checkpoints_under() {
        assert_eq!(Lane::ws(Cluster::MainnetBeta).stream, "helius-ws:mainnet");
        assert_eq!(
            Lane::webhook(Cluster::MainnetBeta).stream,
            "helius-webhook:mainnet"
        );
    }

    /// `'<IngestSource::name()>:<label>'`, as the migration specifies — and no
    /// two lanes on any cluster may share a key, because a shared key is one
    /// cursor making a claim about two orderings.
    #[test]
    fn every_lane_has_its_own_key_and_the_cluster_never_changes_the_source_tag() {
        let mut keys = BTreeSet::new();
        for cluster in Cluster::ALL {
            let (ws, webhook) = (Lane::ws(cluster), Lane::webhook(cluster));
            assert!(ws.stream.starts_with("helius-ws:"), "{}", ws.stream);
            assert!(
                webhook
                    .stream
                    .starts_with(&format!("{}:", crate::inbox::NAME)),
                "{}",
                webhook.stream
            );
            assert!(keys.insert(ws.stream), "duplicate key {}", ws.stream);
            assert!(
                keys.insert(webhook.stream),
                "duplicate key {}",
                webhook.stream
            );
            // `activity.source` is a CHECK-constrained column and says which
            // transport wrote a row, not which chain: the database is the
            // cluster, so the tag stays the same on every one.
            assert_eq!(ws.source, "live");
            assert_eq!(webhook.source, "webhook");
        }
        assert_eq!(keys.len(), 2 * Cluster::ALL.len());
    }
}
