//! `indexer-ingester` — the live pipeline (ALG-623) and the webhook transport.
//!
//! Boot mirrors the api (retrying connect, advisory-locked migrations) and the
//! admin CLI (`#[tokio::main]`, key resolved where it is needed). What is new
//! here is the supervisor: `IngestError` is terminal by contract, and Railway's
//! default `ON_FAILURE` policy gives up after ten retries, so a long-running
//! consumer that relied on the platform to restart it would eventually stay
//! dead. It restarts itself instead, and reserves a non-zero exit for
//! configuration and database failures a restart cannot fix.
//!
//! **This binary now also serves HTTP.** The Helius webhook endpoint runs on
//! this same multi-thread tokio runtime — actix-web branches on
//! `Handle::try_current()` and supports exactly that, whereas
//! `#[actix_web::main]` would build a *current-thread* runtime and put every
//! background task here on one thread. The server is spawned independently of
//! the consumer supervisors, so a consumer restart never stops accepting
//! deliveries.

use std::sync::Arc;
use std::time::Duration;

use actix_web::{web, App, HttpServer};
use indexer_config::{Config, Transport};
use indexer_das::DasClient;
use indexer_ingest::ws::HeliusWs;
use indexer_ingester::consumer::{self, Consumer};
use indexer_ingester::inbox::WebhookInbox;
use indexer_ingester::receiver::{self, WebhookSecret};
use indexer_ingester::schedule;

/// Queued webhook batches waiting on the writer task. Small on purpose: a
/// backlog here means the database is the bottleneck, and a 503 that Helius
/// retries is a better answer than an unbounded queue.
const WRITE_QUEUE: usize = 64;

/// The one-shot argument. No arguments is the daemon; `reconcile` runs whatever
/// the schedule says is due and exits, which is what a Railway cron service
/// needs — it starts the container on a schedule and requires it to exit.
const RECONCILE_ONCE: &str = "reconcile";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    // Deliberately not clap: one optional word, and pulling a parser into the
    // ingester for it would be the tail wagging the dog. An unknown argument is
    // an error rather than a silent daemon start — a cron service that quietly
    // became a long-running process would never exit and never run again.
    let mode = std::env::args().nth(1);
    match mode.as_deref() {
        None => {}
        Some(RECONCILE_ONCE) => return reconcile_once().await,
        Some(other) => anyhow::bail!("unknown argument '{other}' (expected none, or 'reconcile')"),
    }

    let config = Config::try_from_env()?;
    let db = &config.database;
    let pool = indexer_data_model::connect_with_retry(
        db.required_url()?,
        db.max_connections,
        Duration::from_secs(db.connect_timeout_secs),
        Duration::from_secs(60),
    )
    .await?;
    // Advisory-locked inside sqlx, so racing the api or admin is safe.
    indexer_data_model::migrate(&pool).await?;
    log::info!("database migrated");

    let api_key = config.helius.required_api_key()?;
    // One throttled client for everything that reconciles, built here and
    // shared, so the scheduled sweep and the one a consumer spawns on
    // `Connected` draw on a single rate budget instead of two. Helius meters
    // DAS at 10 req/s on the Developer plan; two independent limiters set to
    // that value would ask for 20.
    let reconcile_das = DasClient::new(api_key)?.with_rate_limit(config.reconcile.rps);
    let shutdown = consumer::shutdown_signal();

    // The lane that owns the on-`Connected` reconcile is derived, never
    // configured, so retiring a transport moves it automatically.
    let reconciler = config.ingest.reconciler();
    log::info!(
        "transports: {:?} (reconciling lane: {:?})",
        config.ingest.transports,
        reconciler
    );

    // The webhook receiver: spawned first and independently, so it is accepting
    // deliveries before the consumers finish their boot reconcile and keeps
    // accepting them across every consumer restart.
    let http = if config.ingest.runs(Transport::Webhook) {
        Some(serve(&config, pool.clone(), shutdown.clone())?)
    } else {
        None
    };

    // Spawned once, outside the supervisors: the schedule is not part of any
    // consumer's lifecycle, and re-spawning it on every restart would let a
    // flapping stream either starve reconciliation or run it constantly.
    // Due-ness lives in `backfill_state`, so it survives the restarts anyway.
    let schedule = tokio::spawn(schedule::run(
        pool.clone(),
        reconcile_das.clone(),
        match reconciler {
            Some(Transport::Webhook) => consumer::WEBHOOK,
            _ => consumer::WS,
        },
        config.reconcile.clone(),
        config.rarity.clone(),
        shutdown.clone(),
    ));

    // One supervisor per transport. Two `Consumer`s rather than one merged
    // stream because `SlotCheckpoint` carries no stream field: a single cursor
    // cannot make a true statement about two independent orderings, and taking
    // the minimum of both would pin the WebSocket's cursor during a webhook
    // outage — destroying the very diagnostic a dual run exists to produce.
    let mut lanes = Vec::new();
    if config.ingest.runs(Transport::Ws) {
        lanes.push(Consumer {
            pool: pool.clone(),
            das: DasClient::new(api_key)?,
            reconcile_das: (reconciler == Some(Transport::Ws)).then(|| reconcile_das.clone()),
            reconcile_every: Duration::from_secs(config.reconcile.interval_secs),
            source: Arc::new(HeliusWs::new(api_key)),
            lane: consumer::WS,
        });
    }
    if config.ingest.runs(Transport::Webhook) {
        lanes.push(Consumer {
            pool: pool.clone(),
            das: DasClient::new(api_key)?,
            reconcile_das: (reconciler == Some(Transport::Webhook)).then(|| reconcile_das.clone()),
            reconcile_every: Duration::from_secs(config.reconcile.interval_secs),
            source: Arc::new(WebhookInbox::new(
                pool.clone(),
                // Its own budget: a burst of deliveries must not consume the
                // reconcile's rate limit. `getTransaction` is on the 50/s RPC
                // bucket, not DAS's 10/s.
                DasClient::new(api_key)?.with_rate_limit(config.reconcile.rps * 2),
                config.ingest.clone(),
            )),
            lane: consumer::WEBHOOK,
        });
    }

    let workers: Vec<_> = lanes
        .into_iter()
        .map(|consumer| tokio::spawn(consumer::supervise(consumer, shutdown.clone())))
        .collect();
    for worker in workers {
        if let Err(error) = worker.await {
            log::warn!("a consumer supervisor ended abnormally: {error}");
        }
    }

    // The schedule watches the same shutdown signal, so this is a join, not a
    // wait: a sweep in flight finishes its current step and returns.
    if let Err(error) = schedule.await {
        log::warn!("reconciliation schedule ended abnormally: {error}");
    }
    if let Some(http) = http {
        if let Err(error) = http.await {
            log::warn!("the webhook receiver ended abnormally: {error}");
        }
    }
    log::info!("ingester stopped cleanly");
    Ok(())
}

/// Starts the webhook receiver and the task that owns its database writes.
///
/// The writer task runs here, on the main runtime, and is the only thing in the
/// HTTP path that touches the pool — see `receiver`'s module doc for why a
/// connection opened on an actix worker is worth avoiding.
fn serve(
    config: &Config,
    pool: indexer_data_model::PgPool,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let secret = WebhookSecret::new(config.helius.required_webhook_secret()?);
    let (writer, writes) = receiver::writer(pool, WRITE_QUEUE);
    tokio::spawn(writes);

    let secret = web::Data::new(secret);
    let writer = web::Data::new(writer);
    let server = HttpServer::new(move || {
        App::new()
            .app_data(secret.clone())
            .app_data(writer.clone())
            .configure(receiver::configure)
    })
    // Two, not one-per-core: this endpoint serves a few hundred requests a day
    // and each worker holds a 500 ms timer.
    .workers(2)
    // Wired to the process's own signal handling, which also disables actix's.
    // Two independent shutdown owners in a container that is PID 1 is exactly
    // the kind of thing that becomes a mystery during a Railway grace period.
    .shutdown_signal({
        let mut shutdown = shutdown.clone();
        async move {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    return;
                }
            }
        }
    })
    .bind((config.server.host.as_str(), config.server.port))?
    .run();
    log::info!(
        "webhook receiver listening on [{}]:{}{}",
        config.server.host,
        config.server.port,
        "/webhooks/helius"
    );
    Ok(tokio::spawn(async move {
        if let Err(error) = server.await {
            log::error!("webhook receiver stopped: {error}");
        }
    }))
}

/// Runs every due periodic job once, then exits — the cron entry point.
///
/// Shares `schedule::run_due` with the in-process loop, so the cadence lives in
/// `backfill_state.finished_at` and cannot drift between the two callers. A
/// cron tick that arrives while nothing is due is a few cheap queries and an
/// immediate exit, which is what makes a short cron schedule affordable.
async fn reconcile_once() -> anyhow::Result<()> {
    let config = Config::try_from_env()?;
    let db = &config.database;
    let pool = indexer_data_model::connect_with_retry(
        db.required_url()?,
        db.max_connections,
        Duration::from_secs(db.connect_timeout_secs),
        Duration::from_secs(60),
    )
    .await?;
    // Deliberately no `migrate` here: a cron job racing a deploy's migration is
    // a lock contention no one asked for, and the api and the ingester both
    // migrate at boot already.
    let das =
        DasClient::new(config.helius.required_api_key()?)?.with_rate_limit(config.reconcile.rps);
    // Whichever lane owns the reconcile owns the `from_slot` this records.
    let lane = match config.ingest.reconciler() {
        Some(Transport::Webhook) => consumer::WEBHOOK,
        _ => consumer::WS,
    };

    let mut shutdown = consumer::shutdown_signal();
    let started = std::time::Instant::now();
    let completed = schedule::run_due(
        &pool,
        &das,
        lane,
        &config.reconcile,
        &config.rarity,
        &mut shutdown,
    )
    .await;
    log::info!(
        "reconcile pass {} in {:?}",
        if completed { "finished" } else { "abandoned" },
        started.elapsed()
    );
    Ok(())
}
