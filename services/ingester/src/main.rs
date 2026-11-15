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
//!
//! **Nothing is ingested until the cluster is verified** (PIG-113). The
//! cluster and its endpoints are independent variables, so before any consumer
//! or sweep starts, `SOLANA_RPC_URL` has to state — by its own genesis hash —
//! that it serves `SOLANA_CLUSTER`. The check is the first thing the process
//! does with the network, ahead of the database: a deployment pointed at the
//! wrong chain exits without having opened a connection to Postgres. See
//! [`indexer_ingester::cluster`].

use std::sync::Arc;
use std::time::Duration;

use actix_web::{web, App, HttpServer};
use indexer_config::{Config, Transport};
use indexer_das::DasClient;
use indexer_ingest::ws::HeliusWs;
use indexer_ingester::cluster::{self, Verdict};
use indexer_ingester::consumer::{self, Consumer, Lane};
use indexer_ingester::inbox::WebhookInbox;
use indexer_ingester::receiver::{self, WebhookSecret};
use indexer_ingester::schedule;

/// Queued webhook batches waiting on the writer task. Small on purpose: a
/// backlog here means the database is the bottleneck, and a 503 that Helius
/// retries is a better answer than an unbounded queue.
const WRITE_QUEUE: usize = 64;

/// How long the cluster check runs before anything else happens.
///
/// The webhook receiver's `/health` is the deploy gate, so the order matters.
/// An endpoint on the wrong cluster must fail the *deploy* — exit before the
/// port is bound, so Railway keeps the previous deployment — rather than pass
/// the healthcheck and then crash-loop with nothing left to fall back to. But
/// an endpoint that simply is not answering must not hold the receiver back
/// for the length of an outage: filing a delivery needs no RPC, and Helius
/// drops what it cannot deliver after about three tries.
///
/// So the check gets this long alone, first. One answer takes well under a
/// second and the client's own four attempts span about 3.5 s, so this covers
/// a slow answer and a retry round, and still leaves the database its 60 s
/// inside the 120 s healthcheck window. If it has not answered by then, boot
/// carries on to the receiver and the check resumes — unbounded — before
/// anything that writes is started.
const CLUSTER_CHECK_HEAD_START: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let config = Config::try_from_env()?;
    // Resolved before the database is dialled: a missing endpoint is a
    // configuration error, and it should not take a 60 s connect budget to be
    // told so. No endpoint is derived from the cluster, so each one this
    // process will dial has to be present by name.
    let solana = &config.solana;
    let cluster = solana.cluster;
    let rpc_url = solana.required_rpc_url()?;
    let ws_url = if config.ingest.runs(Transport::Ws) {
        Some(solana.required_ws_url()?)
    } else {
        None
    };
    let api_key = config.helius.required_api_key()?;
    log::info!("cluster: {}", solana.describe());

    // Every client in this process is built here and nowhere else, so there is
    // one place that decides which endpoint a `DasClient` dials.
    let das = || DasClient::with_endpoint(rpc_url, api_key);

    // The cluster check, first and alone — see CLUSTER_CHECK_HEAD_START. A
    // mismatch returns from here: no database connection, no bound port.
    let verifier = das()?;
    let verified = cluster::verify_within(&verifier, solana, CLUSTER_CHECK_HEAD_START).await?;

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

    // One throttled client for everything that reconciles. There is exactly
    // one reconciling task in this process now — the schedule — so this is the
    // whole reconcile budget rather than one share of it. Helius meters DAS at
    // 10 req/s on the Developer plan; two independent limiters set to that
    // value would ask for 20.
    let reconcile_das = das()?.with_rate_limit(config.reconcile.rps);
    let shutdown = consumer::shutdown_signal();

    // Every consumer signals this on `Connected`; the schedule decides what to
    // do about it. A connect no longer runs a sweep of its own — it lowers the
    // sweep's interval to `schedule::RECONNECT_FLOOR`, which is what stops a
    // reconnect storm from being a sweep storm.
    let nudge = Arc::new(tokio::sync::Notify::new());

    // Whose cursor a sweep records as its `from_slot`: derived, never
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

    // No answer inside the head start: the receiver is up and filing, and the
    // check resumes here for as long as it takes. Still nothing is ingested
    // until it passes — the schedule and the consumers below are what write.
    let verdict = if verified {
        Verdict::Verified
    } else {
        cluster::verify(&verifier, solana, shutdown.clone()).await?
    };
    if verdict == Verdict::Interrupted {
        if let Some(http) = http {
            if let Err(error) = http.await {
                log::warn!("the webhook receiver ended abnormally: {error}");
            }
        }
        log::info!("ingester stopped before the cluster was verified; nothing was ingested");
        return Ok(());
    }

    // Spawned once, outside the supervisors: the schedule is not part of any
    // consumer's lifecycle, and re-spawning it on every restart would let a
    // flapping stream either starve reconciliation or run it constantly.
    // Due-ness lives in `backfill_state`, so it survives the restarts anyway.
    let schedule = tokio::spawn(schedule::run(
        pool.clone(),
        reconcile_das.clone(),
        match reconciler {
            Some(Transport::Webhook) => Lane::webhook(cluster),
            _ => Lane::ws(cluster),
        },
        config.reconcile.clone(),
        config.rarity.clone(),
        nudge.clone(),
        shutdown.clone(),
    ));

    // One supervisor per transport. Two `Consumer`s rather than one merged
    // stream because `SlotCheckpoint` carries no stream field: a single cursor
    // cannot make a true statement about two independent orderings, and taking
    // the minimum of both would pin the WebSocket's cursor during a webhook
    // outage — destroying the very diagnostic a dual run exists to produce.
    let mut lanes = Vec::new();
    if let Some(ws_url) = ws_url {
        lanes.push(Consumer {
            pool: pool.clone(),
            das: das()?,
            nudge: nudge.clone(),
            source: Arc::new(HeliusWs::new(ws_url, api_key)),
            lane: Lane::ws(cluster),
        });
    }
    if config.ingest.runs(Transport::Webhook) {
        lanes.push(Consumer {
            pool: pool.clone(),
            das: das()?,
            nudge: nudge.clone(),
            source: Arc::new(WebhookInbox::new(
                pool.clone(),
                // Its own budget: a burst of deliveries must not consume the
                // reconcile's rate limit. `getTransaction` is on the 50/s RPC
                // bucket, not DAS's 10/s.
                das()?.with_rate_limit(config.reconcile.rps * 2),
                config.ingest.clone(),
            )),
            lane: Lane::webhook(cluster),
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
    // What `/health` reports, so the deploy gate also says which chain.
    let cluster = web::Data::new(config.solana.cluster);
    let server = HttpServer::new(move || {
        App::new()
            .app_data(secret.clone())
            .app_data(writer.clone())
            .app_data(cluster.clone())
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
