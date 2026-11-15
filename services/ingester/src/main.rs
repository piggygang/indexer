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
//! cluster and its endpoints are independent variables, so before the schedule
//! or any consumer starts, `SOLANA_RPC_URL` has to state — by its own genesis
//! hash — that it serves `SOLANA_CLUSTER`. The webhook receiver is already up
//! by then, filing deliveries, but its `/health` answers 503 until the check
//! passes: a deployment on the wrong chain, or one that cannot get an answer
//! at all, never passes the deploy gate. See [`indexer_ingester::cluster`].

use std::sync::Arc;
use std::time::Duration;

use actix_web::{web, App, HttpServer};
use indexer_config::{Config, Transport};
use indexer_das::DasClient;
use indexer_ingest::ws::HeliusWs;
use indexer_ingester::cluster::{self, ClusterGate, Verdict};
use indexer_ingester::consumer::{self, Consumer, Lane};
use indexer_ingester::inbox::WebhookInbox;
use indexer_ingester::receiver::{self, WebhookSecret};
use indexer_ingester::schedule;

/// Queued webhook batches waiting on the writer task. Small on purpose: a
/// backlog here means the database is the bottleneck, and a 503 that Helius
/// retries is a better answer than an unbounded queue.
const WRITE_QUEUE: usize = 64;

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

    // Every client in this process is built here and nowhere else, so there is
    // one place that decides which endpoint a `DasClient` dials.
    let das = || DasClient::with_endpoint(rpc_url, api_key);
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
    //
    // Its `/health` answers from this gate, and the gate is shut: the deploy
    // does not go green on a bound port alone.
    let gate = web::Data::new(ClusterGate::new(cluster));
    let http = if config.ingest.runs(Transport::Webhook) {
        Some(serve(
            &config,
            pool.clone(),
            shutdown.clone(),
            gate.clone(),
        )?)
    } else {
        None
    };

    // The cluster check. Everything above only files deliveries; everything
    // below writes ownership and activity, and none of it starts until the RPC
    // endpoint has stated a genesis hash that is this cluster's.
    //
    // A mismatch returns from here with the gate still shut, so the deploy
    // fails and Railway keeps the previous one. An endpoint that does not
    // answer is waited on for as long as it takes — see the module doc for why
    // that is neither a pass nor an exit — and a deploy stuck there fails its
    // healthcheck the same way.
    //
    // One request per round: the loop owns the retrying, and it is the half
    // that can hear a shutdown.
    let verifier = das()?.with_max_attempts(1);
    let verdict = cluster::verify(&verifier, solana, shutdown.clone()).await?;
    if verdict == Verdict::Interrupted {
        if let Some(http) = http {
            if let Err(error) = http.await {
                log::warn!("the webhook receiver ended abnormally: {error}");
            }
        }
        log::info!("ingester stopped before the cluster was verified; nothing was ingested");
        return Ok(());
    }
    gate.open();

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
    gate: web::Data<ClusterGate>,
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
            // What `/health` answers from: which chain, and whether the RPC
            // endpoint has proved it yet.
            .app_data(gate.clone())
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
