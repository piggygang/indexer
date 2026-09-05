//! `indexer-ingester` — the live pipeline (ALG-623).
//!
//! Boot mirrors the api (retrying connect, advisory-locked migrations) and the
//! admin CLI (`#[tokio::main]`, key resolved where it is needed). What is new
//! here is the supervisor: `IngestError` is terminal by contract, and
//! Railway's default `ON_FAILURE` policy gives up after ten retries, so a
//! long-running consumer that relied on the platform to restart it would
//! eventually stay dead. It restarts itself instead, and reserves a non-zero
//! exit for configuration and database failures a restart cannot fix.

use std::sync::Arc;
use std::time::Duration;

use indexer_config::Config;
use indexer_das::DasClient;
use indexer_ingest::ws::HeliusWs;
use indexer_ingester::consumer::{self, Consumer};

/// Backoff between supervisor restarts, so a persistent upstream outage does
/// not become a hot loop.
const RESTART_BACKOFF: [u64; 5] = [1, 5, 15, 30, 60];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

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
    let consumer = Consumer {
        pool,
        das: DasClient::new(api_key)?,
        source: Arc::new(HeliusWs::new(api_key)),
    };

    let shutdown = consumer::shutdown_signal();
    let mut restarts = 0usize;

    loop {
        if *shutdown.borrow() {
            break;
        }
        match consumer.run(shutdown.clone()).await {
            Ok(stats) => {
                log::info!(
                    "consumer stopped: events={} recorded={} redelivered={} dirty={} \
                     parked={} reconnects={} reconciles={}",
                    stats.events,
                    stats.outcome.recorded,
                    stats.outcome.redelivered,
                    stats.outcome.dirty,
                    stats.outcome.parked,
                    stats.reconnects,
                    stats.reconciles,
                );
                if *shutdown.borrow() {
                    break;
                }
            }
            Err(error) => log::error!("consumer failed: {error:#}"),
        }

        if *shutdown.borrow() {
            break;
        }
        let wait = RESTART_BACKOFF[restarts.min(RESTART_BACKOFF.len() - 1)];
        restarts += 1;
        log::warn!("restarting the consumer in {wait}s (restart {restarts})");
        tokio::time::sleep(Duration::from_secs(wait)).await;
    }

    log::info!("ingester stopped cleanly");
    Ok(())
}
