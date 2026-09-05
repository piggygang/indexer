use std::time::Duration;

use actix_web::{middleware::Logger, web, App, HttpServer};
use indexer_api::cache::ResponseCache;
use indexer_api::handlers;
use indexer_config::Config;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let config = Config::try_from_env()?;
    let db = &config.database;
    // Connectivity is retried (a Postgres blip must not become a crash loop
    // under Railway's ON_FAILURE policy) but stays under the 120 s
    // healthcheck window; a failing migration is fatal on purpose — a bad
    // schema change must reject the deploy.
    let pool = indexer_data_model::connect_with_retry(
        db.required_url()?,
        db.max_connections,
        Duration::from_secs(db.connect_timeout_secs),
        Duration::from_secs(60),
    )
    .await?;
    // Advisory-locked inside sqlx, so a concurrent admin/ingester run is safe.
    indexer_data_model::migrate(&pool).await?;
    log::info!("database migrated");

    log::info!(
        "indexer-api listening on [{}]:{}",
        config.server.host,
        config.server.port
    );
    let pool = web::Data::new(pool);
    // One cache for the whole process, not one per worker thread: the point is
    // that a burst of identical requests costs one query, and per-worker caches
    // would multiply that by the worker count.
    let cache = web::Data::new(ResponseCache::default());
    HttpServer::new(move || {
        App::new()
            .app_data(pool.clone())
            .app_data(cache.clone())
            .wrap(Logger::default())
            // Public, unauthenticated, read-only: there are no credentials or
            // cookies to protect, and the Explorer's preview deployments get a
            // fresh origin on every push, which an allowlist would break.
            .wrap(
                actix_cors::Cors::default()
                    .allow_any_origin()
                    .allowed_methods(vec!["GET", "HEAD", "OPTIONS"])
                    .allowed_headers(vec![
                        actix_web::http::header::IF_NONE_MATCH,
                        actix_web::http::header::ACCEPT,
                    ])
                    .max_age(3600),
            )
            .configure(handlers::configure)
            .default_service(web::route().to(handlers::not_found))
    })
    .bind((config.server.host.as_str(), config.server.port))?
    .run()
    .await?;
    Ok(())
}
