mod handlers;

use std::time::Duration;

use actix_web::{middleware::Logger, web, App, HttpServer};
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
    HttpServer::new(move || {
        App::new()
            .app_data(pool.clone())
            .wrap(Logger::default())
            .configure(handlers::configure)
            .default_service(web::route().to(handlers::not_found))
    })
    .bind((config.server.host.as_str(), config.server.port))?
    .run()
    .await?;
    Ok(())
}
