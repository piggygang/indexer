mod handlers;

use actix_web::{middleware::Logger, web, App, HttpServer};
use indexer_config::Config;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let config = Config::try_from_env()?;
    log::info!(
        "indexer-api listening on [{}]:{}",
        config.server.host,
        config.server.port
    );

    HttpServer::new(|| {
        App::new()
            .wrap(Logger::default())
            .configure(handlers::configure)
            .default_service(web::route().to(handlers::not_found))
    })
    .bind((config.server.host.as_str(), config.server.port))?
    .run()
    .await?;
    Ok(())
}
