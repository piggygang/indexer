use std::time::Duration;

use actix_web::{get, web, HttpResponse};
use indexer_data_model::PgPool;

/// Liveness only — no dependency checks, so the Railway healthcheck (deploy
/// gate) never depends on Postgres. Readiness is `/ready`.
/// GIT_SHA is a compile-time env set by the Dockerfile from Railway's
/// RAILWAY_GIT_COMMIT_SHA build arg; local builds report "unknown".
#[get("/health")]
pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "service": env!("CARGO_PKG_NAME"),
        "version": env!("CARGO_PKG_VERSION"),
        "commit": match option_env!("GIT_SHA") {
            Some(sha) if !sha.is_empty() => sha,
            _ => "unknown",
        },
    }))
}

/// Readiness: a bounded DB ping. Outside `/v1` and outside the OpenAPI
/// contract, like `/health`. The pool is optional so a route table built
/// without one (tests) answers 503 rather than failing to extract.
#[get("/ready")]
pub async fn ready(pool: Option<web::Data<PgPool>>) -> HttpResponse {
    let Some(pool) = pool else {
        return unavailable("no database pool configured");
    };
    match indexer_data_model::ping(&pool, Duration::from_secs(2)).await {
        Ok(()) => HttpResponse::Ok().json(serde_json::json!({ "status": "ready" })),
        Err(e) => unavailable(&format!("{e:#}")),
    }
}

fn unavailable(reason: &str) -> HttpResponse {
    HttpResponse::ServiceUnavailable()
        .json(serde_json::json!({ "status": "unavailable", "reason": reason }))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use actix_web::{test, web, App};

    #[actix_web::test]
    async fn health_returns_ok() {
        let app = test::init_service(App::new().configure(crate::handlers::configure)).await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/health").to_request()).await;
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "ok");
        assert_eq!(body["service"], "indexer-api");
    }

    #[actix_web::test]
    async fn ready_without_pool_is_503() {
        let app = test::init_service(App::new().configure(crate::handlers::configure)).await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/ready").to_request()).await;
        assert_eq!(resp.status(), 503);
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "unavailable");
    }

    #[actix_web::test]
    #[ignore = "needs DATABASE_URL (run with -- --include-ignored)"]
    async fn ready_with_pool_is_200() {
        dotenv::dotenv().ok();
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = indexer_data_model::connect(&url, 1, Duration::from_secs(5))
            .await
            .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pool))
                .configure(crate::handlers::configure),
        )
        .await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/ready").to_request()).await;
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "ready");
    }
}
