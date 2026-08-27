use actix_web::{get, HttpResponse};

/// Liveness only — no dependency checks, so the hello deploy stays green
/// before Postgres exists. `/ready` (DB ping) arrives with ALG-619.
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

#[cfg(test)]
mod tests {
    use actix_web::{test, App};

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
}
