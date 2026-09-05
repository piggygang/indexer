pub mod health;
pub mod v1;

use actix_web::{web, HttpResponse};

// Route registration lives here (not inline in main) so tests can build the
// identical route table via `App::new().configure(handlers::configure)`.
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(health::health).service(health::ready);
    v1::configure(cfg);
}

/// The catch-all, in the contract's error shape.
///
/// It used to answer `{"error": "not found"}` — a space rather than the
/// `not_found` enum member, and no `message`. Anything a client can reach
/// under `/v1` by typo should still parse with the generated client.
pub async fn not_found() -> HttpResponse {
    HttpResponse::NotFound().json(serde_json::json!({
        "error": "not_found",
        "message": "no such endpoint",
        "details": null,
    }))
}
