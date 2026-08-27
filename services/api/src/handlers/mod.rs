pub mod health;

use actix_web::{web, HttpResponse};

// Route registration lives here (not inline in main) so tests can build the
// identical route table via `App::new().configure(handlers::configure)`.
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(health::health);
}

pub async fn not_found() -> HttpResponse {
    HttpResponse::NotFound().json(serde_json::json!({ "error": "not found" }))
}
