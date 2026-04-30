//! `/health` — liveness probe.
//!
//! Always returns `200 OK` with a small JSON body. Does not touch the
//! database — by design, this endpoint is for "is the server up?"
//! checks (load balancers, PM2). For "is the data fresh?" use
//! `/health/freshness`.

use crate::AppState;
use axum::{routing::get, Json, Router};
use serde::Serialize;

/// Build the `/health` sub-router.
pub fn router() -> Router<AppState> {
    Router::new().route("/health", get(health))
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    version: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "sorametrics-api",
        version: env!("CARGO_PKG_VERSION"),
    })
}
