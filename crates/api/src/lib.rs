//! SoraMetrics v33 query API (axum).
//!
//! Read-only HTTP service over the `sm.*` schema. Mirrors the contract
//! that the Node.js production frontend expects.
//!
//! Phase 3.1 ships:
//! - `/health` — liveness probe
//! - `/health/freshness` — per-table data freshness derived from
//!   `sm.indexer_state` cursor age
//! - `/history/global/{swaps,transfers,bridges,fee_burns}` — paginated
//!   reads of `sm.live_*` tables
//!
//! Future phases add: per-wallet history, tokens/pools/balance
//! endpoints, Polkamarkt/burns/governance, Socket.IO real-time, Redis
//! cache layer, rate limiting.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms, missing_docs)]

pub mod error;
pub mod routes;
pub mod state;
pub mod util;

pub use state::AppState;

use axum::http::StatusCode;
use axum::Router;
use std::time::Duration;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

/// Build the full router for the API service.
pub fn build_router(state: AppState) -> Router {
    let inner = routes::build(state);

    // Wrap with tower-http middleware: structured request tracing +
    // a hard request timeout (returns 504 instead of holding a request
    // open forever). We keep this tower stack thin in 3.1 and add
    // CORS + rate-limit + cache later.
    inner
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::GATEWAY_TIMEOUT,
            Duration::from_secs(30),
        ))
}
