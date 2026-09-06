//! Route layout for the v33 API.
//!
//! Path conventions deliberately mirror the Node.js production
//! contract so the v6 frontend can be pointed at this backend without
//! per-endpoint adjustments. Where new behaviour is needed it ships
//! as `/v33/*` until the cutover.

use crate::AppState;
use axum::Router;

pub mod freshness;
pub mod health;
pub mod history;
pub mod prices;
pub mod stats;
pub mod tokens;
pub mod wallet;

/// Construct the `/`-mounted router with every route the API serves.
pub fn build(state: AppState) -> Router {
    Router::new()
        .merge(health::router())
        .merge(freshness::router())
        .merge(history::router())
        .merge(tokens::router())
        .merge(prices::router())
        .merge(stats::router())
        .merge(wallet::router())
        .with_state(state)
}
