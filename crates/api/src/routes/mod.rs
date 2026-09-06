//! Route layout for the v33 API.
//!
//! Path conventions deliberately mirror the Node.js production
//! contract so the v6 frontend can be pointed at this backend without
//! per-endpoint adjustments. Where new behaviour is needed it ships
//! as `/v33/*` until the cutover.

use crate::AppState;
use axum::Router;

pub mod chain_state;
pub mod fee_config;
pub mod freshness;
pub mod health;
pub mod history;
pub mod identity;
pub mod misc;
pub mod prices;
pub mod stats;
pub mod tokens;
pub mod wallet;

/// Routes answered from the DB / cheap chain reads (30 s budget).
pub fn build(state: AppState) -> Router {
    Router::new()
        .merge(health::router())
        .merge(freshness::router())
        .merge(history::router())
        .merge(tokens::router())
        .merge(prices::router())
        .merge(stats::router())
        .merge(wallet::router())
        .merge(fee_config::router())
        .merge(identity::router())
        .merge(misc::router())
        .with_state(state)
}

/// Routes that walk whole storage maps (`/pools`, `/holders`); they get
/// their own, longer timeout (the Node used 60 s + a 5 min cache).
pub fn build_scans(state: AppState) -> Router {
    chain_state::router().with_state(state)
}
