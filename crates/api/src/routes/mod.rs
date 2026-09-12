//! Route layout for the v33 API.
//!
//! Path conventions deliberately mirror the Node.js production
//! contract so the v6 frontend can be pointed at this backend without
//! per-endpoint adjustments. Where new behaviour is needed it ships
//! as `/v33/*` until the cutover.

use crate::AppState;
use axum::Router;

pub mod analytics;
pub mod burns;
pub mod chain_state;
pub mod explorer;
pub mod export;
pub mod extrinsics;
pub mod fee_config;
pub mod freshness;
pub mod frontend;
pub mod governance;
pub mod health;
pub mod history;
pub mod identity;
pub mod liquidity;
pub mod lookup;
pub mod media;
pub mod minamoto;
pub mod misc;
pub mod order_book;
pub mod polkamarkt;
pub mod pool_providers;
pub mod prices;
pub mod staking;
pub mod staking_rewards;
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
        .merge(staking::router())
        .merge(staking_rewards::router())
        .merge(stats::router())
        .merge(wallet::router())
        .merge(fee_config::router())
        .merge(export::router())
        .merge(extrinsics::router())
        .merge(identity::router())
        .merge(liquidity::router())
        .merge(order_book::router())
        .merge(misc::router())
        .merge(lookup::router())
        .merge(media::router())
        .merge(polkamarkt::router())
        .merge(governance::router())
        .merge(explorer::router())
        .merge(burns::router())
        .merge(minamoto::router())
        .merge(analytics::router())
        .merge(frontend::router())
        .with_state(state)
}

/// Routes that walk whole storage maps (`/pools`, `/holders`); they get
/// their own, longer timeout (the Node used 60 s + a 5 min cache).
pub fn build_scans(state: AppState) -> Router {
    chain_state::router()
        .merge(pool_providers::router())
        .merge(burns::scan_router())
        .merge(governance::scan_router())
        .with_state(state)
}
