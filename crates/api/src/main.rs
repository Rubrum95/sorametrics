//! `sorametrics-api` — query API entrypoint.
//!
//! Loads config from env, opens the DB pool, starts the axum server.
//! All route logic lives in the library at [`sorametrics_api::routes`].

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use anyhow::{Context, Result};
use sorametrics_api::chain::ChainClient;
use sorametrics_api::state::time_zone_from_env;
use sorametrics_api::{build_router_with_socket, AppState};
use sorametrics_db::{connect as db_connect, DbConfig};
use sorametrics_iroha::{ToriiClient, ToriiConfig};
use sorametrics_telemetry::{init as init_telemetry, LogFormat};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    init_telemetry(LogFormat::Pretty)?;

    let bind: SocketAddr = std::env::var("API_BIND")
        .unwrap_or_else(|_| "127.0.0.1:3001".to_string())
        .parse()
        .context("API_BIND must be a valid socket address")?;

    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let db = db_connect(&DbConfig {
        url: db_url,
        ..DbConfig::default()
    })
    .await
    .context("connecting to PostgreSQL")?;
    info!("DB ready");

    let time_zone = time_zone_from_env().map_err(anyhow::Error::msg)?;
    let chain = ChainClient::from_env().map_err(anyhow::Error::msg)?;
    match &chain {
        Some(c) => info!(endpoints = c.endpoints().len(), "chain client configured"),
        None => info!("WS_ENDPOINTS unset — chain-state routes will answer 503"),
    }
    let torii_cfg = ToriiConfig::from_env().map_err(anyhow::Error::msg)?;
    let torii = ToriiClient::new(torii_cfg).map_err(anyhow::Error::msg)?;
    info!(base = %torii.base_url(), "torii client configured");
    let state = AppState::with_registry(db, time_zone)
        .await
        .context("loading asset registry")?
        .with_chain(chain)
        .with_torii(Some(torii));
    state.spawn_registry_refresh();
    sorametrics_api::routes::staking_rewards::spawn_live_sampler(state.clone());
    sorametrics_api::routes::polkamarkt::spawn_reconcile(state.clone());
    let (app, io) = build_router_with_socket(state.clone());
    sorametrics_api::realtime::spawn(state, io);

    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    info!(bind = %bind, "sorametrics-api listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            info!("ctrl-c received, shutting down");
        })
        .await
        .context("axum serve")?;

    Ok(())
}
