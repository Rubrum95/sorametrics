//! `sorametrics-api` — query API entrypoint.
//!
//! Loads config from env, opens the DB pool, starts the axum server.
//! All route logic lives in the library at [`sorametrics_api::routes`].

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use anyhow::{Context, Result};
use sorametrics_api::state::time_zone_from_env;
use sorametrics_api::{build_router, AppState};
use sorametrics_db::{connect as db_connect, DbConfig};
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
    let state = AppState::with_registry(db, time_zone)
        .await
        .context("loading asset registry")?;
    state.spawn_registry_refresh();
    let app = build_router(state);

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
