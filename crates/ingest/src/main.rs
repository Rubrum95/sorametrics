//! `sorametrics-ingest` — ingestion worker entrypoint.
//!
//! Phase 1.1 wired the substrate WS connection layer + healthcheck.
//! Phase 1.2.1 added the subxt finalized-block subscriber.
//! Phase 1.2.2 wires the swap decoder + DB writes (`sm.live_swaps`).
//! The price sampler task (popular assets → `ts.price_history`) runs
//! alongside the subscriber.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

mod config;
mod substrate;

use anyhow::{Context, Result};
use clap::Parser;
use config::{Cli, Source, SubstrateConfig};
use sorametrics_db::{connect as db_connect, migrate as db_migrate, DbConfig};
use sorametrics_telemetry::{init as init_telemetry, LogFormat};
use std::process;
use std::time::Duration;
use substrate::{
    run_decoder_loop, run_health_loop, run_price_sampler, HealthOutcome, WsConnection,
};
use tokio::sync::watch;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env if present (no-op in production where PM2 sets env directly).
    let _ = dotenvy::dotenv();

    init_telemetry(LogFormat::Pretty)?;
    let cli = Cli::parse();

    match cli.source {
        Source::Substrate => run_substrate().await,
        Source::Iroha => {
            warn!("--source=iroha not yet implemented (Phase 2)");
            Ok(())
        }
        Source::Sorafs => {
            warn!("--source=sorafs not yet implemented (Phase ≥ 6)");
            Ok(())
        }
    }
}

async fn run_substrate() -> Result<()> {
    let cfg = SubstrateConfig::from_env().context("loading substrate config from env")?;
    info!(
        endpoints = cfg.ws_endpoints.len(),
        primary = %cfg.ws_endpoints[0],
        "starting substrate ingest"
    );

    // Connect to the database and apply pending migrations. This is the
    // place where v33's invariant "DB is up before we accept events" is
    // enforced — failing here is fatal.
    let db_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL must be set for substrate ingest")?;
    let db_cfg = DbConfig {
        url: db_url,
        ..DbConfig::default()
    };
    let db = db_connect(&db_cfg)
        .await
        .context("connecting to PostgreSQL")?;
    db_migrate(&db)
        .await
        .context("applying pending migrations")?;
    info!("DB ready");

    let conn = WsConnection::connect(cfg.ws_endpoints.clone(), cfg.connect_timeout)
        .await
        .context("initial substrate WS connect")?;

    // Smoke-test: log peer count + sync state.
    match conn.system_health().await {
        Ok(h) => info!(
            peers = h.peers,
            is_syncing = h.is_syncing,
            "substrate node responsive"
        ),
        Err(e) => warn!(error = %e, "system_health failed post-connect (will retry in loop)"),
    }

    // Cancellation channel for ctrl-c → graceful exit.
    let (cancel_tx, cancel_rx) = watch::channel(false);

    // Healthcheck task: liveness + primary recovery on the lightweight
    // jsonrpsee `WsConnection`.
    let conn_for_health = conn.clone();
    let cancel_health = cancel_rx.clone();
    let cfg_health = cfg.clone();
    let healthcheck_handle = tokio::spawn(async move {
        run_health_loop(
            conn_for_health,
            cfg_health.healthcheck_interval,
            cfg_health.primary_probe_interval,
            cfg_health.connect_timeout,
            cancel_health,
        )
        .await
    });

    // Subscriber task: subxt-side finalized block stream + decoders + DB writes.
    // Uses its own connection so that healthcheck and decode paths are
    // independent (a stuck decoder won't suppress liveness signals).
    let subscriber_endpoints = cfg.ws_endpoints.clone();
    let subscriber_backoff = Duration::from_secs(5);
    let cancel_subscriber = cancel_rx.clone();
    let db_for_subscriber = db.clone();
    let subscriber_handle = tokio::spawn(async move {
        run_decoder_loop(
            subscriber_endpoints,
            db_for_subscriber,
            subscriber_backoff,
            cancel_subscriber,
        )
        .await
    });

    // Price sampler task: popular assets → ts.price_history every
    // `price_sample_interval`, on its own RPC connection.
    let sampler_endpoints = cfg.ws_endpoints.clone();
    let cancel_sampler = cancel_rx.clone();
    let db_for_sampler = db.clone();
    let sampler_period = cfg.price_sample_interval;
    let sweep_period = cfg.price_sweep_interval;
    let sampler_handle = tokio::spawn(async move {
        run_price_sampler(
            sampler_endpoints,
            db_for_sampler,
            sampler_period,
            sweep_period,
            subscriber_backoff,
            cancel_sampler,
        )
        .await
    });

    // Wait for: ctrl-c | healthcheck signals primary-recovery | subscriber
    // or sampler fatal err.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received ctrl-c, shutting down");
            let _ = cancel_tx.send(true);
        }
        outcome = healthcheck_handle => {
            match outcome.context("healthcheck task panicked")? {
                HealthOutcome::PrimaryRecovered { primary } => {
                    info!(primary = %primary, "primary recovered — exiting 0 for PM2 to restart on primary");
                    let _ = cancel_tx.send(true);
                    process::exit(0);
                }
                HealthOutcome::Cancelled => {
                    info!("healthcheck loop cancelled");
                }
            }
        }
        result = subscriber_handle => {
            let inner = result.context("subscriber task panicked")?;
            match inner {
                Ok(()) => info!("subscriber task exited cleanly"),
                Err(e) => warn!(error = %e, "subscriber task ended with error"),
            }
            let _ = cancel_tx.send(true);
        }
        result = sampler_handle => {
            let inner = result.context("price sampler task panicked")?;
            match inner {
                Ok(()) => info!("price sampler exited cleanly"),
                Err(e) => warn!(error = %e, "price sampler ended with error"),
            }
            let _ = cancel_tx.send(true);
        }
    }

    Ok(())
}
