//! `--source=iroha`: Minamoto Torii poller writing `mn.*`.
//!
//! One job per resource, independent intervals, isolated failures
//! (`minamoto/indexer.js`). Every run is recorded in
//! `mn.indexer_state` so `/api/minamoto/indexer/state` shows operator
//! truth. Chain feeds (blocks / transactions / instructions) walk the
//! explorer cursors newest-first; a one-shot backfill at boot pages the
//! whole history, and a reset (a height re-served with a different
//! hash) truncates the chain tables and backfills again.

pub mod config;
pub mod jobs;

pub use config::IrohaConfig;

use anyhow::{Context, Result};
use jobs::{Ctx, JobFn};
use sorametrics_db::mn::record_indexer_run;
use sorametrics_iroha::ToriiClient;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{info, warn};

/// Runs the poller until `cancel` flips to `true`.
pub async fn run_iroha(cfg: IrohaConfig, db: PgPool, cancel: watch::Receiver<bool>) -> Result<()> {
    let torii = ToriiClient::new(cfg.torii.clone()).context("building Torii client")?;
    info!(base = %torii.base_url(), "starting iroha ingest");
    let ctx = Arc::new(Ctx {
        db,
        torii,
        backfill_max_pages: cfg.backfill_max_pages,
        metrics_retention_days: cfg.metrics_retention_days,
    });

    let periodic: [(&'static str, Duration, JobFn); 13] = [
        ("network_state", cfg.poll_network, jobs::network_state),
        ("blocks", cfg.poll_blocks, jobs::blocks),
        ("transactions", cfg.poll_tx, jobs::transactions),
        ("instructions", cfg.poll_tx, jobs::instructions),
        ("domains", cfg.poll_domains, jobs::domains),
        ("accounts", cfg.poll_accounts, jobs::accounts),
        ("assets", cfg.poll_assets, jobs::assets),
        (
            "asset_definitions",
            cfg.poll_assets,
            jobs::asset_definitions,
        ),
        ("peers", cfg.poll_peers, jobs::peers),
        ("prometheus", cfg.poll_prom, jobs::prometheus),
        (
            "metrics_cleanup",
            cfg.metrics_cleanup,
            jobs::metrics_cleanup,
        ),
        ("claims_enrich", cfg.poll_tx * 2, jobs::claims_enrich),
        (
            "claims_v2_resolve",
            cfg.poll_tx * 2,
            jobs::claims_v2_resolve,
        ),
    ];
    let mut handles = Vec::new();
    for (i, (name, every, f)) in periodic.into_iter().enumerate() {
        let ctx = ctx.clone();
        let cancel = cancel.clone();
        // Stagger boot so the jobs do not hit Torii at the same instant.
        let stagger = Duration::from_millis(150 * i as u64);
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(stagger).await;
            schedule(ctx, name, every, cancel, f).await;
        }));
    }

    // One-shot history backfill: blocks first (FK), then transactions,
    // then instructions. Errors are recorded, never fatal.
    {
        let ctx = ctx.clone();
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let one_shot: [(&str, JobFn); 3] = [
                ("blocks_backfill", jobs::blocks_backfill),
                ("transactions_backfill", jobs::transactions_backfill),
                ("instructions_backfill", jobs::instructions_backfill),
            ];
            for (name, f) in one_shot {
                run_once(&ctx, name, f).await;
            }
        }));
    }

    let mut cancel_wait = cancel.clone();
    while !*cancel_wait.borrow() {
        if cancel_wait.changed().await.is_err() {
            break;
        }
    }
    info!("iroha ingest stopping");
    for h in handles {
        h.abort();
    }
    Ok(())
}

/// Runs one job and records the outcome in `mn.indexer_state`.
async fn run_once(ctx: &Ctx, name: &str, f: JobFn) {
    let started = Instant::now();
    match f(ctx).await {
        Ok(value) => {
            info!(job = name, elapsed_ms = started.elapsed().as_millis() as u64, result = %value, "job ok");
            if let Err(e) = record_indexer_run(&ctx.db, name, true, &value, None).await {
                warn!(job = name, error = %e, "could not record job run");
            }
        }
        Err(e) => {
            let msg = format!("{e:#}");
            warn!(job = name, error = %msg, "job FAILED");
            if let Err(e2) =
                record_indexer_run(&ctx.db, name, false, &serde_json::json!({}), Some(&msg)).await
            {
                warn!(job = name, error = %e2, "could not record job error");
            }
        }
    }
}

/// Runs `f` now, then again `every` after each completion.
async fn schedule(
    ctx: Arc<Ctx>,
    name: &'static str,
    every: Duration,
    mut cancel: watch::Receiver<bool>,
    f: JobFn,
) {
    loop {
        if *cancel.borrow() {
            return;
        }
        run_once(&ctx, name, f).await;
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = cancel.changed() => return,
        }
    }
}
