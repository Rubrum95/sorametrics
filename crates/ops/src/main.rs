//! `sorametrics-ops` — admin / operations CLI.
//!
//! Subcommands:
//!
//! | Command | Phase | Status |
//! |---------|-------|--------|
//! | `decode-block --height N` | 1.2.4 | Done |
//! | `backfill --from N --to M [--concurrency N]` | 1.2.7 | Done |
//! | `load-asset-registry` | 3.3 | Done |
//! | `migrate` | 4 | TODO |
//! | `gap-fill` | 4 | TODO |
//! | `replay --table X --from-block N` | 4 | TODO |

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use anyhow::{Context, Result};

mod etl;
use clap::{Parser, Subcommand};
use sorametrics_db::{connect as db_connect, DbConfig};
use sorametrics_substrate::{decode_block_events, BlockDecodeStats, PriceResolver};
use sorametrics_telemetry::{init as init_telemetry, LogFormat};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Instant;
use subxt::backend::legacy::LegacyRpcMethods;
use subxt::backend::rpc::RpcClient;
use subxt::{OnlineClient, SubstrateConfig};
use tokio::sync::Semaphore;
use tracing::{info, warn};

#[derive(Debug, Parser)]
#[command(name = "sorametrics-ops", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Decode all events of one finalized block by height. Writes to
    /// `sm.live_*` tables via the same UPSERT path as the live
    /// subscriber, so re-running it is idempotent.
    DecodeBlock {
        /// Block height to fetch and decode.
        #[arg(long)]
        height: u64,

        /// Substrate WS endpoint. Default: `wss://ws.mof.sora.org`.
        #[arg(long, env = "WS_ENDPOINT", default_value = "wss://ws.mof.sora.org")]
        rpc: String,
    },

    /// Range backfill: decode every finalized block in `[from, to]`
    /// (inclusive) into `sm.live_*`. Uses bounded concurrency for
    /// throughput. Idempotent — safe to re-run on the same range.
    /// Does NOT advance the live `sm.indexer_state` cursor.
    Backfill {
        /// First block height (inclusive).
        #[arg(long)]
        from: u64,

        /// Last block height (inclusive).
        #[arg(long)]
        to: u64,

        /// How many blocks to fetch+decode in parallel. Each unit holds
        /// one open subxt request to the RPC node, so high values can
        /// stress the upstream. Default 8 is conservative.
        #[arg(long, default_value_t = 8)]
        concurrency: usize,

        /// Substrate WS endpoint. Default: `wss://ws.mof.sora.org`.
        #[arg(long, env = "WS_ENDPOINT", default_value = "wss://ws.mof.sora.org")]
        rpc: String,
    },

    /// Bulk-upsert the asset registry from the upstream sora-xor
    /// whitelist (or any URL that returns the same array shape).
    /// Idempotent — re-running just updates existing rows.
    LoadAssetRegistry {
        /// Source URL. Default: official sora-xor whitelist on GitHub.
        #[arg(
            long,
            default_value = "https://raw.githubusercontent.com/sora-xor/polkaswap-token-whitelist-config/master/whitelist.json"
        )]
        url: String,
    },

    /// One-way ETL from the legacy SoraMetrics PostgreSQL into the v33
    /// schema. Read-only on the source, idempotent + resumable on the
    /// target (keyset cursors in `sm.etl_state`). Ends with a MANDATORY
    /// reconciliation (counts + exact sums per block bucket); the
    /// command fails if any bucket mismatches.
    MigrateLegacy {
        /// Legacy database URL (read-only usage).
        #[arg(long, env = "LEGACY_DATABASE_URL")]
        source_url: String,

        /// Comma-separated table list. Default: all.
        #[arg(
            long,
            default_value = "asset_registry,swaps,transfers,bridges,fees,fee_burns,price_history"
        )]
        tables: String,

        /// Rows per batch.
        #[arg(long, default_value_t = 10_000)]
        batch_size: i64,

        /// Skip reconciliation (NOT recommended — mandatory project step).
        #[arg(long, default_value_t = false)]
        skip_reconcile: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    init_telemetry(LogFormat::Pretty)?;

    let cli = Cli::parse();
    match cli.command {
        Command::DecodeBlock { height, rpc } => decode_block(height, &rpc).await,
        Command::Backfill {
            from,
            to,
            concurrency,
            rpc,
        } => backfill(from, to, concurrency, &rpc).await,
        Command::LoadAssetRegistry { url } => load_asset_registry(&url).await,
        Command::MigrateLegacy {
            source_url,
            tables,
            batch_size,
            skip_reconcile,
        } => {
            let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
            let target = db_connect(&DbConfig {
                url: db_url,
                ..DbConfig::default()
            })
            .await
            .context("connecting to target PostgreSQL")?;
            etl::migrate_legacy(
                target,
                etl::EtlOpts {
                    source_url,
                    tables: tables.split(',').map(|s| s.trim().to_string()).collect(),
                    batch_size,
                    skip_reconcile,
                },
            )
            .await
        }
    }
}

async fn decode_block(height: u64, rpc: &str) -> Result<()> {
    // DB connection. We only need to write — no migrations from ops.
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let db = db_connect(&DbConfig {
        url: db_url,
        ..DbConfig::default()
    })
    .await
    .context("connecting to PostgreSQL")?;

    info!(rpc, height, "subxt connecting");

    // Open a low-level RPC client first so we can use the legacy
    // `chain_getBlockHash` method to resolve `height → hash`. The
    // chain-head v2 API exposed by `OnlineClient` only knows about
    // the head/finalized; for arbitrary historical heights we need
    // legacy. We then upgrade the same RPC client into an
    // `OnlineClient` for the storage + block reads.
    let rpc_client = RpcClient::from_url(rpc)
        .await
        .with_context(|| format!("connecting RPC to {rpc}"))?;
    let legacy = LegacyRpcMethods::<SubstrateConfig>::new(rpc_client.clone());
    let height_u32: u32 = height
        .try_into()
        .with_context(|| format!("block height {height} does not fit in u32"))?;
    let hash = legacy
        .chain_get_block_hash(Some(height_u32.into()))
        .await
        .with_context(|| format!("looking up block hash for height {height}"))?
        .with_context(|| format!("no block at height {height}"))?;

    let client = OnlineClient::<SubstrateConfig>::from_rpc_client(rpc_client)
        .await
        .with_context(|| format!("upgrading RPC client to OnlineClient at {rpc}"))?;

    info!(height, hash = %format_args!("{hash:?}"), "fetching block");
    let block = client
        .blocks()
        .at(hash)
        .await
        .with_context(|| format!("fetching block at {hash:?}"))?;

    // Ops decodes are by definition about the past: value events from
    // their hourly price bucket, never from a live quote.
    let prices = PriceResolver::historical(db.clone())
        .await
        .context("loading asset registry for pricing")?;
    let stats = decode_block_events(&block, &db, &prices)
        .await
        .with_context(|| format!("decoding block at height {height}"))?;

    info!(
        height,
        events = stats.events,
        decoded_swaps = stats.decoded_swaps,
        inserted_swaps = stats.inserted_swaps,
        decoded_transfers = stats.decoded_transfers,
        inserted_transfers = stats.inserted_transfers,
        decoded_bridges = stats.decoded_bridges,
        inserted_bridges = stats.inserted_bridges,
        decoded_fee_burns = stats.decoded_fee_burns,
        inserted_fee_burns = stats.inserted_fee_burns,
        decoded_fees = stats.decoded_fees,
        inserted_fees = stats.inserted_fees,
        "block decoded"
    );

    Ok(())
}

/// Range backfill with bounded concurrency.
///
/// Architecture:
///
/// - One shared `OnlineClient` (subxt manages a single WS connection
///   under the hood; the workers all share it).
/// - One shared `LegacyRpcMethods` for height → hash lookups (also
///   reuses the same RPC client).
/// - One shared `PgPool` (connection-pooled internally by sqlx).
/// - A `Semaphore` caps the number of in-flight `decode_block_events`
///   calls. Each worker grabs a permit, processes one block, releases
///   the permit.
/// - Errors per block are logged and do NOT abort the backfill —
///   `ON CONFLICT DO NOTHING` makes a re-run on a partially-failed
///   range fully safe.
async fn backfill(from: u64, to: u64, concurrency: usize, rpc: &str) -> Result<()> {
    if from > to {
        anyhow::bail!("--from ({from}) must be ≤ --to ({to})");
    }
    if concurrency == 0 {
        anyhow::bail!("--concurrency must be ≥ 1");
    }

    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let db = db_connect(&DbConfig {
        url: db_url,
        ..DbConfig::default()
    })
    .await
    .context("connecting to PostgreSQL")?;

    info!(rpc, from, to, concurrency, "subxt connecting");
    let rpc_client = RpcClient::from_url(rpc)
        .await
        .with_context(|| format!("connecting RPC to {rpc}"))?;
    let legacy = Arc::new(LegacyRpcMethods::<SubstrateConfig>::new(rpc_client.clone()));
    let client = Arc::new(
        OnlineClient::<SubstrateConfig>::from_rpc_client(rpc_client)
            .await
            .with_context(|| format!("upgrading RPC client to OnlineClient at {rpc}"))?,
    );

    let prices = Arc::new(
        PriceResolver::historical(db.clone())
            .await
            .context("loading asset registry for pricing")?,
    );
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let total = to - from + 1;
    let started = Instant::now();
    let mut handles = Vec::with_capacity(total as usize);

    for height in from..=to {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        let client = client.clone();
        let legacy = legacy.clone();
        let db = db.clone();
        let prices = prices.clone();

        handles.push(tokio::spawn(async move {
            // Hold the permit for the lifetime of the task.
            let _permit = permit;
            process_one_block(&client, &legacy, &db, &prices, height).await
        }));
    }

    // Aggregate per-block results.
    let mut totals = BlockDecodeStats::default();
    let mut succeeded: u64 = 0;
    let mut failed: u64 = 0;
    let mut last_log = Instant::now();

    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(stats)) => {
                accumulate(&mut totals, &stats);
                succeeded += 1;
            }
            Ok(Err(e)) => {
                warn!(error = %e, "block process failed, skipped");
                failed += 1;
            }
            Err(join_err) => {
                warn!(error = %join_err, "worker panicked");
                failed += 1;
            }
        }

        // Progress log every 100 blocks completed or every 10 seconds,
        // whichever comes first. Rate-limit the chatter on long runs.
        let processed = (idx + 1) as u64;
        if processed % 100 == 0 || last_log.elapsed().as_secs() >= 10 {
            let elapsed = started.elapsed();
            let rate = processed as f64 / elapsed.as_secs_f64().max(0.001);
            let eta = (total - processed) as f64 / rate.max(0.001);
            info!(
                processed,
                total,
                succeeded,
                failed,
                rate_blocks_s = format!("{rate:.1}"),
                eta_s = format!("{eta:.0}"),
                "backfill progress"
            );
            last_log = Instant::now();
        }
    }

    let elapsed = started.elapsed();
    info!(
        from,
        to,
        total,
        succeeded,
        failed,
        elapsed_s = format!("{:.1}", elapsed.as_secs_f64()),
        events = totals.events,
        decoded_swaps = totals.decoded_swaps,
        inserted_swaps = totals.inserted_swaps,
        decoded_transfers = totals.decoded_transfers,
        inserted_transfers = totals.inserted_transfers,
        decoded_bridges = totals.decoded_bridges,
        inserted_bridges = totals.inserted_bridges,
        decoded_fee_burns = totals.decoded_fee_burns,
        inserted_fee_burns = totals.inserted_fee_burns,
        "backfill complete"
    );

    if failed > 0 {
        anyhow::bail!("{failed}/{total} blocks failed; re-run is safe (UPSERT idempotent)");
    }

    Ok(())
}

/// Lookup the block by height, fetch it, decode its events.
///
/// Errors here are per-block; the backfill caller catches them and keeps
/// going for the rest of the range.
async fn process_one_block(
    client: &OnlineClient<SubstrateConfig>,
    legacy: &LegacyRpcMethods<SubstrateConfig>,
    db: &PgPool,
    prices: &PriceResolver,
    height: u64,
) -> Result<BlockDecodeStats> {
    let height_u32: u32 = height
        .try_into()
        .with_context(|| format!("block height {height} does not fit in u32"))?;
    let hash = legacy
        .chain_get_block_hash(Some(height_u32.into()))
        .await
        .with_context(|| format!("looking up block hash for height {height}"))?
        .with_context(|| format!("no block at height {height}"))?;
    let block = client
        .blocks()
        .at(hash)
        .await
        .with_context(|| format!("fetching block at height {height} ({hash:?})"))?;
    decode_block_events(&block, db, prices)
        .await
        .with_context(|| format!("decoding block {height}"))
}

/// Add per-block stats into a running total.
fn accumulate(total: &mut BlockDecodeStats, one: &BlockDecodeStats) {
    total.events += one.events;
    total.decoded_swaps += one.decoded_swaps;
    total.inserted_swaps += one.inserted_swaps;
    total.decoded_transfers += one.decoded_transfers;
    total.inserted_transfers += one.inserted_transfers;
    total.decoded_bridges += one.decoded_bridges;
    total.inserted_bridges += one.inserted_bridges;
    total.decoded_fee_burns += one.decoded_fee_burns;
    total.inserted_fee_burns += one.inserted_fee_burns;
    total.decoded_fees += one.decoded_fees;
    total.inserted_fees += one.inserted_fees;
}

// =============================================================
// load-asset-registry
// =============================================================

/// One row from the upstream whitelist. Field shape is the
/// sora-xor whitelist convention: `address` (the asset_id), `symbol`,
/// `name`, `decimals`, `icon` (data URL, becomes our `logo`).
///
/// Extra fields in the source JSON are ignored (`#[serde(default)]`
/// on optional fields covers the few that may go missing).
#[derive(Debug, serde::Deserialize)]
struct WhitelistEntry {
    address: String,
    symbol: String,
    #[serde(default)]
    name: Option<String>,
    decimals: i16,
    #[serde(default)]
    icon: Option<String>,
}

/// Fetch the whitelist URL, parse, bulk-upsert into `sm.asset_registry`.
async fn load_asset_registry(url: &str) -> Result<()> {
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let db = db_connect(&DbConfig {
        url: db_url,
        ..DbConfig::default()
    })
    .await
    .context("connecting to PostgreSQL")?;

    info!(url, "fetching asset whitelist");
    let bytes = reqwest::get(url)
        .await
        .with_context(|| format!("GET {url}"))?
        .error_for_status()
        .with_context(|| format!("non-2xx from {url}"))?
        .bytes()
        .await
        .context("reading whitelist response body")?;

    let entries: Vec<WhitelistEntry> =
        serde_json::from_slice(&bytes).context("parsing whitelist JSON (expected array)")?;
    info!(entries = entries.len(), "whitelist parsed");

    let mut inserted = 0u64;
    let mut updated = 0u64;
    let mut skipped = 0u64;

    // One transaction for the whole batch — keeps the registry in a
    // consistent state and is faster than one INSERT per row.
    let mut tx = db.begin().await?;

    for entry in entries {
        // Defensive: the canonical asset_id shape is `0x` + 64 hex.
        // Anything malformed in the upstream feed gets logged + skipped
        // rather than poisoning the whole batch.
        if entry.address.len() != 66
            || !entry.address.starts_with("0x")
            || !entry.address[2..].chars().all(|c| c.is_ascii_hexdigit())
        {
            warn!(
                address = %entry.address,
                symbol = %entry.symbol,
                "skipping malformed asset_id"
            );
            skipped += 1;
            continue;
        }

        let res = sqlx::query!(
            r#"
            INSERT INTO sm.asset_registry (asset_id, symbol, name, decimals, logo, whitelisted, updated_at)
            VALUES ($1, $2, $3, $4, $5, true, NOW())
            ON CONFLICT (asset_id) DO UPDATE SET
                symbol      = EXCLUDED.symbol,
                name        = EXCLUDED.name,
                decimals    = EXCLUDED.decimals,
                logo        = EXCLUDED.logo,
                whitelisted = true,
                updated_at  = NOW()
            RETURNING (xmax = 0) AS "is_insert!"
            "#,
            entry.address,
            entry.symbol,
            entry.name,
            entry.decimals,
            entry.icon,
        )
        .fetch_one(&mut *tx)
        .await
        .with_context(|| format!("upserting asset {}", entry.address))?;

        if res.is_insert {
            inserted += 1;
        } else {
            updated += 1;
        }
    }

    tx.commit().await.context("committing whitelist upsert")?;

    info!(
        inserted,
        updated,
        skipped,
        total = inserted + updated,
        "asset registry loaded"
    );

    Ok(())
}
