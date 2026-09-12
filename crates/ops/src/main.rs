//! `sorametrics-ops` — admin / operations CLI.
//!
//! Subcommands:
//!
//! | Command | Phase | Status |
//! |---------|-------|--------|
//! | `decode-block --height N` | 1.2.4 | Done |
//! | `backfill --from N --to M [--concurrency N]` | 1.2.7 | Done |
//! | `load-asset-registry` | 3.3 | Done |
//! | `migrate-legacy --source-url … --tables …` | 4 | Done |
//! | `gap-fill --from N --to M [--dry-run]` | 4 | Done |
//! | `replay --table X --from-block N` | 4 | TODO |

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use anyhow::{Context, Result};

mod etl;
mod etl_mn;
use clap::{Parser, Subcommand};
use sorametrics_db::{connect as db_connect, DbConfig};
use sorametrics_substrate::{decode_block_events, BlockDecodeStats, PriceResolver};
use sorametrics_telemetry::{init as init_telemetry, LogFormat};
use sqlx::PgPool;
use std::collections::HashMap;
use std::process;
use std::sync::Arc;
use std::time::Instant;
use subxt::backend::legacy::LegacyRpcMethods;
use subxt::backend::rpc::RpcClient;
use subxt::utils::H256;
use subxt::{OnlineClient, SubstrateConfig};
use tokio::sync::{Mutex, Semaphore};
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

        /// Archive RPC for quotes at the block of events with no price
        /// bucket (from block 24 943 612 on). Unset: `usd_value` stays NULL.
        #[arg(long, env = "PRICE_ARCHIVE_RPC")]
        price_rpc: Option<String>,
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

        /// Decode blocks of earlier runtimes with the metadata the node
        /// served at that block (archive node required). Without it a
        /// block of another spec fails, as in the live subscriber.
        #[arg(long, default_value_t = false)]
        era_metadata: bool,

        /// Substrate WS endpoint. Default: `wss://ws.mof.sora.org`.
        #[arg(long, env = "WS_ENDPOINT", default_value = "wss://ws.mof.sora.org")]
        rpc: String,

        /// Archive RPC for quotes at the block of events with no price
        /// bucket (from block 24 943 612 on). Unset: `usd_value` stays NULL.
        #[arg(long, env = "PRICE_ARCHIVE_RPC")]
        price_rpc: Option<String>,
    },

    /// Fill the holes of `[from, to]`: every height with no row in
    /// `sm.extrinsics` (each block carries at least `timestamp.set`) is
    /// decoded through the backfill path. Idempotent.
    GapFill {
        /// First block height (inclusive).
        #[arg(long)]
        from: u64,

        /// Last block height (inclusive).
        #[arg(long)]
        to: u64,

        /// Blocks fetched + decoded in parallel.
        #[arg(long, default_value_t = 8)]
        concurrency: usize,

        /// Decode blocks of earlier runtimes with their own metadata
        /// (see `backfill --era-metadata`).
        #[arg(long, default_value_t = false)]
        era_metadata: bool,

        /// Only report the missing heights (count + first ranges).
        #[arg(long, default_value_t = false)]
        dry_run: bool,

        /// Substrate WS endpoint. Default: `wss://ws.mof.sora.org`.
        #[arg(long, env = "WS_ENDPOINT", default_value = "wss://ws.mof.sora.org")]
        rpc: String,

        /// Archive RPC for quotes at the block of events with no price
        /// bucket (from block 24 943 612 on). Unset: `usd_value` stays NULL.
        #[arg(long, env = "PRICE_ARCHIVE_RPC")]
        price_rpc: Option<String>,
    },

    /// Compare the pinned runtime metadata with the node's, pallet by
    /// pallet (metadata hash). Exit 0 when every pinned pallet is
    /// identical, 2 when any drifted or is missing — a runtime upgrade
    /// that needs `subxt metadata` regeneration and a decoder review.
    MetadataCheck {
        /// Substrate WS endpoint. Default: `wss://mof2.sora.org` (archive).
        #[arg(long, env = "WS_ENDPOINT", default_value = "wss://mof2.sora.org")]
        rpc: String,

        /// Compare against the metadata served at this height instead of
        /// the head (archive node required).
        #[arg(long)]
        height: Option<u32>,
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
            default_value = "asset_registry,swaps,transfers,bridges,fees,fee_burns,price_history,liquidity,extrinsics,order_book,val_staking_rewards,supply_snapshots,supply_history,news_episodes,polkamarkt_markets,polkamarkt_trades,polkamarkt_claims,polkamarkt_buybacks,polkamarkt_burns,site_daily,site_events,mn_blocks,mn_accounts,mn_transactions,mn_instructions,mn_domains,mn_asset_definitions,mn_assets,mn_peers,mn_network_state,mn_indexer_state,mn_metrics_snapshots"
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
    // Only the install directory's own `.env`: `dotenvy::dotenv()` walks up
    // the parents and would load a neighbouring project's file.
    let _ = dotenvy::from_path(".env");
    init_telemetry(LogFormat::Pretty)?;

    let cli = Cli::parse();
    match cli.command {
        Command::DecodeBlock {
            height,
            rpc,
            price_rpc,
        } => decode_block(height, &rpc, price_rpc.as_deref()).await,
        Command::Backfill {
            from,
            to,
            concurrency,
            era_metadata,
            rpc,
            price_rpc,
        } => {
            backfill(
                from,
                to,
                concurrency,
                era_metadata,
                &rpc,
                price_rpc.as_deref(),
            )
            .await
        }
        Command::GapFill {
            from,
            to,
            concurrency,
            era_metadata,
            dry_run,
            rpc,
            price_rpc,
        } => {
            gap_fill(
                from,
                to,
                concurrency,
                era_metadata,
                dry_run,
                &rpc,
                price_rpc.as_deref(),
            )
            .await
        }
        Command::MetadataCheck { rpc, height } => metadata_check(&rpc, height).await,
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

async fn decode_block(height: u64, rpc: &str, price_rpc: Option<&str>) -> Result<()> {
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
    let prices = historical_prices(&db, price_rpc).await?;
    let stats = decode_block_events(
        &block,
        &db,
        &prices,
        &client.metadata(),
        client.runtime_version().spec_version,
        &client,
    )
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
        decoded_liquidity = stats.decoded_liquidity,
        inserted_liquidity = stats.inserted_liquidity,
        decoded_order_book = stats.decoded_order_book,
        inserted_order_book = stats.inserted_order_book,
        decoded_val_rewards = stats.decoded_val_rewards,
        fee_burn_aggregates = stats.fee_burn_aggregates,
        polkamarkt_events = stats.polkamarkt_events,
        preimage_events = stats.preimage_events,
        inserted_val_rewards = stats.inserted_val_rewards,
        decoded_extrinsics = stats.decoded_extrinsics,
        inserted_extrinsics = stats.inserted_extrinsics,
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
async fn backfill(
    from: u64,
    to: u64,
    concurrency: usize,
    era_metadata: bool,
    rpc: &str,
    price_rpc: Option<&str>,
) -> Result<()> {
    if from > to {
        anyhow::bail!("--from ({from}) must be ≤ --to ({to})");
    }
    let db = ops_db().await?;
    backfill_heights(
        &db,
        (from..=to).collect(),
        concurrency,
        era_metadata,
        rpc,
        price_rpc,
    )
    .await
}

async fn ops_db() -> Result<PgPool> {
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    db_connect(&DbConfig {
        url: db_url,
        ..DbConfig::default()
    })
    .await
    .context("connecting to PostgreSQL")
}

/// Heights of `[from, to]` without any `sm.extrinsics` row, ascending.
async fn missing_heights(db: &PgPool, from: u64, to: u64) -> Result<Vec<u64>> {
    let (from_i, to_i) = (i64::try_from(from)?, i64::try_from(to)?);
    let rows = sqlx::query!(
        r#"
        SELECT g.h AS "h!"
        FROM generate_series($1::BIGINT, $2::BIGINT) AS g(h)
        LEFT JOIN (SELECT DISTINCT block_height FROM sm.extrinsics
                   WHERE block_height BETWEEN $1 AND $2) e ON e.block_height = g.h
        WHERE e.block_height IS NULL
        ORDER BY g.h
        "#,
        from_i,
        to_i
    )
    .fetch_all(db)
    .await
    .context("scanning sm.extrinsics for gaps")?;
    Ok(rows.into_iter().map(|r| r.h as u64).collect())
}

/// Collapses sorted heights into inclusive ranges.
fn ranges(heights: &[u64]) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for &h in heights {
        match out.last_mut() {
            Some((_, end)) if *end + 1 == h => *end = h,
            _ => out.push((h, h)),
        }
    }
    out
}

async fn gap_fill(
    from: u64,
    to: u64,
    concurrency: usize,
    era_metadata: bool,
    dry_run: bool,
    rpc: &str,
    price_rpc: Option<&str>,
) -> Result<()> {
    if from > to {
        anyhow::bail!("--from ({from}) must be ≤ --to ({to})");
    }
    let db = ops_db().await?;
    let missing = missing_heights(&db, from, to).await?;
    let spans = ranges(&missing);
    info!(
        from,
        to,
        missing = missing.len(),
        ranges = spans.len(),
        "gap scan complete"
    );
    for (a, b) in spans.iter().take(20) {
        info!(from = a, to = b, blocks = b - a + 1, "gap");
    }
    if spans.len() > 20 {
        info!(more = spans.len() - 20, "further gaps not listed");
    }
    if missing.is_empty() || dry_run {
        return Ok(());
    }
    backfill_heights(&db, missing, concurrency, era_metadata, rpc, price_rpc).await
}

/// Bucket-only resolver, plus quotes at the event's block when an
/// archive RPC is given.
async fn historical_prices(db: &PgPool, price_rpc: Option<&str>) -> Result<PriceResolver> {
    let prices = PriceResolver::historical(db.clone())
        .await
        .context("loading asset registry for pricing")?;
    match price_rpc.map(str::trim).filter(|s| !s.is_empty()) {
        Some(url) => {
            let archive = RpcClient::from_url(url)
                .await
                .with_context(|| format!("connecting price archive RPC to {url}"))?;
            info!(url, "historical quotes at block enabled");
            Ok(prices.with_archive(archive))
        }
        None => Ok(prices),
    }
}

async fn backfill_heights(
    db: &PgPool,
    heights: Vec<u64>,
    concurrency: usize,
    era_metadata: bool,
    rpc: &str,
    price_rpc: Option<&str>,
) -> Result<()> {
    if concurrency == 0 {
        anyhow::bail!("--concurrency must be ≥ 1");
    }
    let db = db.clone();
    let (from, to) = match (heights.first(), heights.last()) {
        (Some(a), Some(b)) => (*a, *b),
        _ => return Ok(()),
    };

    info!(
        rpc,
        from,
        to,
        blocks = heights.len(),
        concurrency,
        "subxt connecting"
    );
    let rpc_client = RpcClient::from_url(rpc)
        .await
        .with_context(|| format!("connecting RPC to {rpc}"))?;
    let legacy = Arc::new(LegacyRpcMethods::<SubstrateConfig>::new(rpc_client.clone()));
    let client = OnlineClient::<SubstrateConfig>::from_rpc_client(rpc_client.clone())
        .await
        .with_context(|| format!("upgrading RPC client to OnlineClient at {rpc}"))?;
    let eras = Arc::new(EraClients::new(
        client,
        rpc_client,
        legacy.clone(),
        era_metadata,
    ));

    let prices = Arc::new(historical_prices(&db, price_rpc).await?);
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let total = heights.len() as u64;
    let started = Instant::now();
    let mut handles = Vec::with_capacity(heights.len());

    for height in heights {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .context("backfill semaphore closed")?;
        let eras = eras.clone();
        let legacy = legacy.clone();
        let db = db.clone();
        let prices = prices.clone();

        handles.push(tokio::spawn(async move {
            // Hold the permit for the lifetime of the task.
            let _permit = permit;
            process_one_block(&eras, &legacy, &db, &prices, height).await
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
    eras: &EraClients,
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
    let (client, spec) = eras.client_for(hash).await?;
    let block = client
        .blocks()
        .at(hash)
        .await
        .with_context(|| format!("fetching block at height {height} ({hash:?})"))?;
    decode_block_events(&block, db, prices, &client.metadata(), spec, &client)
        .await
        .with_context(|| format!("decoding block {height}"))
}

/// One subxt client per runtime spec. The base client carries the
/// current metadata; a block of an earlier spec gets a client whose
/// metadata is the one the node served at that block, cached per spec.
/// Static event and storage types decode against it by field shape, so
/// a runtime that changed a shape we depend on fails at that block
/// instead of being misread.
struct EraClients {
    base: OnlineClient<SubstrateConfig>,
    base_spec: u32,
    rpc: RpcClient,
    legacy: Arc<LegacyRpcMethods<SubstrateConfig>>,
    enabled: bool,
    by_spec: Mutex<HashMap<u32, OnlineClient<SubstrateConfig>>>,
}

impl EraClients {
    fn new(
        base: OnlineClient<SubstrateConfig>,
        rpc: RpcClient,
        legacy: Arc<LegacyRpcMethods<SubstrateConfig>>,
        enabled: bool,
    ) -> Self {
        let base_spec = base.runtime_version().spec_version;
        Self {
            base,
            base_spec,
            rpc,
            legacy,
            enabled,
            by_spec: Mutex::new(HashMap::new()),
        }
    }

    /// Client + spec version of the runtime that produced `hash`.
    async fn client_for(&self, hash: H256) -> Result<(OnlineClient<SubstrateConfig>, u32)> {
        if !self.enabled {
            return Ok((self.base.clone(), self.base_spec));
        }
        let spec = self
            .legacy
            .state_get_runtime_version(Some(hash))
            .await
            .with_context(|| format!("runtime version at {hash:?}"))?
            .spec_version;
        if spec == self.base_spec {
            return Ok((self.base.clone(), spec));
        }
        let mut cache = self.by_spec.lock().await;
        if let Some(c) = cache.get(&spec) {
            return Ok((c.clone(), spec));
        }
        let metadata = self
            .legacy
            .state_get_metadata(Some(hash))
            .await
            .with_context(|| format!("metadata for spec {spec} (archive node required)"))?;
        let client = OnlineClient::<SubstrateConfig>::from_rpc_client(self.rpc.clone())
            .await
            .with_context(|| format!("creating client for spec {spec}"))?;
        client.set_metadata(metadata);
        info!(spec, "era metadata loaded");
        cache.insert(spec, client.clone());
        Ok((client, spec))
    }
}

/// Add per-block stats into a running total.
#[cfg(test)]
mod gap_tests {
    use super::ranges;

    #[test]
    fn collapses_consecutive_heights() {
        assert_eq!(ranges(&[]), vec![]);
        assert_eq!(ranges(&[5]), vec![(5, 5)]);
        assert_eq!(ranges(&[1, 2, 3, 7, 8, 10]), vec![(1, 3), (7, 8), (10, 10)]);
    }
}

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
    total.decoded_liquidity += one.decoded_liquidity;
    total.inserted_liquidity += one.inserted_liquidity;
    total.decoded_order_book += one.decoded_order_book;
    total.inserted_order_book += one.inserted_order_book;
    total.decoded_val_rewards += one.decoded_val_rewards;
    total.fee_burn_aggregates += one.fee_burn_aggregates;
    total.polkamarkt_events += one.polkamarkt_events;
    total.preimage_events += one.preimage_events;
    total.inserted_val_rewards += one.inserted_val_rewards;
    total.decoded_extrinsics += one.decoded_extrinsics;
    total.inserted_extrinsics += one.inserted_extrinsics;
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
/// Per-pallet hash comparison of the pinned metadata against the node.
async fn metadata_check(rpc: &str, height: Option<u32>) -> Result<()> {
    use subxt::ext::codec::Decode;
    let pinned = subxt::Metadata::decode(&mut &sorametrics_substrate::PINNED_METADATA[..])
        .context("decoding pinned metadata")?;
    let rpc_client = RpcClient::from_url(rpc)
        .await
        .with_context(|| format!("connecting RPC to {rpc}"))?;
    let legacy = LegacyRpcMethods::<SubstrateConfig>::new(rpc_client.clone());
    let (live, spec) = match height {
        Some(h) => {
            let hash = legacy
                .chain_get_block_hash(Some(h.into()))
                .await?
                .with_context(|| format!("no block at height {h}"))?;
            let version = legacy.state_get_runtime_version(Some(hash)).await?;
            let meta = legacy
                .state_get_metadata(Some(hash))
                .await
                .with_context(|| format!("metadata at height {h}"))?;
            (meta, version.spec_version)
        }
        None => {
            let client = OnlineClient::<SubstrateConfig>::from_rpc_client(rpc_client)
                .await
                .context("upgrading RPC client to OnlineClient")?;
            (client.metadata(), client.runtime_version().spec_version)
        }
    };
    let outcome = compare_pallets(&pinned, &live);
    for name in &outcome.missing {
        warn!(pallet = name, "MISSING on the node");
    }
    for name in &outcome.drift {
        warn!(pallet = name, "DRIFT: pallet metadata hash differs");
    }
    info!(
        spec,
        pinned = pinned.pallets().count(),
        same = outcome.same,
        drift = outcome.drift.len(),
        missing = outcome.missing.len(),
        "metadata check"
    );
    if outcome.drift.is_empty() && outcome.missing.is_empty() {
        Ok(())
    } else {
        // Distinct exit code so CI can flag it without failing the build.
        process::exit(2);
    }
}

/// Result of [`compare_pallets`].
struct PalletDrift {
    same: usize,
    drift: Vec<String>,
    missing: Vec<String>,
}

fn compare_pallets(pinned: &subxt::Metadata, live: &subxt::Metadata) -> PalletDrift {
    let mut out = PalletDrift {
        same: 0,
        drift: Vec::new(),
        missing: Vec::new(),
    };
    for p in pinned.pallets() {
        match live.pallet_by_name(p.name()) {
            None => out.missing.push(p.name().to_string()),
            Some(l) if l.hash() == p.hash() => out.same += 1,
            Some(_) => out.drift.push(p.name().to_string()),
        }
    }
    out
}

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
