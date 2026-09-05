//! Asset pricing: DAI quotes from the chain + USD valuation of events.
//!
//! Mechanism ported verbatim from the Node (`index.js::getPriceInDai`,
//! `updateKeyPrices`, `db_pg.js::updatePriceHistory`):
//!
//! - Price source is the `liquidityProxy_quote` RPC — the same primitive
//!   Polkaswap uses to display prices. DEX 0, empty source list and
//!   `Disabled` filter mean "route across every DEX / pair".
//! - The quoted input is infinitesimal (`0.000001` of the asset, i.e.
//!   `10^(decimals - 6)` raw) so the output approaches mid price and is
//!   almost independent of pool depth. DAI has 18 decimals, so
//!   `price_usd = out_raw / 10^12`.
//! - DAI itself is `1`. A `null` quote (no route / illiquid) is "no
//!   price": the event's `usd_value` stays `NULL`, and the miss is
//!   cached for the TTL so the node isn't hammered.
//! - Live quotes are cached 60 s per asset (`PRICE_TTL`).
//! - Every successful live quote is folded into the hourly bucket of
//!   `ts.price_history` (running mean) — that is how the Node builds
//!   its sparkline history.
//!
//! What differs from the Node, deliberately: the Node valued every
//! event with an *instantaneous* quote because it only ever indexed
//! live. v33 also backfills history, where the current price would be
//! wrong. [`PriceResolver`] therefore values an event with a live quote
//! only when the event is younger than [`LIVE_WINDOW`]; older events
//! use the mean of their own hourly bucket (which the ETL migrated from
//! the legacy `sm.price_history` and the live sampler keeps extending).

use bigdecimal::{BigDecimal, RoundingMode};
use num_bigint::BigInt;
use rust_decimal::Decimal;
use serde::Deserialize;
use sorametrics_core::chain::AssetId;
use sorametrics_core::time::Timestamp;
use sorametrics_db::sm::load_asset_registry;
use sorametrics_db::ts::{price_at_bucket, upsert_price_sample};
use sorametrics_db::DbError;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use subxt::backend::rpc::{rpc_params, RpcClient};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// DAI asset id on SORA mainnet (`0x020006…`).
pub const DAI_ASSET_ID: &str = "0x0200060000000000000000000000000000000000000000000000000000000000";

/// Decimals assumed for an asset missing from `sm.asset_registry`
/// (Node: `Number(decimals) || 18`).
pub const DEFAULT_DECIMALS: u32 = 18;

/// Per-asset live quote cache lifetime (Node: 60 000 ms).
pub const PRICE_TTL: Duration = Duration::from_secs(60);

/// Events younger than this are valued with a live quote; older ones
/// with their hourly bucket mean.
pub const LIVE_WINDOW: Duration = Duration::from_secs(3600);

/// Symbols the Node re-quotes every minute (`updateKeyPrices::POPULAR`).
/// Drives the periodic sampler that keeps `ts.price_history` growing
/// even when no event touches these assets.
pub const POPULAR_SYMBOLS: [&str; 16] = [
    "XOR", "VAL", "PSWAP", "ETH", "DAI", "TBCD", "KUSD", "DEO", "KEN", "KGOLD", "KXOR", "VXOR",
    "XSTUSD", "XST", "KARMA", "CERES",
];

/// Errors from the price pipeline.
#[derive(Debug, Error)]
pub enum PriceError {
    /// RPC transport / JSON error (boxed: `subxt::Error` is large).
    #[error("quote rpc: {0}")]
    Rpc(#[source] Box<subxt::Error>),

    /// The node answered but the `amount` field was not a decimal integer.
    #[error("quote for {asset}: unparseable amount {amount:?}")]
    BadAmount {
        /// Asset that was quoted.
        asset: String,
        /// Raw `amount` string returned by the node.
        amount: String,
    },

    /// DB error reading the registry or price buckets.
    #[error("db: {0}")]
    Db(#[from] DbError),
}

impl From<subxt::Error> for PriceError {
    fn from(e: subxt::Error) -> Self {
        Self::Rpc(Box::new(e))
    }
}

/// Wire shape of a non-null `liquidityProxy_quote` result. Only
/// `amount` is used; the rest (`fee`, `route`, `rewards`,
/// `amount_without_impact`) is ignored.
#[derive(Debug, Deserialize)]
struct QuoteOutcome {
    amount: String,
}

/// Raw input amount for a quote: `0.000001` of the asset, i.e.
/// `10^(decimals - 6)` (Node: `10n ** BigInt(Math.max(decInt - 6, 0))`).
pub fn quote_input_raw(decimals: u32) -> String {
    let exp = decimals.saturating_sub(6);
    let mut s = String::with_capacity(exp as usize + 1);
    s.push('1');
    for _ in 0..exp {
        s.push('0');
    }
    s
}

/// USD price from the raw DAI output of an infinitesimal-input quote:
/// `(out / 10^18) / 0.000001 = out / 10^12`. `None` when the result is
/// not a strictly positive finite number (Node: `safePrice`).
pub fn price_from_quote_amount(out_raw: &str) -> Option<Decimal> {
    let out = Decimal::from_str(out_raw).ok()?;
    let price = out.checked_div(Decimal::from(1_000_000_000_000_u64))?;
    (price > Decimal::ZERO).then_some(price)
}

/// `raw_amount / 10^decimals * price`, rounded half-up to the 6
/// fractional digits of the `NUMERIC(38,6)` `usd_value` columns (the
/// rounding PostgreSQL itself would apply on insert).
pub fn usd_value(raw_amount: &BigDecimal, decimals: u32, price: Decimal) -> BigDecimal {
    let scale = BigDecimal::new(BigInt::from(1), -(decimals as i64));
    let human = raw_amount / scale;
    let price_bd = BigDecimal::from_str(&price.to_string()).unwrap_or_default();
    (human * price_bd).with_scale_round(6, RoundingMode::HalfUp)
}

/// Ask the node for the DAI price of one asset. `Ok(None)` = the chain
/// has no route for it (illiquid / unlisted), which is not an error.
pub async fn quote_price_in_dai(
    rpc: &RpcClient,
    asset: &AssetId,
    decimals: u32,
) -> Result<Option<Decimal>, PriceError> {
    if asset.as_str() == DAI_ASSET_ID {
        return Ok(Some(Decimal::ONE));
    }
    let outcome: Option<QuoteOutcome> = rpc
        .request(
            "liquidityProxy_quote",
            rpc_params![
                0_u32,
                asset.as_str(),
                DAI_ASSET_ID,
                quote_input_raw(decimals),
                "WithDesiredInput",
                Vec::<String>::new(),
                "Disabled"
            ],
        )
        .await?;
    match outcome {
        None => Ok(None),
        Some(q) => match price_from_quote_amount(&q.amount) {
            Some(p) => Ok(Some(p)),
            None if Decimal::from_str(&q.amount).is_ok() => Ok(None),
            None => Err(PriceError::BadAmount {
                asset: asset.as_str().to_string(),
                amount: q.amount,
            }),
        },
    }
}

#[derive(Clone, Copy, Debug)]
struct CachedQuote {
    price: Option<Decimal>,
    at: Instant,
}

#[derive(Clone, Debug)]
struct RegistryEntry {
    symbol: String,
    decimals: u32,
}

/// Values events in USD. One instance per ingest session / ops run.
///
/// - [`PriceResolver::live`] holds an RPC client: recent events are
///   quoted on demand (cached [`PRICE_TTL`]), and each successful quote
///   also lands in `ts.price_history`.
/// - [`PriceResolver::historical`] has no RPC: every event is valued
///   from its hourly bucket. Used by `ops backfill` / `decode-block`.
#[derive(Clone)]
pub struct PriceResolver {
    db: PgPool,
    rpc: Option<RpcClient>,
    registry: Arc<HashMap<String, RegistryEntry>>,
    quotes: Arc<Mutex<HashMap<String, CachedQuote>>>,
}

impl PriceResolver {
    /// Live mode: quote via `rpc` for events inside [`LIVE_WINDOW`].
    pub async fn live(db: PgPool, rpc: RpcClient) -> Result<Self, PriceError> {
        Self::new(db, Some(rpc)).await
    }

    /// Historical mode: bucket lookups only, no RPC.
    pub async fn historical(db: PgPool) -> Result<Self, PriceError> {
        Self::new(db, None).await
    }

    async fn new(db: PgPool, rpc: Option<RpcClient>) -> Result<Self, PriceError> {
        let registry = load_asset_registry(&db)
            .await?
            .into_iter()
            .map(|a| {
                (
                    a.asset_id,
                    RegistryEntry {
                        symbol: a.symbol,
                        decimals: a.decimals.max(0) as u32,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        debug!(
            assets = registry.len(),
            "price resolver loaded asset registry"
        );
        Ok(Self {
            db,
            rpc,
            registry: Arc::new(registry),
            quotes: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Decimals of `asset` per the registry, or [`DEFAULT_DECIMALS`].
    pub fn decimals_of(&self, asset: &AssetId) -> u32 {
        self.registry
            .get(asset.as_str())
            .map(|e| e.decimals)
            .unwrap_or(DEFAULT_DECIMALS)
    }

    /// Whether this resolver can issue live quotes.
    pub fn is_live(&self) -> bool {
        self.rpc.is_some()
    }

    /// USD value of `raw_amount` of `asset` at `at`, or `None` when no
    /// price is known for that asset at that time.
    pub async fn usd_value_at(
        &self,
        asset: &AssetId,
        raw_amount: &BigDecimal,
        at: Timestamp,
    ) -> Result<Option<BigDecimal>, PriceError> {
        let decimals = self.decimals_of(asset);
        let price = match self.price_at(asset, decimals, at).await? {
            Some(p) => p,
            None => return Ok(None),
        };
        Ok(Some(usd_value(raw_amount, decimals, price)))
    }

    async fn price_at(
        &self,
        asset: &AssetId,
        decimals: u32,
        at: Timestamp,
    ) -> Result<Option<Decimal>, PriceError> {
        if asset.as_str() == DAI_ASSET_ID {
            return Ok(Some(Decimal::ONE));
        }
        let age = at.elapsed_until(Timestamp::now());
        let is_recent = age.to_std().map(|d| d <= LIVE_WINDOW).unwrap_or(true);
        if self.rpc.is_some() && is_recent {
            return self.live_price(asset, decimals).await;
        }
        let bucket = at.hour_bucket_secs();
        let mean = price_at_bucket(&self.db, asset.as_str(), bucket).await?;
        Ok(mean
            .and_then(Decimal::from_f64_retain)
            .filter(|p| *p > Decimal::ZERO))
    }

    /// Cached live quote (TTL [`PRICE_TTL`]); on a miss, quotes the
    /// node and records the sample in `ts.price_history`.
    pub async fn live_price(
        &self,
        asset: &AssetId,
        decimals: u32,
    ) -> Result<Option<Decimal>, PriceError> {
        let rpc = match &self.rpc {
            Some(r) => r,
            None => return Ok(None),
        };
        {
            let cache = self.quotes.lock().await;
            if let Some(c) = cache.get(asset.as_str()) {
                if c.at.elapsed() < PRICE_TTL {
                    return Ok(c.price);
                }
            }
        }
        let price = quote_price_in_dai(rpc, asset, decimals).await?;
        self.quotes.lock().await.insert(
            asset.as_str().to_string(),
            CachedQuote {
                price,
                at: Instant::now(),
            },
        );
        if let Some(p) = price {
            self.record_sample(asset, p).await?;
        } else {
            debug!(asset = asset.as_str(), "no DAI route, price unknown");
        }
        Ok(price)
    }

    /// Fold a price sample into the current hour bucket.
    async fn record_sample(&self, asset: &AssetId, price: Decimal) -> Result<(), PriceError> {
        let price_f64 = match price.to_string().parse::<f64>() {
            Ok(v) if v.is_finite() && v > 0.0 => v,
            _ => {
                warn!(asset = asset.as_str(), price = %price, "price not representable as f64, sample skipped");
                return Ok(());
            }
        };
        let bucket = Timestamp::now().hour_bucket_secs();
        upsert_price_sample(&self.db, asset.as_str(), bucket, price_f64).await?;
        Ok(())
    }

    /// Registry assets whose symbol is in [`POPULAR_SYMBOLS`], in that
    /// order. Symbols absent from the registry are skipped (the Node
    /// does the same via `ASSETS.find`).
    pub fn popular_assets(&self) -> Vec<AssetId> {
        let by_symbol: HashMap<&str, &str> = self
            .registry
            .iter()
            .map(|(id, e)| (e.symbol.as_str(), id.as_str()))
            .collect();
        POPULAR_SYMBOLS
            .iter()
            .filter_map(|sym| by_symbol.get(sym).map(|id| AssetId::new(*id)))
            .collect()
    }

    /// Quote every popular asset once and record the samples. One tick
    /// of the Node's `updateKeyPrices` loop. Per-asset RPC failures are
    /// logged and skipped so one bad asset never starves the rest; the
    /// last one is reported in the outcome so the caller can tell a
    /// dead connection (nothing priced, errors) from illiquid assets. A
    /// DB failure propagates.
    pub async fn sample_popular(&self) -> Result<SampleOutcome, PriceError> {
        let rpc = match &self.rpc {
            Some(r) => r,
            None => return Ok(SampleOutcome::default()),
        };
        let mut outcome = SampleOutcome::default();
        for asset in self.popular_assets() {
            let decimals = self.decimals_of(&asset);
            match quote_price_in_dai(rpc, &asset, decimals).await {
                Ok(Some(p)) => {
                    self.quotes.lock().await.insert(
                        asset.as_str().to_string(),
                        CachedQuote {
                            price: Some(p),
                            at: Instant::now(),
                        },
                    );
                    self.record_sample(&asset, p).await?;
                    outcome.priced += 1;
                }
                Ok(None) => {
                    debug!(asset = asset.as_str(), "popular asset has no DAI route");
                    outcome.no_route += 1;
                }
                Err(PriceError::Db(e)) => return Err(PriceError::Db(e)),
                Err(e) => {
                    warn!(asset = asset.as_str(), error = %e, "popular quote failed");
                    outcome.failed += 1;
                    outcome.last_error = Some(e);
                }
            }
        }
        Ok(outcome)
    }
}

/// Result of one [`PriceResolver::sample_popular`] pass.
#[derive(Debug, Default)]
pub struct SampleOutcome {
    /// Assets that got a price and were recorded.
    pub priced: usize,
    /// Assets the chain has no DAI route for.
    pub no_route: usize,
    /// Assets whose quote RPC failed.
    pub failed: usize,
    /// The last RPC failure, if any.
    pub last_error: Option<PriceError>,
}

impl SampleOutcome {
    /// `true` when nothing was priced and every attempt errored — the
    /// signature of a dead RPC connection rather than illiquid assets.
    pub fn looks_disconnected(&self) -> bool {
        self.priced == 0 && self.failed > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_input_is_ten_to_decimals_minus_six() {
        assert_eq!(quote_input_raw(18), "1000000000000");
        assert_eq!(quote_input_raw(6), "1");
        assert_eq!(quote_input_raw(2), "1");
    }

    #[test]
    fn price_matches_production_xor_quote() {
        // mof2 quote XOR→DAI with input 10^12 (2026-09-05), and the
        // price sorametrics.org showed at the same moment.
        let p = price_from_quote_amount("4408349907991").unwrap();
        assert_eq!(p.to_string(), "4.408349907991");
    }

    #[test]
    fn zero_or_garbage_quote_is_no_price() {
        assert_eq!(price_from_quote_amount("0"), None);
        assert_eq!(price_from_quote_amount("abc"), None);
        assert_eq!(price_from_quote_amount(""), None);
    }

    #[test]
    fn usd_value_scales_by_decimals_and_rounds_to_6() {
        // 2.5 XOR at 4.408349907991 → 11.020874769978 → 11.020875
        let raw = BigDecimal::from_str("2500000000000000000").unwrap();
        let p = Decimal::from_str("4.408349907991").unwrap();
        assert_eq!(usd_value(&raw, 18, p).to_string(), "11.020875");
    }

    #[test]
    fn usd_value_one_planck_is_zero_at_6dp() {
        let raw = BigDecimal::from(1_u64);
        let p = Decimal::from_str("4.4").unwrap();
        assert_eq!(usd_value(&raw, 18, p), BigDecimal::from(0_u64));
    }

    #[test]
    fn usd_value_rounds_half_up_not_truncates() {
        // 1 unit at 0.0000005 → 0.000001 (half-up), not 0.000000
        let raw = BigDecimal::from(1_000_000_u64);
        let p = Decimal::from_str("0.0000005").unwrap();
        assert_eq!(usd_value(&raw, 6, p).to_string(), "0.000001");
    }

    #[test]
    fn usd_value_with_six_decimals_asset() {
        let raw = BigDecimal::from(1_500_000_u64);
        let p = Decimal::from_str("2").unwrap();
        assert_eq!(usd_value(&raw, 6, p).to_string(), "3.000000");
    }
}
