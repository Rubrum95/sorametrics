//! Chain scans on the legacy contract: `/pools`, `/holders/:assetId`.
//!
//! `/pools?page&limit&base` (`index.js`): every `poolXYK.reserves`
//! entry whose base and target are whitelisted assets, as
//! `{ base: <asset row>, target: <asset row>, reserves: { base, target },
//! basePrice, targetPrice }` where the reserves are the raw planck
//! digits with thousands separators (polkadot-js `toHuman`), sorted by
//! TVL (reserves × prices) descending; `?base=<symbol>` filters
//! (`all` = no filter); page 1-based, limit default 10 / max 100;
//! `{ data, total, page, totalPages }`. Scan cached 60 s.
//!
//! `/holders/:assetId?page`: full scan of `system.account` (XOR, free
//! above 1) or `tokens.accounts` (free above 0.1 human units), sorted
//! by balance desc, 25 per page, `{ page, totalHolders, totalPages,
//! data: [{ address, balance, balanceStr }] }` with `balanceStr` as
//! `toFormat(2)` (thousands separators, 2 decimals). Scan cached 5 min
//! per asset — it walks every account on chain, like the Node did.

use crate::legacy::decimals_for;
use crate::state::Registry;
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use bigdecimal::{BigDecimal, RoundingMode, ToPrimitive};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use sorametrics_core::chain::ss58_encode_sora;
use sorametrics_db::sm::RegistryAsset;
use sorametrics_db::ts::latest_prices;
use sorametrics_substrate::runtime::sora;
use std::collections::HashMap;
use std::time::Duration;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/pools", get(pools))
        .route("/holders/:asset_id", get(holders))
}

const XOR_ASSET_ID: &str = "0x0200000000000000000000000000000000000000000000000000000000000000";
const POOLS_TTL: Duration = Duration::from_secs(60);
const HOLDERS_TTL: Duration = Duration::from_secs(300);
const HOLDERS_PAGE: usize = 25;
/// How long a request waits for a fresh scan before answering 503
/// (the Node blocked up to 60 s on its RPC timeout).
const SCAN_WAIT: Duration = Duration::from_secs(60);
const SCAN_POLL: Duration = Duration::from_millis(500);

/// Serve `key` from the scan cache; on a miss run `scan` in the
/// background (deduplicated per key) and wait up to [`SCAN_WAIT`] for
/// it. A scan that outlives the wait keeps running and lands in the
/// cache for the next request, which is what makes the first call to a
/// cold `/holders` survive a slow public node.
async fn cached_or_scan<T, F, Fut>(
    state: &AppState,
    key: &str,
    ttl: Duration,
    scan: F,
) -> Result<T, ApiError>
where
    T: serde::de::DeserializeOwned + serde::Serialize + Send + 'static,
    F: FnOnce(AppState) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<T, ApiError>> + Send + 'static,
{
    if let Some(v) = state.cached_scan(key, ttl).await {
        return serde_json::from_value(v).map_err(|e| ApiError::Internal(e.to_string()));
    }
    if state.begin_scan(key).await {
        let st = state.clone();
        let k = key.to_string();
        tokio::spawn(async move {
            match scan(st.clone()).await {
                Ok(v) => match serde_json::to_value(&v) {
                    Ok(json) => st.store_scan(&k, json).await,
                    Err(e) => tracing::error!(error = %e, key = %k, "scan serialisation failed"),
                },
                Err(e) => tracing::warn!(error = %e, key = %k, "background scan failed"),
            }
            st.end_scan(&k).await;
        });
    }
    let deadline = tokio::time::Instant::now() + SCAN_WAIT;
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(SCAN_POLL).await;
        if let Some(v) = state.cached_scan(key, ttl).await {
            return serde_json::from_value(v).map_err(|e| ApiError::Internal(e.to_string()));
        }
    }
    Err(ApiError::ScanPending)
}

/// Thousands separators on an integer string (`toHuman` / `toFormat`).
pub fn group_thousands(int_digits: &str) -> String {
    let mut out = String::with_capacity(int_digits.len() + int_digits.len() / 3);
    let n = int_digits.len();
    for (i, c) in int_digits.chars().enumerate() {
        if i > 0 && (n - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// BigNumber `toFormat(2)`: grouped integer part, 2 decimals half-up.
pub fn to_format_2(v: &BigDecimal) -> String {
    let s = v.with_scale_round(2, RoundingMode::HalfUp).to_string();
    let (int, frac) = s.split_once('.').unwrap_or((&s, "00"));
    let (sign, digits) = int.strip_prefix('-').map_or(("", int), |d| ("-", d));
    format!("{sign}{}.{frac:0<2}", group_thousands(digits))
}

fn human(raw: u128, decimals: u32) -> BigDecimal {
    BigDecimal::from(BigInt::from(raw)) / BigDecimal::new(BigInt::from(1), -(decimals as i64))
}

// =============================================================
// /pools
// =============================================================

#[derive(Clone, Serialize, Deserialize)]
struct AssetRow {
    symbol: String,
    name: String,
    decimals: i16,
    #[serde(rename = "assetId")]
    asset_id: String,
    logo: String,
}

impl AssetRow {
    fn from_registry(a: &RegistryAsset) -> Self {
        Self {
            symbol: a.symbol.clone(),
            name: a.name.clone().unwrap_or_default(),
            decimals: a.decimals,
            asset_id: a.asset_id.clone(),
            logo: a.logo.clone().unwrap_or_default(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PoolReserves {
    base: String,
    target: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Pool {
    base: AssetRow,
    target: AssetRow,
    reserves: PoolReserves,
    #[serde(rename = "basePrice")]
    base_price: f64,
    #[serde(rename = "targetPrice")]
    target_price: f64,
}

#[derive(Debug, Deserialize)]
struct PoolsQuery {
    page: Option<i64>,
    limit: Option<i64>,
    base: Option<String>,
}

#[derive(Serialize)]
struct PoolsResponse {
    data: Vec<Pool>,
    total: usize,
    page: i64,
    #[serde(rename = "totalPages")]
    total_pages: usize,
}

/// Raw reserves entry: (base asset id, target asset id, base, target).
struct RawPool {
    base: String,
    target: String,
    reserves: (u128, u128),
}

/// Extract both `AssetId32` keys from a `Blake2_128Concat` double-map
/// key: `… ‖ hash16 ‖ base32 ‖ hash16 ‖ target32`.
fn pool_keys(key_bytes: &[u8]) -> Option<(String, String)> {
    let n = key_bytes.len();
    if n < 96 {
        return None;
    }
    let target = &key_bytes[n - 32..];
    let base = &key_bytes[n - 80..n - 48];
    Some((
        format!("0x{}", hex::encode(base)),
        format!("0x{}", hex::encode(target)),
    ))
}

async fn scan_pools(state: &AppState) -> Result<Vec<Pool>, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let raw: Vec<RawPool> = chain
        .with_client(|client| async move {
            let at = client.storage().at_latest().await?;
            let mut stream = at.iter(sora::storage().pool_xyk().reserves_iter()).await?;
            let mut out = Vec::new();
            while let Some(kv) = stream.next().await {
                let kv = kv?;
                if let Some((base, target)) = pool_keys(&kv.key_bytes) {
                    out.push(RawPool {
                        base,
                        target,
                        reserves: kv.value,
                    });
                }
            }
            Ok(out)
        })
        .await?;

    let registry = state.registry.read().await;
    let mut pools: Vec<(Pool, String, String)> = raw
        .into_iter()
        .filter_map(|p| {
            let base = registry.get(&p.base).filter(|a| a.whitelisted)?;
            let target = registry.get(&p.target).filter(|a| a.whitelisted)?;
            Some((
                Pool {
                    base: AssetRow::from_registry(base),
                    target: AssetRow::from_registry(target),
                    reserves: PoolReserves {
                        base: group_thousands(&p.reserves.0.to_string()),
                        target: group_thousands(&p.reserves.1.to_string()),
                    },
                    base_price: 0.0,
                    target_price: 0.0,
                },
                p.reserves.0.to_string(),
                p.reserves.1.to_string(),
            ))
        })
        .collect();
    drop(registry);

    let mut ids: Vec<String> = pools
        .iter()
        .flat_map(|(p, _, _)| [p.base.asset_id.clone(), p.target.asset_id.clone()])
        .collect();
    ids.sort();
    ids.dedup();
    let prices: HashMap<String, f64> = latest_prices(&state.db, &ids)
        .await?
        .into_iter()
        .map(|p| (p.asset_id, p.price_usd))
        .collect();

    let mut ranked: Vec<(f64, Pool)> = pools
        .drain(..)
        .map(|(mut p, rb, rt)| {
            p.base_price = prices.get(&p.base.asset_id).copied().unwrap_or(0.0);
            p.target_price = prices.get(&p.target.asset_id).copied().unwrap_or(0.0);
            let tvl = rb.parse::<f64>().unwrap_or(0.0) / 10f64.powi(p.base.decimals as i32)
                * p.base_price
                + rt.parse::<f64>().unwrap_or(0.0) / 10f64.powi(p.target.decimals as i32)
                    * p.target_price;
            (tvl, p)
        })
        .collect();
    ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    Ok(ranked.into_iter().map(|(_, p)| p).collect())
}

async fn pools(
    State(state): State<AppState>,
    Query(q): Query<PoolsQuery>,
) -> Result<Json<PoolsResponse>, ApiError> {
    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(10);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::BadRequest(
            "limit must be between 1 and 100".into(),
        ));
    }
    let all: Vec<Pool> = cached_or_scan(&state, "pools", POOLS_TTL, |st| async move {
        scan_pools(&st).await
    })
    .await?;
    let filtered: Vec<Pool> = match q.base.as_deref() {
        Some(b) if b != "all" => all.into_iter().filter(|p| p.base.symbol == b).collect(),
        _ => all,
    };
    let total = filtered.len();
    let total_pages = total.div_ceil(limit as usize);
    let start = ((page - 1) * limit) as usize;
    let data = filtered
        .into_iter()
        .skip(start)
        .take(limit as usize)
        .collect();
    Ok(Json(PoolsResponse {
        data,
        total,
        page,
        total_pages,
    }))
}

// =============================================================
// /holders/:assetId
// =============================================================

#[derive(Clone, Serialize, Deserialize)]
struct Holder {
    address: String,
    balance: f64,
    #[serde(rename = "balanceStr")]
    balance_str: String,
}

#[derive(Debug, Deserialize)]
struct PageQuery {
    page: Option<i64>,
}

#[derive(Serialize)]
struct HoldersResponse {
    page: i64,
    #[serde(rename = "totalHolders")]
    total_holders: usize,
    #[serde(rename = "totalPages")]
    total_pages: usize,
    data: Vec<Holder>,
}

fn holder(account: &[u8; 32], amount: BigDecimal) -> Holder {
    Holder {
        address: ss58_encode_sora(account),
        balance: amount.to_f64().unwrap_or(0.0),
        balance_str: to_format_2(&amount),
    }
}

async fn scan_holders(state: &AppState, asset_id: &str) -> Result<Vec<Holder>, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let decimals = {
        let registry: tokio::sync::RwLockReadGuard<'_, Registry> = state.registry.read().await;
        decimals_for(&registry, asset_id)
    };
    let want = asset_id.to_string();
    let is_xor = asset_id == XOR_ASSET_ID;
    let mut list: Vec<Holder> = chain
        .with_client(|client| async move {
            let at = client.storage().at_latest().await?;
            let mut out = Vec::new();
            if is_xor {
                let mut stream = at.iter(sora::storage().system().account_iter()).await?;
                let one = BigDecimal::from(1);
                while let Some(kv) = stream.next().await {
                    let kv = kv?;
                    let amount = human(kv.value.data.free, 18);
                    if amount > one {
                        let n = kv.key_bytes.len();
                        let acc: [u8; 32] = kv.key_bytes[n - 32..].try_into().unwrap_or([0; 32]);
                        out.push(holder(&acc, amount));
                    }
                }
            } else {
                let mut stream = at.iter(sora::storage().tokens().accounts_iter()).await?;
                let dust = BigDecimal::new(BigInt::from(1), 1); // 0.1
                while let Some(kv) = stream.next().await {
                    let kv = kv?;
                    let n = kv.key_bytes.len();
                    if n < 96 {
                        continue;
                    }
                    let asset = format!("0x{}", hex::encode(&kv.key_bytes[n - 32..]));
                    if asset != want {
                        continue;
                    }
                    let amount = human(kv.value.free, decimals);
                    if amount > dust {
                        // `Accounts`: Blake2_128Concat(account) ‖ Twox64Concat(asset)
                        // → … ‖ hash16 ‖ account32 ‖ hash8 ‖ asset32.
                        let acc: [u8; 32] =
                            kv.key_bytes[n - 72..n - 40].try_into().unwrap_or([0; 32]);
                        out.push(holder(&acc, amount));
                    }
                }
            }
            Ok(out)
        })
        .await?;
    list.sort_by(|a, b| {
        b.balance
            .partial_cmp(&a.balance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(list)
}

async fn holders(
    State(state): State<AppState>,
    Path(asset_id): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<HoldersResponse>, ApiError> {
    let asset_id = crate::util::validate_asset_id(&asset_id)?;
    let page = q.page.unwrap_or(1).max(1);
    let key = format!("holders:{asset_id}");
    let id = asset_id.clone();
    let list: Vec<Holder> = cached_or_scan(&state, &key, HOLDERS_TTL, move |st| async move {
        scan_holders(&st, &id).await
    })
    .await?;
    let total_holders = list.len();
    let total_pages = total_holders.div_ceil(HOLDERS_PAGE);
    let start = ((page - 1) as usize) * HOLDERS_PAGE;
    let data = list.into_iter().skip(start).take(HOLDERS_PAGE).collect();
    Ok(Json(HoldersResponse {
        page,
        total_holders,
        total_pages,
        data,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn thousands_grouping_matches_to_human() {
        assert_eq!(
            group_thousands("769723431438391587"),
            "769,723,431,438,391,587"
        );
        assert_eq!(group_thousands("12"), "12");
        assert_eq!(group_thousands("1234"), "1,234");
        assert_eq!(group_thousands("0"), "0");
    }

    #[test]
    fn to_format_2_matches_bignumber() {
        assert_eq!(
            to_format_2(&BigDecimal::from_str("29453911.090306096").unwrap()),
            "29,453,911.09"
        );
        assert_eq!(to_format_2(&BigDecimal::from_str("5").unwrap()), "5.00");
        assert_eq!(
            to_format_2(&BigDecimal::from_str("1234.5").unwrap()),
            "1,234.50"
        );
    }

    #[test]
    fn tokens_accounts_key_layout() {
        // prefix32 ‖ blake2_128(acc)16 ‖ acc32 ‖ twox64(asset)8 ‖ asset32 = 120 bytes
        let mut key = vec![0u8; 32];
        key.extend([1u8; 16]);
        key.extend([0xaa; 32]);
        key.extend([2u8; 8]);
        key.extend([0xbb; 32]);
        let n = key.len();
        assert_eq!(n, 120);
        assert_eq!(&key[n - 72..n - 40], &[0xaa; 32]);
        assert_eq!(&key[n - 32..], &[0xbb; 32]);
    }

    #[test]
    fn pool_keys_take_trailing_asset_ids() {
        let mut key = vec![0u8; 32]; // pallet+storage prefix
        key.extend([1u8; 16]);
        key.extend([0xaa; 32]);
        key.extend([2u8; 16]);
        key.extend([0xbb; 32]);
        let (b, t) = pool_keys(&key).unwrap();
        assert_eq!(b, format!("0x{}", "aa".repeat(32)));
        assert_eq!(t, format!("0x{}", "bb".repeat(32)));
        assert!(pool_keys(&[0u8; 10]).is_none());
    }
}
