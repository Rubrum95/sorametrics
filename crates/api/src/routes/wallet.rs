//! Chain-state wallet routes (group D): `POST /balances` and
//! `GET /balance/:address` (same rows without `assetId`, as an array).
//!
//! Mechanism (`index.js::getAddressBalances`): XOR from
//! `system.account(addr).data.free`, listed only when `> 0`; every other
//! asset from `tokens.accounts(addr, *)` with `free > 0.0001` human
//! units; `amount` as a 4-decimal string, `usdValue` as a 2-decimal
//! string at the latest quote (`0.00` when unknown), rows sorted by
//! USD descending; `totalUsd` is the numeric sum.
//!
//! Body `{ addresses: [ss58, …] }` (max 100, every one `[1-9A-HJ-NP-Za-km-z]{46,50}`);
//! response `{ result: [{ address, tokens, totalUsd }] }`. Unlike the
//! Node, `assetId` is the bare `0x…` hex (the Node leaked the codec's
//! `{"code":…}` JSON for non-XOR assets).

use crate::legacy::{decimals_for, logo_for, symbol_for};
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::{get, post},
    Json, Router,
};
use bigdecimal::{BigDecimal, RoundingMode};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use sorametrics_core::chain::ss58_decode;
use sorametrics_core::chain::AssetId;
use sorametrics_db::ts::{illiquid_assets, latest_prices, valuation_prices};
use sorametrics_substrate::price::{quote_sell_in_dai, DAI_ASSET_ID};
use sorametrics_substrate::runtime::sora;
use std::collections::HashMap;
use subxt::utils::AccountId32;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/balances", post(balances))
        .route("/balance/:address", get(balance))
        .route("/wallet/realizable/:address", get(realizable))
}

/// `/wallet/info`, on the long-timeout router: a wallet with half a
/// million rows is computed in the background and may outlive the
/// 30 s budget of the fast routes.
pub fn scan_router() -> Router<AppState> {
    Router::new().route("/wallet/info/:address", get(wallet_info))
}

const XOR_ASSET_ID: &str = "0x0200000000000000000000000000000000000000000000000000000000000000";

#[derive(Deserialize)]
struct BalancesBody {
    addresses: Option<Vec<String>>,
}

#[derive(Serialize)]
struct TokenBalance {
    symbol: String,
    logo: String,
    amount: String,
    #[serde(rename = "usdValue")]
    usd_value: String,
    /// Latest quote, the figure the Tokens page shows (also for illiquid
    /// assets, which `usdValue` leaves out); `null` when never quoted.
    price: Option<f64>,
    #[serde(rename = "assetId")]
    asset_id: String,
    /// `true` when the asset failed the depth check: it is not valued
    /// (`usdValue` 0.00) because its quoted price is not a market price.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    illiquid: bool,
}

#[derive(Serialize)]
struct WalletBalances {
    address: String,
    tokens: Vec<TokenBalance>,
    #[serde(rename = "totalUsd")]
    total_usd: f64,
}

#[derive(Serialize)]
struct BalancesResponse {
    result: Vec<WalletBalances>,
}

/// Node `VALID_SS58`: base58 alphabet, 46–50 chars.
pub fn looks_like_ss58(s: &str) -> bool {
    (46..=50).contains(&s.len())
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() && !matches!(c, '0' | 'O' | 'I' | 'l'))
}

/// Raw free balance of an account for an asset.
struct RawHolding {
    asset_id: String,
    free: u128,
}

/// A holding that passed the dust filter, ready to price.
struct Holding {
    symbol: String,
    logo: String,
    asset_id: String,
    amount: BigDecimal,
}

/// `toFixed(n)`: half-up to `n` decimals, always showing them.
fn fmt_fixed(v: &BigDecimal, decimals: i64) -> String {
    let r = v.with_scale_round(decimals, RoundingMode::HalfUp);
    let s = r.to_string();
    match s.find('.') {
        Some(dot) => {
            let frac = s.len() - dot - 1;
            format!(
                "{s}{}",
                "0".repeat((decimals as usize).saturating_sub(frac))
            )
        }
        None => format!("{s}.{}", "0".repeat(decimals as usize)),
    }
}

fn human(free: u128, decimals: u32) -> BigDecimal {
    BigDecimal::from(BigInt::from(free)) / BigDecimal::new(BigInt::from(1), -(decimals as i64))
}

async fn balances(
    State(state): State<AppState>,
    Json(body): Json<BalancesBody>,
) -> Result<Json<BalancesResponse>, ApiError> {
    let Some(addresses) = body.addresses else {
        return Ok(Json(BalancesResponse { result: Vec::new() }));
    };
    if addresses.len() > 100 {
        return Err(ApiError::BadRequest("Max 100 addresses per request".into()));
    }
    if addresses.iter().any(|a| !looks_like_ss58(a)) {
        return Err(ApiError::BadRequest(
            "Invalid address format in list".into(),
        ));
    }
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;

    let mut result = Vec::with_capacity(addresses.len());
    for address in addresses {
        result.push(wallet_balances(&state, chain, address).await?);
    }
    Ok(Json(BalancesResponse { result }))
}

/// Free balances of an account: XOR from `system.account`, the rest from
/// `tokens.accounts`.
async fn fetch_holdings(
    chain: &crate::chain::ChainClient,
    address: &str,
) -> Result<Vec<RawHolding>, ApiError> {
    let (bytes, _) = ss58_decode(address)
        .map_err(|_| ApiError::BadRequest("Invalid address format in list".into()))?;
    let account = AccountId32(bytes);
    let holdings = chain
        .with_client(|client| async move {
            let at = client.storage().at_latest().await?;
            let mut out = Vec::new();
            let sys = at
                .fetch(&sora::storage().system().account(&account))
                .await?;
            if let Some(info) = sys {
                if info.data.free > 0 {
                    out.push(RawHolding {
                        asset_id: XOR_ASSET_ID.to_string(),
                        free: info.data.free,
                    });
                }
            }
            let mut stream = at
                .iter(sora::storage().tokens().accounts_iter1(&account))
                .await?;
            while let Some(kv) = stream.next().await {
                let kv = kv?;
                // Double map hashed with *_Concat: the AssetId32 code is
                // the trailing 32 bytes of the storage key.
                let Some(code) = kv
                    .key_bytes
                    .len()
                    .checked_sub(32)
                    .map(|i| &kv.key_bytes[i..])
                else {
                    continue;
                };
                out.push(RawHolding {
                    asset_id: format!("0x{}", hex::encode(code)),
                    free: kv.value.free,
                });
            }
            Ok(out)
        })
        .await?;
    Ok(holdings)
}

/// One wallet's priced holdings (`getAddressBalances`).
async fn wallet_balances(
    state: &AppState,
    chain: &crate::chain::ChainClient,
    address: String,
) -> Result<WalletBalances, ApiError> {
    let holdings = fetch_holdings(chain, &address).await?;
    let registry = state.registry.read().await;
    let threshold = BigDecimal::new(BigInt::from(1), 4); // 0.0001
    let mut kept: Vec<Holding> = Vec::new();
    for h in holdings {
        let decimals = decimals_for(&registry, &h.asset_id);
        let amount = human(h.free, decimals);
        let keep = if h.asset_id == XOR_ASSET_ID {
            h.free > 0
        } else {
            amount > threshold
        };
        if keep {
            kept.push(Holding {
                symbol: symbol_for_wallet(&registry, &h.asset_id),
                logo: logo_for(&registry, &h.asset_id),
                asset_id: h.asset_id,
                amount,
            });
        }
    }
    drop(registry);

    let asset_ids: Vec<String> = kept.iter().map(|h| h.asset_id.clone()).collect();
    let prices: HashMap<String, f64> = valuation_prices(&state.db, &asset_ids)
        .await?
        .into_iter()
        .map(|p| (p.asset_id, p.price_usd))
        .collect();
    let illiquid: std::collections::HashSet<String> = illiquid_assets(&state.db, &asset_ids)
        .await?
        .into_iter()
        .collect();
    let quoted: HashMap<String, f64> = latest_prices(&state.db, &asset_ids)
        .await?
        .into_iter()
        .map(|p| (p.asset_id, p.price_usd))
        .collect();

    let mut tokens: Vec<(f64, TokenBalance)> = kept
        .into_iter()
        .map(|h| {
            let price = prices.get(&h.asset_id).copied().unwrap_or(0.0);
            let usd = (&h.amount * BigDecimal::try_from(price).unwrap_or_default())
                .with_scale_round(2, RoundingMode::HalfUp);
            (
                usd.to_string().parse::<f64>().unwrap_or(0.0),
                TokenBalance {
                    symbol: h.symbol,
                    logo: h.logo,
                    amount: fmt_fixed(&h.amount, 4),
                    usd_value: fmt_fixed(&usd, 2),
                    price: quoted.get(&h.asset_id).copied(),
                    illiquid: illiquid.contains(&h.asset_id),
                    asset_id: h.asset_id,
                },
            )
        })
        .collect();
    tokens.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let total_usd: f64 = tokens.iter().map(|(u, _)| *u).sum();
    Ok(WalletBalances {
        address,
        tokens: tokens.into_iter().map(|(_, t)| t).collect(),
        total_usd,
    })
}

#[derive(Serialize)]
struct SimpleBalance {
    symbol: String,
    logo: String,
    amount: String,
    #[serde(rename = "usdValue")]
    usd_value: String,
    /// See [`TokenBalance::price`].
    price: Option<f64>,
    /// See [`TokenBalance::illiquid`]. Omitted when false.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    illiquid: bool,
}

/// Node `/balance/:address`: same rows, no `assetId`, bare array;
/// any failure → `[]`.
async fn balance(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<Vec<SimpleBalance>>, ApiError> {
    let address = crate::util::validate_address(&address)?;
    let Some(chain) = state.chain.as_ref() else {
        return Ok(Json(Vec::new()));
    };
    let rows = match wallet_balances(&state, chain, address).await {
        Ok(w) => w.tokens,
        Err(_) => Vec::new(),
    };
    Ok(Json(
        rows.into_iter()
            .map(|t| SimpleBalance {
                symbol: t.symbol,
                logo: t.logo,
                amount: t.amount,
                usd_value: t.usd_value,
                price: t.price,
                illiquid: t.illiquid,
            })
            .collect(),
    ))
}

// ---------------------------------------------------------------------
// /wallet/realizable/:address
// ---------------------------------------------------------------------

/// Shares of a holding the realizable value can be asked for.
const REALIZABLE_PCTS: &[u32] = &[10, 25, 50, 100];
const REALIZABLE_TTL: std::time::Duration = std::time::Duration::from_secs(5 * 60);
/// Simultaneous `liquidityProxy_quote` calls per request.
const QUOTE_CONCURRENCY: usize = 8;

#[derive(Deserialize)]
struct RealizableQuery {
    #[serde(default, deserialize_with = "crate::util::lenient_i64")]
    pct: Option<i64>,
}

#[derive(Serialize, Deserialize)]
struct RealizableToken {
    symbol: String,
    #[serde(rename = "assetId")]
    asset_id: String,
    /// Whole holding and the share of it being sold, human units.
    amount: String,
    #[serde(rename = "soldAmount")]
    sold_amount: String,
    /// Latest marginal quote (the Tokens page price).
    price: Option<f64>,
    /// `soldAmount x price`.
    #[serde(rename = "marginalUsd")]
    marginal_usd: Option<f64>,
    /// DAI the chain pays for selling `soldAmount` now
    /// (`liquidityProxy.quote`, DAI = 1 USD); `null` = no route.
    #[serde(rename = "realizableUsd")]
    realizable_usd: Option<f64>,
}

#[derive(Serialize, Deserialize)]
struct RealizableResponse {
    address: String,
    pct: u32,
    tokens: Vec<RealizableToken>,
    #[serde(rename = "totalMarginalUsd")]
    total_marginal_usd: f64,
    #[serde(rename = "totalRealizableUsd")]
    total_realizable_usd: f64,
}

/// `free x pct / 100` in raw units (integer division, as a sale would be).
pub fn share_raw(free: u128, pct: u32) -> u128 {
    free / 100 * u128::from(pct) + free % 100 * u128::from(pct) / 100
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

/// What selling `pct` % of every holding would pay right now. Each token
/// is quoted on its own: selling several at once through shared pools
/// would pay less than the sum.
async fn realizable(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(q): Query<RealizableQuery>,
) -> Result<Json<RealizableResponse>, ApiError> {
    let address = crate::util::validate_address(&address)?;
    let pct = u32::try_from(q.pct.unwrap_or(100)).unwrap_or(0);
    if !REALIZABLE_PCTS.contains(&pct) {
        return Err(ApiError::BadRequest("pct must be 10, 25, 50 or 100".into()));
    }
    let key = format!("realizable:{address}:{pct}");
    if let Some(v) = state.cached_scan(&key, REALIZABLE_TTL).await {
        return serde_json::from_value(v)
            .map(Json)
            .map_err(|e| ApiError::Internal(e.to_string()));
    }
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let holdings = fetch_holdings(chain, &address).await?;

    let registry = state.registry.read().await;
    let threshold = BigDecimal::new(BigInt::from(1), 4);
    let kept: Vec<(RawHolding, u32, String)> = holdings
        .into_iter()
        .filter_map(|h| {
            let decimals = decimals_for(&registry, &h.asset_id);
            let keep = if h.asset_id == XOR_ASSET_ID {
                h.free > 0
            } else {
                human(h.free, decimals) > threshold
            };
            keep.then(|| {
                let symbol = symbol_for_wallet(&registry, &h.asset_id);
                (h, decimals, symbol)
            })
        })
        .collect();
    drop(registry);

    let asset_ids: Vec<String> = kept.iter().map(|(h, _, _)| h.asset_id.clone()).collect();
    let quoted: HashMap<String, f64> = latest_prices(&state.db, &asset_ids)
        .await?
        .into_iter()
        .map(|p| (p.asset_id, p.price_usd))
        .collect();

    let rpc = chain.rpc().await?;
    let mut tokens = Vec::with_capacity(kept.len());
    for batch in kept.chunks(QUOTE_CONCURRENCY) {
        let quotes = futures::future::join_all(batch.iter().map(|(h, decimals, _)| {
            let rpc = rpc.clone();
            let sold = share_raw(h.free, pct);
            let asset_id = h.asset_id.clone();
            let decimals = *decimals;
            async move {
                if sold == 0 {
                    return Ok(None);
                }
                if asset_id == DAI_ASSET_ID {
                    return Ok(human(sold, decimals).to_string().parse::<f64>().ok());
                }
                quote_sell_in_dai(&rpc, &AssetId::new(asset_id), &sold.to_string()).await
            }
        }))
        .await;
        for ((h, decimals, symbol), quote) in batch.iter().zip(quotes) {
            let sold = share_raw(h.free, pct);
            let sold_human = human(sold, *decimals);
            let price = quoted.get(&h.asset_id).copied();
            let marginal_usd = price.and_then(|p| {
                sold_human
                    .to_string()
                    .parse::<f64>()
                    .ok()
                    .map(|a| round2(a * p))
            });
            let realizable_usd = quote
                .map_err(|e| ApiError::Internal(format!("quote {}: {e}", h.asset_id)))?
                .map(round2);
            tokens.push(RealizableToken {
                symbol: symbol.clone(),
                asset_id: h.asset_id.clone(),
                amount: fmt_fixed(&human(h.free, *decimals), 4),
                sold_amount: fmt_fixed(&sold_human, 4),
                price,
                marginal_usd,
                realizable_usd,
            });
        }
    }
    tokens.sort_by(|a, b| {
        b.realizable_usd
            .unwrap_or(0.0)
            .partial_cmp(&a.realizable_usd.unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let response = RealizableResponse {
        address,
        pct,
        total_marginal_usd: round2(tokens.iter().filter_map(|t| t.marginal_usd).sum()),
        total_realizable_usd: round2(tokens.iter().filter_map(|t| t.realizable_usd).sum()),
        tokens,
    };
    let json = serde_json::to_value(&response).map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store_scan(&key, json).await;
    Ok(Json(response))
}

/// Node: `assetInfo?.symbol || 'UNK'` (no `0xXXXX` fallback here).
fn symbol_for_wallet(registry: &crate::state::Registry, asset_id: &str) -> String {
    match registry.get(asset_id) {
        Some(_) => symbol_for(registry, asset_id),
        None => "UNK".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ss58_shape_check_matches_node_regex() {
        assert!(looks_like_ss58(
            "cnWeiModLdWS4hC75QeZxEANGNUmekog7YaaJ9PevFH1UTnhh"
        ));
        assert!(!looks_like_ss58("0x1234"));
        assert!(!looks_like_ss58(
            "cnWeiModLdWS4hC75QeZxEANGNUmekog7YaaJ9PevFH1UTnh0"
        ));
    }

    #[test]
    fn fixed_keeps_trailing_zeros() {
        assert_eq!(fmt_fixed(&BigDecimal::from(0), 2), "0.00");
        assert_eq!(
            fmt_fixed(&human(1_500_000_000_000_000_000, 18), 4),
            "1.5000"
        );
        assert_eq!(
            fmt_fixed(&human(110_704_412_345_678_901_234, 18), 4),
            "110.7044"
        );
    }

    #[test]
    fn human_scales_by_decimals() {
        assert_eq!(
            human(1_500_000_000_000_000_000, 18)
                .normalized()
                .to_string(),
            "1.5"
        );
        assert_eq!(human(1234, 2).normalized().to_string(), "12.34");
    }
}

// ---------------------------------------------------------------------
// /wallet/info/:address  (db_pg.js::getWalletInfo + the whale score;
// swap USD per `sm.swap_usd`)
// ---------------------------------------------------------------------

const WALLET_INFO_TTL: std::time::Duration = std::time::Duration::from_secs(15 * 60);

#[derive(Serialize, Deserialize)]
struct ModuleCount {
    section: String,
    count: String,
}

#[derive(Serialize, Deserialize)]
struct TopToken {
    symbol: String,
    total_usd: f64,
    trades: String,
}

#[derive(Serialize, Deserialize)]
struct Contact {
    counterparty: String,
    tx_count: String,
    total_usd: f64,
}

#[derive(Serialize, Deserialize)]
struct CountUsd {
    count: i64,
    usd: f64,
}

#[derive(Serialize, Deserialize)]
struct WhaleBreakdown {
    volume: i64,
    frequency: i64,
    diversity: i64,
}

#[derive(Serialize, Deserialize)]
struct WalletInfo {
    #[serde(rename = "firstTx")]
    first_tx: Option<String>,
    #[serde(rename = "lastTx")]
    last_tx: Option<String>,
    #[serde(rename = "txCount")]
    tx_count: i64,
    #[serde(rename = "successCount")]
    success_count: i64,
    #[serde(rename = "daysActive")]
    days_active: i64,
    modules: Vec<ModuleCount>,
    #[serde(rename = "governanceTx")]
    governance_tx: i64,
    #[serde(rename = "swapCount")]
    swap_count: i64,
    #[serde(rename = "swapAvgUsd")]
    swap_avg_usd: f64,
    #[serde(rename = "swapMaxUsd")]
    swap_max_usd: f64,
    #[serde(rename = "swapTotalVolume")]
    swap_total_volume: f64,
    #[serde(rename = "topTokens")]
    top_tokens: Vec<TopToken>,
    #[serde(rename = "uniqueTokens")]
    unique_tokens: i64,
    #[serde(rename = "topContacts")]
    top_contacts: Vec<Contact>,
    #[serde(rename = "transfersOut")]
    transfers_out: CountUsd,
    #[serde(rename = "transfersIn")]
    transfers_in: CountUsd,
    #[serde(rename = "lpDeposits")]
    lp_deposits: i64,
    #[serde(rename = "lpWithdrawals")]
    lp_withdrawals: i64,
    #[serde(rename = "lpDepositedUsd")]
    lp_deposited_usd: f64,
    #[serde(rename = "lpWithdrawnUsd")]
    lp_withdrawn_usd: f64,
    #[serde(rename = "lpUniquePools")]
    lp_unique_pools: i64,
    #[serde(rename = "bridgeIncoming")]
    bridge_incoming: CountUsd,
    #[serde(rename = "bridgeOutgoing")]
    bridge_outgoing: CountUsd,
    #[serde(rename = "bridgeUniqueNetworks")]
    bridge_unique_networks: i64,
    #[serde(rename = "whaleScore")]
    whale_score: i64,
    #[serde(rename = "whaleTier")]
    whale_tier: String,
    #[serde(rename = "whaleBreakdown")]
    whale_breakdown: WhaleBreakdown,
}

/// Node whale score: `min(40, round(volume/500000×40)) + min(30,
/// round(tx/5000×30)) + min(30, round(diversity/30×30))`.
pub fn whale(volume: f64, tx_count: i64, diversity: i64) -> (i64, i64, i64, &'static str) {
    let v = ((volume / 500_000.0) * 40.0).round().min(40.0) as i64;
    let f = ((tx_count as f64 / 5000.0) * 30.0).round().min(30.0) as i64;
    let d = ((diversity as f64 / 30.0) * 30.0).round().min(30.0) as i64;
    let score = v + f + d;
    let tier = if score > 90 {
        "Megawhale"
    } else if score > 75 {
        "Whale"
    } else if score > 50 {
        "Dolphin"
    } else if score > 25 {
        "Fish"
    } else {
        "Shrimp"
    };
    (v, f, d, tier)
}

fn f(v: Option<BigDecimal>) -> f64 {
    v.and_then(|b| bigdecimal::ToPrimitive::to_f64(&b))
        .unwrap_or(0.0)
}

async fn wallet_info(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<WalletInfo>, ApiError> {
    let address = crate::util::validate_address(&address)?;
    let key = format!("wallet-info:{address}");
    super::chain_state::cached_or_scan(&state, &key, WALLET_INFO_TTL, move |st| {
        compute_wallet_info(st, address)
    })
    .await
    .map(Json)
}

#[derive(Deserialize)]
struct SectionCount(String, i64);

#[derive(Deserialize)]
struct AssetVolume(String, Option<f64>, i64);

#[derive(Deserialize)]
struct CounterpartyVolume(String, i64, Option<f64>);

/// One pass per table over the wallet's rows, in one transaction with a
/// long statement timeout (a wallet with half a million extrinsics needs
/// minutes cold) and no parallel workers (the container's small
/// `/dev/shm` makes parallel aggregates fail).
async fn compute_wallet_info(state: AppState, address: String) -> Result<WalletInfo, ApiError> {
    let mut tx = state.db.begin().await?;
    sqlx::query!(
        r#"SELECT set_config('statement_timeout', '300s', true) AS "a!",
                  set_config('max_parallel_workers_per_gather', '0', true) AS "b!""#
    )
    .fetch_one(&mut *tx)
    .await?;
    let ext = sqlx::query!(
        r#"WITH e AS MATERIALIZED (
               SELECT section, success, block_timestamp FROM sm.extrinsics WHERE signer = $1
           ), s AS (SELECT section, COUNT(*)::bigint AS n FROM e GROUP BY section)
           SELECT (SELECT MIN(block_timestamp) FROM e) AS "first",
                  (SELECT MAX(block_timestamp) FROM e) AS "last",
                  (SELECT COUNT(*) FROM e)::bigint AS "tx_count!",
                  (SELECT COUNT(*) FILTER (WHERE success) FROM e)::bigint AS "success_count!",
                  (SELECT COUNT(DISTINCT FLOOR(EXTRACT(EPOCH FROM block_timestamp) / 86400)) FROM e)::bigint
                      AS "days_active!",
                  (SELECT COALESCE(jsonb_agg(jsonb_build_array(section, n) ORDER BY n DESC, section), '[]'::jsonb)
                     FROM s) AS "sections!""#,
        address
    )
    .fetch_one(&mut *tx)
    .await?;
    let sections: Vec<SectionCount> =
        serde_json::from_value(ext.sections).map_err(|e| ApiError::Internal(e.to_string()))?;
    let governance: i64 = sections
        .iter()
        .filter(|s| {
            matches!(
                s.0.as_str(),
                "democracy" | "council" | "electionsPhragmen" | "technicalCommittee"
            )
        })
        .map(|s| s.1)
        .sum();
    let modules: Vec<SectionCount> = sections.into_iter().take(10).collect();
    let swaps = sqlx::query!(
        r#"WITH s AS MATERIALIZED (
               SELECT input_asset_id AS i, output_asset_id AS o, sm.swap_usd(usd_value, output_usd_value) AS usd
               FROM sm.swaps WHERE caller = $1
           ), a AS (
               SELECT asset, SUM(usd) AS total, COUNT(*)::bigint AS trades
               FROM (SELECT i AS asset, usd FROM s UNION ALL SELECT o, usd FROM s) u GROUP BY asset
           )
           SELECT (SELECT COUNT(*) FROM s)::bigint AS "swap_count!",
                  (SELECT COALESCE(AVG(usd), 0) FROM s) AS "avg_usd: BigDecimal",
                  (SELECT COALESCE(MAX(usd), 0) FROM s) AS "max_usd: BigDecimal",
                  (SELECT COALESCE(SUM(usd), 0) FROM s) AS "total_vol: BigDecimal",
                  (SELECT COUNT(*) FROM a)::bigint AS "unique_tokens!",
                  (SELECT COALESCE(jsonb_agg(jsonb_build_array(asset, total, trades)
                                             ORDER BY total DESC NULLS LAST, asset), '[]'::jsonb)
                     FROM (SELECT * FROM a ORDER BY total DESC NULLS LAST, asset LIMIT 10) t) AS "top!""#,
        address
    )
    .fetch_one(&mut *tx)
    .await?;
    let token_rows: Vec<AssetVolume> =
        serde_json::from_value(swaps.top).map_err(|e| ApiError::Internal(e.to_string()))?;
    let unique_tokens = swaps.unique_tokens;
    let tr = sqlx::query!(
        r#"WITH t AS MATERIALIZED (
               SELECT from_address, to_address, usd_value, block_timestamp FROM sm.transfers WHERE from_address = $1
               UNION ALL
               SELECT from_address, to_address, usd_value, block_timestamp FROM sm.transfers
               WHERE to_address = $1 AND from_address <> $1
           ), c AS (
               SELECT counterparty, COUNT(*)::bigint AS n, SUM(usd_value) AS usd FROM (
                   SELECT to_address AS counterparty, usd_value FROM t WHERE from_address = $1
                   UNION ALL SELECT from_address, usd_value FROM t WHERE to_address = $1
               ) u GROUP BY counterparty
           )
           SELECT (SELECT COUNT(*) FILTER (WHERE from_address = $1) FROM t)::bigint AS "out_count!",
                  (SELECT COALESCE(SUM(usd_value) FILTER (WHERE from_address = $1), 0) FROM t)
                      AS "out_usd: BigDecimal",
                  (SELECT COUNT(*) FILTER (WHERE to_address = $1) FROM t)::bigint AS "in_count!",
                  (SELECT COALESCE(SUM(usd_value) FILTER (WHERE to_address = $1), 0) FROM t)
                      AS "in_usd: BigDecimal",
                  (SELECT MIN(block_timestamp) FROM t) AS "first",
                  (SELECT MAX(block_timestamp) FROM t) AS "last",
                  (SELECT COALESCE(jsonb_agg(jsonb_build_array(counterparty, n, usd)
                                             ORDER BY usd DESC NULLS LAST, counterparty), '[]'::jsonb)
                     FROM (SELECT * FROM c ORDER BY usd DESC NULLS LAST, counterparty LIMIT 10) x) AS "contacts!""#,
        address
    )
    .fetch_one(&mut *tx)
    .await?;
    let contacts: Vec<CounterpartyVolume> =
        serde_json::from_value(tr.contacts).map_err(|e| ApiError::Internal(e.to_string()))?;
    let lp = sqlx::query!(
        r#"SELECT COUNT(*) FILTER (WHERE kind = 'deposit')::bigint AS "deposits!",
                  COUNT(*) FILTER (WHERE kind = 'withdraw')::bigint AS "withdrawals!",
                  COALESCE(SUM(usd_value) FILTER (WHERE kind = 'deposit'), 0) AS "deposited_usd: BigDecimal",
                  COALESCE(SUM(usd_value) FILTER (WHERE kind = 'withdraw'), 0) AS "withdrawn_usd: BigDecimal",
                  COUNT(DISTINCT base_asset_id || '-' || target_asset_id)::bigint AS "unique_pools!"
           FROM sm.liquidity_events WHERE caller = $1"#,
        address
    )
    .fetch_one(&mut *tx)
    .await?;
    let br = sqlx::query!(
        r#"SELECT COUNT(*) FILTER (WHERE direction = 'in')::bigint AS "incoming_count!",
                  COUNT(*) FILTER (WHERE direction = 'out')::bigint AS "outgoing_count!",
                  COALESCE(SUM(usd_value) FILTER (WHERE direction = 'in'), 0) AS "incoming_usd: BigDecimal",
                  COALESCE(SUM(usd_value) FILTER (WHERE direction = 'out'), 0) AS "outgoing_usd: BigDecimal",
                  COUNT(DISTINCT network)::bigint AS "unique_networks!"
           FROM sm.bridges WHERE caller = $1 OR counterparty = $1"#,
        address
    )
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    let ms = |t: Option<chrono::DateTime<chrono::Utc>>| t.map(|t| t.timestamp_millis().to_string());
    let registry = state.registry.read().await;
    let top_tokens = token_rows
        .into_iter()
        .map(|AssetVolume(asset_id, total_usd, trades)| TopToken {
            symbol: symbol_for(&registry, &asset_id),
            total_usd: total_usd.unwrap_or(0.0),
            trades: trades.to_string(),
        })
        .collect();
    let swap_total = f(swaps.total_vol);
    let diversity = unique_tokens + lp.unique_pools + br.unique_networks;
    let (v, fq, d, tier) = whale(swap_total, ext.tx_count, diversity);
    let info = WalletInfo {
        first_tx: ms(ext.first).or_else(|| ms(tr.first)),
        last_tx: ms(ext.last).or_else(|| ms(tr.last)),
        tx_count: ext.tx_count,
        success_count: ext.success_count,
        days_active: ext.days_active,
        modules: modules
            .into_iter()
            .map(|SectionCount(section, count)| ModuleCount {
                section,
                count: count.to_string(),
            })
            .collect(),
        governance_tx: governance,
        swap_count: swaps.swap_count,
        swap_avg_usd: f(swaps.avg_usd),
        swap_max_usd: f(swaps.max_usd),
        swap_total_volume: swap_total,
        top_tokens,
        unique_tokens,
        top_contacts: contacts
            .into_iter()
            .map(
                |CounterpartyVolume(counterparty, tx_count, total_usd)| Contact {
                    counterparty,
                    tx_count: tx_count.to_string(),
                    total_usd: total_usd.unwrap_or(0.0),
                },
            )
            .collect(),
        transfers_out: CountUsd {
            count: tr.out_count,
            usd: f(tr.out_usd),
        },
        transfers_in: CountUsd {
            count: tr.in_count,
            usd: f(tr.in_usd),
        },
        lp_deposits: lp.deposits,
        lp_withdrawals: lp.withdrawals,
        lp_deposited_usd: f(lp.deposited_usd),
        lp_withdrawn_usd: f(lp.withdrawn_usd),
        lp_unique_pools: lp.unique_pools,
        bridge_incoming: CountUsd {
            count: br.incoming_count,
            usd: f(br.incoming_usd),
        },
        bridge_outgoing: CountUsd {
            count: br.outgoing_count,
            usd: f(br.outgoing_usd),
        },
        bridge_unique_networks: br.unique_networks,
        whale_score: v + fq + d,
        whale_tier: tier.to_string(),
        whale_breakdown: WhaleBreakdown {
            volume: v,
            frequency: fq,
            diversity: d,
        },
    };
    Ok(info)
}

#[cfg(test)]
mod info_tests {
    use super::*;

    #[test]
    fn share_raw_is_exact_and_never_overflows() {
        assert_eq!(share_raw(1_000, 10), 100);
        assert_eq!(share_raw(999, 50), 499);
        assert_eq!(share_raw(7, 100), 7);
        assert_eq!(share_raw(u128::MAX, 100), u128::MAX);
        assert_eq!(share_raw(u128::MAX, 25), u128::MAX / 4);
    }

    #[test]
    fn whale_score_matches_prod_wallet() {
        // prod: volume 8783.83, 1617 tx, 38 tokens + 28 pools + 0 networks → 1 + 10 + 30 = 41 "Fish"
        let (v, f, d, tier) = whale(8783.830666506332, 1617, 38 + 28);
        assert_eq!((v, f, d, tier), (1, 10, 30, "Fish"));
        assert_eq!(whale(0.0, 0, 0).3, "Shrimp");
    }
}
