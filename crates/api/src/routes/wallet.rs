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
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use bigdecimal::{BigDecimal, RoundingMode};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use sorametrics_core::chain::ss58_decode;
use sorametrics_db::ts::latest_prices;
use sorametrics_substrate::runtime::sora;
use std::collections::HashMap;
use subxt::utils::AccountId32;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/balances", post(balances))
        .route("/balance/:address", get(balance))
        .route("/wallet/info/:address", get(wallet_info))
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
    #[serde(rename = "assetId")]
    asset_id: String,
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

/// One wallet's priced holdings (`getAddressBalances`).
async fn wallet_balances(
    state: &AppState,
    chain: &crate::chain::ChainClient,
    address: String,
) -> Result<WalletBalances, ApiError> {
    let (bytes, _) = ss58_decode(&address)
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
    let prices: HashMap<String, f64> = latest_prices(&state.db, &asset_ids)
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
            })
            .collect(),
    ))
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
// /wallet/info/:address  (db_pg.js::getWalletInfo + the whale score)
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
    if let Some(v) = state.cached_scan(&key, WALLET_INFO_TTL).await {
        return serde_json::from_value(v)
            .map(Json)
            .map_err(|e| ApiError::Internal(e.to_string()));
    }
    let ext = sqlx::query!(
        r#"SELECT MIN(block_timestamp) AS "first", MAX(block_timestamp) AS "last", COUNT(*)::bigint AS "tx_count!",
                  COUNT(*) FILTER (WHERE success)::bigint AS "success_count!",
                  COUNT(DISTINCT FLOOR(EXTRACT(EPOCH FROM block_timestamp) / 86400))::bigint AS "days_active!"
           FROM sm.extrinsics WHERE signer = $1"#,
        address
    )
    .fetch_one(&state.db)
    .await?;
    let modules = sqlx::query!(
        r#"SELECT section, COUNT(*)::bigint AS "count!" FROM sm.extrinsics WHERE signer = $1
           GROUP BY section ORDER BY COUNT(*) DESC LIMIT 10"#,
        address
    )
    .fetch_all(&state.db)
    .await?;
    let governance = sqlx::query_scalar!(
        r#"SELECT COUNT(*)::bigint AS "c!" FROM sm.extrinsics WHERE signer = $1
           AND section IN ('democracy', 'council', 'electionsPhragmen', 'technicalCommittee')"#,
        address
    )
    .fetch_one(&state.db)
    .await?;
    let swaps = sqlx::query!(
        r#"SELECT COUNT(*)::bigint AS "swap_count!", COALESCE(AVG(usd_value), 0) AS "avg_usd: BigDecimal",
                  COALESCE(MAX(usd_value), 0) AS "max_usd: BigDecimal", COALESCE(SUM(usd_value), 0) AS "total_vol: BigDecimal"
           FROM sm.swaps WHERE caller = $1"#,
        address
    )
    .fetch_one(&state.db)
    .await?;
    let token_rows = sqlx::query!(
        r#"SELECT asset_id AS "asset_id!", SUM(usd) AS "total_usd: BigDecimal", COUNT(*)::bigint AS "trades!" FROM (
               SELECT input_asset_id AS asset_id, usd_value AS usd FROM sm.swaps WHERE caller = $1
               UNION ALL SELECT output_asset_id, output_usd_value FROM sm.swaps WHERE caller = $1
           ) u GROUP BY asset_id ORDER BY SUM(usd) DESC NULLS LAST LIMIT 10"#,
        address
    )
    .fetch_all(&state.db)
    .await?;
    let unique_tokens = sqlx::query_scalar!(
        r#"SELECT COUNT(DISTINCT a)::bigint AS "c!" FROM (
               SELECT input_asset_id AS a FROM sm.swaps WHERE caller = $1
               UNION SELECT output_asset_id FROM sm.swaps WHERE caller = $1) u"#,
        address
    )
    .fetch_one(&state.db)
    .await?;
    let contacts = sqlx::query!(
        r#"SELECT counterparty AS "counterparty!", COUNT(*)::bigint AS "tx_count!", SUM(usd_value) AS "total_usd: BigDecimal" FROM (
               SELECT to_address AS counterparty, usd_value FROM sm.transfers WHERE from_address = $1
               UNION ALL SELECT from_address, usd_value FROM sm.transfers WHERE to_address = $1
           ) u GROUP BY counterparty ORDER BY SUM(usd_value) DESC NULLS LAST LIMIT 10"#,
        address
    )
    .fetch_all(&state.db)
    .await?;
    let tr = sqlx::query!(
        r#"SELECT COUNT(*) FILTER (WHERE from_address = $1)::bigint AS "out_count!",
                  COALESCE(SUM(usd_value) FILTER (WHERE from_address = $1), 0) AS "out_usd: BigDecimal",
                  COUNT(*) FILTER (WHERE to_address = $1)::bigint AS "in_count!",
                  COALESCE(SUM(usd_value) FILTER (WHERE to_address = $1), 0) AS "in_usd: BigDecimal",
                  MIN(block_timestamp) AS "first", MAX(block_timestamp) AS "last"
           FROM sm.transfers WHERE from_address = $1 OR to_address = $1"#,
        address
    )
    .fetch_one(&state.db)
    .await?;
    let lp = sqlx::query!(
        r#"SELECT COUNT(*) FILTER (WHERE kind = 'deposit')::bigint AS "deposits!",
                  COUNT(*) FILTER (WHERE kind = 'withdraw')::bigint AS "withdrawals!",
                  COALESCE(SUM(usd_value) FILTER (WHERE kind = 'deposit'), 0) AS "deposited_usd: BigDecimal",
                  COALESCE(SUM(usd_value) FILTER (WHERE kind = 'withdraw'), 0) AS "withdrawn_usd: BigDecimal",
                  COUNT(DISTINCT base_asset_id || '-' || target_asset_id)::bigint AS "unique_pools!"
           FROM sm.liquidity_events WHERE caller = $1"#,
        address
    )
    .fetch_one(&state.db)
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
    .fetch_one(&state.db)
    .await?;
    let ms = |t: Option<chrono::DateTime<chrono::Utc>>| t.map(|t| t.timestamp_millis().to_string());
    let registry = state.registry.read().await;
    let top_tokens = token_rows
        .into_iter()
        .map(|r| TopToken {
            symbol: symbol_for(&registry, &r.asset_id),
            total_usd: f(r.total_usd),
            trades: r.trades.to_string(),
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
            .map(|m| ModuleCount {
                section: m.section,
                count: m.count.to_string(),
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
            .map(|c| Contact {
                counterparty: c.counterparty,
                tx_count: c.tx_count.to_string(),
                total_usd: f(c.total_usd),
            })
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
    if let Ok(v) = serde_json::to_value(&info) {
        state.store_scan(&key, v).await;
    }
    Ok(Json(info))
}

#[cfg(test)]
mod info_tests {
    use super::*;

    #[test]
    fn whale_score_matches_prod_wallet() {
        // prod: volume 8783.83, 1617 tx, 38 tokens + 28 pools + 0 networks → 1 + 10 + 30 = 41 "Fish"
        let (v, f, d, tier) = whale(8783.830666506332, 1617, 38 + 28);
        assert_eq!((v, f, d, tier), (1, 10, 30, "Fish"));
        assert_eq!(whale(0.0, 0, 0).3, "Shrimp");
    }
}
