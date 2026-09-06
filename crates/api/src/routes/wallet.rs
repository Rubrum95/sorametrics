//! Chain-state wallet routes (group D): `POST /balances`.
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
use axum::{extract::State, routing::post, Json, Router};
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
    Router::new().route("/balances", post(balances))
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
        result.push(WalletBalances {
            address,
            tokens: tokens.into_iter().map(|(_, t)| t).collect(),
            total_usd,
        });
    }
    Ok(Json(BalancesResponse { result }))
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
