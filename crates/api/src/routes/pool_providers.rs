//! Liquidity-provider reads from `poolXYK` storage (group D):
//! `/pool/providers` and `/wallet/liquidity/:address`.
//!
//! `/pool/providers?base=<id>&target=<id>` (`index.js`): the pool
//! account from `poolXYK.properties(base, target)` (or the reversed
//! pair), then every `poolXYK.poolProviders(pool, *)` entry as
//! `{ address, balance }` (LP tokens / 1e18), sorted desc. 400 without
//! both ids; `[]` when the pair has no pool. Cached 90 s.
//!
//! `/wallet/liquidity/:address`: every pool (`properties` entries,
//! cached 30 min) → the wallet's `poolProviders(pool, address)` read in
//! batches of 100 with one `state_queryStorageAt` per batch (the
//! Node's `.multi`); for each non-zero position: `share = balance /
//! totalIssuances(pool)`, amounts = `reserves × share / 10^decimals`,
//! value at the latest prices; positions under $0.10 or with a
//! non-whitelisted token are dropped. `[{ base, target, amountBase,
//! amountTarget, value, share }]` sorted by value desc. Cached 15 min.

use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sorametrics_core::chain::{ss58_decode, ss58_encode_sora};
use sorametrics_db::sm::RegistryAsset;
use sorametrics_db::ts::latest_prices;
use sorametrics_substrate::runtime::sora;
use sorametrics_substrate::runtime::sora::runtime_types::common::primitives::AssetId32;
use sorametrics_substrate::runtime::sora::runtime_types::common::primitives::_allowed_deprecated::PredefinedAssetId;
use std::collections::HashMap;
use std::time::Duration;
use subxt::ext::codec::Decode;
use subxt::utils::AccountId32;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/pool/providers", get(providers))
        .route("/wallet/liquidity/:address", get(wallet_liquidity))
}

const PROVIDERS_TTL: Duration = Duration::from_secs(90);
const POOL_PROPS_TTL: Duration = Duration::from_secs(30 * 60);
const LP_TTL: Duration = Duration::from_secs(15 * 60);
const BATCH: usize = 100;

fn asset(id: &str) -> Option<AssetId32<PredefinedAssetId>> {
    let bytes = hex::decode(id.strip_prefix("0x")?).ok()?;
    let code: [u8; 32] = bytes.try_into().ok()?;
    Some(AssetId32 {
        code,
        __ignore: Default::default(),
    })
}

fn lp_units(raw: u128) -> f64 {
    raw as f64 / 1e18
}

// =============================================================
// /pool/providers
// =============================================================

#[derive(Debug, Deserialize)]
struct ProvidersQuery {
    base: Option<String>,
    target: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Provider {
    address: String,
    balance: f64,
}

async fn providers(
    State(state): State<AppState>,
    Query(q): Query<ProvidersQuery>,
) -> Result<Json<Vec<Provider>>, ApiError> {
    let (Some(base), Some(target)) = (q.base, q.target) else {
        return Err(ApiError::BadRequest("Missing base or target".into()));
    };
    let base = crate::util::validate_asset_id(&base)?;
    let target = crate::util::validate_asset_id(&target)?;
    let key = format!("providers:{base}:{target}");
    if let Some(v) = state.cached_scan(&key, PROVIDERS_TTL).await {
        return serde_json::from_value(v)
            .map(Json)
            .map_err(|e| ApiError::Internal(e.to_string()));
    }
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let (Some(b), Some(t)) = (asset(&base), asset(&target)) else {
        return Err(ApiError::BadRequest("Invalid asset ID format".into()));
    };
    let list: Vec<Provider> = chain
        .with_client(|client| async move {
            let at = client.storage().at_latest().await?;
            let props = match at
                .fetch(&sora::storage().pool_xyk().properties(&b, &t))
                .await?
            {
                Some(p) => Some(p),
                None => {
                    at.fetch(&sora::storage().pool_xyk().properties(&t, &b))
                        .await?
                }
            };
            let Some((pool_account, _fees_account)) = props else {
                return Ok(Vec::new());
            };
            let mut stream = at
                .iter(
                    sora::storage()
                        .pool_xyk()
                        .pool_providers_iter1(&pool_account),
                )
                .await?;
            let mut out = Vec::new();
            while let Some(kv) = stream.next().await {
                let kv = kv?;
                // Blake2_128Concat(pool) ‖ Blake2_128Concat(provider): last 32.
                let n = kv.key_bytes.len();
                let acc: [u8; 32] = kv.key_bytes[n - 32..].try_into().unwrap_or([0; 32]);
                out.push(Provider {
                    address: ss58_encode_sora(&acc),
                    balance: lp_units(kv.value),
                });
            }
            out.sort_by(|a, b| {
                b.balance
                    .partial_cmp(&a.balance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            Ok(out)
        })
        .await?;
    let v = serde_json::to_value(&list).map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store_scan(&key, v).await;
    Ok(Json(list))
}

// =============================================================
// /wallet/liquidity/:address
// =============================================================

/// One pool from `properties` entries: assets + pool account.
#[derive(Clone, Serialize, Deserialize)]
struct PoolRef {
    base: String,
    target: String,
    account: [u8; 32],
}

async fn all_pools(state: &AppState) -> Result<Vec<PoolRef>, ApiError> {
    if let Some(v) = state.cached_scan("pool-properties", POOL_PROPS_TTL).await {
        return serde_json::from_value(v).map_err(|e| ApiError::Internal(e.to_string()));
    }
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let pools: Vec<PoolRef> = chain
        .with_client(|client| async move {
            let at = client.storage().at_latest().await?;
            let mut stream = at
                .iter(sora::storage().pool_xyk().properties_iter())
                .await?;
            let mut out = Vec::new();
            while let Some(kv) = stream.next().await {
                let kv = kv?;
                let n = kv.key_bytes.len();
                if n < 96 {
                    continue;
                }
                out.push(PoolRef {
                    base: format!("0x{}", hex::encode(&kv.key_bytes[n - 80..n - 48])),
                    target: format!("0x{}", hex::encode(&kv.key_bytes[n - 32..])),
                    account: kv.value.0 .0,
                });
            }
            Ok(out)
        })
        .await?;
    let v = serde_json::to_value(&pools).map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store_scan("pool-properties", v).await;
    Ok(pools)
}

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
struct Position {
    base: AssetRow,
    target: AssetRow,
    #[serde(rename = "amountBase")]
    amount_base: f64,
    #[serde(rename = "amountTarget")]
    amount_target: f64,
    value: f64,
    share: f64,
}

/// Wallet LP balances for a batch of pools via one `state_queryStorageAt`.
async fn provider_balances(
    state: &AppState,
    pools: &[PoolRef],
    wallet: &AccountId32,
) -> Result<Vec<u128>, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let client = chain.client().await?;
    let legacy = chain.legacy_rpc().await?;
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(pools.len());
    for p in pools {
        let addr = sora::storage()
            .pool_xyk()
            .pool_providers(AccountId32(p.account), wallet);
        let bytes = client
            .storage()
            .address_bytes(&addr)
            .map_err(|e| ApiError::Chain(e.into()))?;
        keys.push(bytes);
    }
    let sets = legacy
        .state_query_storage_at(keys.iter().map(Vec::as_slice), None)
        .await
        .map_err(|e| ApiError::Chain(e.into()))?;
    let mut by_key: HashMap<Vec<u8>, u128> = HashMap::new();
    for set in sets {
        for (k, v) in set.changes {
            if let Some(bytes) = v {
                if let Ok(amount) = u128::decode(&mut &bytes.0[..]) {
                    by_key.insert(k.0, amount);
                }
            }
        }
    }
    Ok(keys
        .iter()
        .map(|k| by_key.get(k).copied().unwrap_or(0))
        .collect())
}

async fn wallet_liquidity(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<Vec<Position>>, ApiError> {
    let address = crate::util::validate_address(&address)?;
    let key = format!("wallet-lp:{address}");
    if let Some(v) = state.cached_scan(&key, LP_TTL).await {
        return serde_json::from_value(v)
            .map(Json)
            .map_err(|e| ApiError::Internal(e.to_string()));
    }
    let (bytes, _) =
        ss58_decode(&address).map_err(|_| ApiError::BadRequest("Invalid address format".into()))?;
    let wallet = AccountId32(bytes);
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;

    let pools = all_pools(&state).await?;
    let mut active: Vec<(PoolRef, u128)> = Vec::new();
    for chunk in pools.chunks(BATCH) {
        let balances = provider_balances(&state, chunk, &wallet).await?;
        for (p, bal) in chunk.iter().zip(balances) {
            if bal > 0 {
                active.push((p.clone(), bal));
            }
        }
    }

    let registry = state.registry.read().await;
    let mut ids: Vec<String> = active
        .iter()
        .flat_map(|(p, _)| [p.base.clone(), p.target.clone()])
        .collect();
    ids.sort();
    ids.dedup();
    let prices: HashMap<String, f64> = latest_prices(&state.db, &ids)
        .await?
        .into_iter()
        .map(|p| (p.asset_id, p.price_usd))
        .collect();

    let mut positions = Vec::new();
    for (p, balance) in active {
        let (Some(base_tok), Some(target_tok)) = (
            registry.get(&p.base).filter(|a| a.whitelisted),
            registry.get(&p.target).filter(|a| a.whitelisted),
        ) else {
            continue;
        };
        let (Some(b), Some(t)) = (asset(&p.base), asset(&p.target)) else {
            continue;
        };
        let pool_account = AccountId32(p.account);
        let (total, reserves) = chain
            .with_client(|client| async move {
                let at = client.storage().at_latest().await?;
                let total = at
                    .fetch(&sora::storage().pool_xyk().total_issuances(&pool_account))
                    .await?
                    .unwrap_or(0);
                let reserves = at
                    .fetch_or_default(&sora::storage().pool_xyk().reserves(&b, &t))
                    .await?;
                Ok((total, reserves))
            })
            .await?;
        if total == 0 {
            continue;
        }
        let share = balance as f64 / total as f64;
        let amount_base = reserves.0 as f64 * share / 10f64.powi(base_tok.decimals as i32);
        let amount_target = reserves.1 as f64 * share / 10f64.powi(target_tok.decimals as i32);
        let value = amount_base * prices.get(&p.base).copied().unwrap_or(0.0)
            + amount_target * prices.get(&p.target).copied().unwrap_or(0.0);
        if !value.is_finite() || value < 0.10 {
            continue;
        }
        positions.push(Position {
            base: AssetRow::from_registry(base_tok),
            target: AssetRow::from_registry(target_tok),
            amount_base,
            amount_target,
            value,
            share,
        });
    }
    drop(registry);
    positions.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let v = serde_json::to_value(&positions).map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store_scan(&key, v).await;
    Ok(Json(positions))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_parses_hex_ids() {
        let id = format!("0x{}", "02".repeat(32));
        assert_eq!(asset(&id).unwrap().code, [2u8; 32]);
        assert!(asset("0x1234").is_none());
        assert!(asset("zz").is_none());
    }

    #[test]
    fn lp_units_divides_by_1e18() {
        assert_eq!(lp_units(219_850_767_053_763_720_000), 219.85076705376372);
    }
}
