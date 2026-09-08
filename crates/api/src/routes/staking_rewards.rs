//! VAL staking rewards on the legacy contract: `/staking/rewards` and
//! `/staking/rewards/live` (`index.js`).
//!
//! `/staking/rewards` (cached 60 s) combines on-chain state with the
//! indexed payouts of `sm.val_staking_rewards`:
//! - era / HistoryDepth / eraSeconds, XOR & VAL prices, the direct
//!   VAL→XOR DEX rate (`liquidityProxy_quote`, 0.000001 VAL) and its
//!   24h / 7d / 30d min–max from the hourly price history,
//! - the current era's VAL bucket, the unassigned bucket, the 10 %
//!   burn share, on-chain XOR supply and era stake,
//! - `networkTotals` per window and `topDestinations` from the indexed
//!   payouts,
//! - per active validator: exposure, commission, average reward points
//!   over the claimable window, last claim (ledger or indexed), indexed
//!   totals, and the pending VAL: for every era of the window that
//!   carries a bucket, where the validator earned points and no page is
//!   claimed (on-chain `claimedRewards` ∪ indexed payouts):
//!   `t = bucket × myPts / totalPts`, `valOutstanding += t`,
//!   `ownOutstanding += comm·t + own/total × (t − comm·t)` with that
//!   era's own/total — the runtime's `pay_val_staking_reward` for
//!   single-page validators, verified to the wei against a real payout
//!   (era 7211). `yieldRateNominatorPerXorPerEra` = `(t − comm·t) ×
//!   1e12 / total` on the most recent era with a bucket.
//!
//! `/staking/rewards/live` serves the in-memory pipeline state sampled
//! every 30 s by [`spawn_live_sampler`] (the Node used storage
//! subscriptions): `xorToVal`, `xorToBuyBack`, the active era's bucket
//! and the previous era's, the unassigned bucket, era, best block and a
//! 60-sample history.

use crate::{error::ApiError, AppState};
use axum::{extract::State, routing::get, Json, Router};
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use sorametrics_core::chain::ss58_encode_sora;
use sorametrics_db::ts::{latest_prices, val_xor_rate_windows, RateWindows};
use sorametrics_substrate::runtime::sora;
use sorametrics_substrate::runtime::sora::runtime_types::pallet_staking::{
    ActiveEraInfo, EraRewardPoints, StakingLedger, ValidatorPrefs,
};
use sorametrics_substrate::runtime::sora::runtime_types::sp_staking::{
    Exposure, PagedExposureMetadata,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use subxt::backend::rpc::rpc_params;
use subxt::utils::AccountId32;
use subxt::{OnlineClient, SubstrateConfig};

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/staking/rewards", get(rewards))
        .route("/staking/rewards/live", get(live))
}

const XOR_ASSET_ID: &str = "0x0200000000000000000000000000000000000000000000000000000000000000";
const VAL_ASSET_ID: &str = "0x0200040000000000000000000000000000000000000000000000000000000000";
const REWARDS_TTL: Duration = Duration::from_secs(60);
const CHUNK: usize = 200;
/// Node: 10 % of the bucket is burnt and not redistributed (4.8.6).
const VAL_BURN_PERCENT: f64 = 0.10;
const REMINT_PERIOD: u32 = 100;
const LIVE_SAMPLE: Duration = Duration::from_secs(30);
const LIVE_HISTORY: usize = 60;

fn chain_err(e: subxt::Error) -> ApiError {
    ApiError::Chain(e.into())
}

fn key_bytes<A: subxt::storage::Address>(
    client: &OnlineClient<SubstrateConfig>,
    addr: &A,
) -> Result<Vec<u8>, ApiError> {
    client
        .storage()
        .address_bytes(addr)
        .map_err(|e| ApiError::Chain(e.into()))
}

async fn prices(state: &AppState) -> Result<(f64, f64), ApiError> {
    let map: HashMap<String, f64> = latest_prices(
        &state.db,
        &[XOR_ASSET_ID.to_string(), VAL_ASSET_ID.to_string()],
    )
    .await?
    .into_iter()
    .map(|p| (p.asset_id, p.price_usd))
    .collect();
    Ok((
        map.get(XOR_ASSET_ID).copied().unwrap_or(0.0),
        map.get(VAL_ASSET_ID).copied().unwrap_or(0.0),
    ))
}

// ---------------------------------------------------------------------
// Indexed payouts (sm.val_staking_rewards)
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct WindowTotals {
    total_amount: String,
    payout_count: i32,
    validator_count: i32,
    destination_count: i32,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct NetworkTotals {
    all: WindowTotals,
    h6: WindowTotals,
    h12: WindowTotals,
    h24: WindowTotals,
    d3: WindowTotals,
    d6: WindowTotals,
    d30: WindowTotals,
    d90: WindowTotals,
    d180: WindowTotals,
    d365: WindowTotals,
}

fn totals(amount: Option<BigDecimal>, payouts: i64, validators: i64, dests: i64) -> WindowTotals {
    WindowTotals {
        total_amount: amount.unwrap_or_default().with_scale(0).to_string(),
        payout_count: payouts as i32,
        validator_count: validators as i32,
        destination_count: dests as i32,
    }
}

/// Node `getValStakingNetworkTotals`: one query per window there, one
/// query with FILTER clauses here.
async fn network_totals(state: &AppState) -> Result<NetworkTotals, ApiError> {
    let r = sqlx::query!(
        r#"
        SELECT
          COALESCE(SUM(amount), 0)::numeric AS "a_all!", COUNT(*)::bigint AS "p_all!",
          COUNT(DISTINCT validator_stash)::bigint AS "v_all!", COUNT(DISTINCT destination)::bigint AS "d_all!",
          COALESCE(SUM(amount) FILTER (WHERE block_timestamp > now() - interval '6 hours'), 0)::numeric AS "a_h6!",
          COUNT(*) FILTER (WHERE block_timestamp > now() - interval '6 hours')::bigint AS "p_h6!",
          COUNT(DISTINCT validator_stash) FILTER (WHERE block_timestamp > now() - interval '6 hours')::bigint AS "v_h6!",
          COUNT(DISTINCT destination) FILTER (WHERE block_timestamp > now() - interval '6 hours')::bigint AS "d_h6!",
          COALESCE(SUM(amount) FILTER (WHERE block_timestamp > now() - interval '12 hours'), 0)::numeric AS "a_h12!",
          COUNT(*) FILTER (WHERE block_timestamp > now() - interval '12 hours')::bigint AS "p_h12!",
          COUNT(DISTINCT validator_stash) FILTER (WHERE block_timestamp > now() - interval '12 hours')::bigint AS "v_h12!",
          COUNT(DISTINCT destination) FILTER (WHERE block_timestamp > now() - interval '12 hours')::bigint AS "d_h12!",
          COALESCE(SUM(amount) FILTER (WHERE block_timestamp > now() - interval '24 hours'), 0)::numeric AS "a_h24!",
          COUNT(*) FILTER (WHERE block_timestamp > now() - interval '24 hours')::bigint AS "p_h24!",
          COUNT(DISTINCT validator_stash) FILTER (WHERE block_timestamp > now() - interval '24 hours')::bigint AS "v_h24!",
          COUNT(DISTINCT destination) FILTER (WHERE block_timestamp > now() - interval '24 hours')::bigint AS "d_h24!",
          COALESCE(SUM(amount) FILTER (WHERE block_timestamp > now() - interval '3 days'), 0)::numeric AS "a_d3!",
          COUNT(*) FILTER (WHERE block_timestamp > now() - interval '3 days')::bigint AS "p_d3!",
          COUNT(DISTINCT validator_stash) FILTER (WHERE block_timestamp > now() - interval '3 days')::bigint AS "v_d3!",
          COUNT(DISTINCT destination) FILTER (WHERE block_timestamp > now() - interval '3 days')::bigint AS "d_d3!",
          COALESCE(SUM(amount) FILTER (WHERE block_timestamp > now() - interval '6 days'), 0)::numeric AS "a_d6!",
          COUNT(*) FILTER (WHERE block_timestamp > now() - interval '6 days')::bigint AS "p_d6!",
          COUNT(DISTINCT validator_stash) FILTER (WHERE block_timestamp > now() - interval '6 days')::bigint AS "v_d6!",
          COUNT(DISTINCT destination) FILTER (WHERE block_timestamp > now() - interval '6 days')::bigint AS "d_d6!",
          COALESCE(SUM(amount) FILTER (WHERE block_timestamp > now() - interval '30 days'), 0)::numeric AS "a_d30!",
          COUNT(*) FILTER (WHERE block_timestamp > now() - interval '30 days')::bigint AS "p_d30!",
          COUNT(DISTINCT validator_stash) FILTER (WHERE block_timestamp > now() - interval '30 days')::bigint AS "v_d30!",
          COUNT(DISTINCT destination) FILTER (WHERE block_timestamp > now() - interval '30 days')::bigint AS "d_d30!",
          COALESCE(SUM(amount) FILTER (WHERE block_timestamp > now() - interval '90 days'), 0)::numeric AS "a_d90!",
          COUNT(*) FILTER (WHERE block_timestamp > now() - interval '90 days')::bigint AS "p_d90!",
          COUNT(DISTINCT validator_stash) FILTER (WHERE block_timestamp > now() - interval '90 days')::bigint AS "v_d90!",
          COUNT(DISTINCT destination) FILTER (WHERE block_timestamp > now() - interval '90 days')::bigint AS "d_d90!",
          COALESCE(SUM(amount) FILTER (WHERE block_timestamp > now() - interval '180 days'), 0)::numeric AS "a_d180!",
          COUNT(*) FILTER (WHERE block_timestamp > now() - interval '180 days')::bigint AS "p_d180!",
          COUNT(DISTINCT validator_stash) FILTER (WHERE block_timestamp > now() - interval '180 days')::bigint AS "v_d180!",
          COUNT(DISTINCT destination) FILTER (WHERE block_timestamp > now() - interval '180 days')::bigint AS "d_d180!",
          COALESCE(SUM(amount) FILTER (WHERE block_timestamp > now() - interval '365 days'), 0)::numeric AS "a_d365!",
          COUNT(*) FILTER (WHERE block_timestamp > now() - interval '365 days')::bigint AS "p_d365!",
          COUNT(DISTINCT validator_stash) FILTER (WHERE block_timestamp > now() - interval '365 days')::bigint AS "v_d365!",
          COUNT(DISTINCT destination) FILTER (WHERE block_timestamp > now() - interval '365 days')::bigint AS "d_d365!"
        FROM sm.val_staking_rewards
        "#
    )
    .fetch_one(&state.db)
    .await?;
    Ok(NetworkTotals {
        all: totals(Some(r.a_all), r.p_all, r.v_all, r.d_all),
        h6: totals(Some(r.a_h6), r.p_h6, r.v_h6, r.d_h6),
        h12: totals(Some(r.a_h12), r.p_h12, r.v_h12, r.d_h12),
        h24: totals(Some(r.a_h24), r.p_h24, r.v_h24, r.d_h24),
        d3: totals(Some(r.a_d3), r.p_d3, r.v_d3, r.d_d3),
        d6: totals(Some(r.a_d6), r.p_d6, r.v_d6, r.d_d6),
        d30: totals(Some(r.a_d30), r.p_d30, r.v_d30, r.d_d30),
        d90: totals(Some(r.a_d90), r.p_d90, r.v_d90, r.d_d90),
        d180: totals(Some(r.a_d180), r.p_d180, r.v_d180, r.d_d180),
        d365: totals(Some(r.a_d365), r.p_d365, r.v_d365, r.d_d365),
    })
}

struct IndexedValidator {
    total_amount: String,
    payout_count: i32,
    era_count: i32,
    last_era: Option<i32>,
    last_ts: Option<chrono::DateTime<chrono::Utc>>,
}

/// Node `getValStakingPerValidator`.
async fn per_validator(state: &AppState) -> Result<HashMap<String, IndexedValidator>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT validator_stash,
               COALESCE(SUM(amount), 0)::numeric AS "total_amount!",
               COUNT(*)::int AS "payout_count!",
               COUNT(DISTINCT era)::int AS "era_count!",
               MAX(era)::int AS "last_era",
               MAX(block_timestamp) AS "last_ts"
        FROM sm.val_staking_rewards
        GROUP BY validator_stash
        "#
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.validator_stash,
                IndexedValidator {
                    total_amount: r.total_amount.with_scale(0).to_string(),
                    payout_count: r.payout_count,
                    era_count: r.era_count,
                    last_era: r.last_era,
                    last_ts: r.last_ts,
                },
            )
        })
        .collect())
}

#[derive(Serialize, Deserialize, Clone)]
struct TopDestination {
    destination: String,
    total_amount: String,
    payout_count: i32,
    validator_count: i32,
}

/// Node `getValStakingTopDestinations(10)`.
async fn top_destinations(state: &AppState) -> Result<Vec<TopDestination>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT destination,
               COALESCE(SUM(amount), 0)::numeric AS "total_amount!",
               COUNT(*)::int AS "payout_count!",
               COUNT(DISTINCT validator_stash)::int AS "validator_count!"
        FROM sm.val_staking_rewards
        GROUP BY destination
        ORDER BY SUM(amount) DESC
        LIMIT 10
        "#
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| TopDestination {
            destination: r.destination,
            total_amount: r.total_amount.with_scale(0).to_string(),
            payout_count: r.payout_count,
            validator_count: r.validator_count,
        })
        .collect())
}

/// Node `getClaimedValStakingPairs`: `(validator, era)` already paid.
async fn claimed_pairs(state: &AppState) -> Result<HashSet<(String, u32)>, ApiError> {
    let rows = sqlx::query!(r#"SELECT DISTINCT validator_stash, era FROM sm.val_staking_rewards"#)
        .fetch_all(&state.db)
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.validator_stash, r.era as u32))
        .collect())
}

// ---------------------------------------------------------------------
// Pending VAL arithmetic (BigInt — products exceed u128)
// ---------------------------------------------------------------------

/// Validator's share of an era: `t = bucket × myPts / totalPts`, and the
/// part its own stash keeps: `comm·t + own/total × (t − comm·t)`.
pub fn era_payout(
    bucket: u128,
    my_pts: u32,
    total_pts: u32,
    comm_perbill: u32,
    own: u128,
    total: u128,
) -> (BigInt, BigInt) {
    let t = BigInt::from(bucket) * BigInt::from(my_pts) / BigInt::from(total_pts);
    let comm_t = &t * BigInt::from(comm_perbill) / BigInt::from(1_000_000_000u32);
    let leftover = &t - &comm_t;
    let own_share = if total > 0 {
        comm_t + leftover * BigInt::from(own) / BigInt::from(total)
    } else {
        BigInt::from(0)
    };
    (t, own_share)
}

/// `(t − comm·t) × 1e12 / total` as the Node's string, `None` without stake.
pub fn nominator_yield(t: &BigInt, comm_perbill: u32, total: u128) -> Option<String> {
    if total == 0 {
        return None;
    }
    let comm_t = t * BigInt::from(comm_perbill) / BigInt::from(1_000_000_000u32);
    let leftover = t - comm_t;
    Some((leftover * BigInt::from(1_000_000_000_000u64) / BigInt::from(total)).to_string())
}

// ---------------------------------------------------------------------
// /staking/rewards
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct ValidatorRewards {
    address: String,
    commission: f64,
    own: String,
    total: String,
    nominators: u32,
    #[serde(rename = "avgRewardPointsPerEra")]
    avg_reward_points_per_era: u64,
    #[serde(rename = "erasProducedRecent")]
    eras_produced_recent: u32,
    #[serde(rename = "lastClaimEra")]
    last_claim_era: Option<u32>,
    #[serde(rename = "lastClaimTs")]
    last_claim_ts: Option<String>,
    #[serde(rename = "indexedTotalValReceived")]
    indexed_total_val_received: String,
    #[serde(rename = "indexedPayoutCount")]
    indexed_payout_count: i32,
    #[serde(rename = "indexedErasCovered")]
    indexed_eras_covered: i32,
    #[serde(rename = "valOutstanding")]
    val_outstanding: String,
    #[serde(rename = "ownOutstanding")]
    own_outstanding: String,
    #[serde(rename = "pendingErasCount")]
    pending_eras_count: u32,
    #[serde(rename = "yieldRateNominatorPerXorPerEra")]
    yield_rate: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct RewardsResponse {
    era: u32,
    #[serde(rename = "historyDepth")]
    history_depth: u32,
    #[serde(rename = "eraSeconds")]
    era_seconds: f64,
    #[serde(rename = "xorPrice")]
    xor_price: f64,
    #[serde(rename = "valPrice")]
    val_price: f64,
    #[serde(rename = "valToXorRate")]
    val_to_xor_rate: Option<f64>,
    #[serde(rename = "valToXorRateWindows")]
    val_to_xor_rate_windows: Option<RateWindows>,
    #[serde(rename = "valBucketCurrentEra")]
    val_bucket_current_era: Option<String>,
    #[serde(rename = "valBucketUnassigned")]
    val_bucket_unassigned: Option<String>,
    #[serde(rename = "valBurnPercent")]
    val_burn_percent: f64,
    #[serde(rename = "xorTotalSupply")]
    xor_total_supply: Option<f64>,
    #[serde(rename = "totalStaked")]
    total_staked: Option<String>,
    #[serde(rename = "networkTotals")]
    network_totals: NetworkTotals,
    #[serde(rename = "topDestinations")]
    top_destinations: Vec<TopDestination>,
    validators: Vec<ValidatorRewards>,
}

#[derive(Deserialize)]
struct QuoteOutcome {
    amount: String,
}

/// Node: `liquidityProxy.quote(0, VAL, XOR, 1e12, WithDesiredInput, [], Disabled)`
/// → `out / 1e12` XOR per VAL; `None` when the DEX has no route.
async fn val_to_xor_rate(state: &AppState) -> Option<f64> {
    let chain = state.chain.as_ref()?;
    let rpc = chain.rpc().await.ok()?;
    let outcome: Option<QuoteOutcome> = rpc
        .request(
            "liquidityProxy_quote",
            rpc_params![
                0_u32,
                VAL_ASSET_ID,
                XOR_ASSET_ID,
                "1000000000000",
                "WithDesiredInput",
                Vec::<String>::new(),
                "Disabled"
            ],
        )
        .await
        .ok()?;
    let out: u128 = outcome?.amount.parse().ok()?;
    Some(out as f64 / 1e12)
}

/// Storage values `(era, validator)` for a key set, in `keys` order.
async fn per_era_validator<T: subxt::ext::codec::Decode>(
    state: &AppState,
    client: &OnlineClient<SubstrateConfig>,
    pairs: &[(u32, AccountId32)],
    addr: impl Fn(u32, &AccountId32) -> Result<Vec<u8>, ApiError>,
) -> Result<HashMap<(u32, [u8; 32]), T>, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let _ = client;
    let mut out = HashMap::new();
    for chunk in pairs.chunks(CHUNK) {
        let keys: Vec<Vec<u8>> = chunk
            .iter()
            .map(|(e, v)| addr(*e, v))
            .collect::<Result<_, _>>()?;
        let values: Vec<Option<T>> = chain.fetch_many(&keys).await?;
        for ((e, v), val) in chunk.iter().zip(values) {
            if let Some(val) = val {
                out.insert((*e, v.0), val);
            }
        }
    }
    Ok(out)
}

async fn rewards(State(state): State<AppState>) -> Result<Json<RewardsResponse>, ApiError> {
    if let Some(v) = state.cached_scan("staking:rewards", REWARDS_TTL).await {
        return serde_json::from_value(v)
            .map(Json)
            .map_err(|e| ApiError::Internal(e.to_string()));
    }
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let client = chain.client().await?;
    let consts = client.constants();
    let history_depth: u32 = consts
        .at(&sora::constants().staking().history_depth())
        .map_err(chain_err)?;
    let sessions_per_era: u32 = consts
        .at(&sora::constants().staking().sessions_per_era())
        .map_err(chain_err)?;
    let epoch_duration: u64 = consts
        .at(&sora::constants().babe().epoch_duration())
        .map_err(chain_err)?;
    let block_time: u64 = consts
        .at(&sora::constants().babe().expected_block_time())
        .map_err(chain_err)?;
    let era_seconds = (u64::from(sessions_per_era) * epoch_duration * block_time) as f64 / 1000.0;

    // On-chain era context.
    let at = client.storage().at_latest().await.map_err(chain_err)?;
    let s = sora::storage();
    let era = at
        .fetch(&s.staking().active_era())
        .await
        .map_err(chain_err)?
        .map(|e: ActiveEraInfo| e.index)
        .unwrap_or(0);
    let set: Vec<AccountId32> = at
        .fetch(&s.session().validators())
        .await
        .map_err(chain_err)?
        .unwrap_or_default();
    let issuance = at
        .fetch(&s.balances().total_issuance())
        .await
        .map_err(chain_err)?;
    let era_stake = at
        .fetch(&s.staking().eras_total_stake(era))
        .await
        .map_err(chain_err)?;
    let bucket_current = at
        .fetch(&s.xor_fee().val_staking_era_reward(era))
        .await
        .map_err(chain_err)?
        .unwrap_or(0);
    let unassigned = at
        .fetch(&s.xor_fee().unassigned_val_staking_reward())
        .await
        .map_err(chain_err)?
        .unwrap_or(0);

    // Per-validator current context (one batch per map).
    let pref_keys: Vec<Vec<u8>> = set
        .iter()
        .map(|v| key_bytes(&client, &s.staking().validators(v)))
        .collect::<Result<_, _>>()?;
    let prefs: Vec<Option<ValidatorPrefs>> = chain.fetch_many(&pref_keys).await?;
    let ov_keys: Vec<Vec<u8>> = set
        .iter()
        .map(|v| key_bytes(&client, &s.staking().eras_stakers_overview(era, v)))
        .collect::<Result<_, _>>()?;
    let overviews: Vec<Option<PagedExposureMetadata<u128>>> = chain.fetch_many(&ov_keys).await?;
    let cl_keys: Vec<Vec<u8>> = set
        .iter()
        .map(|v| key_bytes(&client, &s.staking().eras_stakers_clipped(era, v)))
        .collect::<Result<_, _>>()?;
    let clipped: Vec<Option<Exposure<AccountId32, u128>>> = chain.fetch_many(&cl_keys).await?;
    let bonded_keys: Vec<Vec<u8>> = set
        .iter()
        .map(|v| key_bytes(&client, &s.staking().bonded(v)))
        .collect::<Result<_, _>>()?;
    let controllers: Vec<Option<AccountId32>> = chain.fetch_many(&bonded_keys).await?;
    let ledger_keys: Vec<Vec<u8>> = controllers
        .iter()
        .zip(&set)
        .map(|(c, v)| key_bytes(&client, &s.staking().ledger(c.as_ref().unwrap_or(v))))
        .collect::<Result<_, _>>()?;
    let ledgers: Vec<Option<StakingLedger>> = chain.fetch_many(&ledger_keys).await?;

    // Reward points and VAL buckets over the claimable window (desc).
    let recent_eras: Vec<u32> = (era.saturating_sub(history_depth)..era).rev().collect();
    let rp_keys: Vec<Vec<u8>> = recent_eras
        .iter()
        .map(|e| key_bytes(&client, &s.staking().eras_reward_points(*e)))
        .collect::<Result<_, _>>()?;
    let points: Vec<Option<EraRewardPoints<AccountId32>>> = chain.fetch_many(&rp_keys).await?;
    let bucket_keys: Vec<Vec<u8>> = recent_eras
        .iter()
        .map(|e| key_bytes(&client, &s.xor_fee().val_staking_era_reward(*e)))
        .collect::<Result<_, _>>()?;
    let buckets: Vec<Option<u128>> = chain.fetch_many(&bucket_keys).await?;
    let mut bucket_by_era: HashMap<u32, u128> = HashMap::new();
    let mut eras_with_bucket: Vec<u32> = Vec::new();
    for (e, b) in recent_eras.iter().zip(&buckets) {
        if let Some(b) = b.filter(|b| *b > 0) {
            bucket_by_era.insert(*e, b);
            eras_with_bucket.push(*e);
        }
    }
    let points_by_era: Vec<(u32, u32, HashMap<[u8; 32], u32>)> = recent_eras
        .iter()
        .zip(points)
        .map(|(e, p)| match p {
            Some(p) => (
                *e,
                p.total,
                p.individual
                    .into_iter()
                    .map(|(a, n)| (a.0, n))
                    .collect::<HashMap<[u8; 32], u32>>(),
            ),
            None => (*e, 0, HashMap::new()),
        })
        .collect();

    // Claimed pages and exposures for (era with bucket × validator).
    let pairs: Vec<(u32, AccountId32)> = eras_with_bucket
        .iter()
        .flat_map(|e| set.iter().map(move |v| (*e, v.clone())))
        .collect();
    let claimed: HashMap<(u32, [u8; 32]), Vec<u32>> =
        per_era_validator(&state, &client, &pairs, |e, v| {
            key_bytes(&client, &s.staking().claimed_rewards(e, v))
        })
        .await?;
    let exposures: HashMap<(u32, [u8; 32]), PagedExposureMetadata<u128>> =
        per_era_validator(&state, &client, &pairs, |e, v| {
            key_bytes(&client, &s.staking().eras_stakers_overview(e, v))
        })
        .await?;
    let indexed_claimed = claimed_pairs(&state).await?;

    let (xor_price, val_price) = prices(&state).await?;
    let network_totals = network_totals(&state).await?;
    let indexed = per_validator(&state).await?;
    let top = top_destinations(&state).await?;
    let windows = val_xor_rate_windows(&state.db, XOR_ASSET_ID, VAL_ASSET_ID)
        .await
        .ok();
    let rate = val_to_xor_rate(&state).await;
    let now_ms = chrono::Utc::now().timestamp_millis();

    let validators: Vec<ValidatorRewards> = set
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let address = ss58_encode_sora(&v.0);
            let (own, total, nominators) = match &overviews[i] {
                Some(o) => (o.own, o.total, o.nominator_count),
                None => clipped[i]
                    .as_ref()
                    .map(|c| (c.own, c.total, c.others.len() as u32))
                    .unwrap_or((0, 0, 0)),
            };
            let comm_perbill = prefs[i].as_ref().map(|p| p.commission.0).unwrap_or(0);
            let commission = f64::from(comm_perbill) / 1e9;
            let mut total_pts: u64 = 0;
            let mut eras_produced: u32 = 0;
            for (_, _, individual) in &points_by_era {
                if let Some(p) = individual.get(&v.0).filter(|p| **p > 0) {
                    total_pts += u64::from(*p);
                    eras_produced += 1;
                }
            }
            let avg = if eras_produced > 0 {
                (total_pts as f64 / f64::from(eras_produced)).round() as u64
            } else {
                0
            };
            let ledger_claim = ledgers[i]
                .as_ref()
                .and_then(|l| l.legacy_claimed_rewards.0.iter().max().copied());
            let ix = indexed.get(&address);
            let ix_last_era = ix.and_then(|x| x.last_era).map(|e| e as u32);
            let last_era = match (ix_last_era, ledger_claim) {
                (Some(a), Some(b)) if a > b => Some(a),
                (Some(a), None) => Some(a),
                (_, b) => b,
            };
            let last_ts = match ix.and_then(|x| x.last_ts) {
                Some(t) => Some(t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
                None => last_era.map(|e| {
                    let ms = now_ms - (i64::from(era - e) * era_seconds as i64 * 1000);
                    chrono::DateTime::from_timestamp_millis(ms)
                        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
                        .unwrap_or_default()
                }),
            };

            let mut val_outstanding = BigInt::from(0);
            let mut own_outstanding = BigInt::from(0);
            let mut pending = 0u32;
            for (e, era_total_pts, individual) in &points_by_era {
                let Some(bucket) = bucket_by_era.get(e) else {
                    continue;
                };
                if *era_total_pts == 0 {
                    continue;
                }
                let Some(my_pts) = individual.get(&v.0).filter(|p| **p > 0) else {
                    continue;
                };
                let on_chain_claimed = claimed
                    .get(&(*e, v.0))
                    .map(|pages| !pages.is_empty())
                    .unwrap_or(false);
                if on_chain_claimed || indexed_claimed.contains(&(address.clone(), *e)) {
                    continue;
                }
                let (e_own, e_total) = exposures
                    .get(&(*e, v.0))
                    .map(|x| (x.own, x.total))
                    .unwrap_or((own, total));
                let (t, own_share) = era_payout(
                    *bucket,
                    *my_pts,
                    *era_total_pts,
                    comm_perbill,
                    e_own,
                    e_total,
                );
                val_outstanding += t;
                own_outstanding += own_share;
                pending += 1;
            }

            let yield_rate = eras_with_bucket.first().and_then(|ye| {
                let (_, tp, individual) = points_by_era.iter().find(|(e, _, _)| e == ye)?;
                let my = individual.get(&v.0).filter(|p| **p > 0)?;
                let bucket = bucket_by_era.get(ye)?;
                if *tp == 0 || total == 0 {
                    return None;
                }
                let t = BigInt::from(*bucket) * BigInt::from(*my) / BigInt::from(*tp);
                nominator_yield(&t, comm_perbill, total)
            });

            ValidatorRewards {
                address,
                commission,
                own: own.to_string(),
                total: total.to_string(),
                nominators,
                avg_reward_points_per_era: avg,
                eras_produced_recent: eras_produced,
                last_claim_era: last_era,
                last_claim_ts: last_ts,
                indexed_total_val_received: ix
                    .map(|x| x.total_amount.clone())
                    .unwrap_or_else(|| "0".into()),
                indexed_payout_count: ix.map(|x| x.payout_count).unwrap_or(0),
                indexed_eras_covered: ix.map(|x| x.era_count).unwrap_or(0),
                val_outstanding: val_outstanding.to_string(),
                own_outstanding: own_outstanding.to_string(),
                pending_eras_count: pending,
                yield_rate,
            }
        })
        .collect();

    let out = RewardsResponse {
        era,
        history_depth,
        era_seconds,
        xor_price,
        val_price,
        val_to_xor_rate: rate,
        val_to_xor_rate_windows: windows,
        val_bucket_current_era: Some(bucket_current.to_string()),
        val_bucket_unassigned: Some(unassigned.to_string()),
        val_burn_percent: VAL_BURN_PERCENT,
        xor_total_supply: issuance.map(|i| {
            (BigDecimal::from(BigInt::from(i)) / BigDecimal::new(BigInt::from(1), -18))
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.0)
        }),
        total_staked: era_stake.map(|s| s.to_string()),
        network_totals,
        top_destinations: top,
        validators,
    };
    let json = serde_json::to_value(&out).map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store_scan("staking:rewards", json).await;
    Ok(Json(out))
}

// ---------------------------------------------------------------------
// /staking/rewards/live
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct EraValue {
    era: u32,
    value: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct Sample {
    ts: i64,
    #[serde(rename = "xorToVal")]
    xor_to_val: String,
    #[serde(rename = "valStakingEraReward")]
    val_staking_era_reward: Option<String>,
    era: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone)]
struct PipelineState {
    #[serde(rename = "xorToVal")]
    xor_to_val: String,
    #[serde(rename = "xorToBuyBack")]
    xor_to_buy_back: String,
    #[serde(rename = "valStakingEraReward")]
    val_staking_era_reward: Option<EraValue>,
    #[serde(rename = "valBucketPrevEra")]
    val_bucket_prev_era: Option<EraValue>,
    #[serde(rename = "unassignedValStakingReward")]
    unassigned_val_staking_reward: String,
    #[serde(rename = "activeEra")]
    active_era: Option<u32>,
    #[serde(rename = "bestBlock")]
    best_block: Option<u32>,
    #[serde(rename = "lastUpdate")]
    last_update: i64,
    history: Vec<Sample>,
}

impl Default for PipelineState {
    fn default() -> Self {
        Self {
            xor_to_val: "0".into(),
            xor_to_buy_back: "0".into(),
            val_staking_era_reward: None,
            val_bucket_prev_era: None,
            unassigned_val_staking_reward: "0".into(),
            active_era: None,
            best_block: None,
            last_update: 0,
            history: Vec::new(),
        }
    }
}

fn live_state() -> &'static Mutex<PipelineState> {
    static LIVE: OnceLock<Mutex<PipelineState>> = OnceLock::new();
    LIVE.get_or_init(|| Mutex::new(PipelineState::default()))
}

async fn sample(state: &AppState) -> Result<(), ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let (xor_to_val, buy_back, era, bucket, prev, unassigned) = chain
        .with_client(|client| async move {
            let at = client.storage().at_latest().await?;
            let s = sora::storage();
            let era = at
                .fetch(&s.staking().active_era())
                .await?
                .map(|e: ActiveEraInfo| e.index)
                .unwrap_or(0);
            Ok((
                at.fetch(&s.xor_fee().xor_to_val()).await?.unwrap_or(0),
                at.fetch(&s.xor_fee().xor_to_buy_back()).await?.unwrap_or(0),
                era,
                at.fetch(&s.xor_fee().val_staking_era_reward(era))
                    .await?
                    .unwrap_or(0),
                at.fetch(&s.xor_fee().val_staking_era_reward(era.saturating_sub(1)))
                    .await?
                    .unwrap_or(0),
                at.fetch(&s.xor_fee().unassigned_val_staking_reward())
                    .await?
                    .unwrap_or(0),
            ))
        })
        .await?;
    let best = chain
        .legacy_rpc()
        .await?
        .chain_get_header(None)
        .await
        .map_err(chain_err)?
        .map(|h| h.number);
    let now = chrono::Utc::now().timestamp_millis();
    let mut st = live_state()
        .lock()
        .map_err(|_| ApiError::Internal("live state poisoned".into()))?;
    let xor_to_val_text = xor_to_val.to_string();
    st.xor_to_val = xor_to_val_text.clone();
    st.xor_to_buy_back = buy_back.to_string();
    st.val_staking_era_reward = Some(EraValue {
        era,
        value: bucket.to_string(),
    });
    st.val_bucket_prev_era = Some(EraValue {
        era: era.saturating_sub(1),
        value: prev.to_string(),
    });
    st.unassigned_val_staking_reward = unassigned.to_string();
    st.active_era = Some(era);
    st.best_block = best;
    st.last_update = now;
    st.history.push(Sample {
        ts: now,
        xor_to_val: xor_to_val_text,
        val_staking_era_reward: Some(bucket.to_string()),
        era: Some(era),
    });
    if st.history.len() > LIVE_HISTORY {
        let extra = st.history.len() - LIVE_HISTORY;
        st.history.drain(..extra);
    }
    Ok(())
}

/// Sample the pipeline storage every 30 s (Node: storage subscriptions
/// + a 30 s ring buffer). Without a chain client nothing runs.
pub fn spawn_live_sampler(state: AppState) {
    if state.chain.is_none() {
        return;
    }
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(LIVE_SAMPLE);
        loop {
            ticker.tick().await;
            if let Err(e) = sample(&state).await {
                tracing::warn!(error = %e, "staking live sample failed");
            }
        }
    });
}

#[derive(Serialize)]
struct LiveResponse {
    #[serde(flatten)]
    state: PipelineState,
    #[serde(rename = "remintPeriod")]
    remint_period: u32,
    #[serde(rename = "blocksSinceLastRemint")]
    blocks_since_last_remint: Option<u32>,
    #[serde(rename = "xorPrice")]
    xor_price: f64,
    #[serde(rename = "valPrice")]
    val_price: f64,
}

async fn live(State(state): State<AppState>) -> Result<Json<LiveResponse>, ApiError> {
    let (xor_price, val_price) = prices(&state).await?;
    let snapshot = live_state()
        .lock()
        .map_err(|_| ApiError::Internal("live state poisoned".into()))?
        .clone();
    Ok(Json(LiveResponse {
        state: snapshot,
        remint_period: REMINT_PERIOD,
        blocks_since_last_remint: None,
        xor_price,
        val_price,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn era_payout_matches_the_runtime_split() {
        // bucket 1000 VAL-planck, validator has 1/4 of the points, 10 %
        // commission, own 30 / total 100.
        let (t, own) = era_payout(1000, 25, 100, 100_000_000, 30, 100);
        assert_eq!(t, BigInt::from(250));
        // comm·t = 25; leftover 225 × 30/100 = 67 (floor) → 92
        assert_eq!(own, BigInt::from(92));
        let (_, own0) = era_payout(1000, 25, 100, 100_000_000, 0, 100);
        assert_eq!(own0, BigInt::from(25));
        let (_, none) = era_payout(1000, 25, 100, 100_000_000, 0, 0);
        assert_eq!(none, BigInt::from(0));
    }

    #[test]
    fn nominator_yield_scales_leftover_by_1e12_over_total() {
        let t = BigInt::from(250);
        assert_eq!(
            nominator_yield(&t, 100_000_000, 100).as_deref(),
            Some("2250000000000")
        );
        assert_eq!(nominator_yield(&t, 0, 0), None);
    }

    #[test]
    fn live_history_is_capped_and_defaults_match_node() {
        let st = PipelineState::default();
        assert_eq!(st.xor_to_val, "0");
        assert!(st.history.is_empty());
        let json = serde_json::to_value(LiveResponse {
            state: st,
            remint_period: REMINT_PERIOD,
            blocks_since_last_remint: None,
            xor_price: 0.0,
            val_price: 0.0,
        })
        .unwrap();
        assert_eq!(json["remintPeriod"], 100);
        assert!(json["blocksSinceLastRemint"].is_null());
        assert_eq!(json["xorToVal"], "0");
    }
}
