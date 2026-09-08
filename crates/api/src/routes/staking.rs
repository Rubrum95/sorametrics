//! Staking chain-state reads on the legacy contract:
//! `/wallet/staking/:address`, `/staking/validators`, `/staking/network`,
//! `/staking/recent-blocks` (`index.js`).
//!
//! - `/wallet/staking/:address`: `staking.ledger(address)`, else the
//!   ledger of `staking.bonded(address)`; none → `{ staked: 0,
//!   unbonding: 0, rewards: 0, usdValue: 0, validators: [] }`. Otherwise
//!   `staked = active / 1e18`, `unbonding = (total − active) / 1e18`,
//!   `rewards: 0`, USD at the XOR price, `validators` = nomination
//!   targets. Cached 15 min per address.
//! - `/staking/validators`: the active set (`session.validators`) with
//!   prefs, era exposure (`erasStakersOverview`, `erasStakersClipped`
//!   fallback), identity display, and `erasSincePayout` in days (1 dp)
//!   from the ledger's claimed eras; `{ era, validatorCount,
//!   validators, xorPrice }`. Cached 2 min.
//! - `/staking/network`: era / session / block progress, issuance and
//!   stake (`toFixed` strings), validator counts, min bonds, the last
//!   era with a non-zero `erasValidatorReward`, unbonding period.
//!   Cached 30 s.
//! - `/staking/recent-blocks`: the last 15 blocks with the BABE author
//!   (pre-runtime digest authority index → session validator), its
//!   display name, extrinsic count, age in seconds and timestamp.

use crate::routes::identity::display_names;
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use bigdecimal::{BigDecimal, RoundingMode, ToPrimitive};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use sorametrics_core::chain::{ss58_decode, ss58_encode_sora};
use sorametrics_db::ts::latest_prices;
use sorametrics_substrate::runtime::sora;
use sorametrics_substrate::runtime::sora::runtime_types::pallet_staking::{
    ActiveEraInfo, StakingLedger, ValidatorPrefs,
};
use sorametrics_substrate::runtime::sora::runtime_types::sp_staking::{
    Exposure, PagedExposureMetadata,
};
use std::collections::HashMap;
use std::time::Duration;
use subxt::config::substrate::DigestItem;
use subxt::utils::AccountId32;
use subxt::{OnlineClient, SubstrateConfig};

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/wallet/staking/:address", get(wallet_staking))
        .route("/staking/validators", get(validators))
        .route("/staking/network", get(network))
        .route("/staking/recent-blocks", get(recent_blocks))
}

const XOR_ASSET_ID: &str = "0x0200000000000000000000000000000000000000000000000000000000000000";
const WALLET_TTL: Duration = Duration::from_secs(15 * 60);
const VALIDATORS_TTL: Duration = Duration::from_secs(2 * 60);
const NETWORK_TTL: Duration = Duration::from_secs(30);
const RECENT_BLOCKS_TTL: Duration = Duration::from_secs(6);
const RECENT_BLOCKS: u32 = 15;

fn planck_to_f64(raw: u128) -> f64 {
    (BigDecimal::from(BigInt::from(raw)) / BigDecimal::new(BigInt::from(1), -18))
        .to_f64()
        .unwrap_or(0.0)
}

/// BigNumber `div('1e18').toFixed(n)`.
fn planck_fixed(raw: u128, scale: i64) -> String {
    let v = (BigDecimal::from(BigInt::from(raw)) / BigDecimal::new(BigInt::from(1), -18))
        .with_scale_round(scale, RoundingMode::HalfUp);
    format!("{v:.prec$}", prec = scale as usize)
}

/// `parseFloat(x.toFixed(n))`.
fn round_to(v: f64, decimals: i32) -> f64 {
    let m = 10f64.powi(decimals);
    (v * m).round() / m
}

async fn xor_price(state: &AppState) -> Result<f64, ApiError> {
    let prices: HashMap<String, f64> = latest_prices(&state.db, &[XOR_ASSET_ID.to_string()])
        .await?
        .into_iter()
        .map(|p| (p.asset_id, p.price_usd))
        .collect();
    Ok(prices.get(XOR_ASSET_ID).copied().unwrap_or(0.0))
}

fn account(address: &str) -> Result<AccountId32, ApiError> {
    let (bytes, _) =
        ss58_decode(address).map_err(|_| ApiError::BadRequest("Invalid address format".into()))?;
    Ok(AccountId32(bytes))
}

async fn cached<T, F, Fut>(state: &AppState, key: &str, ttl: Duration, f: F) -> Result<T, ApiError>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, ApiError>>,
{
    if let Some(v) = state.cached_scan(key, ttl).await {
        return serde_json::from_value(v).map_err(|e| ApiError::Internal(e.to_string()));
    }
    let v = f().await?;
    let json = serde_json::to_value(&v).map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store_scan(key, json).await;
    Ok(v)
}

struct EraConsts {
    sessions_per_era: u32,
    epoch_duration: u64,
    expected_block_time: u64,
    bonding_duration: u32,
    history_depth: u32,
}

fn era_consts(client: &OnlineClient<SubstrateConfig>) -> Result<EraConsts, Box<subxt::Error>> {
    let c = client.constants();
    Ok(EraConsts {
        sessions_per_era: c.at(&sora::constants().staking().sessions_per_era())?,
        epoch_duration: c.at(&sora::constants().babe().epoch_duration())?,
        expected_block_time: c.at(&sora::constants().babe().expected_block_time())?,
        bonding_duration: c.at(&sora::constants().staking().bonding_duration())?,
        history_depth: c.at(&sora::constants().staking().history_depth())?,
    })
}

/// Best (head) block number — `at_latest()` in subxt is the FINALIZED
/// block, the Node's `bestBlock` is the head header.
async fn head_number(
    legacy: &subxt::backend::legacy::LegacyRpcMethods<SubstrateConfig>,
) -> Result<u32, ApiError> {
    Ok(legacy
        .chain_get_header(None)
        .await
        .map_err(ChainErr)?
        .map(|h| h.number)
        .unwrap_or(0))
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

// ---------------------------------------------------------------------
// /wallet/staking/:address
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct WalletStaking {
    staked: f64,
    unbonding: f64,
    rewards: f64,
    #[serde(rename = "usdValue")]
    usd_value: f64,
    #[serde(rename = "stakedUsd", skip_serializing_if = "Option::is_none")]
    staked_usd: Option<f64>,
    #[serde(rename = "unbondingUsd", skip_serializing_if = "Option::is_none")]
    unbonding_usd: Option<f64>,
    validators: Vec<String>,
}

async fn wallet_staking(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<WalletStaking>, ApiError> {
    let address = crate::util::validate_address(&address)?;
    let key = format!("staking:wallet:{address}");
    let v = cached(&state, &key, WALLET_TTL, || async {
        let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
        let who = account(&address)?;
        let (ledger, targets) = chain
            .with_client(|client| async move {
                let at = client.storage().at_latest().await?;
                let mut ledger = at.fetch(&sora::storage().staking().ledger(&who)).await?;
                if ledger.is_none() {
                    if let Some(controller) =
                        at.fetch(&sora::storage().staking().bonded(&who)).await?
                    {
                        ledger = at
                            .fetch(&sora::storage().staking().ledger(&controller))
                            .await?;
                    }
                }
                let targets = at
                    .fetch(&sora::storage().staking().nominators(&who))
                    .await?
                    .map(|n| {
                        n.targets
                            .0
                            .iter()
                            .map(|t| ss58_encode_sora(&t.0))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                Ok((ledger, targets))
            })
            .await?;
        let Some(ledger) = ledger else {
            return Ok(WalletStaking {
                staked: 0.0,
                unbonding: 0.0,
                rewards: 0.0,
                usd_value: 0.0,
                staked_usd: None,
                unbonding_usd: None,
                validators: Vec::new(),
            });
        };
        let staked = planck_to_f64(ledger.active);
        let unbonding = planck_to_f64(ledger.total.saturating_sub(ledger.active));
        let price = xor_price(&state).await?;
        Ok(WalletStaking {
            staked,
            unbonding,
            rewards: 0.0,
            usd_value: (staked + unbonding) * price,
            staked_usd: Some(staked * price),
            unbonding_usd: Some(unbonding * price),
            validators: targets,
        })
    })
    .await?;
    Ok(Json(v))
}

// ---------------------------------------------------------------------
// /staking/validators
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct ValidatorRow {
    address: String,
    identity: Option<String>,
    commission: f64,
    #[serde(rename = "totalStake")]
    total_stake: f64,
    #[serde(rename = "ownStake")]
    own_stake: f64,
    #[serde(rename = "otherStake")]
    other_stake: f64,
    #[serde(rename = "nominatorsCount")]
    nominators_count: u32,
    #[serde(rename = "isBlocked")]
    is_blocked: bool,
    #[serde(rename = "erasSincePayout")]
    eras_since_payout: Option<f64>,
}

#[derive(Serialize, Deserialize)]
struct ValidatorsResponse {
    era: u32,
    #[serde(rename = "validatorCount")]
    validator_count: usize,
    validators: Vec<ValidatorRow>,
    #[serde(rename = "xorPrice")]
    xor_price: f64,
}

/// Exposure `(total, own, nominator_count)` for `(era, validator)`
/// from `erasStakersOverview`, else the clipped exposure, else zeros.
async fn exposures(
    state: &AppState,
    client: &OnlineClient<SubstrateConfig>,
    era: u32,
    validators: &[AccountId32],
) -> Result<Vec<(u128, u128, u32)>, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let mut ov_keys = Vec::with_capacity(validators.len());
    let mut cl_keys = Vec::with_capacity(validators.len());
    for v in validators {
        ov_keys.push(key_bytes(
            client,
            &sora::storage().staking().eras_stakers_overview(era, v),
        )?);
        cl_keys.push(key_bytes(
            client,
            &sora::storage().staking().eras_stakers_clipped(era, v),
        )?);
    }
    let overviews: Vec<Option<PagedExposureMetadata<u128>>> = chain.fetch_many(&ov_keys).await?;
    let clipped: Vec<Option<Exposure<AccountId32, u128>>> = chain.fetch_many(&cl_keys).await?;
    Ok(overviews
        .into_iter()
        .zip(clipped)
        .map(|(o, c)| match o {
            Some(o) => (o.total, o.own, o.nominator_count),
            None => c
                .map(|c| (c.total, c.own, c.others.len() as u32))
                .unwrap_or((0, 0, 0)),
        })
        .collect())
}

/// Ledger claimed eras per validator (`claimedRewards` else
/// `legacyClaimedRewards`), via `bonded` → `ledger`.
async fn claimed_eras(
    state: &AppState,
    client: &OnlineClient<SubstrateConfig>,
    validators: &[AccountId32],
) -> Result<Vec<Vec<u32>>, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let bonded_keys: Vec<Vec<u8>> = validators
        .iter()
        .map(|v| key_bytes(client, &sora::storage().staking().bonded(v)))
        .collect::<Result<_, _>>()?;
    let controllers: Vec<Option<AccountId32>> = chain.fetch_many(&bonded_keys).await?;
    let ledger_keys: Vec<Vec<u8>> = controllers
        .iter()
        .zip(validators)
        .map(|(c, v)| {
            key_bytes(
                client,
                &sora::storage().staking().ledger(c.as_ref().unwrap_or(v)),
            )
        })
        .collect::<Result<_, _>>()?;
    let ledgers: Vec<Option<StakingLedger>> = chain.fetch_many(&ledger_keys).await?;
    Ok(ledgers
        .into_iter()
        .map(|l| l.map(|l| l.legacy_claimed_rewards.0).unwrap_or_default())
        .collect())
}

async fn validators(State(state): State<AppState>) -> Result<Json<ValidatorsResponse>, ApiError> {
    let v = cached(&state, "staking:validators", VALIDATORS_TTL, || async {
        let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
        let client = chain.client().await?;
        let (era, set) = chain
            .with_client(|client| async move {
                let at = client.storage().at_latest().await?;
                let era = at
                    .fetch(&sora::storage().staking().active_era())
                    .await?
                    .map(|e: ActiveEraInfo| e.index)
                    .unwrap_or(0);
                let set = at
                    .fetch(&sora::storage().session().validators())
                    .await?
                    .unwrap_or_default();
                Ok((era, set))
            })
            .await?;
        let consts = era_consts(&client).map_err(boxed_chain)?;
        let era_ms =
            u64::from(consts.sessions_per_era) * consts.epoch_duration * consts.expected_block_time;
        let addresses: Vec<String> = set.iter().map(|a| ss58_encode_sora(&a.0)).collect();

        let pref_keys: Vec<Vec<u8>> = set
            .iter()
            .map(|v| key_bytes(&client, &sora::storage().staking().validators(v)))
            .collect::<Result<_, _>>()?;
        let prefs: Vec<Option<ValidatorPrefs>> = chain.fetch_many(&pref_keys).await?;
        let exposures = exposures(&state, &client, era, &set).await?;
        let claimed = claimed_eras(&state, &client, &set).await?;
        let names = display_names(&state, &addresses).await;
        let xor_price = xor_price(&state).await?;

        let validators = addresses
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let (total, own, nominators) = exposures[i];
                let (commission_perbill, blocked) = prefs[i]
                    .as_ref()
                    .map(|p| (p.commission.0, p.blocked))
                    .unwrap_or((0, false));
                let commission = round_to(f64::from(commission_perbill) / 1e9 * 100.0, 2);
                let total_stake = planck_to_f64(total);
                let own_stake = planck_to_f64(own);
                let other_stake = planck_to_f64(total.saturating_sub(own));
                let eras_since_payout = claimed[i].iter().max().map(|last| {
                    let eras_since = f64::from(era.saturating_sub(*last));
                    round_to(eras_since * era_ms as f64 / 86_400_000.0, 1)
                });
                ValidatorRow {
                    address: addr.clone(),
                    identity: names.get(addr).cloned(),
                    commission,
                    total_stake,
                    own_stake,
                    other_stake,
                    nominators_count: nominators,
                    is_blocked: blocked,
                    eras_since_payout,
                }
            })
            .collect::<Vec<_>>();
        Ok(ValidatorsResponse {
            era,
            validator_count: validators.len(),
            validators,
            xor_price,
        })
    })
    .await?;
    Ok(Json(v))
}

/// `subxt::Error` → `ApiError::Chain` for the constants reads.
#[allow(non_snake_case)]
fn ChainErr(e: subxt::Error) -> ApiError {
    ApiError::Chain(e.into())
}

fn boxed_chain(e: Box<subxt::Error>) -> ApiError {
    ApiError::Chain((*e).into())
}

// ---------------------------------------------------------------------
// /staking/network
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct NetworkResponse {
    #[serde(rename = "activeEra")]
    active_era: u32,
    #[serde(rename = "currentEra")]
    current_era: u32,
    #[serde(rename = "eraStart")]
    era_start: Option<u64>,
    #[serde(rename = "sessionIndex")]
    session_index: u32,
    #[serde(rename = "sessionsPerEra")]
    sessions_per_era: u32,
    #[serde(rename = "sessionProgress")]
    session_progress: i64,
    #[serde(rename = "eraProgress")]
    era_progress: f64,
    #[serde(rename = "expectedBlockTime")]
    expected_block_time: u64,
    #[serde(rename = "bestBlock")]
    best_block: u32,
    #[serde(rename = "finalizedBlock")]
    finalized_block: u32,
    #[serde(rename = "totalIssuance")]
    total_issuance: String,
    #[serde(rename = "totalStaked")]
    total_staked: String,
    #[serde(rename = "stakingRatio")]
    staking_ratio: Option<String>,
    #[serde(rename = "validatorCount")]
    validator_count: usize,
    #[serde(rename = "avgBlockTime")]
    avg_block_time: f64,
    era: u32,
    #[serde(rename = "totalStake")]
    total_stake: String,
    #[serde(rename = "totalStakeUsd")]
    total_stake_usd: Option<String>,
    #[serde(rename = "epochProgress")]
    epoch_progress: String,
    #[serde(rename = "epochsPerEra")]
    epochs_per_era: u32,
    #[serde(rename = "epochDuration")]
    epoch_duration: String,
    #[serde(rename = "activeValidators")]
    active_validators: usize,
    #[serde(rename = "waitingValidators")]
    waiting_validators: u32,
    #[serde(rename = "validatorTarget")]
    validator_target: u32,
    #[serde(rename = "minNominatorBond")]
    min_nominator_bond: String,
    #[serde(rename = "minValidatorBond")]
    min_validator_bond: String,
    #[serde(rename = "lastRewardEra")]
    last_reward_era: Option<u32>,
    #[serde(rename = "lastRewardAmount")]
    last_reward_amount: Option<String>,
    #[serde(rename = "idealStakeRate")]
    ideal_stake_rate: Option<f64>,
    #[serde(rename = "currentInflation")]
    current_inflation: f64,
    #[serde(rename = "unbondingDays")]
    unbonding_days: f64,
    #[serde(rename = "unbondingEras")]
    unbonding_eras: u32,
    #[serde(rename = "eraStartedAgo")]
    era_started_ago: String,
}

/// `(sessionProgress, eraProgress)` / `epochProgress` labels as the Node.
fn progress(
    session: u32,
    era_start_session: u32,
    sessions_per_era: u32,
    best: u32,
    epoch_blocks: u64,
) -> (i64, f64, String) {
    let session_progress = i64::from(session) - i64::from(era_start_session);
    let era_progress = round_to(
        session_progress as f64 / f64::from(sessions_per_era) * 100.0,
        1,
    );
    let blocks_into = if epoch_blocks > 0 {
        u64::from(best) % epoch_blocks
    } else {
        0
    };
    let epoch_progress = round_to(blocks_into as f64 / epoch_blocks.max(1) as f64 * 100.0, 1);
    (session_progress, era_progress, format!("{epoch_progress}%"))
}

fn epoch_label(epoch_seconds: f64) -> String {
    if epoch_seconds >= 3600.0 {
        format!("{:.1}h", epoch_seconds / 3600.0)
    } else {
        format!("{:.0}min", epoch_seconds / 60.0)
    }
}

async fn network(State(state): State<AppState>) -> Result<Json<NetworkResponse>, ApiError> {
    let v = cached(&state, "staking:network", NETWORK_TTL, || async {
        let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
        let client = chain.client().await?;
        let consts = era_consts(&client).map_err(boxed_chain)?;
        let legacy = chain.legacy_rpc().await?;
        let best = head_number(&legacy).await?;
        let finalized_hash = legacy.chain_get_finalized_head().await.map_err(ChainErr)?;
        let finalized = client
            .blocks()
            .at(finalized_hash)
            .await
            .map_err(ChainErr)?
            .number();
        let now_ms = chrono::Utc::now().timestamp_millis();

        struct Raw {
            active: Option<ActiveEraInfo>,
            current: Option<u32>,
            session: u32,
            set_len: usize,
            target: u32,
            intents: u32,
            min_nom: u128,
            min_val: u128,
            era_start_session: Option<u32>,
            issuance: u128,
            era_stake: u128,
        }
        let raw = chain
            .with_client(|client| async move {
                let at = client.storage().at_latest().await?;
                let s = sora::storage();
                let active = at.fetch(&s.staking().active_era()).await?;
                let era = active.as_ref().map(|a| a.index).unwrap_or(0);
                Ok(Raw {
                    current: at.fetch(&s.staking().current_era()).await?,
                    session: at.fetch(&s.session().current_index()).await?.unwrap_or(0),
                    set_len: at
                        .fetch(&s.session().validators())
                        .await?
                        .map(|v| v.len())
                        .unwrap_or(0),
                    target: at.fetch(&s.staking().validator_count()).await?.unwrap_or(0),
                    intents: at
                        .fetch(&s.staking().counter_for_validators())
                        .await?
                        .unwrap_or(0),
                    min_nom: at
                        .fetch(&s.staking().min_nominator_bond())
                        .await?
                        .unwrap_or(0),
                    min_val: at
                        .fetch(&s.staking().min_validator_bond())
                        .await?
                        .unwrap_or(0),
                    era_start_session: at.fetch(&s.staking().eras_start_session_index(era)).await?,
                    issuance: at.fetch(&s.balances().total_issuance()).await?.unwrap_or(0),
                    era_stake: at
                        .fetch(&s.staking().eras_total_stake(era))
                        .await?
                        .unwrap_or(0),
                    active,
                })
            })
            .await?;
        let era = raw.active.as_ref().map(|a| a.index).unwrap_or(0);
        let era_start = raw.active.as_ref().and_then(|a| a.start);
        let era_seconds = (u64::from(consts.sessions_per_era)
            * consts.epoch_duration
            * consts.expected_block_time) as f64
            / 1000.0;
        let epoch_seconds = (consts.epoch_duration * consts.expected_block_time) as f64 / 1000.0;
        let unbonding_days = round_to(
            f64::from(consts.bonding_duration) * era_seconds / 86_400.0,
            1,
        );
        let (session_progress, era_progress, epoch_progress) = progress(
            raw.session,
            raw.era_start_session.unwrap_or(0),
            consts.sessions_per_era,
            best,
            consts.epoch_duration,
        );
        let total_issuance = planck_fixed(raw.issuance, 2);
        let total_staked = planck_fixed(raw.era_stake, 2);
        let issuance_f: f64 = total_issuance.parse().unwrap_or(0.0);
        let staked_f: f64 = total_staked.parse().unwrap_or(0.0);
        let staking_ratio =
            (issuance_f > 0.0).then(|| format!("{:.4}", staked_f / issuance_f * 100.0));

        // Last era with a non-zero validator reward within HistoryDepth.
        let eras: Vec<u32> = (era.saturating_sub(consts.history_depth)..era)
            .rev()
            .collect();
        let reward_keys: Vec<Vec<u8>> = eras
            .iter()
            .map(|e| {
                key_bytes(
                    &client,
                    &sora::storage().staking().eras_validator_reward(*e),
                )
            })
            .collect::<Result<_, _>>()?;
        let rewards: Vec<Option<u128>> = chain.fetch_many(&reward_keys).await?;
        let last = eras
            .iter()
            .zip(rewards)
            .find_map(|(e, r)| r.filter(|v| *v > 0).map(|v| (*e, planck_fixed(v, 2))));

        let xor_price = xor_price(&state).await?;
        let total_stake_usd =
            (staked_f > 0.0 && xor_price > 0.0).then(|| format!("{:.2}", staked_f * xor_price));
        let era_started_ago = era_start
            .map(|s| format!("{} min ago", (now_ms - s as i64).div_euclid(60_000)))
            .unwrap_or_default();
        Ok(NetworkResponse {
            active_era: era,
            current_era: raw.current.unwrap_or(era),
            era_start,
            session_index: raw.session,
            sessions_per_era: consts.sessions_per_era,
            session_progress,
            era_progress,
            expected_block_time: consts.expected_block_time,
            best_block: best,
            finalized_block: finalized,
            total_issuance,
            total_staked: total_staked.clone(),
            staking_ratio,
            validator_count: raw.set_len,
            avg_block_time: consts.expected_block_time as f64 / 1000.0,
            era,
            total_stake: total_staked,
            total_stake_usd,
            epoch_progress,
            epochs_per_era: consts.sessions_per_era,
            epoch_duration: epoch_label(epoch_seconds),
            active_validators: raw.set_len,
            waiting_validators: raw.intents.saturating_sub(raw.set_len as u32),
            validator_target: raw.target,
            min_nominator_bond: planck_fixed(raw.min_nom, 4),
            min_validator_bond: planck_fixed(raw.min_val, 4),
            last_reward_era: last.as_ref().map(|(e, _)| *e),
            last_reward_amount: last.map(|(_, a)| a),
            ideal_stake_rate: None,
            current_inflation: 0.0,
            unbonding_days,
            unbonding_eras: consts.bonding_duration,
            era_started_ago,
        })
    })
    .await?;
    Ok(Json(v))
}

// ---------------------------------------------------------------------
// /staking/recent-blocks
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct RecentBlock {
    number: u32,
    hash: String,
    validator: Option<String>,
    #[serde(rename = "validatorName")]
    validator_name: Option<String>,
    extrinsics: usize,
    age: i64,
    timestamp: i64,
}

#[derive(Serialize, Deserialize)]
struct RecentBlocksResponse {
    blocks: Vec<RecentBlock>,
}

/// BABE pre-runtime digest → authority index (Primary / SecondaryPlain /
/// SecondaryVRF all start with the `u32` authority index).
pub fn babe_authority_index(logs: &[DigestItem]) -> Option<u32> {
    logs.iter().find_map(|l| match l {
        DigestItem::PreRuntime(id, data)
            if id == b"BABE" && data.len() >= 5 && (1..=3).contains(&data[0]) =>
        {
            Some(u32::from_le_bytes([data[1], data[2], data[3], data[4]]))
        }
        _ => None,
    })
}

async fn recent_blocks(
    State(state): State<AppState>,
) -> Result<Json<RecentBlocksResponse>, ApiError> {
    let v = cached(
        &state,
        "staking:recent-blocks",
        RECENT_BLOCKS_TTL,
        || async {
            let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
            let client = chain.client().await?;
            let legacy = chain.legacy_rpc().await?;
            let best = head_number(&legacy).await?;
            let set: Vec<String> = client
                .storage()
                .at_latest()
                .await
                .map_err(ChainErr)?
                .fetch(&sora::storage().session().validators())
                .await
                .map_err(ChainErr)?
                .unwrap_or_default()
                .iter()
                .map(|a| ss58_encode_sora(&a.0))
                .collect();
            let now_ms = chrono::Utc::now().timestamp_millis();
            let mut blocks = Vec::with_capacity(RECENT_BLOCKS as usize);
            for n in (best.saturating_sub(RECENT_BLOCKS - 1)..=best).rev() {
                let Some(hash) = legacy
                    .chain_get_block_hash(Some(n.into()))
                    .await
                    .map_err(ChainErr)?
                else {
                    continue;
                };
                let block = client.blocks().at(hash).await.map_err(ChainErr)?;
                let author = babe_authority_index(&block.header().digest.logs)
                    .and_then(|i| set.get(i as usize).cloned());
                let exts = block.extrinsics().await.map_err(ChainErr)?;
                let mut ts_ms: i64 = 0;
                for ext in exts.iter() {
                    if let Ok(Some(set)) = ext.as_extrinsic::<sora::timestamp::calls::types::Set>()
                    {
                        ts_ms = set.now as i64;
                        break;
                    }
                }
                blocks.push(RecentBlock {
                    number: n,
                    hash: format!("0x{}", hex::encode(hash.0)),
                    validator: author,
                    validator_name: None,
                    extrinsics: exts.len(),
                    age: ((now_ms - ts_ms) / 1000).max(0),
                    timestamp: ts_ms,
                });
            }
            let addrs: Vec<String> = {
                let mut a: Vec<String> =
                    blocks.iter().filter_map(|b| b.validator.clone()).collect();
                a.sort();
                a.dedup();
                a
            };
            let names = display_names(&state, &addrs).await;
            for b in blocks.iter_mut() {
                b.validator_name = b.validator.as_ref().and_then(|v| names.get(v).cloned());
            }
            Ok(RecentBlocksResponse { blocks })
        },
    )
    .await?;
    Ok(Json(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planck_helpers_match_bignumber() {
        assert_eq!(planck_fixed(231_960_012_819_361_308_153, 2), "231.96");
        assert_eq!(planck_fixed(0, 4), "0.0000");
        assert_eq!(planck_to_f64(472_352_166_981_040_930), 0.47235216698104093);
    }

    #[test]
    fn progress_and_labels_match_node() {
        let (sp, ep, epoch) = progress(47027, 47024, 6, 27_574_338, 600);
        assert_eq!(sp, 3);
        assert_eq!(ep, 50.0);
        assert_eq!(epoch, "23%");
        assert_eq!(epoch_label(3600.0), "1.0h");
        assert_eq!(epoch_label(600.0), "10min");
        assert_eq!(round_to(0.75 / 1e9 * 1e9 * 100.0 / 100.0 * 75.0, 2), 56.25);
    }

    #[test]
    fn babe_digest_yields_authority_index() {
        let mut data = vec![2u8];
        data.extend_from_slice(&7u32.to_le_bytes());
        data.extend_from_slice(&[0u8; 8]);
        let logs = vec![
            DigestItem::Other(vec![1]),
            DigestItem::PreRuntime(*b"BABE", data),
        ];
        assert_eq!(babe_authority_index(&logs), Some(7));
        assert_eq!(
            babe_authority_index(&[DigestItem::PreRuntime(*b"aura", vec![1, 2, 3, 4, 5])]),
            None
        );
    }
}
