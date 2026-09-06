//! `/stats/fee-config` — live fee parameters (`index.js::computeFeeConfig`).
//!
//! Reads `transactionPayment.nextFeeMultiplier`, `xorFee.multiplier`,
//! `xorFee.remintPeriod`, `xorFee.xorToVal`, `xorFee.xorToBuyBack` and
//! the head, plus `TransactionByteFee` gated by spec version (`1e-7`
//! from spec 124, a raw runtime `pub const` not exposed in metadata),
//! and `samples`: `paymentInfo` of a representative transfer, swap and
//! bridge call — a dummy-signed extrinsic run through
//! `TransactionPaymentApi_query_info`, exactly what polkadot-js does.
//! SORA routes those calls through `XorFee::CustomFees`, so the fee is
//! flat per class and cannot be decomposed; we report the total.
//!
//! Cached 15 s in-process (Node `FEE_CONFIG_TTL`).
//!
//! `lenBytes` is the encoded size of the unsigned extrinsic
//! (length prefix + version + call); the Node measured polkadot-js's
//! `tx.toHex()`, which may differ by a few bytes.

use crate::chain::SoraConfig;
use crate::{error::ApiError, AppState};
use axum::{extract::State, routing::get, Json, Router};
use serde::{Deserialize, Serialize};
use sorametrics_substrate::runtime::sora;
use sorametrics_substrate::runtime::sora::runtime_types::common::primitives::{
    AssetId32, FilterMode, LiquiditySourceType,
};
use sorametrics_substrate::runtime::sora::runtime_types::common::swap_amount::SwapAmount;
use std::time::Duration;
use subxt::config::DefaultExtrinsicParamsBuilder;
use subxt::ext::codec::{Decode, Encode};
use subxt::tx::Payload;
use subxt::utils::{AccountId32, MultiSignature, H160};
use subxt::OnlineClient;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new().route("/stats/fee-config", get(fee_config))
}

const TTL: Duration = Duration::from_secs(15);
const XOR: [u8; 32] =
    hex_literal(b"0200000000000000000000000000000000000000000000000000000000000000");
const VAL: [u8; 32] =
    hex_literal(b"0200040000000000000000000000000000000000000000000000000000000000");
/// Node `dummy` account used for `paymentInfo`.
const DUMMY: &str = "cnVS46aLyfRHTossU1ZEXaw6Eok1Lk9NeMdhJsSNzp7ywJLEq";
const ONE_XOR: u128 = 1_000_000_000_000_000_000;

const fn hex_literal(s: &[u8; 64]) -> [u8; 32] {
    let mut out = [0u8; 32];
    let mut i = 0;
    while i < 32 {
        out[i] = (hex_nibble(s[2 * i]) << 4) | hex_nibble(s[2 * i + 1]);
        i += 1;
    }
    out
}

const fn hex_nibble(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0,
    }
}

/// Node `TRANSACTION_BYTE_FEE_BY_SPEC`.
pub fn transaction_byte_fee(spec_version: u32) -> f64 {
    if spec_version >= 124 {
        1e-7
    } else {
        0.0
    }
}

/// `FixedU128` / balance planck → XOR units (Node: `Number(BigInt(v)) / 1e18`).
fn big(v: u128) -> f64 {
    v as f64 / 1e18
}

#[derive(Clone, Serialize, Deserialize)]
struct Sample {
    fee: f64,
    #[serde(rename = "weightRefTime")]
    weight_ref_time: u64,
    class: String,
    #[serde(rename = "lenBytes")]
    len_bytes: usize,
    #[serde(rename = "hypotheticalLenFee")]
    hypothetical_len_fee: f64,
}

#[derive(Clone, Serialize, Deserialize, Default)]
struct Samples {
    #[serde(skip_serializing_if = "Option::is_none")]
    transfer: Option<Sample>,
    #[serde(skip_serializing_if = "Option::is_none")]
    swap: Option<Sample>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bridge: Option<Sample>,
}

#[derive(Clone, Serialize, Deserialize)]
struct FeeConfig {
    #[serde(rename = "specVersion")]
    spec_version: u32,
    #[serde(rename = "nextFeeMultiplier")]
    next_fee_multiplier: f64,
    #[serde(rename = "xorFeeMultiplier")]
    xor_fee_multiplier: f64,
    #[serde(rename = "xorToValXor")]
    xor_to_val_xor: f64,
    #[serde(rename = "xorToBuyBackXor")]
    xor_to_buy_back_xor: f64,
    #[serde(rename = "remintPeriodBlocks")]
    remint_period_blocks: u32,
    #[serde(rename = "transactionByteFee")]
    transaction_byte_fee: f64,
    samples: Samples,
    #[serde(rename = "asOfBlock")]
    as_of_block: u32,
    #[serde(rename = "asOfTs")]
    as_of_ts: i64,
}

/// `sp_runtime::Weight` (both fields compact-encoded).
#[derive(Decode)]
#[codec(crate = subxt::ext::codec)]
struct Weight {
    #[codec(compact)]
    ref_time: u64,
    #[codec(compact)]
    _proof_size: u64,
}

/// `pallet_transaction_payment::RuntimeDispatchInfo<Balance, Weight>`.
#[derive(Decode)]
#[codec(crate = subxt::ext::codec)]
struct RuntimeDispatchInfo {
    weight: Weight,
    class: u8,
    partial_fee: u128,
}

fn class_label(c: u8) -> &'static str {
    match c {
        0 => "Normal",
        1 => "Operational",
        2 => "Mandatory",
        _ => "Unknown",
    }
}

/// `paymentInfo(dummy)`: dummy-sign the call and ask the runtime.
async fn sample<C: Payload>(
    sora: &OnlineClient<SoraConfig>,
    call: &C,
    byte_fee: f64,
) -> Result<Sample, subxt::Error> {
    let dummy: AccountId32 = DUMMY
        .parse()
        .map_err(|_| subxt::Error::Other("dummy account is not valid ss58".into()))?;
    let params = DefaultExtrinsicParamsBuilder::<SoraConfig>::new()
        .nonce(0)
        .build();
    let partial = sora.tx().create_partial_signed_offline(call, params)?;
    let signed =
        partial.sign_with_address_and_signature(&dummy, &MultiSignature::Sr25519([0u8; 64]));
    let bytes = signed.encoded().to_vec();
    // Unsigned length as polkadot-js's `tx.toHex()` would measure it:
    // compact(len) + version byte + call.
    let call_bytes = sora.tx().call_data(call)?;
    let unsigned_len = {
        let body_len = 1 + call_bytes.len();
        body_len + subxt::ext::codec::Compact(body_len as u32).encoded_size()
    };
    // `query_info(uxt: UncheckedExtrinsic, len: u32)`: the opaque
    // extrinsic already carries its compact length prefix, so the
    // parameters are the raw extrinsic bytes followed by `len`.
    let mut params_bytes = bytes.clone();
    params_bytes.extend((bytes.len() as u32).encode());
    let info: RuntimeDispatchInfo = sora
        .runtime_api()
        .at_latest()
        .await?
        .call_raw("TransactionPaymentApi_query_info", Some(&params_bytes))
        .await?;
    Ok(Sample {
        fee: big(info.partial_fee),
        weight_ref_time: info.weight.ref_time,
        class: class_label(info.class).to_string(),
        len_bytes: unsigned_len,
        hypothetical_len_fee: unsigned_len as f64 * byte_fee,
    })
}

async fn compute(state: &AppState) -> Result<FeeConfig, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let sora = chain.sora_client().await?;
    let cfg = chain
        .with_client(|client| async move {
            let at = client.storage().at_latest().await?;
            let nfm = at
                .fetch_or_default(&sora::storage().transaction_payment().next_fee_multiplier())
                .await?;
            let xfm = at
                .fetch_or_default(&sora::storage().xor_fee().multiplier())
                .await?;
            let remint = at
                .fetch_or_default(&sora::storage().xor_fee().remint_period())
                .await?;
            let to_val = at
                .fetch_or_default(&sora::storage().xor_fee().xor_to_val())
                .await?;
            let to_buy = at
                .fetch_or_default(&sora::storage().xor_fee().xor_to_buy_back())
                .await?;
            let head = client.blocks().at_latest().await?.number();
            let spec_version = client.runtime_version().spec_version;
            let byte_fee = transaction_byte_fee(spec_version);

            let dummy: AccountId32 = DUMMY
                .parse()
                .map_err(|_| subxt::Error::Other("dummy account is not valid ss58".into()))?;
            let xor = xor_asset();
            let val = AssetId32 {
                code: VAL,
                __ignore: Default::default(),
            };
            let transfer_call = sora::tx().assets().transfer(xor_asset(), dummy, ONE_XOR);
            let swap_call = sora::tx().liquidity_proxy().swap(
                0,
                xor,
                val,
                SwapAmount::WithDesiredInput {
                    desired_amount_in: ONE_XOR,
                    min_amount_out: 0,
                },
                vec![LiquiditySourceType::XYKPool],
                FilterMode::Disabled,
            );
            let bridge_call = sora::tx().eth_bridge().transfer_to_sidechain(
                xor_asset(),
                H160([0u8; 20]),
                ONE_XOR,
                0,
            );
            // Node: a failed sample is simply absent (`catch → null`).
            let logged = |name: &'static str, r: Result<Sample, subxt::Error>| match r {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::warn!(sample = name, error = %e, "fee sample failed");
                    None
                }
            };
            let samples = Samples {
                transfer: logged("transfer", sample(&sora, &transfer_call, byte_fee).await),
                swap: logged("swap", sample(&sora, &swap_call, byte_fee).await),
                bridge: logged("bridge", sample(&sora, &bridge_call, byte_fee).await),
            };

            Ok(FeeConfig {
                spec_version,
                next_fee_multiplier: big(nfm.0),
                xor_fee_multiplier: big(xfm.0),
                xor_to_val_xor: big(to_val),
                xor_to_buy_back_xor: big(to_buy),
                remint_period_blocks: remint,
                transaction_byte_fee: byte_fee,
                samples,
                as_of_block: head,
                as_of_ts: chrono::Utc::now().timestamp_millis(),
            })
        })
        .await?;
    Ok(cfg)
}

fn xor_asset(
) -> AssetId32<sora::runtime_types::common::primitives::_allowed_deprecated::PredefinedAssetId> {
    AssetId32 {
        code: XOR,
        __ignore: Default::default(),
    }
}

async fn fee_config(State(state): State<AppState>) -> Result<Json<FeeConfig>, ApiError> {
    if let Some(v) = state.cached_scan("fee-config", TTL).await {
        let cfg: FeeConfig =
            serde_json::from_value(v).map_err(|e| ApiError::Internal(e.to_string()))?;
        return Ok(Json(cfg));
    }
    let cfg = compute(&state).await?;
    let v = serde_json::to_value(&cfg).map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store_scan("fee-config", v).await;
    Ok(Json(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_fee_gated_by_spec() {
        assert_eq!(transaction_byte_fee(123), 0.0);
        assert_eq!(transaction_byte_fee(124), 1e-7);
        assert_eq!(transaction_byte_fee(130), 1e-7);
    }

    #[test]
    fn hex_literal_decodes() {
        assert_eq!(XOR[0], 0x02);
        assert_eq!(VAL[2], 0x04);
        assert_eq!(big(142_856_875_128_153_320_000), 142.85687512815332);
    }

    #[test]
    fn dispatch_class_labels() {
        assert_eq!(class_label(0), "Normal");
        assert_eq!(class_label(2), "Mandatory");
    }
}
