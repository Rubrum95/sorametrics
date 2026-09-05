//! Typed decoders: subxt `EventDetails` → `core::sora_v2::*`.
//!
//! Each decoder is a small pure function: it inspects an event by
//! `pallet_name + variant_name`, attempts to decode it via the typed
//! event from the `runtime` codegen module, and returns
//! `Ok(Some(...))` if it matches, `Ok(None)` if it's a different
//! event type, `Err(...)` if decoding failed.
//!
//! Account addresses are stored as SS58 (SORA prefix 69, "cn…") —
//! matching the legacy DB and the frontend contract (Bloque 1 decision,
//! 2026-04-30). Asset ids remain `0x`-prefixed lowercase hex of the
//! 32-byte `AssetId32.code`.

use crate::runtime::sora;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use num_bigint::BigInt;
use sorametrics_core::chain::{ss58_encode_sora, Address, AssetId, BlockHeight};
use sorametrics_core::sora_v2::{
    BridgeDirection, FeeBurnKind, V2Bridge, V2FeeBurn, V2Swap, V2Transfer,
};
use sorametrics_core::time::Timestamp;
use subxt::{events::EventDetails, SubstrateConfig};
use thiserror::Error;

/// Type alias for the SubNetworkId enum reachable from runtime codegen.
type SubNetworkId = sora::runtime_types::bridge_types::SubNetworkId;

/// Errors when decoding an event.
///
/// `Subxt::source` is boxed because `subxt::Error` is ~160 bytes, which
/// would make the whole enum unwieldy in `Result<_, DecodeError>` slots
/// (clippy lint `result_large_err`).
#[derive(Debug, Error)]
pub enum DecodeError {
    /// Subxt-level decode error (codec mismatch, etc.).
    #[error("decode {pallet}::{variant}: {source}")]
    Subxt {
        /// The pallet name we attempted.
        pallet: &'static str,
        /// The variant name we attempted.
        variant: &'static str,
        /// Underlying subxt error (boxed to keep enum size small).
        #[source]
        source: Box<subxt::Error>,
    },
}

/// Convert a 32-byte array to a `0x`-prefixed lowercase hex string.
fn bytes32_hex(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(2 + 64);
    s.push_str("0x");
    s.push_str(&hex::encode(bytes));
    s
}

/// Convert `u128` (raw on-chain Balance) into [`BigDecimal`].
///
/// `BigDecimal` is arbitrary-precision (backed by `num_bigint::BigInt`)
/// so any `u128` value fits without overflow. This replaces the earlier
/// `u128_to_decimal` which had to reject values above `2^96 - 1` —
/// the new path handles SORA's pre-denomination XOR balances
/// (historically ~5.7e32) that those bounds rejected.
///
/// Implementation note: we go via `BigInt::from(u128)` because the
/// `From<u128> for BigDecimal` impl is gated behind a feature in some
/// versions; this path is unconditionally available.
fn u128_to_bigdecimal(amount: u128) -> BigDecimal {
    BigDecimal::from(BigInt::from(amount))
}

/// Coordinates piped through every decoder: identifies the position of
/// the event within the block and supplies the block timestamp.
#[derive(Clone, Copy, Debug)]
pub struct EventCoords {
    /// Block height.
    pub block_height: BlockHeight,
    /// Wall-clock timestamp from the block's `timestamp.set` inherent.
    pub block_timestamp: Timestamp,
    /// Position of the extrinsic the event was emitted from (0-based).
    /// `None` means the event is not associated with a user extrinsic
    /// (e.g., on_finalize hook). For DB storage we map `None` to the
    /// last extrinsic index in the block (per Substrate convention) but
    /// for now we surface as a strong type.
    pub extrinsic_id: u32,
    /// Position of the event within the block's flat events list.
    pub event_id: u32,
    /// Hash of the extrinsic the event belongs to. `None` for events
    /// emitted outside an extrinsic (Initialization/Finalization).
    pub extrinsic_hash: Option<[u8; 32]>,
}

impl EventCoords {
    /// The extrinsic hash as `0x`-hex, ready for storage.
    fn extrinsic_hash_hex(&self) -> Option<String> {
        self.extrinsic_hash.as_ref().map(bytes32_hex)
    }
}

/// Decode `liquidityProxy.Exchange` into a [`V2Swap`].
///
/// Returns `Ok(None)` if the event is a different pallet/variant.
/// Returns `Ok(Some(swap))` on a successful match + decode.
pub fn decode_swap(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2Swap>, DecodeError> {
    if ev.pallet_name() != "LiquidityProxy" || ev.variant_name() != "Exchange" {
        return Ok(None);
    }

    let exchange = ev
        .as_event::<sora::liquidity_proxy::events::Exchange>()
        .map_err(|e| DecodeError::Subxt {
            pallet: "LiquidityProxy",
            variant: "Exchange",
            // `as_event` surfaces `subxt_core::Error`; bubble through the
            // top-level subxt error type for uniform handling.
            source: Box::new(e.into()),
        })?;

    // `as_event::<T>()` returns `Result<Option<T>, _>`. The outer Result is
    // for decode errors; the inner Option is None if the event tag didn't
    // match. We already gated on names above, so an inner `None` here is
    // unexpected — treat as a decode bug rather than a graceful skip.
    let exchange = match exchange {
        Some(e) => e,
        None => {
            return Err(DecodeError::Subxt {
                pallet: "LiquidityProxy",
                variant: "Exchange",
                source: Box::new(subxt::Error::Other(
                    "name match but as_event returned None — codegen / metadata drift".into(),
                )),
            });
        }
    };

    // Field layout (subxt codegen, 8 unnamed fields):
    //   0 caller (AccountId32)
    //   1 dex_id (u32)
    //   2 input_asset_id  (AssetId32 { code: [u8; 32] })
    //   3 output_asset_id (AssetId32 { code: [u8; 32] })
    //   4 input_amount (u128)
    //   5 output_amount (u128)
    //   6 fee (OutcomeFee — currently unused; will populate sm.live_fees later)
    //   7 liquidity_sources (Vec<LiquiditySourceId>)
    let caller = ss58_encode_sora(&exchange.0 .0);
    let _dex_id = exchange.1; // not stored yet; reserved for future analytics
    let input_asset = bytes32_hex(&exchange.2.code);
    let output_asset = bytes32_hex(&exchange.3.code);
    let input_amount = u128_to_bigdecimal(exchange.4);
    let output_amount = u128_to_bigdecimal(exchange.5);

    Ok(Some(V2Swap {
        block_height: coords.block_height,
        extrinsic_id: coords.extrinsic_id,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        caller: Address::new(caller),
        input_asset: AssetId::new(input_asset),
        input_amount,
        output_asset: AssetId::new(output_asset),
        output_amount,
        // USD value populated later by a price-history join in the API layer.
        usd_value: None,
        timestamp: coords.block_timestamp,
    }))
}

/// Convert a u64 unix-millis timestamp (as provided by Substrate's
/// `timestamp.set` inherent) into a [`Timestamp`].
pub fn timestamp_from_millis(millis: u64) -> Timestamp {
    let secs = (millis / 1000) as i64;
    let nanos = ((millis % 1000) * 1_000_000) as u32;
    Timestamp::new(DateTime::<Utc>::from_timestamp(secs, nanos).unwrap_or_else(Utc::now))
}

/// Render `SubNetworkId` for the `network` label of a `V2Bridge`. Exact
/// string format matches the Node legacy convention (CLAUDE.md sesión
/// 2026-04-25): "Substrate: <name>" / "Parachain: <name>" / "TON".
fn substrate_network_label(id: &SubNetworkId) -> &'static str {
    match id {
        SubNetworkId::Mainnet => "Mainnet",
        SubNetworkId::Kusama => "Kusama",
        SubNetworkId::Polkadot => "Polkadot",
        SubNetworkId::Rococo => "Rococo",
        SubNetworkId::Alphanet => "Alphanet",
        SubNetworkId::Liberland => "Liberland",
    }
}

/// Decode `Assets::Transfer` into a [`V2Transfer`].
///
/// The unnamed tuple shape from codegen is `(from, to, asset, amount)`.
pub fn decode_transfer(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2Transfer>, DecodeError> {
    if ev.pallet_name() != "Assets" || ev.variant_name() != "Transfer" {
        return Ok(None);
    }

    let t = ev
        .as_event::<sora::assets::events::Transfer>()
        .map_err(|e| DecodeError::Subxt {
            pallet: "Assets",
            variant: "Transfer",
            source: Box::new(e.into()),
        })?;
    let t = match t {
        Some(t) => t,
        None => {
            return Err(DecodeError::Subxt {
                pallet: "Assets",
                variant: "Transfer",
                source: Box::new(subxt::Error::Other(
                    "Assets::Transfer name match but as_event returned None".into(),
                )),
            });
        }
    };

    let amount = u128_to_bigdecimal(t.3);

    Ok(Some(V2Transfer {
        block_height: coords.block_height,
        extrinsic_id: coords.extrinsic_id,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        from: Address::new(ss58_encode_sora(&t.0 .0)),
        to: Address::new(ss58_encode_sora(&t.1 .0)),
        asset: AssetId::new(bytes32_hex(&t.2.code)),
        amount,
        // USD value populated later by a price-history join in the API layer.
        usd_value: None,
        timestamp: coords.block_timestamp,
    }))
}

/// Decode any of the Hashi v2 bridge events into a [`V2Bridge`].
///
/// Handles 6 distinct events across 3 pallets:
///
/// | Pallet              | Variant | Direction (from SORA's PoV) | network label  |
/// |---------------------|---------|-----------------------------|----------------|
/// | SubstrateBridgeApp  | Burned  | `Out`                       | `Substrate: …` |
/// | SubstrateBridgeApp  | Minted  | `In`                        | `Substrate: …` |
/// | ParachainBridgeApp  | Burned  | `Out`                       | `Parachain: …` |
/// | ParachainBridgeApp  | Minted  | `In`                        | `Parachain: …` |
/// | JettonApp           | Burned  | `Out`                       | `TON`          |
/// | JettonApp           | Minted  | `In`                        | `TON`          |
///
/// `caller` is always the SORA-side address: `sender` on Burned (the
/// SORA account that initiated the burn), `recipient` on Minted (the
/// SORA account receiving the minted tokens). The cross-chain
/// counterparty is intentionally not stored — when 1.2.4 adds the
/// extended bridge schema we'll capture it then.
pub fn decode_bridge(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2Bridge>, DecodeError> {
    let pallet = ev.pallet_name();
    let variant = ev.variant_name();

    match (pallet, variant) {
        ("SubstrateBridgeApp", "Burned") => decode_substrate_bridge_burned(ev, coords),
        ("SubstrateBridgeApp", "Minted") => decode_substrate_bridge_minted(ev, coords),
        ("ParachainBridgeApp", "Burned") => decode_parachain_bridge_burned(ev, coords),
        ("ParachainBridgeApp", "Minted") => decode_parachain_bridge_minted(ev, coords),
        ("JettonApp", "Burned") => decode_jetton_burned(ev, coords),
        ("JettonApp", "Minted") => decode_jetton_minted(ev, coords),
        _ => Ok(None),
    }
}

// -- SubstrateBridgeApp --------------------------------------------------

fn decode_substrate_bridge_burned(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2Bridge>, DecodeError> {
    let b = require_event::<sora::substrate_bridge_app::events::Burned>(
        ev,
        "SubstrateBridgeApp",
        "Burned",
    )?;
    Ok(Some(V2Bridge {
        block_height: coords.block_height,
        extrinsic_id: coords.extrinsic_id,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        direction: BridgeDirection::Out,
        network: format!("Substrate: {}", substrate_network_label(&b.network_id)),
        caller: Address::new(ss58_encode_sora(&b.sender.0)),
        asset: AssetId::new(bytes32_hex(&b.asset_id.code)),
        amount: u128_to_bigdecimal(b.amount),
        timestamp: coords.block_timestamp,
    }))
}

fn decode_substrate_bridge_minted(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2Bridge>, DecodeError> {
    let m = require_event::<sora::substrate_bridge_app::events::Minted>(
        ev,
        "SubstrateBridgeApp",
        "Minted",
    )?;
    Ok(Some(V2Bridge {
        block_height: coords.block_height,
        extrinsic_id: coords.extrinsic_id,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        direction: BridgeDirection::In,
        network: format!("Substrate: {}", substrate_network_label(&m.network_id)),
        caller: Address::new(ss58_encode_sora(&m.recipient.0)),
        asset: AssetId::new(bytes32_hex(&m.asset_id.code)),
        amount: u128_to_bigdecimal(m.amount),
        timestamp: coords.block_timestamp,
    }))
}

// -- ParachainBridgeApp --------------------------------------------------

fn decode_parachain_bridge_burned(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2Bridge>, DecodeError> {
    let b = require_event::<sora::parachain_bridge_app::events::Burned>(
        ev,
        "ParachainBridgeApp",
        "Burned",
    )?;
    // Field layout: (network_id, asset_id, sender SORA, recipient parachain, amount)
    Ok(Some(V2Bridge {
        block_height: coords.block_height,
        extrinsic_id: coords.extrinsic_id,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        direction: BridgeDirection::Out,
        network: format!("Parachain: {}", substrate_network_label(&b.0)),
        caller: Address::new(ss58_encode_sora(&b.2 .0)),
        asset: AssetId::new(bytes32_hex(&b.1.code)),
        amount: u128_to_bigdecimal(b.4),
        timestamp: coords.block_timestamp,
    }))
}

fn decode_parachain_bridge_minted(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2Bridge>, DecodeError> {
    let m = require_event::<sora::parachain_bridge_app::events::Minted>(
        ev,
        "ParachainBridgeApp",
        "Minted",
    )?;
    // Field layout: (network_id, asset_id, sender opt parachain, recipient SORA, amount)
    Ok(Some(V2Bridge {
        block_height: coords.block_height,
        extrinsic_id: coords.extrinsic_id,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        direction: BridgeDirection::In,
        network: format!("Parachain: {}", substrate_network_label(&m.0)),
        caller: Address::new(ss58_encode_sora(&m.3 .0)),
        asset: AssetId::new(bytes32_hex(&m.1.code)),
        amount: u128_to_bigdecimal(m.4),
        timestamp: coords.block_timestamp,
    }))
}

// -- JettonApp (TON) -----------------------------------------------------

fn decode_jetton_burned(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2Bridge>, DecodeError> {
    let b = require_event::<sora::jetton_app::events::Burned>(ev, "JettonApp", "Burned")?;
    Ok(Some(V2Bridge {
        block_height: coords.block_height,
        extrinsic_id: coords.extrinsic_id,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        direction: BridgeDirection::Out,
        network: "TON".to_string(),
        caller: Address::new(ss58_encode_sora(&b.sender.0)),
        asset: AssetId::new(bytes32_hex(&b.asset_id.code)),
        amount: u128_to_bigdecimal(b.amount),
        timestamp: coords.block_timestamp,
    }))
}

fn decode_jetton_minted(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2Bridge>, DecodeError> {
    let m = require_event::<sora::jetton_app::events::Minted>(ev, "JettonApp", "Minted")?;
    Ok(Some(V2Bridge {
        block_height: coords.block_height,
        extrinsic_id: coords.extrinsic_id,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        direction: BridgeDirection::In,
        network: "TON".to_string(),
        caller: Address::new(ss58_encode_sora(&m.recipient.0)),
        asset: AssetId::new(bytes32_hex(&m.asset_id.code)),
        amount: u128_to_bigdecimal(m.amount),
        timestamp: coords.block_timestamp,
    }))
}

/// Decode either of the `XorFee` events into a [`V2FeeBurn`].
///
/// `XorFee::FeeWithdrawn(payer, amount)` → `kind = FeeWithdrawn`, `referrer = None`.
/// `XorFee::ReferrerRewarded(referee, referrer, amount)` → `kind = ReferrerRewarded`,
/// `payer = referee`, `referrer = Some(referrer)`. Other XorFee variants
/// (governance updates) return `Ok(None)` — they do not represent a fee
/// movement.
pub fn decode_fee_burn(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2FeeBurn>, DecodeError> {
    if ev.pallet_name() != "XorFee" {
        return Ok(None);
    }

    match ev.variant_name() {
        "FeeWithdrawn" => {
            let f =
                require_event::<sora::xor_fee::events::FeeWithdrawn>(ev, "XorFee", "FeeWithdrawn")?;
            Ok(Some(V2FeeBurn {
                block_height: coords.block_height,
                extrinsic_id: coords.extrinsic_id,
                event_id: coords.event_id,
                extrinsic_hash: coords.extrinsic_hash_hex(),
                kind: FeeBurnKind::FeeWithdrawn,
                payer: Address::new(ss58_encode_sora(&f.0 .0)),
                referrer: None,
                amount: u128_to_bigdecimal(f.1),
                timestamp: coords.block_timestamp,
            }))
        }
        "ReferrerRewarded" => {
            let r = require_event::<sora::xor_fee::events::ReferrerRewarded>(
                ev,
                "XorFee",
                "ReferrerRewarded",
            )?;
            Ok(Some(V2FeeBurn {
                block_height: coords.block_height,
                extrinsic_id: coords.extrinsic_id,
                event_id: coords.event_id,
                extrinsic_hash: coords.extrinsic_hash_hex(),
                kind: FeeBurnKind::ReferrerRewarded,
                payer: Address::new(ss58_encode_sora(&r.0 .0)),
                referrer: Some(Address::new(ss58_encode_sora(&r.1 .0))),
                amount: u128_to_bigdecimal(r.2),
                timestamp: coords.block_timestamp,
            }))
        }
        _ => Ok(None),
    }
}

/// Helper: assert the inner `Option<T>` is `Some`; otherwise return a
/// `DecodeError::Subxt` with the appropriate pallet/variant labels.
/// Saves repeating the same pattern in every bridge decoder.
fn require_event<T>(
    ev: &EventDetails<SubstrateConfig>,
    pallet: &'static str,
    variant: &'static str,
) -> Result<T, DecodeError>
where
    T: subxt::events::StaticEvent,
{
    let inner = ev.as_event::<T>().map_err(|e| DecodeError::Subxt {
        pallet,
        variant,
        source: Box::new(e.into()),
    })?;
    inner.ok_or_else(|| DecodeError::Subxt {
        pallet,
        variant,
        source: Box::new(subxt::Error::Other(format!(
            "{pallet}::{variant} name match but as_event returned None — codegen drift"
        ))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes32_hex_lowercase_with_prefix() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xAB;
        bytes[31] = 0xCD;
        let s = bytes32_hex(&bytes);
        assert!(s.starts_with("0x"));
        assert_eq!(s.len(), 2 + 64);
        assert!(s.starts_with("0xab"));
        assert!(s.ends_with("cd"));
    }

    #[test]
    fn u128_to_bigdecimal_small_values() {
        let d = u128_to_bigdecimal(123_456_u128);
        assert_eq!(d.to_string(), "123456");
    }

    #[test]
    fn u128_to_bigdecimal_zero() {
        assert_eq!(u128_to_bigdecimal(0).to_string(), "0");
    }

    #[test]
    fn u128_to_bigdecimal_handles_u128_max() {
        // The whole reason we switched away from `rust_decimal::Decimal`:
        // u128::MAX must round-trip exactly, no overflow.
        let d = u128_to_bigdecimal(u128::MAX);
        assert_eq!(d.to_string(), u128::MAX.to_string());
    }

    #[test]
    fn u128_to_bigdecimal_handles_pre_denomination_xor() {
        // Real value seen on SORA mainnet block 22M (pre-denomination XOR
        // transfer). Used to overflow `Decimal::MAX = 2^96-1`; with
        // BigDecimal it round-trips cleanly.
        let real_pre_denom: u128 = 570_043_412_340_088_622_093_476_999_544_777;
        let d = u128_to_bigdecimal(real_pre_denom);
        assert_eq!(d.to_string(), real_pre_denom.to_string());
    }

    #[test]
    fn timestamp_from_millis_truncates_correctly() {
        let ts = timestamp_from_millis(1_700_000_000_500);
        let dt = ts.inner();
        assert_eq!(dt.timestamp(), 1_700_000_000);
        assert_eq!(dt.timestamp_subsec_millis(), 500);
    }

    #[test]
    fn substrate_network_label_covers_all_variants() {
        // Touch every variant so adding a new one to SubNetworkId without
        // updating the label table fails to compile (match is exhaustive).
        let labels = [
            (SubNetworkId::Mainnet, "Mainnet"),
            (SubNetworkId::Kusama, "Kusama"),
            (SubNetworkId::Polkadot, "Polkadot"),
            (SubNetworkId::Rococo, "Rococo"),
            (SubNetworkId::Alphanet, "Alphanet"),
            (SubNetworkId::Liberland, "Liberland"),
        ];
        for (id, expected) in &labels {
            assert_eq!(substrate_network_label(id), *expected);
        }
    }
}
