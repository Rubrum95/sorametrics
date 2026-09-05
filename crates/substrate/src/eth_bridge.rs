//! Classic Ethereum bridge (`EthBridge` pallet, federated multisig) —
//! the bridge that still carries all ETH↔SORA traffic (SCCP-ETH is not
//! deployed). Mechanism = the Node's live indexer + `mv_bridges`:
//!
//! - **Outgoing**: the `eth_bridge.transfer_to_sidechain` extrinsic. The
//!   Node reads the call args from the extrinsic (and skips the events
//!   of that extrinsic to avoid double counting). We anchor the row on
//!   the `EthBridge::RequestRegistered(hash)` event that call emits, so
//!   the `(block, extrinsic, event)` PK is a real event position, and
//!   fill the fields from the call in the same phase — no extra RPC.
//! - **Incoming**: `EthBridge::IncomingRequestFinalized(hash)`. The
//!   event only carries the request hash; like the Node
//!   (`api.query.ethBridge.requests(0, hash)`) we read the request from
//!   storage at that block and keep it when it is an
//!   `IncomingRequest::Transfer`. One storage RPC per incoming
//!   transfer, which is rare.
//!
//! `network_id` 0 is Ethereum mainnet — the only EVM network registered
//! on the classic bridge and the id the Node hardcodes.

use crate::decoder::{DecodeError, EventCoords};
use crate::runtime::sora;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use sorametrics_core::chain::{ss58_encode_sora, Address, AssetId};
use sorametrics_core::sora_v2::{BridgeDirection, V2Bridge};
use std::collections::HashMap;
use subxt::blocks::{Block, Extrinsics};
use subxt::events::EventDetails;
use subxt::{OnlineClient, SubstrateConfig};

type OffchainRequest = sora::runtime_types::eth_bridge::requests::OffchainRequest;
type IncomingRequest = sora::runtime_types::eth_bridge::requests::IncomingRequest;

/// Network label the legacy contract uses for the classic bridge.
pub const ETHEREUM_NETWORK_LABEL: &str = "Ethereum";

/// Ethereum mainnet id on the classic bridge (Node: `requests(0, hash)`).
pub const ETHEREUM_NETWORK_ID: u32 = 0;

/// Args of a `transfer_to_sidechain` call plus its signer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutgoingTransferCall {
    /// SORA account that signed the extrinsic.
    pub signer: [u8; 32],
    /// Destination Ethereum address.
    pub to: [u8; 20],
    /// `0x`-hex asset id.
    pub asset_id: String,
    /// Raw amount.
    pub amount: u128,
}

/// Signer account from the extrinsic address bytes. SORA's `Address`
/// is the raw `AccountId32` (32 bytes); a `MultiAddress::Id` prefix
/// (`0x00` + 32 bytes) is tolerated in case the runtime ever switches.
fn signer_from_address_bytes(bytes: &[u8]) -> Option<[u8; 32]> {
    let raw = match bytes.len() {
        32 => bytes,
        33 if bytes[0] == 0 => &bytes[1..],
        _ => return None,
    };
    raw.try_into().ok()
}

/// Every `transfer_to_sidechain` call in the block, by extrinsic index.
pub fn outgoing_calls(
    extrinsics: &Extrinsics<SubstrateConfig, OnlineClient<SubstrateConfig>>,
) -> HashMap<u32, OutgoingTransferCall> {
    let mut out = HashMap::new();
    for ext in extrinsics.iter() {
        let call = match ext.as_extrinsic::<sora::eth_bridge::calls::types::TransferToSidechain>() {
            Ok(Some(c)) => c,
            // Not this call, or an unrelated extrinsic we can't decode.
            Ok(None) | Err(_) => continue,
        };
        let signer = match ext.address_bytes().and_then(signer_from_address_bytes) {
            Some(s) => s,
            None => continue,
        };
        out.insert(
            ext.index(),
            OutgoingTransferCall {
                signer,
                to: call.to.0,
                asset_id: bytes32_hex(&call.asset_id.code),
                amount: call.amount,
            },
        );
    }
    out
}

/// `EthBridge::RequestRegistered` emitted by a `transfer_to_sidechain`
/// call → outgoing bridge row. `Ok(None)` for any other event, or for
/// a `RequestRegistered` from a different call (add_asset, peers…).
pub fn decode_eth_outgoing(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
    calls: &HashMap<u32, OutgoingTransferCall>,
) -> Result<Option<V2Bridge>, DecodeError> {
    if ev.pallet_name() != "EthBridge" || ev.variant_name() != "RequestRegistered" {
        return Ok(None);
    }
    let call = match calls.get(&coords.extrinsic_id) {
        Some(c) => c,
        None => return Ok(None),
    };
    Ok(Some(V2Bridge {
        block_height: coords.block_height,
        extrinsic_id: coords.extrinsic_id,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        direction: BridgeDirection::Out,
        network: ETHEREUM_NETWORK_LABEL.to_string(),
        caller: Address::new(ss58_encode_sora(&call.signer)),
        counterparty: Some(format!("0x{}", hex::encode(call.to))),
        asset: AssetId::new(call.asset_id.clone()),
        amount: BigDecimal::from(BigInt::from(call.amount)),
        usd_value: None,
        timestamp: coords.block_timestamp,
    }))
}

/// Request hash of an `EthBridge::IncomingRequestFinalized` event.
pub fn eth_incoming_hash(
    ev: &EventDetails<SubstrateConfig>,
) -> Result<Option<[u8; 32]>, DecodeError> {
    if ev.pallet_name() != "EthBridge" || ev.variant_name() != "IncomingRequestFinalized" {
        return Ok(None);
    }
    let e = ev
        .as_event::<sora::eth_bridge::events::IncomingRequestFinalized>()
        .map_err(|e| DecodeError::Subxt {
            pallet: "EthBridge",
            variant: "IncomingRequestFinalized",
            source: Box::new(e.into()),
        })?;
    Ok(e.map(|e| e.0 .0))
}

/// Resolve a finalized incoming request from storage at the block →
/// incoming bridge row when it is a `Transfer`; `Ok(None)` for the
/// other request kinds (peer changes, migrations…) or a missing entry.
pub async fn decode_eth_incoming(
    block: &Block<SubstrateConfig, OnlineClient<SubstrateConfig>>,
    coords: EventCoords,
    hash: [u8; 32],
) -> Result<Option<V2Bridge>, subxt::Error> {
    let addr = sora::storage()
        .eth_bridge()
        .requests(ETHEREUM_NETWORK_ID, subxt::utils::H256(hash));
    let request = block.storage().fetch(&addr).await?;
    let transfer = match request {
        Some(OffchainRequest::Incoming(IncomingRequest::Transfer(t), _)) => t,
        _ => return Ok(None),
    };
    Ok(Some(V2Bridge {
        block_height: coords.block_height,
        extrinsic_id: coords.extrinsic_id,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        direction: BridgeDirection::In,
        network: ETHEREUM_NETWORK_LABEL.to_string(),
        caller: Address::new(ss58_encode_sora(&transfer.to.0)),
        counterparty: Some(format!("0x{}", hex::encode(transfer.from.0))),
        asset: AssetId::new(bytes32_hex(&transfer.asset_id.code)),
        amount: BigDecimal::from(BigInt::from(transfer.amount)),
        usd_value: None,
        timestamp: coords.block_timestamp,
    }))
}

fn bytes32_hex(bytes: &[u8; 32]) -> String {
    format!("0x{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signer_accepts_raw_account_and_multiaddress_id() {
        let acc = [7u8; 32];
        assert_eq!(signer_from_address_bytes(&acc), Some(acc));
        let mut multi = vec![0u8];
        multi.extend_from_slice(&acc);
        assert_eq!(signer_from_address_bytes(&multi), Some(acc));
        assert_eq!(signer_from_address_bytes(&[1u8; 20]), None);
        let mut other = vec![1u8];
        other.extend_from_slice(&acc);
        assert_eq!(signer_from_address_bytes(&other), None);
    }
}
