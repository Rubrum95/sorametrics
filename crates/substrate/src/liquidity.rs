//! Pool liquidity events — the Node's `live_liquidity_events` mechanism
//! (`index.js`, the `poolXYK` extrinsic loop):
//!
//! For every `poolXYK.deposit_liquidity` / `withdraw_liquidity`
//! extrinsic whose phase carries `System::ExtrinsicSuccess`, one row
//! with base = `*_asset_a`, target = `*_asset_b`, signer as caller, and
//! the amounts read from the extrinsic's own transfer events: the LAST
//! `Tokens::Transfer` whose currency equals the base / target asset,
//! and — when any `Balances::Transfer` (native XOR) is present — the
//! FIRST one overrides the base amount. Extrinsics without success, or
//! not decodable as those calls, produce nothing.

use crate::decoder::EventCoords;
use crate::runtime::sora;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use sorametrics_core::chain::{ss58_encode_sora, Address, AssetId};
use sorametrics_core::sora_v2::{LiquidityKind, V2Liquidity};
use std::collections::HashMap;
use subxt::blocks::Extrinsics;
use subxt::events::EventDetails;
use subxt::{OnlineClient, SubstrateConfig};

/// A liquidity call found in the block body.
#[derive(Clone, Debug)]
pub struct LiquidityCall {
    signer: [u8; 32],
    base: String,
    target: String,
    kind: LiquidityKind,
    hash: [u8; 32],
}

fn signer_from_address_bytes(bytes: &[u8]) -> Option<[u8; 32]> {
    let raw = match bytes.len() {
        32 => bytes,
        33 if bytes[0] == 0 => &bytes[1..],
        _ => return None,
    };
    raw.try_into().ok()
}

fn hex32(b: &[u8; 32]) -> String {
    format!("0x{}", hex::encode(b))
}

/// Every deposit / withdraw call in the block, by extrinsic index.
pub fn liquidity_calls(
    extrinsics: &Extrinsics<SubstrateConfig, OnlineClient<SubstrateConfig>>,
) -> HashMap<u32, LiquidityCall> {
    let mut out = HashMap::new();
    for ext in extrinsics.iter() {
        let Some(signer) = ext.address_bytes().and_then(signer_from_address_bytes) else {
            continue;
        };
        let hash = ext.hash().0;
        let call = match ext.as_extrinsic::<sora::pool_xyk::calls::types::DepositLiquidity>() {
            Ok(Some(c)) => LiquidityCall {
                signer,
                base: hex32(&c.input_asset_a.code),
                target: hex32(&c.input_asset_b.code),
                kind: LiquidityKind::Deposit,
                hash,
            },
            _ => match ext.as_extrinsic::<sora::pool_xyk::calls::types::WithdrawLiquidity>() {
                Ok(Some(c)) => LiquidityCall {
                    signer,
                    base: hex32(&c.output_asset_a.code),
                    target: hex32(&c.output_asset_b.code),
                    kind: LiquidityKind::Withdraw,
                    hash,
                },
                _ => continue,
            },
        };
        out.insert(ext.index(), call);
    }
    out
}

/// Per-extrinsic facts gathered from its events.
#[derive(Debug, Default, Clone)]
pub struct LiquidityFacts {
    succeeded: bool,
    tokens_by_currency: HashMap<String, u128>,
    first_balances_transfer: Option<u128>,
}

impl LiquidityFacts {
    /// Record one event of the extrinsic. Decode failures of the two
    /// transfer events are reported so the caller can log them.
    pub fn observe(&mut self, ev: &EventDetails<SubstrateConfig>) -> Result<(), Box<subxt::Error>> {
        match (ev.pallet_name(), ev.variant_name()) {
            ("System", "ExtrinsicSuccess") => self.succeeded = true,
            ("Tokens", "Transfer") => {
                if let Some(t) = ev
                    .as_event::<sora::tokens::events::Transfer>()
                    .map_err(|e| Box::new(e.into()))?
                {
                    self.tokens_by_currency
                        .insert(hex32(&t.currency_id.code), t.amount);
                }
            }
            ("Balances", "Transfer") => {
                if self.first_balances_transfer.is_none() {
                    if let Some(t) = ev
                        .as_event::<sora::balances::events::Transfer>()
                        .map_err(|e| Box::new(e.into()))?
                    {
                        self.first_balances_transfer = Some(t.amount);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Build the row for `call`, or `None` when the extrinsic failed.
    pub fn into_event(self, call: &LiquidityCall, coords: EventCoords) -> Option<V2Liquidity> {
        if !self.succeeded {
            return None;
        }
        let mut base = self
            .tokens_by_currency
            .get(&call.base)
            .copied()
            .unwrap_or(0);
        let target = self
            .tokens_by_currency
            .get(&call.target)
            .copied()
            .unwrap_or(0);
        if let Some(b) = self.first_balances_transfer {
            base = b;
        }
        Some(V2Liquidity {
            block_height: coords.block_height,
            extrinsic_id: coords.extrinsic_id,
            extrinsic_hash: Some(hex32(&call.hash)),
            caller: Address::new(ss58_encode_sora(&call.signer)),
            base_asset: AssetId::new(call.base.clone()),
            target_asset: AssetId::new(call.target.clone()),
            base_amount: BigDecimal::from(BigInt::from(base)),
            target_amount: BigDecimal::from(BigInt::from(target)),
            usd_value: None,
            kind: call.kind,
            timestamp: coords.block_timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sorametrics_core::chain::BlockHeight;
    use sorametrics_core::time::Timestamp;

    fn coords() -> EventCoords {
        EventCoords {
            block_height: BlockHeight(10),
            block_timestamp: Timestamp::now(),
            extrinsic_id: 1,
            event_id: 0,
            extrinsic_hash: None,
        }
    }

    fn call() -> LiquidityCall {
        LiquidityCall {
            signer: [7u8; 32],
            base: "0xbase".into(),
            target: "0xtarget".into(),
            kind: LiquidityKind::Deposit,
            hash: [9u8; 32],
        }
    }

    #[test]
    fn failed_extrinsic_yields_nothing() {
        let f = LiquidityFacts::default();
        assert!(f.into_event(&call(), coords()).is_none());
    }

    #[test]
    fn amounts_come_from_transfers_and_native_overrides_base() {
        let mut f = LiquidityFacts {
            succeeded: true,
            ..Default::default()
        };
        f.tokens_by_currency.insert("0xbase".into(), 5);
        f.tokens_by_currency.insert("0xtarget".into(), 7);
        let e = f.clone().into_event(&call(), coords()).unwrap();
        assert_eq!(e.base_amount, BigDecimal::from(5));
        assert_eq!(e.target_amount, BigDecimal::from(7));
        f.first_balances_transfer = Some(11);
        let e = f.into_event(&call(), coords()).unwrap();
        assert_eq!(e.base_amount, BigDecimal::from(11));
        assert_eq!(e.kind, LiquidityKind::Deposit);
        assert_eq!(
            e.extrinsic_hash.as_deref(),
            Some(&*format!("0x{}", "09".repeat(32)))
        );
    }
}
