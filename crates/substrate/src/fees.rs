//! Per-extrinsic network fees — the Node's `live_fees` mechanism
//! (`index.js` "FEES TRACKING"): for each extrinsic that emitted
//! `TransactionPayment::TransactionFeePaid { who, actual_fee, tip }`,
//! one row with a category decided by the extrinsic's own events:
//! `Swap` if a `LiquidityProxy::Exchange` was emitted, else `Bridge` if
//! any `EthBridge` / `Bridge` / `Multisig` event, else `Transfer` if an
//! `Assets` or `Balances` event whose name contains `Transfer`, else
//! `Other`. `amount` is the raw `actual_fee` (the Node stored `/1e18`,
//! the DB layer converts); USD is amount × XOR price at the block.

use crate::decoder::{DecodeError, EventCoords};
use crate::runtime::sora;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use sorametrics_core::chain::{ss58_encode_sora, Address};
use sorametrics_core::sora_v2::{FeeType, V2Fee};
use subxt::events::EventDetails;
use subxt::SubstrateConfig;

/// What one extrinsic's events tell us, accumulated event by event.
#[derive(Debug, Default, Clone)]
pub struct ExtrinsicFeeFacts {
    has_swap: bool,
    has_bridge: bool,
    has_transfer: bool,
    /// `(payer, actual_fee, coords of the fee event)`.
    fee: Option<([u8; 32], u128, EventCoords)>,
}

impl ExtrinsicFeeFacts {
    /// Record one event of the extrinsic.
    pub fn observe(
        &mut self,
        ev: &EventDetails<SubstrateConfig>,
        coords: EventCoords,
    ) -> Result<(), DecodeError> {
        let (pallet, variant) = (ev.pallet_name(), ev.variant_name());
        match (pallet, variant) {
            ("LiquidityProxy", "Exchange") => self.has_swap = true,
            ("EthBridge", _) | ("Bridge", _) | ("Multisig", _) => self.has_bridge = true,
            ("Assets", v) | ("Balances", v) if v.contains("Transfer") => self.has_transfer = true,
            ("TransactionPayment", "TransactionFeePaid") => {
                let paid = ev
                    .as_event::<sora::transaction_payment::events::TransactionFeePaid>()
                    .map_err(|e| DecodeError::Subxt {
                        pallet: "TransactionPayment",
                        variant: "TransactionFeePaid",
                        source: Box::new(e.into()),
                    })?;
                if let Some(p) = paid {
                    self.fee = Some((p.who.0, p.actual_fee, coords));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Legacy category from the observed events.
    pub fn fee_type(&self) -> FeeType {
        if self.has_swap {
            FeeType::Swap
        } else if self.has_bridge {
            FeeType::Bridge
        } else if self.has_transfer {
            FeeType::Transfer
        } else {
            FeeType::Other
        }
    }

    /// The fee row, if the extrinsic paid a non-zero fee (the legacy
    /// `mv_fees` keeps `network_fee != '0'` only; a zero-fee row carries
    /// no information for fee analytics).
    pub fn into_fee(self) -> Option<V2Fee> {
        let fee_type = self.fee_type();
        let (payer, amount, coords) = self.fee?;
        if amount == 0 {
            return None;
        }
        Some(V2Fee {
            block_height: coords.block_height,
            extrinsic_id: coords.extrinsic_id,
            fee_type,
            payer: Address::new(ss58_encode_sora(&payer)),
            amount: BigDecimal::from(BigInt::from(amount)),
            usd_value: None,
            timestamp: coords.block_timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_precedence_is_swap_bridge_transfer_other() {
        let mut f = ExtrinsicFeeFacts::default();
        assert_eq!(f.fee_type(), FeeType::Other);
        f.has_transfer = true;
        assert_eq!(f.fee_type(), FeeType::Transfer);
        f.has_bridge = true;
        assert_eq!(f.fee_type(), FeeType::Bridge);
        f.has_swap = true;
        assert_eq!(f.fee_type(), FeeType::Swap);
    }

    #[test]
    fn zero_fee_means_no_row() {
        let coords = EventCoords {
            block_height: sorametrics_core::chain::BlockHeight(1),
            block_timestamp: sorametrics_core::time::Timestamp::now(),
            extrinsic_id: 0,
            event_id: 0,
            extrinsic_hash: None,
        };
        let f = ExtrinsicFeeFacts {
            fee: Some(([1u8; 32], 0, coords)),
            ..Default::default()
        };
        assert!(f.into_fee().is_none());
        let f = ExtrinsicFeeFacts {
            fee: Some(([1u8; 32], 5, coords)),
            ..Default::default()
        };
        assert_eq!(f.into_fee().unwrap().amount, BigDecimal::from(5));
    }

    #[test]
    fn no_fee_event_means_no_row() {
        let f = ExtrinsicFeeFacts {
            has_swap: true,
            ..Default::default()
        };
        assert!(f.into_fee().is_none());
    }
}
