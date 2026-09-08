//! VAL staking payouts — the Node's `sm.val_staking_rewards` mechanism
//! (`index.js`, the `xorFee.ValStakingRewardPaid` filter): every event
//! `(stash, dest, era, page, amount)` becomes one row. The event exists
//! since runtime 4.8.6; earlier blocks simply carry none.

use crate::decoder::{DecodeError, EventCoords};
use crate::runtime::sora;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use sorametrics_core::chain::{ss58_encode_sora, Address};
use sorametrics_core::sora_v2::V2ValStakingReward;
use subxt::events::EventDetails;
use subxt::SubstrateConfig;

/// Decode one `xorFee.ValStakingRewardPaid`, or `None` for any other event.
pub fn decode_val_staking_reward(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
    block_hash: &[u8; 32],
) -> Result<Option<V2ValStakingReward>, DecodeError> {
    if ev.pallet_name() != "XorFee" || ev.variant_name() != "ValStakingRewardPaid" {
        return Ok(None);
    }
    let paid = ev
        .as_event::<sora::xor_fee::events::ValStakingRewardPaid>()
        .map_err(|e| DecodeError::Subxt {
            pallet: "XorFee",
            variant: "ValStakingRewardPaid",
            source: Box::new(e.into()),
        })?
        .ok_or_else(|| DecodeError::Subxt {
            pallet: "XorFee",
            variant: "ValStakingRewardPaid",
            source: Box::new(subxt::Error::Other(
                "name match but as_event returned None — codegen / metadata drift".into(),
            )),
        })?;
    Ok(Some(V2ValStakingReward {
        block_height: coords.block_height,
        block_hash: format!("0x{}", hex::encode(block_hash)),
        era: paid.2,
        page: paid.3,
        validator_stash: Address::new(ss58_encode_sora(&paid.0 .0)),
        destination: Address::new(ss58_encode_sora(&paid.1 .0)),
        amount: BigDecimal::from(BigInt::from(paid.4)),
        timestamp: coords.block_timestamp,
    }))
}
