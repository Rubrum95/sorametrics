//! Per-block fee / burn aggregate — the Node's `fee_burns_indexer.js`:
//!
//! - `fees_paid_xor` = Σ `xorFee.FeeWithdrawn`, `ref_paid_xor` = Σ
//!   `xorFee.ReferrerRewarded` (both already decoded as fee events),
//! - `ref_redirected_xor = max(0, fees × ref/total − ref_paid)` and
//!   `remint_xor_burned = fees × xor/total` with the runtime weights by
//!   spec version (4.7.x 10/20/50/20 = 100; 4.8.2–4.8.4 10/20/50/5 = 85;
//!   ≥ 4.8.6 10/35/40/0 = 85),
//! - a remint is a drop of `xorFee.xorToVal` or `xorToBuyBack` above
//!   0.001 XOR vs the previous block; in that block the VAL / KUSD /
//!   TBCD `tokens.Withdrawn` amounts are the remint burns.
//!
//! The previous buckets are the Node's in-memory `prev`; here they are
//! read from storage at the PARENT block, so the detection is exact and
//! order-independent (backfills can run concurrently). The two extra
//! reads only happen in blocks that withdrew one of the remint assets.

use crate::runtime::sora;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use sorametrics_core::chain::BlockHeight;
use sorametrics_core::sora_v2::{FeeBurnKind, V2FeeBurn, V2FeeBurnAggregate};
use subxt::events::EventDetails;
use subxt::SubstrateConfig;

const VAL: &str = "0x0200040000000000000000000000000000000000000000000000000000000000";
const KUSD: &str = "0x02000c0000000000000000000000000000000000000000000000000000000000";
const TBCD: &str = "0x02000d0000000000000000000000000000000000000000000000000000000000";
/// Node `MIN_REMINT_DROP_XOR` = 0.001 XOR, in planck.
const MIN_REMINT_DROP_PLANCK: u128 = 1_000_000_000_000_000;

/// Fee distribution weights of a runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Weights {
    /// Referrer share.
    pub referrer: u32,
    /// XOR burnt directly.
    pub xor: u32,
    /// VAL bucket.
    pub val: u32,
    /// KUSD buy-back.
    pub kusd: u32,
    /// Sum of the shares.
    pub total: u32,
}

/// Node `refWeight()`.
pub const fn weights_for(spec_version: u32) -> Weights {
    if spec_version >= 128 {
        Weights {
            referrer: 10,
            xor: 35,
            val: 40,
            kusd: 0,
            total: 85,
        }
    } else if spec_version >= 120 {
        Weights {
            referrer: 10,
            xor: 20,
            val: 50,
            kusd: 5,
            total: 85,
        }
    } else {
        Weights {
            referrer: 10,
            xor: 20,
            val: 50,
            kusd: 20,
            total: 100,
        }
    }
}

fn human(planck: &BigDecimal) -> BigDecimal {
    planck / BigDecimal::new(BigInt::from(1), -18)
}

/// Raw `tokens.Withdrawn` amounts of the remint assets in the block.
#[derive(Debug, Default, Clone)]
pub struct WithdrawnByAsset {
    val: BigDecimal,
    kusd: BigDecimal,
    tbcd: BigDecimal,
}

impl WithdrawnByAsset {
    /// Record a `Tokens::Withdrawn` if it concerns VAL / KUSD / TBCD.
    pub fn observe(&mut self, ev: &EventDetails<SubstrateConfig>) -> Result<(), Box<subxt::Error>> {
        if ev.pallet_name() != "Tokens" || ev.variant_name() != "Withdrawn" {
            return Ok(());
        }
        let Some(w) = ev
            .as_event::<sora::tokens::events::Withdrawn>()
            .map_err(|e| Box::new(e.into()))?
        else {
            return Ok(());
        };
        let id = format!("0x{}", hex::encode(w.currency_id.code));
        let amount = BigDecimal::from(BigInt::from(w.amount));
        match id.as_str() {
            VAL => self.val += amount,
            KUSD => self.kusd += amount,
            TBCD => self.tbcd += amount,
            _ => {}
        }
        Ok(())
    }

    /// Whether any remint asset was withdrawn (the only case where the
    /// bucket comparison matters).
    pub fn is_empty(&self) -> bool {
        use bigdecimal::Zero;
        self.val.is_zero() && self.kusd.is_zero() && self.tbcd.is_zero()
    }
}

/// Node: `prev.xorToVal − xtv > 0.001 || prev.xorToBuyBack − xtb > 0.001`
/// (raw planck).
pub fn is_remint(prev: (u128, u128), now: (u128, u128)) -> bool {
    prev.0.saturating_sub(now.0) > MIN_REMINT_DROP_PLANCK
        || prev.1.saturating_sub(now.1) > MIN_REMINT_DROP_PLANCK
}

/// Build the block's aggregate row.
pub fn aggregate(
    coords_height: BlockHeight,
    ts_millis: i64,
    fee_events: &[V2FeeBurn],
    withdrawn: &WithdrawnByAsset,
    weights: Weights,
    remint: bool,
) -> V2FeeBurnAggregate {
    let mut fees = BigDecimal::from(0);
    let mut ref_paid = BigDecimal::from(0);
    for f in fee_events {
        match f.kind {
            FeeBurnKind::FeeWithdrawn => fees += human(&f.amount),
            FeeBurnKind::ReferrerRewarded => ref_paid += human(&f.amount),
        }
    }
    let total = BigDecimal::from(weights.total);
    let ref_ideal = &fees * BigDecimal::from(weights.referrer) / &total;
    let redirected = &ref_ideal - &ref_paid;
    let zero = BigDecimal::from(0);
    let ref_redirected = if redirected > zero {
        redirected
    } else {
        zero.clone()
    };
    let remint_xor = &fees * BigDecimal::from(weights.xor) / &total;
    let (val, kusd, tbcd) = if remint {
        (
            human(&withdrawn.val),
            human(&withdrawn.kusd),
            human(&withdrawn.tbcd),
        )
    } else {
        (zero.clone(), zero.clone(), zero)
    };
    V2FeeBurnAggregate {
        block_height: coords_height,
        ts_millis,
        fees_paid_xor: fees,
        ref_paid_xor: ref_paid,
        ref_redirected_xor: ref_redirected,
        remint_xor_burned: remint_xor,
        remint_val_burned: val,
        remint_kusd_burned: kusd,
        remint_tbcd_burned: tbcd,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sorametrics_core::chain::Address;
    use sorametrics_core::time::Timestamp;
    use std::str::FromStr;

    fn fee(kind: FeeBurnKind, planck: u128) -> V2FeeBurn {
        V2FeeBurn {
            block_height: BlockHeight(1),
            extrinsic_id: 0,
            event_id: 0,
            extrinsic_hash: None,
            kind,
            payer: Address::new("a".to_string()),
            referrer: None,
            amount: BigDecimal::from(BigInt::from(planck)),
            timestamp: Timestamp::now(),
        }
    }

    #[test]
    fn weights_follow_spec_version() {
        assert_eq!(weights_for(130).xor, 35);
        assert_eq!(weights_for(124).kusd, 5);
        assert_eq!(weights_for(100).total, 100);
    }

    #[test]
    fn aggregate_matches_indexer_formulas() {
        // 0.1 XOR of fees, 0.005 XOR paid to a referrer, spec 130.
        let fees = vec![
            fee(FeeBurnKind::FeeWithdrawn, 100_000_000_000_000_000),
            fee(FeeBurnKind::ReferrerRewarded, 5_000_000_000_000_000),
        ];
        let w = WithdrawnByAsset {
            val: BigDecimal::from(BigInt::from(2_000_000_000_000_000_000u128)),
            ..Default::default()
        };
        let row = aggregate(BlockHeight(7), 1000, &fees, &w, weights_for(130), true);
        assert_eq!(row.fees_paid_xor, BigDecimal::from_str("0.1").unwrap());
        assert_eq!(row.ref_paid_xor, BigDecimal::from_str("0.005").unwrap());
        // ideal referrer = 0.1 × 10/85 = 0.011764705882…; redirected = ideal − 0.005
        let ideal =
            BigDecimal::from_str("0.1").unwrap() * BigDecimal::from(10) / BigDecimal::from(85);
        assert_eq!(
            row.ref_redirected_xor,
            ideal - BigDecimal::from_str("0.005").unwrap()
        );
        assert_eq!(
            row.remint_xor_burned,
            BigDecimal::from_str("0.1").unwrap() * BigDecimal::from(35) / BigDecimal::from(85)
        );
        assert_eq!(row.remint_val_burned, BigDecimal::from(2));
        assert!(row.has_activity());
        let quiet = aggregate(BlockHeight(8), 1000, &[], &w, weights_for(130), false);
        assert!(!quiet.has_activity());
    }

    #[test]
    fn remint_is_a_bucket_drop_above_the_threshold() {
        let big = 5_000_000_000_000_000_000u128;
        let small = 1_000_000_000_000_000_000u128;
        assert!(is_remint((big, 0), (small, 0)));
        assert!(is_remint((0, big), (0, small)));
        assert!(!is_remint((small, 0), (big, 0)));
        assert!(!is_remint((small + 10, 0), (small, 0)));
    }
}
