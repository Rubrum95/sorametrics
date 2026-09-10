//! Polkamarkt events — the Node's `polkamarkt` handler block in
//! `index.js`: every event of the pallet that touches the replica is
//! decoded into a [`PmChange`]; `MarketCreated` is then hydrated from
//! `Markets` / `Conditions` storage at the block (creator, condition,
//! close block, collateral, mechanism, status, question, oracle,
//! resolution source), best-effort like the Node (`Unknown` / empty
//! when storage cannot be read).

use crate::decoder::{DecodeError, EventCoords};
use crate::runtime::sora;
use crate::runtime::sora::runtime_types::pallet_polkamarkt::{
    BinaryOutcome, MarketMechanism, MarketStatus, TradeSide,
};
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use sorametrics_core::chain::ss58_encode_sora;
use sorametrics_core::sora_v2::{PmChange, PmMarket, V2PolkamarktEvent};
use subxt::events::EventDetails;
use subxt::{OnlineClient, SubstrateConfig};

fn raw(v: u128) -> BigDecimal {
    BigDecimal::from(BigInt::from(v))
}

/// `MarketStatus` → the Node's text.
pub const fn status_label(s: &MarketStatus) -> &'static str {
    match s {
        MarketStatus::Open => "Open",
        MarketStatus::Locked => "Locked",
        MarketStatus::Resolved => "Resolved",
        MarketStatus::Cancelled => "Cancelled",
    }
}

/// `MarketMechanism` → the Node's text.
pub const fn mechanism_label(m: &MarketMechanism) -> &'static str {
    match m {
        MarketMechanism::LegacyAmm => "LegacyAmm",
        MarketMechanism::OrderBook => "OrderBook",
        MarketMechanism::DynamicPariMutuel => "DynamicPariMutuel",
        MarketMechanism::MigratedLegacy => "MigratedLegacy",
    }
}

/// `BinaryOutcome` → `Yes | No`.
pub const fn outcome_label(o: &BinaryOutcome) -> &'static str {
    match o {
        BinaryOutcome::Yes => "Yes",
        BinaryOutcome::No => "No",
    }
}

const fn side_label(s: &TradeSide) -> &'static str {
    match s {
        TradeSide::Buy => "Buy",
        TradeSide::Sell => "Sell",
    }
}

fn err(variant: &'static str, e: subxt::Error) -> DecodeError {
    DecodeError::Subxt {
        pallet: "Polkamarkt",
        variant,
        source: Box::new(e),
    }
}

fn get<T: subxt::events::StaticEvent>(
    ev: &EventDetails<SubstrateConfig>,
    variant: &'static str,
) -> Result<T, DecodeError> {
    ev.as_event::<T>()
        .map_err(|e| err(variant, e.into()))?
        .ok_or_else(|| {
            err(
                variant,
                subxt::Error::Other(
                    "name match but as_event returned None — codegen / metadata drift".into(),
                ),
            )
        })
}

/// Decode one `polkamarkt` event, or `None` for pallets / variants the
/// replica ignores.
pub fn decode_polkamarkt(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2PolkamarktEvent>, DecodeError> {
    if ev.pallet_name() != "Polkamarkt" {
        return Ok(None);
    }
    use sora::polkamarkt::events as e;
    let change = match ev.variant_name() {
        "MarketCreated" => {
            let x: e::MarketCreated = get(ev, "MarketCreated")?;
            PmChange::MarketCreated {
                market_id: x.market_id,
                seed_liquidity: raw(x.seed_liquidity),
            }
        }
        "TradeExecuted" => {
            let x: e::TradeExecuted = get(ev, "TradeExecuted")?;
            PmChange::Trade {
                market_id: x.market_id,
                trader: ss58_encode_sora(&x.trader.0),
                side: side_label(&x.side).into(),
                outcome: outcome_label(&x.outcome).into(),
                collateral: raw(x.collateral_amount),
                shares: raw(x.share_amount),
                fee: raw(x.fee_amount),
            }
        }
        "MarketLocked" => {
            let x: e::MarketLocked = get(ev, "MarketLocked")?;
            PmChange::Status {
                market_id: x.market_id,
                status: "Locked".into(),
                resolution: None,
            }
        }
        "MarketResolved" => {
            let x: e::MarketResolved = get(ev, "MarketResolved")?;
            PmChange::Status {
                market_id: x.market_id,
                status: "Resolved".into(),
                resolution: Some(outcome_label(&x.outcome).into()),
            }
        }
        "MarketCancelled" => {
            let x: e::MarketCancelled = get(ev, "MarketCancelled")?;
            PmChange::Status {
                market_id: x.market_id,
                status: "Cancelled".into(),
                resolution: None,
            }
        }
        "MarketEmergencyCancelled" => {
            let x: e::MarketEmergencyCancelled = get(ev, "MarketEmergencyCancelled")?;
            PmChange::Status {
                market_id: x.market_id,
                status: "Cancelled".into(),
                resolution: None,
            }
        }
        "LegacyMarketMigrated" => {
            let x: e::LegacyMarketMigrated = get(ev, "LegacyMarketMigrated")?;
            PmChange::LegacyMigrated {
                market_id: x.market_id,
                status: status_label(&x.status).into(),
            }
        }
        "MarketClaimed" => {
            let x: e::MarketClaimed = get(ev, "MarketClaimed")?;
            PmChange::Claim {
                market_id: x.market_id,
                account: ss58_encode_sora(&x.trader.0),
                kind: "payout".into(),
                amount: raw(x.payout),
            }
        }
        "CreatorFeesClaimed" => {
            let x: e::CreatorFeesClaimed = get(ev, "CreatorFeesClaimed")?;
            PmChange::Claim {
                market_id: x.market_id,
                account: ss58_encode_sora(&x.creator.0),
                kind: "creator_fees".into(),
                amount: raw(x.amount),
            }
        }
        "DpmResidualBurned" => {
            let x: e::DpmResidualBurned = get(ev, "DpmResidualBurned")?;
            PmChange::Burn {
                market_id: Some(x.market_id),
                kind: "dpm_residual".into(),
                amount: raw(x.amount),
            }
        }
        "LegacyMigrationResidualRouted" => {
            let x: e::LegacyMigrationResidualRouted = get(ev, "LegacyMigrationResidualRouted")?;
            PmChange::Burn {
                market_id: None,
                kind: "legacy_migration_residual".into(),
                amount: raw(x.amount),
            }
        }
        "XorBuybackSwept" => {
            let x: e::XorBuybackSwept = get(ev, "XorBuybackSwept")?;
            PmChange::Buyback {
                kusd_spent: raw(x.collateral_amount),
                xor_burned: raw(x.xor_burned),
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(V2PolkamarktEvent {
        block_height: coords.block_height,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        ts_millis: coords.block_timestamp.0.timestamp_millis(),
        change,
    }))
}

fn utf8(bytes: &[u8]) -> Option<String> {
    let s = String::from_utf8_lossy(bytes).to_string();
    (!s.is_empty()).then_some(s)
}

/// Node `MarketCreated` hydration: `Markets[id]` + `Conditions[condition_id]`
/// at `block_hash`; storage misses leave the Node's defaults.
pub async fn hydrate_market(
    client: &OnlineClient<SubstrateConfig>,
    block_hash: subxt::utils::H256,
    market_id: u32,
    seed_liquidity: BigDecimal,
    block_height: sorametrics_core::chain::BlockHeight,
    ts_millis: i64,
) -> Result<PmMarket, subxt::Error> {
    let at = client.storage().at(block_hash);
    let s = sora::storage().polkamarkt();
    let mut m = PmMarket {
        market_id,
        condition_id: 0,
        creator: "Unknown".into(),
        close_block: 0,
        collateral_asset: String::new(),
        seed_liquidity,
        status: "Open".into(),
        question: None,
        oracle: None,
        resolution_source: None,
        mechanism: None,
        block_height,
        ts_millis,
    };
    if let Some(market) = at.fetch(&s.markets(market_id)).await? {
        m.creator = ss58_encode_sora(&market.creator.0);
        m.condition_id = market.condition_id;
        m.close_block = market.close_block;
        m.collateral_asset = format!("0x{}", hex::encode(market.collateral_asset.code));
        m.mechanism = Some(mechanism_label(&market.mechanism).into());
        m.status = status_label(&market.status).into();
        if let Some(c) = at.fetch(&s.conditions(market.condition_id)).await? {
            m.question = utf8(&c.question.0);
            m.oracle = utf8(&c.oracle.0);
            m.resolution_source = utf8(&c.resolution_source.0);
        }
    }
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_match_the_node_strings() {
        assert_eq!(status_label(&MarketStatus::Cancelled), "Cancelled");
        assert_eq!(
            mechanism_label(&MarketMechanism::DynamicPariMutuel),
            "DynamicPariMutuel"
        );
        assert_eq!(outcome_label(&BinaryOutcome::No), "No");
        assert_eq!(side_label(&TradeSide::Sell), "Sell");
        assert_eq!(utf8(b""), None);
        assert_eq!(utf8(b"Will X?").as_deref(), Some("Will X?"));
    }
}
