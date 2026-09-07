//! Order book events — the Node's `live_order_book_events` mechanism
//! (`index.js`, the `orderBook` event loop):
//!
//! Every `orderBook` event of the five kinds below becomes one row.
//! `LimitOrderPlaced` / `LimitOrderExecuted` carry side, price and
//! amount; `MarketOrderExecuted` carries direction, amount and average
//! price (no order id); `LimitOrderCanceled` / `LimitOrderFilled` only
//! the order id and owner. Other `orderBook` events (book created /
//! updated, status changes, expiration failures) produce nothing.
//!
//! Prices and amounts are `BalanceUnit` fixed-point values with 18
//! decimals; the Node stored `inner / 1e18` and so do we. Where the
//! chain reports an amount as `OrderAmount::{Base, Quote}` (executed
//! and market orders) the Node's `parseOrderBookValue` could not read
//! the enum and wrote `''`; we take the inner value and remember which
//! leg it is, so `usd_value` can be computed from the quote-asset price
//! (the `mv_order_book_events` formula).

use crate::decoder::{DecodeError, EventCoords};
use crate::runtime::sora;
use crate::runtime::sora::runtime_types::common::balance_unit::BalanceUnit;
use crate::runtime::sora::runtime_types::common::primitives::PriceVariant;
use crate::runtime::sora::runtime_types::order_book::types::OrderAmount;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use sorametrics_core::chain::{ss58_encode_sora, Address, AssetId};
use sorametrics_core::sora_v2::{OrderBookEventType, OrderSide, V2OrderBookEvent};
use subxt::events::EventDetails;
use subxt::SubstrateConfig;

/// Decimals of every `BalanceUnit` (`common::fixed::FixedInner`).
const BALANCE_UNIT_DECIMALS: i64 = 18;

/// `inner / 1e18` as an exact decimal.
pub fn balance_unit_value(unit: &BalanceUnit) -> BigDecimal {
    BigDecimal::new(BigInt::from(unit.inner), BALANCE_UNIT_DECIMALS)
}

fn hex32(b: &[u8; 32]) -> String {
    format!("0x{}", hex::encode(b))
}

fn side_of(v: &PriceVariant) -> OrderSide {
    match v {
        PriceVariant::Buy => OrderSide::Buy,
        PriceVariant::Sell => OrderSide::Sell,
    }
}

/// An amount as the chain reports it: in base or in quote units.
#[derive(Clone, Debug, PartialEq)]
pub enum Leg {
    /// Amount denominated in the base asset.
    Base(BigDecimal),
    /// Amount denominated in the quote asset.
    Quote(BigDecimal),
}

fn leg_of(a: &OrderAmount) -> Leg {
    match a {
        OrderAmount::Base(u) => Leg::Base(balance_unit_value(u)),
        OrderAmount::Quote(u) => Leg::Quote(balance_unit_value(u)),
    }
}

/// The `amount` column and the quote-terms amount that prices the row:
/// `amount × price` for base amounts, the amount itself for quote ones.
pub fn amount_and_quote(leg: &Leg, price: &BigDecimal) -> (BigDecimal, BigDecimal) {
    match leg {
        Leg::Base(a) => (a.clone(), a * price),
        Leg::Quote(q) => (q.clone(), q.clone()),
    }
}

struct Decoded {
    event_type: OrderBookEventType,
    owner: [u8; 32],
    order_id: Option<u128>,
    base: [u8; 32],
    quote: [u8; 32],
    side: Option<OrderSide>,
    price: Option<BigDecimal>,
    amount: Option<Leg>,
}

fn decode_err(variant: &'static str, e: subxt::Error) -> DecodeError {
    DecodeError::Subxt {
        pallet: "OrderBook",
        variant,
        source: Box::new(e),
    }
}

fn missing(variant: &'static str) -> DecodeError {
    decode_err(
        variant,
        subxt::Error::Other(
            "name match but as_event returned None — codegen / metadata drift".into(),
        ),
    )
}

fn decode_named(ev: &EventDetails<SubstrateConfig>) -> Result<Option<Decoded>, DecodeError> {
    use sora::order_book::events as e;
    let d = match ev.variant_name() {
        "LimitOrderPlaced" => {
            let x = ev
                .as_event::<e::LimitOrderPlaced>()
                .map_err(|err| decode_err("LimitOrderPlaced", err.into()))?
                .ok_or_else(|| missing("LimitOrderPlaced"))?;
            Decoded {
                event_type: OrderBookEventType::Placed,
                owner: x.owner_id.0,
                order_id: Some(x.order_id),
                base: x.order_book_id.base.code,
                quote: x.order_book_id.quote.code,
                side: Some(side_of(&x.side)),
                price: Some(balance_unit_value(&x.price)),
                amount: Some(Leg::Base(balance_unit_value(&x.amount))),
            }
        }
        "LimitOrderCanceled" => {
            let x = ev
                .as_event::<e::LimitOrderCanceled>()
                .map_err(|err| decode_err("LimitOrderCanceled", err.into()))?
                .ok_or_else(|| missing("LimitOrderCanceled"))?;
            Decoded {
                event_type: OrderBookEventType::Canceled,
                owner: x.owner_id.0,
                order_id: Some(x.order_id),
                base: x.order_book_id.base.code,
                quote: x.order_book_id.quote.code,
                side: None,
                price: None,
                amount: None,
            }
        }
        "LimitOrderExecuted" => {
            let x = ev
                .as_event::<e::LimitOrderExecuted>()
                .map_err(|err| decode_err("LimitOrderExecuted", err.into()))?
                .ok_or_else(|| missing("LimitOrderExecuted"))?;
            Decoded {
                event_type: OrderBookEventType::Executed,
                owner: x.owner_id.0,
                order_id: Some(x.order_id),
                base: x.order_book_id.base.code,
                quote: x.order_book_id.quote.code,
                side: Some(side_of(&x.side)),
                price: Some(balance_unit_value(&x.price)),
                amount: Some(leg_of(&x.amount)),
            }
        }
        "LimitOrderFilled" => {
            let x = ev
                .as_event::<e::LimitOrderFilled>()
                .map_err(|err| decode_err("LimitOrderFilled", err.into()))?
                .ok_or_else(|| missing("LimitOrderFilled"))?;
            Decoded {
                event_type: OrderBookEventType::Filled,
                owner: x.owner_id.0,
                order_id: Some(x.order_id),
                base: x.order_book_id.base.code,
                quote: x.order_book_id.quote.code,
                side: None,
                price: None,
                amount: None,
            }
        }
        "MarketOrderExecuted" => {
            let x = ev
                .as_event::<e::MarketOrderExecuted>()
                .map_err(|err| decode_err("MarketOrderExecuted", err.into()))?
                .ok_or_else(|| missing("MarketOrderExecuted"))?;
            Decoded {
                event_type: OrderBookEventType::Market,
                owner: x.owner_id.0,
                order_id: None,
                base: x.order_book_id.base.code,
                quote: x.order_book_id.quote.code,
                side: Some(side_of(&x.direction)),
                price: Some(balance_unit_value(&x.average_price)),
                amount: Some(leg_of(&x.amount)),
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(d))
}

/// Decode one `orderBook` event into a row (`usd_value` unset), or
/// `None` when the event is not one of the five indexed kinds.
pub fn decode_order_book(
    ev: &EventDetails<SubstrateConfig>,
    coords: EventCoords,
) -> Result<Option<V2OrderBookEvent>, DecodeError> {
    if ev.pallet_name() != "OrderBook" {
        return Ok(None);
    }
    let Some(d) = decode_named(ev)? else {
        return Ok(None);
    };
    let (amount, quote_amount) = match (&d.amount, &d.price) {
        (Some(leg), Some(price)) => {
            let (a, q) = amount_and_quote(leg, price);
            (Some(a), Some(q))
        }
        _ => (None, None),
    };
    Ok(Some(V2OrderBookEvent {
        block_height: coords.block_height,
        extrinsic_id: coords.extrinsic_id,
        event_id: coords.event_id,
        extrinsic_hash: coords.extrinsic_hash_hex(),
        event_type: d.event_type,
        wallet: Address::new(ss58_encode_sora(&d.owner)),
        order_id: d.order_id,
        base_asset: AssetId::new(hex32(&d.base)),
        quote_asset: AssetId::new(hex32(&d.quote)),
        side: d.side,
        price: d.price,
        amount,
        quote_amount,
        usd_value: None,
        timestamp: coords.block_timestamp,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn unit(inner: u128) -> BalanceUnit {
        BalanceUnit {
            inner,
            is_divisible: true,
        }
    }

    #[test]
    fn balance_unit_is_inner_over_1e18() {
        // The real KXOR/XOR placed order: price 0.001, amount 100 000 000.
        assert_eq!(
            balance_unit_value(&unit(1_000_000_000_000_000)),
            BigDecimal::from_str("0.001").unwrap()
        );
        let amount = u128::from_str_radix("52b7d2dcc80cd2e4000000", 16).unwrap();
        assert_eq!(
            balance_unit_value(&unit(amount)),
            BigDecimal::from_str("100000000").unwrap()
        );
    }

    #[test]
    fn quote_terms_follow_the_leg() {
        let price = BigDecimal::from_str("0.001").unwrap();
        let (a, q) = amount_and_quote(&Leg::Base(BigDecimal::from(100_000_000)), &price);
        assert_eq!(a, BigDecimal::from(100_000_000));
        assert_eq!(q, BigDecimal::from_str("100000.000").unwrap());
        let (a, q) = amount_and_quote(&Leg::Quote(BigDecimal::from(5)), &price);
        assert_eq!(a, BigDecimal::from(5));
        assert_eq!(q, BigDecimal::from(5));
    }

    #[test]
    fn labels_are_the_live_vocabulary() {
        assert_eq!(OrderBookEventType::Placed.label(), "placed");
        assert_eq!(OrderBookEventType::Market.label(), "market");
        assert_eq!(side_of(&PriceVariant::Buy).label(), "buy");
        assert_eq!(side_of(&PriceVariant::Sell).label(), "sell");
    }
}
