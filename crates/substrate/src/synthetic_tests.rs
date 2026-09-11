//! Decoder tests on SCALE-encoded synthetic events: a `Vec<EventRecord>`
//! built from the generated runtime types is encoded, decoded back with
//! the pinned metadata into `EventDetails`, and fed to the decoders. No
//! network, no database.

use crate::decoder::{decode_fee_burn, decode_swap, decode_transfer, EventCoords};
use crate::runtime::sora::runtime_types::{
    common::outcome_fee::OutcomeFee, common::primitives::AssetId32, frame_system::EventRecord,
    frame_system::Phase, framenode_runtime::RuntimeEvent,
};
use sorametrics_core::chain::{ss58_encode_sora, BlockHeight};
use sorametrics_core::time::Timestamp;
use std::str::FromStr;
use subxt::events::{EventDetails, Events};
use subxt::ext::codec::{Decode, Encode};
use subxt::utils::{AccountId32, H256};
use subxt::{Metadata, SubstrateConfig};

fn metadata() -> Metadata {
    let bytes: &[u8] = include_bytes!("../metadata/sora-mainnet.scale");
    Metadata::decode(&mut &bytes[..]).expect("pinned metadata decodes")
}

fn events_from(
    records: Vec<EventRecord<RuntimeEvent, H256>>,
) -> Vec<EventDetails<SubstrateConfig>> {
    let bytes = records.encode();
    Events::<SubstrateConfig>::decode_from(bytes, metadata())
        .iter()
        .map(|e| e.expect("synthetic event decodes"))
        .collect()
}

fn record(event: RuntimeEvent) -> EventRecord<RuntimeEvent, H256> {
    EventRecord {
        phase: Phase::ApplyExtrinsic(2),
        event,
        topics: Vec::new(),
    }
}

fn coords() -> EventCoords {
    EventCoords {
        block_height: BlockHeight(27_600_000),
        block_timestamp: Timestamp::new(
            chrono::DateTime::from_timestamp(1_780_000_000, 0).unwrap(),
        ),
        extrinsic_id: 2,
        event_id: 5,
        extrinsic_hash: Some([0xab; 32]),
    }
}

fn asset(
    byte: u8,
) -> AssetId32<
    crate::runtime::sora::runtime_types::common::primitives::_allowed_deprecated::PredefinedAssetId,
> {
    let mut code = [0u8; 32];
    code[0] = 0x02;
    code[1] = byte;
    AssetId32 {
        code,
        __ignore: Default::default(),
    }
}

const ALICE: [u8; 32] = [0x11; 32];
const BOB: [u8; 32] = [0x22; 32];

#[test]
fn swap_exchange_round_trips() {
    use crate::runtime::sora::runtime_types::liquidity_proxy::pallet::Event as LpEvent;
    let ev = RuntimeEvent::LiquidityProxy(LpEvent::Exchange(
        AccountId32(ALICE),
        0,
        asset(0x00),
        asset(0x04),
        1_500_000_000_000_000_000u128,
        12_345_678_901_234_567_890u128,
        OutcomeFee(Vec::new()),
        Vec::new(),
    ));
    let events = events_from(vec![record(ev)]);
    assert_eq!(events.len(), 1);
    let swap = decode_swap(&events[0], coords())
        .unwrap()
        .expect("swap decoded");
    assert_eq!(swap.caller.as_str(), ss58_encode_sora(&ALICE));
    assert_eq!(
        swap.input_asset.as_str(),
        "0x0200000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(
        swap.output_asset.as_str(),
        "0x0204000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(swap.input_amount.to_string(), "1500000000000000000");
    assert_eq!(swap.output_amount.to_string(), "12345678901234567890");
    assert_eq!(swap.extrinsic_id, 2);
    assert_eq!(swap.event_id, 5);
    assert_eq!(
        swap.extrinsic_hash.as_deref(),
        Some(&format!("0x{}", "ab".repeat(32))[..])
    );
    assert!(decode_transfer(&events[0], coords()).unwrap().is_none());
    assert!(decode_fee_burn(&events[0], coords()).unwrap().is_none());
}

#[test]
fn balances_and_tokens_transfers() {
    use crate::runtime::sora::runtime_types::orml_tokens::module::Event as TokEvent;
    use crate::runtime::sora::runtime_types::pallet_balances::pallet::Event as BalEvent;
    let events = events_from(vec![
        record(RuntimeEvent::Balances(BalEvent::Transfer {
            from: AccountId32(ALICE),
            to: AccountId32(BOB),
            amount: 5_000_000_000_000_000_000u128,
        })),
        record(RuntimeEvent::Tokens(TokEvent::Transfer {
            currency_id: asset(0x0c),
            from: AccountId32(BOB),
            to: AccountId32(ALICE),
            amount: 7u128,
        })),
    ]);
    assert_eq!(events.len(), 2);
    let xor = decode_transfer(&events[0], coords())
        .unwrap()
        .expect("balances transfer");
    assert_eq!(xor.from.as_str(), ss58_encode_sora(&ALICE));
    assert_eq!(xor.to.as_str(), ss58_encode_sora(&BOB));
    assert_eq!(
        xor.asset.as_str(),
        "0x0200000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(xor.amount.to_string(), "5000000000000000000");
    let tok = decode_transfer(&events[1], coords())
        .unwrap()
        .expect("tokens transfer");
    assert_eq!(
        tok.asset.as_str(),
        "0x020c000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(tok.amount.to_string(), "7");
    assert!(decode_swap(&events[1], coords()).unwrap().is_none());
}

#[test]
fn technical_account_transfers_are_skipped() {
    use crate::runtime::sora::runtime_types::pallet_balances::pallet::Event as BalEvent;
    let mut tech = [0x33u8; 32];
    tech[..16].copy_from_slice(&sorametrics_core::chain::TECH_ACCOUNT_MAGIC_PREFIX);
    let events = events_from(vec![record(RuntimeEvent::Balances(BalEvent::Transfer {
        from: AccountId32(tech),
        to: AccountId32(BOB),
        amount: 1,
    }))]);
    assert!(decode_transfer(&events[0], coords()).unwrap().is_none());
}

#[test]
fn xor_fee_events() {
    use crate::runtime::sora::runtime_types::xor_fee::pallet::Event as FeeEvent;
    let events = events_from(vec![
        record(RuntimeEvent::XorFee(FeeEvent::FeeWithdrawn(
            AccountId32(ALICE),
            700_000_000_000_000u128,
        ))),
        record(RuntimeEvent::XorFee(FeeEvent::ReferrerRewarded(
            AccountId32(ALICE),
            AccountId32(BOB),
            70_000_000_000_000u128,
        ))),
    ]);
    let w = decode_fee_burn(&events[0], coords())
        .unwrap()
        .expect("fee withdrawn");
    assert_eq!(w.amount.to_string(), "700000000000000");
    assert!(w.referrer.is_none());
    let r = decode_fee_burn(&events[1], coords())
        .unwrap()
        .expect("referrer rewarded");
    assert_eq!(
        r.referrer.as_ref().map(|a| a.as_str().to_string()),
        Some(ss58_encode_sora(&BOB))
    );
    assert_eq!(r.amount.to_string(), "70000000000000");
}

#[test]
fn substrate_bridge_burned_and_jetton_minted() {
    use crate::decoder::decode_bridge;
    use crate::runtime::sora::runtime_types::bridge_types::ton::TonAddress;
    use crate::runtime::sora::runtime_types::bridge_types::{GenericAccount, SubNetworkId};
    use crate::runtime::sora::runtime_types::jetton_app::pallet::Event as JettonEvent;
    use crate::runtime::sora::runtime_types::substrate_bridge_app::pallet::Event as SubEvent;
    use sorametrics_core::sora_v2::BridgeDirection;
    let events = events_from(vec![
        record(RuntimeEvent::SubstrateBridgeApp(SubEvent::Burned {
            network_id: SubNetworkId::Kusama,
            asset_id: asset(0x00),
            sender: AccountId32(ALICE),
            recipient: GenericAccount::Sora(AccountId32(BOB)),
            amount: 42_000_000_000_000_000_000u128,
        })),
        record(RuntimeEvent::JettonApp(JettonEvent::Minted {
            asset_id: asset(0x04),
            sender: TonAddress {
                workchain: 0,
                address: H256([0x5a; 32]),
            },
            recipient: AccountId32(ALICE),
            amount: 9u128,
        })),
    ]);
    let out = decode_bridge(&events[0], coords())
        .unwrap()
        .expect("burned decoded");
    assert!(matches!(out.direction, BridgeDirection::Out));
    assert_eq!(out.network, "Substrate: Kusama");
    assert_eq!(out.caller.as_str(), ss58_encode_sora(&ALICE));
    assert_eq!(
        out.counterparty.as_deref(),
        Some(ss58_encode_sora(&BOB).as_str())
    );
    assert_eq!(out.amount.to_string(), "42000000000000000000");
    let inc = decode_bridge(&events[1], coords())
        .unwrap()
        .expect("minted decoded");
    assert!(matches!(inc.direction, BridgeDirection::In));
    assert_eq!(inc.network, "TON");
    assert_eq!(inc.caller.as_str(), ss58_encode_sora(&ALICE));
    assert_eq!(
        inc.counterparty.as_deref(),
        Some(format!("0:{}", "5a".repeat(32)).as_str())
    );
    assert_eq!(inc.amount.to_string(), "9");
    assert!(decode_swap(&events[0], coords()).unwrap().is_none());
}

#[test]
fn order_book_placed_and_market() {
    use crate::order_book::decode_order_book;
    use crate::runtime::sora::order_book::calls::types::place_limit_order::OrderBookId;
    use crate::runtime::sora::runtime_types::common::balance_unit::BalanceUnit;
    use crate::runtime::sora::runtime_types::common::primitives::PriceVariant;
    use crate::runtime::sora::runtime_types::order_book::pallet::Event as ObEvent;
    use crate::runtime::sora::runtime_types::order_book::types::OrderAmount;
    use sorametrics_core::sora_v2::{OrderBookEventType, OrderSide};
    let book = || OrderBookId {
        dex_id: 0,
        base: asset(0x0c),
        quote: asset(0x00),
    };
    let unit = |inner: u128| BalanceUnit {
        inner,
        is_divisible: true,
    };
    let events = events_from(vec![
        record(RuntimeEvent::OrderBook(ObEvent::LimitOrderPlaced {
            order_book_id: book(),
            order_id: 77,
            owner_id: AccountId32(ALICE),
            side: PriceVariant::Buy,
            price: unit(500_000_000_000_000_000),
            amount: unit(2_000_000_000_000_000_000),
            lifetime: 3_600_000,
        })),
        record(RuntimeEvent::OrderBook(ObEvent::MarketOrderExecuted {
            order_book_id: book(),
            owner_id: AccountId32(BOB),
            direction: PriceVariant::Sell,
            amount: OrderAmount::Quote(unit(3_000_000_000_000_000_000)),
            average_price: unit(1_500_000_000_000_000_000),
            to: None,
        })),
    ]);
    let placed = decode_order_book(&events[0], coords())
        .unwrap()
        .expect("placed");
    assert!(matches!(placed.event_type, OrderBookEventType::Placed));
    assert_eq!(placed.order_id, Some(77));
    assert!(matches!(placed.side, Some(OrderSide::Buy)));
    let dec = |v: &str| bigdecimal::BigDecimal::from_str(v).unwrap();
    assert_eq!(placed.price, Some(dec("0.5")));
    assert_eq!(placed.amount, Some(dec("2")));
    assert_eq!(placed.quote_amount, Some(dec("1")));
    let market = decode_order_book(&events[1], coords())
        .unwrap()
        .expect("market");
    assert!(matches!(market.event_type, OrderBookEventType::Market));
    assert!(market.order_id.is_none());
    assert!(matches!(market.side, Some(OrderSide::Sell)));
    // A quote-denominated leg is stored as reported (`amount_and_quote`).
    assert_eq!(market.amount, Some(dec("3")));
    assert_eq!(market.quote_amount, Some(dec("3")));
    assert_eq!(market.price, Some(dec("1.5")));
}

#[test]
fn val_staking_reward_paid() {
    use crate::runtime::sora::runtime_types::xor_fee::pallet::Event as FeeEvent;
    use crate::val_staking::decode_val_staking_reward;
    let events = events_from(vec![record(RuntimeEvent::XorFee(
        FeeEvent::ValStakingRewardPaid(
            AccountId32(ALICE),
            AccountId32(BOB),
            1234,
            0,
            88_000_000_000_000_000u128,
        ),
    ))]);
    let r = decode_val_staking_reward(&events[0], coords(), &[0xcd; 32])
        .unwrap()
        .expect("reward");
    assert_eq!(r.era, 1234);
    assert_eq!(r.page, 0);
    assert_eq!(r.validator_stash.as_str(), ss58_encode_sora(&ALICE));
    assert_eq!(r.destination.as_str(), ss58_encode_sora(&BOB));
    assert_eq!(r.amount.to_string(), "88000000000000000");
    assert_eq!(r.block_hash, format!("0x{}", "cd".repeat(32)));
    assert!(decode_fee_burn(&events[0], coords()).unwrap().is_none());
}
