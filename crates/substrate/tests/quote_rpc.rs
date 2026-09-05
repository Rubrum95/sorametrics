//! Network test: the raw `liquidityProxy_quote` call against SORA
//! mainnet. Ignored by default (needs `wss://mof2.sora.org`); run with
//! `cargo test -p sorametrics-substrate --test quote_rpc -- --ignored`.

use rust_decimal::Decimal;
use sorametrics_core::chain::AssetId;
use sorametrics_substrate::price::{quote_price_in_dai, DAI_ASSET_ID};
use subxt::backend::rpc::RpcClient;

const XOR: &str = "0x0200000000000000000000000000000000000000000000000000000000000000";
const GARBAGE: &str = "0x0200990000000000000000000000000000000000000000000000000000000000";

#[tokio::test]
#[ignore = "needs network access to wss://mof2.sora.org"]
async fn xor_quote_is_positive_and_dai_is_one() {
    let rpc = RpcClient::from_url("wss://mof2.sora.org")
        .await
        .expect("connect mof2");

    let xor = quote_price_in_dai(&rpc, &AssetId::new(XOR), 18)
        .await
        .expect("quote XOR")
        .expect("XOR must have a DAI route");
    assert!(xor > Decimal::ZERO);
    assert!(xor < Decimal::from(10_000), "XOR price implausible: {xor}");

    let dai = quote_price_in_dai(&rpc, &AssetId::new(DAI_ASSET_ID), 18)
        .await
        .expect("quote DAI");
    assert_eq!(dai, Some(Decimal::ONE));

    let none = quote_price_in_dai(&rpc, &AssetId::new(GARBAGE), 18)
        .await
        .expect("quote unknown asset must not error");
    assert_eq!(none, None);
}
