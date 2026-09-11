//! Feasibility probe: decode the events of a block from an EARLIER runtime
//! era with the metadata the node served at that block, then run the
//! static bridge / swap decoders on them.
//!
//! cargo run -p sorametrics-substrate --example era_decode -- <height> [wss url]

use sorametrics_core::chain::BlockHeight;
use sorametrics_core::time::Timestamp;
use sorametrics_substrate::decoder::{decode_bridge, decode_swap, decode_transfer, EventCoords};
use subxt::backend::legacy::LegacyRpcMethods;
use subxt::backend::rpc::RpcClient;
use subxt::{Metadata, OnlineClient, SubstrateConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let height: u32 = args.next().expect("height").parse()?;
    let url = args
        .next()
        .unwrap_or_else(|| "wss://mof2.sora.org".to_string());
    let rpc = RpcClient::from_url(&url).await?;
    let legacy = LegacyRpcMethods::<SubstrateConfig>::new(rpc.clone());
    let hash = legacy
        .chain_get_block_hash(Some(height.into()))
        .await?
        .expect("hash");
    let version = legacy.state_get_runtime_version(Some(hash)).await?;
    let meta: Metadata = legacy.state_get_metadata(Some(hash)).await?;
    println!(
        "height {height} spec {} metadata pallets {}",
        version.spec_version,
        meta.pallets().count()
    );
    let client = OnlineClient::<SubstrateConfig>::from_rpc_client(rpc).await?;
    client.set_metadata(meta);
    let block = client.blocks().at(hash).await?;
    let events = block.events().await?;
    let coords = EventCoords {
        block_height: BlockHeight(height as u64),
        block_timestamp: Timestamp::now(),
        extrinsic_id: 0,
        event_id: 0,
        extrinsic_hash: None,
    };
    let (mut n, mut bridges, mut swaps, mut transfers, mut errors) = (0, 0, 0, 0, 0);
    for ev in events.iter() {
        let ev = match ev {
            Ok(e) => e,
            Err(e) => {
                errors += 1;
                println!("event decode error: {e}");
                continue;
            }
        };
        n += 1;
        match decode_bridge(&ev, coords) {
            Ok(Some(b)) => {
                bridges += 1;
                println!(
                    "BRIDGE {} {:?} {} {} -> {:?} {}",
                    b.network,
                    b.direction,
                    b.caller.as_str(),
                    b.asset.as_str(),
                    b.counterparty,
                    b.amount
                );
            }
            Ok(None) => {}
            Err(e) => {
                errors += 1;
                println!(
                    "bridge decode error at {}::{}: {e}",
                    ev.pallet_name(),
                    ev.variant_name()
                );
            }
        }
        match decode_swap(&ev, coords) {
            Ok(Some(_)) => swaps += 1,
            Ok(None) => {}
            Err(e) => {
                errors += 1;
                println!("swap decode error: {e}");
            }
        }
        match decode_transfer(&ev, coords) {
            Ok(Some(_)) => transfers += 1,
            Ok(None) => {}
            Err(e) => {
                errors += 1;
                println!("transfer decode error: {e}");
            }
        }
    }
    println!("events {n} bridges {bridges} swaps {swaps} transfers {transfers} errors {errors}");
    Ok(())
}
