//! Feasibility probe for a CI compatibility check: per-pallet metadata
//! hashes of the pinned file vs the live node.
//!
//! cargo run -p sorametrics-substrate --example metadata_drift -- [wss url] [height]
//!
//! With a height, the comparison is against the metadata the node served
//! at that block (an archive node is required for old heights).

use subxt::backend::legacy::LegacyRpcMethods;
use subxt::backend::rpc::RpcClient;
use subxt::ext::codec::Decode;
use subxt::{Metadata, OnlineClient, SubstrateConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "wss://mof2.sora.org".to_string());
    let pinned_bytes: &[u8] = include_bytes!("../metadata/sora-mainnet.scale");
    let pinned = Metadata::decode(&mut &pinned_bytes[..])?;
    let height: Option<u32> = std::env::args().nth(2).map(|h| h.parse()).transpose()?;
    let rpc = RpcClient::from_url(&url).await?;
    let legacy = LegacyRpcMethods::<SubstrateConfig>::new(rpc.clone());
    let client = OnlineClient::<SubstrateConfig>::from_rpc_client(rpc).await?;
    let (live, spec) = match height {
        Some(h) => {
            let hash = legacy
                .chain_get_block_hash(Some(h.into()))
                .await?
                .expect("hash");
            let v = legacy.state_get_runtime_version(Some(hash)).await?;
            (legacy.state_get_metadata(Some(hash)).await?, v.spec_version)
        }
        None => (client.metadata(), client.runtime_version().spec_version),
    };
    println!(
        "compared spec {spec} · pinned pallets {} · live pallets {}",
        pinned.pallets().count(),
        live.pallets().count()
    );
    let (mut same, mut drift, mut missing) = (0, 0, 0);
    for p in pinned.pallets() {
        match live.pallet_by_name(p.name()) {
            None => {
                missing += 1;
                println!("MISSING {}", p.name());
            }
            Some(l) if l.hash() == p.hash() => same += 1,
            Some(_) => {
                drift += 1;
                println!("DRIFT   {}", p.name());
            }
        }
    }
    println!("same {same} drift {drift} missing {missing}");
    Ok(())
}
