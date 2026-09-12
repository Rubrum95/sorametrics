//! Shared SORA v2 (Substrate) library.
//!
//! Owns the subxt runtime codegen, event decoders, and the per-block
//! processor. Used by:
//!
//! - `sorametrics-ingest`: streams finalized blocks and applies the
//!   decoder pipeline live.
//! - `sorametrics-ops`: applies the same pipeline to a single block by
//!   height (for backfill, gap fill, replay, debug).
//!
//! Keeping this crate separate from `ingest` lets `ops` stay lean — it
//! doesn't pull in `jsonrpsee`, `clap`, `dotenvy` or any of the
//! ingest-binary-only dependencies.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms, missing_docs)]

pub mod block;
pub mod decoder;
pub mod eth_bridge;
pub mod extrinsics;
pub mod fee_burns_agg;
pub mod fees;
pub mod governance;
pub mod liquidity;
pub mod order_book;
pub mod polkamarkt;
pub mod price;
pub mod runtime;
#[cfg(test)]
mod synthetic_tests;
pub mod val_staking;

/// Keys per `state_getKeysPaged` call when iterating a storage map
/// (the node's maximum). subxt's legacy backend defaults to 64, which
/// turns the 58k-entry `tokens.accounts` walk of `/holders` into ~1 800
/// round trips instead of ~120.
pub const STORAGE_PAGE_SIZE: u32 = 1000;

/// A `LegacyBackend` over `rpc` with [`STORAGE_PAGE_SIZE`].
pub fn legacy_backend<T: subxt::Config>(
    rpc: subxt::backend::rpc::RpcClient,
) -> std::sync::Arc<subxt::backend::legacy::LegacyBackend<T>> {
    std::sync::Arc::new(
        subxt::backend::legacy::LegacyBackend::builder()
            .storage_page_size(STORAGE_PAGE_SIZE)
            .build(rpc),
    )
}

/// `OnlineClient::from_rpc_client` with [`STORAGE_PAGE_SIZE`] pages.
pub async fn online_client<T: subxt::Config>(
    rpc: subxt::backend::rpc::RpcClient,
) -> Result<subxt::OnlineClient<T>, subxt::Error> {
    subxt::OnlineClient::<T>::from_backend(legacy_backend::<T>(rpc)).await
}

pub use block::{decode_block_events, BlockDecodeStats, BlockProcessError};

/// The pinned SORA mainnet metadata the `sora` module is generated from
/// (`metadata/sora-mainnet.scale`, v15, the 29 pallets we decode).
pub const PINNED_METADATA: &[u8] = include_bytes!("../metadata/sora-mainnet.scale");
pub use decoder::{
    decode_bridge, decode_fee_burn, decode_swap, decode_transfer, timestamp_from_millis,
    DecodeError, EventCoords,
};
pub use price::{BlockRef, PriceError, PriceResolver, SampleOutcome, HISTORICAL_QUOTE_MIN_HEIGHT};
