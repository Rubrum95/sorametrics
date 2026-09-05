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
pub mod fees;
pub mod price;
pub mod runtime;

pub use block::{decode_block_events, BlockDecodeStats, BlockProcessError};
pub use decoder::{
    decode_bridge, decode_fee_burn, decode_swap, decode_transfer, timestamp_from_millis,
    DecodeError, EventCoords,
};
pub use price::{PriceError, PriceResolver, SampleOutcome};
