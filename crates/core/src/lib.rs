//! Core domain types for SoraMetrics v33.
//!
//! Canonical types shared across all services (ingest, api, ops). Each type is:
//! - Strict (newtype wrappers around primitives, never raw `String`/`u64`)
//! - Serializable (`serde::{Serialize, Deserialize}`)
//! - Immutable by default
//!
//! Modules are split by domain:
//! - [`chain`]: generic on-chain primitives (hashes, heights, addresses, asset ids)
//! - [`sora_v2`]: SORA Substrate v2 specific events
//! - [`minamoto`]: Minamoto / Iroha 3 specific events
//! - [`time`]: timestamp wrappers and freshness helpers

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms, missing_docs)]

pub mod chain;
pub mod minamoto;
pub mod sora_v2;
pub mod time;

pub use chain::{Address, AssetId, BlockHash, BlockHeight, Bytes32};
pub use time::{FreshnessStatus, Timestamp};
