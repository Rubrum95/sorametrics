//! Generated SORA mainnet runtime types.
//!
//! The `sora` module is produced by `subxt` from the pinned metadata at
//! `crates/ingest/metadata/sora-mainnet.scale`. See `metadata/README.md`
//! for regeneration instructions.
//!
//! All event variants and storage entries we use are reachable through:
//!
//! - `runtime::sora::system::events::*`
//! - `runtime::sora::timestamp::events::*`
//! - `runtime::sora::assets::events::*`
//! - `runtime::sora::liquidity_proxy::events::*`
//! - `runtime::sora::xor_fee::events::*`
//! - `runtime::sora::substrate_bridge_app::events::*`
//! - `runtime::sora::parachain_bridge_app::events::*`
//! - `runtime::sora::jetton_app::events::*`
//! - `runtime::sora::bridge_multisig::events::*`
//!
//! Phase 1.2.1 only sets up the codegen and wires a high-level
//! finalized-block subscription. Concrete event decoders land in 1.2.2.

#![allow(missing_docs, clippy::too_many_arguments, clippy::unreadable_literal)]

#[subxt::subxt(runtime_metadata_path = "metadata/sora-mainnet.scale")]
pub mod sora {}
