//! Minamoto / Iroha 3 (SORA Nexus) shared library.
//!
//! - [`torii`]: HTTP client for the Torii REST API with timeout,
//!   exponential-backoff retry and a short in-memory cache (the Node's
//!   `minamoto/torii_client.js`).
//! - [`dto`]: response shapes of the Torii routes the indexer and the
//!   API consume, typed against the route catalogue of
//!   `hyperledger-iroha/iroha` `optimizations@cfa5e8ce77` (2026-09-10).
//! - [`prom`]: Prometheus text exposition parser (the Node's
//!   `minamoto/prom_parser.js`).
//!
//! Shared by `sorametrics-ingest --source=iroha` (polling jobs) and
//! `sorametrics-api` (`/api/minamoto/*` passthroughs).

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms, missing_docs)]

pub mod dto;
pub mod prom;
pub mod torii;

pub use torii::{ToriiClient, ToriiConfig, ToriiError};
