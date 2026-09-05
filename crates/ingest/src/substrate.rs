//! Ingest-side substrate machinery.
//!
//! - [`connection`]: lightweight jsonrpsee `WsConnection` with failover,
//!   used by the healthcheck (NOT subxt — subxt opens its own client).
//! - [`health`]: liveness probe + primary-recovery loop. Returns
//!   `HealthOutcome::PrimaryRecovered` to trigger a clean PM2 restart.
//! - [`subscriber`]: subxt-side finalized block stream, dispatched to
//!   the shared [`sorametrics_substrate::decode_block_events`].
//! - [`prices`]: 60 s popular-asset price sampler feeding
//!   `ts.price_history` (the Node's `updateKeyPrices` loop).
//!
//! Decoder + runtime types live in the `sorametrics-substrate` library
//! crate so they can be reused by `sorametrics-ops` (decode-block CLI)
//! without pulling in jsonrpsee / clap / dotenvy.

pub mod connection;
pub mod health;
pub mod prices;
pub mod subscriber;

pub use connection::WsConnection;
pub use health::{run_health_loop, HealthOutcome};
pub use prices::run_price_sampler;
pub use subscriber::run_decoder_loop;
