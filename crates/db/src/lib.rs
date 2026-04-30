//! PostgreSQL + TimescaleDB layer for SoraMetrics v33.
//!
//! Phase 0 scope:
//! - Connection pool factory
//! - Migration runner pointed at the workspace `migrations/` directory
//! - Schema names exposed as constants
//!
//! Later phases add typed query helpers per schema (`sm`, `mn`, `ts`,
//! `analytics`). They will live in modules of the same name.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms, missing_docs)]

pub mod sm;

use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use thiserror::Error;

/// SORA v2 indexed state. Aislado: nunca cruza a `MN`.
pub const SCHEMA_SM: &str = "sm";

/// Minamoto / Iroha 3 indexed state. Aislado: nunca cruza a `SM`.
pub const SCHEMA_MN: &str = "mn";

/// TimescaleDB hypertables (price history, metrics snapshots, OHLCV).
pub const SCHEMA_TS: &str = "ts";

/// Read-only analytical views joining `sm` and `mn`. Never written directly.
pub const SCHEMA_ANALYTICS: &str = "analytics";

/// Errors surfaced by the db layer.
#[derive(Debug, Error)]
pub enum DbError {
    /// Underlying sqlx error (connection, query, parse).
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// Migration failed.
    #[error("migration: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct DbConfig {
    /// `postgres://user:pass@host:port/db` URL.
    pub url: String,
    /// Maximum connections held in the pool.
    pub max_connections: u32,
    /// Connect timeout.
    pub connect_timeout: Duration,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_connections: 10,
            connect_timeout: Duration::from_secs(5),
        }
    }
}

/// Creates a connection pool. Does not run migrations.
pub async fn connect(config: &DbConfig) -> Result<PgPool, DbError> {
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(config.connect_timeout)
        .connect(&config.url)
        .await?;
    Ok(pool)
}

/// Runs pending migrations from the workspace `migrations/` directory.
///
/// Migrations are versioned numerically (`NNNN_description.sql`) and idempotent.
/// Re-running this against a fully migrated db is a no-op.
pub async fn migrate(pool: &PgPool) -> Result<(), DbError> {
    sqlx::migrate!("../../migrations").run(pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_constants_are_lowercase_idents() {
        for s in [SCHEMA_SM, SCHEMA_MN, SCHEMA_TS, SCHEMA_ANALYTICS] {
            assert!(s.chars().all(|c| c.is_ascii_lowercase() || c == '_'));
            assert!(!s.is_empty());
        }
    }

    #[test]
    fn db_config_defaults_reasonable() {
        let cfg = DbConfig::default();
        assert!(cfg.max_connections > 0);
        assert!(cfg.connect_timeout > Duration::ZERO);
    }
}
