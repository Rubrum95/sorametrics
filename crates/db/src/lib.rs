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

pub mod mn;
pub mod sm;
pub mod ts;

use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use std::str::FromStr;
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

    /// Caller-supplied value cannot be stored (bad hash length, hex…).
    #[error("invalid input: {0}")]
    Invalid(String),
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
    /// Server-side `statement_timeout` for every connection of the pool.
    pub statement_timeout: Option<Duration>,
    /// Plan every execution with its actual parameters
    /// (`plan_cache_mode = force_custom_plan`).
    pub custom_plans: bool,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_connections: 10,
            connect_timeout: Duration::from_secs(5),
            statement_timeout: None,
            custom_plans: false,
        }
    }
}

/// Creates a connection pool. Does not run migrations.
pub async fn connect(config: &DbConfig) -> Result<PgPool, DbError> {
    let mut options = PgConnectOptions::from_str(&config.url)?;
    if let Some(timeout) = config.statement_timeout {
        options = options.options([("statement_timeout", format!("{}ms", timeout.as_millis()))]);
    }
    if config.custom_plans {
        options = options.options([("plan_cache_mode", "force_custom_plan")]);
    }
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(config.connect_timeout)
        .connect_with(options)
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

    #[tokio::test]
    async fn session_options_reach_the_server() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let pool = connect(&DbConfig {
            url,
            statement_timeout: Some(Duration::from_millis(1234)),
            custom_plans: true,
            ..DbConfig::default()
        })
        .await
        .unwrap();
        let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&pool)
            .await
            .unwrap();
        let mode: String = sqlx::query_scalar("SHOW plan_cache_mode")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(timeout, "1234ms");
        assert_eq!(mode, "force_custom_plan");
    }
}
