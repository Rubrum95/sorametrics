//! Shared application state passed to every handler via axum's
//! [`State`](axum::extract::State) extractor.

use chrono_tz::Tz;
use sorametrics_db::sm::{load_asset_registry, RegistryAsset};
use sorametrics_db::DbError;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Time zone the legacy Node rendered `time` strings in (its process
/// ran with the VPS clock, Europe/Madrid). The frontend parses that
/// string as browser-local time, so the API must keep producing the
/// same wall-clock text. Override with `API_TIME_ZONE`.
pub const DEFAULT_TIME_ZONE: Tz = chrono_tz::Europe::Madrid;

/// How often the in-memory asset registry is reloaded from the DB.
pub const REGISTRY_REFRESH: Duration = Duration::from_secs(300);

/// In-memory snapshot of `sm.asset_registry`, indexed both ways.
#[derive(Default)]
pub struct Registry {
    by_id: HashMap<String, RegistryAsset>,
}

impl Registry {
    /// Build from rows.
    pub fn from_rows(rows: Vec<RegistryAsset>) -> Self {
        Self {
            by_id: rows.into_iter().map(|a| (a.asset_id.clone(), a)).collect(),
        }
    }

    /// Registry entry for an asset id, if listed.
    pub fn get(&self, asset_id: &str) -> Option<&RegistryAsset> {
        self.by_id.get(asset_id)
    }

    /// Asset ids whose symbol contains `needle` (case-insensitive) — the
    /// legacy `UPPER(symbol) LIKE '%F%'` filter, resolved to ids.
    pub fn asset_ids_matching(&self, needle: &str) -> Vec<String> {
        let up = needle.to_uppercase();
        let mut ids: Vec<String> = self
            .by_id
            .values()
            .filter(|a| a.symbol.to_uppercase().contains(&up))
            .map(|a| a.asset_id.clone())
            .collect();
        ids.sort();
        ids
    }

    /// Number of assets loaded.
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// `true` when nothing is loaded.
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

/// Cheap-to-clone state container (pool + `Arc`s).
#[derive(Clone)]
pub struct AppState {
    /// PostgreSQL connection pool.
    pub db: PgPool,
    /// Asset registry snapshot, refreshed by [`AppState::spawn_registry_refresh`].
    pub registry: Arc<RwLock<Registry>>,
    /// Zone for legacy `time` strings.
    pub time_zone: Tz,
}

impl AppState {
    /// Constructs from a live pool with an EMPTY registry (tests, or
    /// callers that will load it themselves).
    pub fn new(db: PgPool) -> Self {
        Self {
            db,
            registry: Arc::new(RwLock::new(Registry::default())),
            time_zone: DEFAULT_TIME_ZONE,
        }
    }

    /// Loads the registry once from the DB. Fails loudly: an API with
    /// no symbols would render every row as `0xXXXX`.
    pub async fn with_registry(db: PgPool, time_zone: Tz) -> Result<Self, DbError> {
        let rows = load_asset_registry(&db).await?;
        info!(assets = rows.len(), zone = %time_zone, "asset registry loaded");
        Ok(Self {
            db,
            registry: Arc::new(RwLock::new(Registry::from_rows(rows))),
            time_zone,
        })
    }

    /// Reloads the registry from the DB every [`REGISTRY_REFRESH`].
    /// A failed reload keeps the previous snapshot and logs.
    pub fn spawn_registry_refresh(&self) {
        let db = self.db.clone();
        let registry = self.registry.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(REGISTRY_REFRESH);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match load_asset_registry(&db).await {
                    Ok(rows) => {
                        *registry.write().await = Registry::from_rows(rows);
                    }
                    Err(e) => warn!(error = %e, "asset registry refresh failed; keeping previous"),
                }
            }
        });
    }
}

/// Parses `API_TIME_ZONE` (IANA name) or falls back to [`DEFAULT_TIME_ZONE`].
pub fn time_zone_from_env() -> Result<Tz, String> {
    match std::env::var("API_TIME_ZONE") {
        Ok(v) => v
            .parse::<Tz>()
            .map_err(|e| format!("API_TIME_ZONE '{v}' is not an IANA zone: {e}")),
        Err(_) => Ok(DEFAULT_TIME_ZONE),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(id: &str, sym: &str) -> RegistryAsset {
        RegistryAsset {
            asset_id: id.to_string(),
            symbol: sym.to_string(),
            decimals: 18,
            logo: None,
        }
    }

    #[test]
    fn symbol_filter_is_case_insensitive_substring() {
        let r = Registry::from_rows(vec![
            asset("0x01", "XOR"),
            asset("0x02", "KXOR"),
            asset("0x03", "DAI"),
        ]);
        assert_eq!(r.asset_ids_matching("xor"), vec!["0x01", "0x02"]);
        assert_eq!(r.asset_ids_matching("DAI"), vec!["0x03"]);
        assert!(r.asset_ids_matching("zzz").is_empty());
    }
}
