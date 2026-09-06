//! `/health`, `/health/rpc-source`, `/api/version` — the legacy
//! liveness / source probes (`index.js`).
//!
//! - `/health`: `{ status: "ok", uptime (s), wsConnected, timestamp (ms) }`.
//!   Never touches the DB; for data freshness use `/health/freshness`.
//! - `/health/rpc-source`: `{ active, label, isPrimary, isLocal,
//!   connected, endpoints }` — which WS endpoint the chain client is on.
//!   `label` is `"sorametrics"` for loopback, else the host.
//! - `/api/version`: `{ version }` with `Cache-Control: no-store`.

use crate::AppState;
use axum::{extract::State, http::header, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;

/// Legacy `SERVER_VERSION` the PWA compares to force refreshes.
pub const SERVER_VERSION: &str = "v4.0";

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/health/rpc-source", get(rpc_source))
        .route("/api/version", get(version))
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    uptime: u64,
    #[serde(rename = "wsConnected")]
    ws_connected: bool,
    timestamp: i64,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let ws_connected = match &state.chain {
        Some(c) => c.active_endpoint().await.is_some(),
        None => false,
    };
    Json(HealthResponse {
        status: "ok",
        uptime: state.started_at.elapsed().as_secs(),
        ws_connected,
        timestamp: chrono::Utc::now().timestamp_millis(),
    })
}

#[derive(Serialize)]
struct RpcSource {
    active: Option<String>,
    label: String,
    #[serde(rename = "isPrimary")]
    is_primary: bool,
    #[serde(rename = "isLocal")]
    is_local: bool,
    connected: bool,
    endpoints: Vec<String>,
}

/// Node: loopback → "sorametrics", else the bare host.
pub fn endpoint_label(url: &url::Url) -> (String, bool) {
    let host = url.host_str().unwrap_or("unknown");
    let is_local = url.scheme() == "ws" && matches!(host, "127.0.0.1" | "localhost");
    if is_local {
        ("sorametrics".to_string(), true)
    } else {
        (host.to_string(), false)
    }
}

async fn rpc_source(State(state): State<AppState>) -> Json<RpcSource> {
    let Some(chain) = &state.chain else {
        return Json(RpcSource {
            active: None,
            label: "unknown".into(),
            is_primary: false,
            is_local: false,
            connected: false,
            endpoints: Vec::new(),
        });
    };
    let endpoints: Vec<String> = chain.endpoints().iter().map(|u| u.to_string()).collect();
    match chain.active_endpoint().await {
        Some(active) => {
            let (label, is_local) = endpoint_label(&active);
            let is_primary = chain.endpoints().first() == Some(&active);
            Json(RpcSource {
                active: Some(active.to_string()),
                label,
                is_primary,
                is_local,
                connected: true,
                endpoints,
            })
        }
        None => Json(RpcSource {
            active: None,
            label: "unknown".into(),
            is_primary: false,
            is_local: false,
            connected: false,
            endpoints,
        }),
    }
}

#[derive(Serialize)]
struct Version {
    version: &'static str,
}

async fn version() -> impl IntoResponse {
    (
        [(header::CACHE_CONTROL, "no-store, no-cache, must-revalidate")],
        Json(Version {
            version: SERVER_VERSION,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_follow_the_node() {
        let (l, local) = endpoint_label(&url::Url::parse("ws://127.0.0.1:9944").unwrap());
        assert_eq!((l.as_str(), local), ("sorametrics", true));
        let (l, local) = endpoint_label(&url::Url::parse("wss://mof2.sora.org").unwrap());
        assert_eq!((l.as_str(), local), ("mof2.sora.org", false));
    }
}
