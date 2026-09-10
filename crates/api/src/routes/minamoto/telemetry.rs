//! Telemetry / governance / kaigi / NFT passthroughs and the peers list.
//!
//! What the Node proxied and the `optimizations` Torii no longer serves
//! to anonymous callers answers 503 with the reason.

use super::{js_parse_int, torii};
use crate::error::ApiError;
use crate::AppState;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use sorametrics_db::mn;
use sorametrics_iroha::torii::EXPLORER_DEFAULT_LIMIT;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/telemetry/peers-info", get(peers_info))
        .route("/telemetry/propagation", get(propagation))
        .route("/telemetry/sumeragi", get(sumeragi_telemetry))
        .route("/sumeragi/roles", get(sumeragi_roles))
        .route("/gov/council", get(gov_council))
        .route("/gov/unlocks", get(gov_unlocks))
        .route("/kaigi/relays", get(kaigi_relays))
        .route("/explorer/nfts", get(explorer_nfts))
        .route("/explorer/rwas", get(explorer_rwas))
        .route("/peers", get(peers))
}

async fn peers_info(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(torii(&state)?.peers_info_raw().await?))
}

async fn propagation(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(torii(&state)?.propagation().await?))
}

async fn sumeragi_telemetry() -> Result<Json<Value>, ApiError> {
    Err(ApiError::ToriiRouteGone(
        "/v1/sumeragi/telemetry was removed from the Iroha Torii catalogue (optimizations@cfa5e8ce77)",
    ))
}

/// The roster view needed `/peers`, `/v1/sumeragi/telemetry` and
/// `/v1/sumeragi/collectors`; all three are gone and the replacements
/// (`/v1/peers`, `/v1/sumeragi/leader`) require an operator signature.
async fn sumeragi_roles() -> Result<Json<Value>, ApiError> {
    Err(ApiError::ToriiRouteGone(
        "/v1/sumeragi/collectors and /v1/sumeragi/telemetry were removed; /v1/peers and /v1/sumeragi/leader are operator-only",
    ))
}

async fn gov_council() -> Result<Json<Value>, ApiError> {
    Err(ApiError::ToriiRouteGone(
        "/v1/gov/council/current was removed from the Iroha Torii catalogue",
    ))
}

async fn gov_unlocks() -> Result<Json<Value>, ApiError> {
    Err(ApiError::ToriiRouteGone(
        "/v1/gov/unlocks/stats now requires a canonical account signature",
    ))
}

async fn kaigi_relays() -> Result<Json<Value>, ApiError> {
    Err(ApiError::ToriiRouteGone(
        "/v1/kaigi/relays now requires an operator signature",
    ))
}

/// `?cursor&limit` (`per_page` accepted as `limit`); the Torii page is
/// returned verbatim.
#[derive(Debug, Deserialize)]
pub struct CursorQuery {
    /// Resume token.
    pub cursor: Option<String>,
    /// Page size.
    pub limit: Option<String>,
    /// Legacy alias of `limit`.
    pub per_page: Option<String>,
}

fn cursor_args(q: &CursorQuery) -> (Option<&str>, u32) {
    let limit = q
        .limit
        .as_deref()
        .or(q.per_page.as_deref())
        .and_then(js_parse_int)
        .filter(|l| *l > 0)
        .map(|l| l.min(100) as u32)
        .unwrap_or(EXPLORER_DEFAULT_LIMIT);
    (q.cursor.as_deref().filter(|c| !c.is_empty()), limit)
}

async fn explorer_nfts(
    State(state): State<AppState>,
    Query(q): Query<CursorQuery>,
) -> Result<Json<Value>, ApiError> {
    let (cursor, limit) = cursor_args(&q);
    Ok(Json(torii(&state)?.explorer_nfts(cursor, limit).await?))
}

async fn explorer_rwas(
    State(state): State<AppState>,
    Query(q): Query<CursorQuery>,
) -> Result<Json<Value>, ApiError> {
    let (cursor, limit) = cursor_args(&q);
    Ok(Json(torii(&state)?.explorer_rwas(cursor, limit).await?))
}

async fn peers(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!({ "items": mn::list_peers(&state.db).await? })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_args_defaults_and_aliases() {
        let q = CursorQuery {
            cursor: None,
            limit: None,
            per_page: Some("50".into()),
        };
        assert_eq!(cursor_args(&q), (None, 50));
        let q = CursorQuery {
            cursor: Some("abc".into()),
            limit: Some("500".into()),
            per_page: None,
        };
        assert_eq!(cursor_args(&q), (Some("abc"), 100));
        let q = CursorQuery {
            cursor: Some(String::new()),
            limit: Some("0".into()),
            per_page: None,
        };
        assert_eq!(cursor_args(&q), (None, EXPLORER_DEFAULT_LIMIT));
    }
}
