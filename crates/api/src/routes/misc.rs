//! `/currency-rates` — USD→EUR rate proxy (`index.js`): fetched from
//! `https://open.er-api.com/v6/latest/USD`, cached 1 h, `0.92` when the
//! upstream fails (the Node's seed value).

use crate::{error::ApiError, AppState};
use axum::{extract::State, routing::get, Json, Router};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new().route("/currency-rates", get(currency_rates))
}

const TTL: Duration = Duration::from_secs(3600);
const FALLBACK_EUR: f64 = 0.92;
const UPSTREAM: &str = "https://open.er-api.com/v6/latest/USD";

#[derive(Serialize, Deserialize)]
struct Rates {
    #[serde(rename = "EUR")]
    eur: f64,
}

#[derive(Deserialize)]
struct Upstream {
    rates: Option<UpstreamRates>,
}

#[derive(Deserialize)]
struct UpstreamRates {
    #[serde(rename = "EUR")]
    eur: Option<f64>,
}

async fn fetch_eur() -> Option<f64> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .ok()?;
    let body: Upstream = client.get(UPSTREAM).send().await.ok()?.json().await.ok()?;
    body.rates.and_then(|r| r.eur).filter(|v| *v > 0.0)
}

async fn currency_rates(State(state): State<AppState>) -> Result<Json<Rates>, ApiError> {
    if let Some(v) = state.cached_scan("currency-rates", TTL).await {
        let r: Rates = serde_json::from_value(v).map_err(|e| ApiError::Internal(e.to_string()))?;
        return Ok(Json(r));
    }
    match fetch_eur().await {
        Some(eur) => {
            let r = Rates { eur };
            let v = serde_json::to_value(&r).map_err(|e| ApiError::Internal(e.to_string()))?;
            state.store_scan("currency-rates", v).await;
            Ok(Json(r))
        }
        None => {
            tracing::warn!("currency-rates upstream failed; serving fallback");
            Ok(Json(Rates { eur: FALLBACK_EUR }))
        }
    }
}
