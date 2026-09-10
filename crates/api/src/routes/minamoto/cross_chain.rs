//! Cross-chain XOR migration (SORA v2 burn → Minamoto claim).

use super::{clamp_page, js_parse_int, PageQuery};
use crate::error::ApiError;
use crate::AppState;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sorametrics_db::mn;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/cross-chain/stats", get(stats))
        .route("/cross-chain/timeseries", get(timeseries))
        .route("/cross-chain/claims", get(claims))
        .route("/cross-chain/mint-history", get(mint_history))
        .route("/cross-chain/pending-burns", get(pending_burns))
}

/// The bridge's bootstrap block on SORA v2 (the live widget's constant).
pub const BRIDGE_SINCE_BLOCK: i64 = 25_867_650;

fn iso(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// JS number rendering: integral values without a fraction.
fn js_num(v: f64) -> Value {
    if v.fract() == 0.0 && v.abs() < 9_007_199_254_740_992.0 {
        json!(v as i64)
    } else {
        json!(v)
    }
}

async fn stats(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(mn::get_cross_chain_stats(&state.db).await?))
}

/// `?hours` (1..=720, default 168).
#[derive(Debug, Deserialize)]
pub struct HoursQuery {
    /// Window.
    pub hours: Option<String>,
}

async fn timeseries(
    State(state): State<AppState>,
    Query(q): Query<HoursQuery>,
) -> Result<Json<Value>, ApiError> {
    let hours = q
        .hours
        .as_deref()
        .and_then(js_parse_int)
        .filter(|h| *h != 0)
        .unwrap_or(168)
        .clamp(1, 720);
    let series = mn::get_cross_chain_timeseries(&state.db, hours as i32).await?;
    Ok(Json(json!({ "hours": hours, "series": series })))
}

async fn claims(
    State(state): State<AppState>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, ApiError> {
    let (page, per_page) = clamp_page(&q, 50);
    Ok(Json(json!(
        mn::list_claims(&state.db, page, per_page).await?
    )))
}

/// Every XOR Mint categorised by source; amounts summed as JS numbers
/// (Iroha stores XOR in user units, no 1e18 scaling).
async fn mint_history(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let Some(xor_id) = mn::xor_definition_id(&state.db).await? else {
        return Ok(Json(
            json!({ "total_raw": "0", "by_source": {}, "timeline": [], "xor_asset_id": null }),
        ));
    };
    let rows = mn::xor_mints(&state.db, &xor_id).await?;
    struct Bucket {
        count: u64,
        amount: f64,
        recipients: std::collections::HashSet<String>,
        first_at: String,
        last_at: String,
    }
    let mut total = 0.0f64;
    let mut order: Vec<String> = Vec::new();
    let mut by_source: std::collections::HashMap<String, Bucket> = Default::default();
    let mut timeline = Vec::with_capacity(rows.len());
    for r in &rows {
        let amt: f64 = r
            .amount_raw
            .as_deref()
            .unwrap_or("0")
            .parse()
            .unwrap_or(f64::NAN);
        total += amt;
        let at = iso(r.created_at);
        let b = by_source.entry(r.source.clone()).or_insert_with(|| {
            order.push(r.source.clone());
            Bucket {
                count: 0,
                amount: 0.0,
                recipients: Default::default(),
                first_at: at.clone(),
                last_at: at.clone(),
            }
        });
        b.count += 1;
        b.amount += amt;
        if let Some(rec) = &r.recipient {
            b.recipients.insert(rec.clone());
        }
        b.last_at = at.clone();
        timeline.push(json!({
            "source": r.source,
            "block": r.block,
            "at": at,
            "amount": js_num(amt),
            "minter": r.minter,
            "recipient": r.recipient,
            "tx_hash": r.tx_hash,
            "v2_burn_tx": r.v2_burn_tx,
            "v2_block": r.v2_block,
            "v2_signer": r.v2_signer,
        }));
    }
    let mut summary = Map::new();
    for k in order {
        let b = &by_source[&k];
        summary.insert(
            k,
            json!({
                "count": b.count,
                "amount": js_num(b.amount),
                "recipients": b.recipients.len(),
                "first_at": b.first_at,
                "last_at": b.last_at,
            }),
        );
    }
    Ok(Json(json!({
        "total_xor": js_num(total),
        "total_mints": rows.len(),
        "xor_asset_id": xor_id,
        "xor_storage_note": "Amounts are already in XOR units (no 18-decimal scaling). Iroha 3 Numeric is arbitrary-precision rational; the bridge converts v2 raw 1e18 to Minamoto Numeric units when minting.",
        "by_source": Value::Object(summary),
        "timeline": timeline,
    })))
}

/// `?status=pending|claimed|pre_reset|all&limit`.
#[derive(Debug, Deserialize)]
pub struct BurnsQuery {
    /// State filter.
    pub status: Option<String>,
    /// Max burns (1..=500, default 100).
    pub limit: Option<String>,
}

/// SORA v2 burns carrying the `soraNexusXorClaim` remark, joined with
/// their Minamoto claim. Burns older than the current chain's block #1
/// belong to a wiped chain (`pre_reset`).
async fn pending_burns(
    State(state): State<AppState>,
    Query(q): Query<BurnsQuery>,
) -> Result<Json<Value>, ApiError> {
    let status = match q.status.as_deref() {
        Some(s @ ("pending" | "claimed" | "pre_reset" | "all")) => s.to_string(),
        _ => "all".to_string(),
    };
    let limit = q
        .limit
        .as_deref()
        .and_then(js_parse_int)
        .filter(|l| *l != 0)
        .unwrap_or(100)
        .clamp(1, 500);
    let cutoff = mn::genesis_created_at(&state.db).await?;
    let rows = mn::list_cross_chain_burns(&state.db, BRIDGE_SINCE_BLOCK, limit).await?;
    let mut counts = json!({ "total": 0, "claimed": 0, "pending": 0, "pre_reset": 0 });
    let mut all = Vec::with_capacity(rows.len());
    for r in rows {
        let state_label = if r.mn_tx_hash.is_some() {
            "claimed"
        } else if cutoff.is_some_and(|c| r.v2_at < c) {
            "pre_reset"
        } else {
            "pending"
        };
        counts["total"] = json!(counts["total"].as_i64().unwrap_or(0) + 1);
        counts[state_label] = json!(counts[state_label].as_i64().unwrap_or(0) + 1);
        all.push(json!({
            "v2_tx_hash": r.v2_tx_hash,
            "v2_block": r.v2_block,
            "v2_signer": r.v2_signer,
            "v2_at": iso(r.v2_at),
            "amount_xor": r.raw_amount.as_deref().map(planck_text_to_xor),
            "recipient_i105": r.recipient_i105,
            "mn_tx_hash": r.mn_tx_hash.as_deref().map(mn::hex_of),
            "mn_block": r.mn_block,
            "mn_at": r.mn_at.map(iso),
            "state": state_label,
            "claimed": state_label == "claimed",
        }));
    }
    let items: Vec<Value> = if status == "all" {
        all
    } else {
        all.into_iter().filter(|it| it["state"] == status).collect()
    };
    Ok(Json(
        json!({ "status": status, "limit": limit, "counts": counts, "items": items }),
    ))
}

/// `toHuman` planck text (`50,000,000,000,000,000,000`) → `Number/1e18`
/// → `toString()`.
fn planck_text_to_xor(raw: &str) -> String {
    let digits: String = raw.chars().filter(|c| *c != ',').collect();
    let v: f64 = digits.parse().unwrap_or(f64::NAN);
    format!("{}", v / 1e18)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planck_text_renders_like_js() {
        assert_eq!(planck_text_to_xor("50,000,000,000,000,000,000"), "50");
        assert_eq!(planck_text_to_xor("12500000000000000000"), "12.5");
        assert_eq!(planck_text_to_xor("100000000000000000"), "0.1");
    }

    #[test]
    fn js_number_rendering() {
        assert_eq!(js_num(20.0).to_string(), "20");
        assert_eq!(js_num(4018.5).to_string(), "4018.5");
    }
}
