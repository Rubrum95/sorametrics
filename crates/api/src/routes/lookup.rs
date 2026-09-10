//! `/lookup/usd-value/:extrinsicId` (`db_pg.js::lookupExtrinsicUsdValue`):
//! the USD value of an extrinsic `"block-index"`, searched in transfers,
//! swaps, bridges and liquidity in that order. A stored value renders
//! at 1 dp; a missing one falls back to `amount × current price` of the
//! asset. Legacy rows are matched by their own id forms: the bare
//! index, `block-index`, or the extrinsic hash (the subsquid `he.id`).
//! Nothing found → `{ usd_value: null }`; bad id → 400.

use crate::legacy::{decimals_for, fmt_usd};
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use bigdecimal::{BigDecimal, ToPrimitive};
use num_bigint::BigInt;
use serde::Serialize;
use sorametrics_db::ts::latest_prices;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new().route("/lookup/usd-value/:extrinsic_id", get(usd_value))
}

#[derive(Serialize)]
struct Lookup {
    usd_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<&'static str>,
}

/// `"<block>-<index>"` → `(block, index)`.
pub fn parse_extrinsic_id(raw: &str) -> Option<(i64, i64)> {
    let (b, i) = raw.split_once('-')?;
    if b.is_empty()
        || i.is_empty()
        || !b.bytes().all(|c| c.is_ascii_digit())
        || !i.bytes().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    Some((b.parse().ok()?, i.parse().ok()?))
}

/// Node `computeFallbackUsd`: human amount × current price.
async fn fallback_usd(state: &AppState, asset_id: &str, raw: &BigDecimal) -> Result<f64, ApiError> {
    let decimals = {
        let registry = state.registry.read().await;
        decimals_for(&registry, asset_id)
    };
    let price = latest_prices(&state.db, &[asset_id.to_string()])
        .await?
        .into_iter()
        .find(|p| p.asset_id == asset_id)
        .map(|p| p.price_usd)
        .unwrap_or(0.0);
    if price <= 0.0 {
        return Ok(0.0);
    }
    let human = raw / BigDecimal::new(BigInt::from(1), -(decimals as i64));
    Ok(human.to_f64().unwrap_or(0.0) * price)
}

fn stored(v: Option<&BigDecimal>) -> f64 {
    fmt_usd(v)
}

async fn usd_value(
    State(state): State<AppState>,
    Path(raw): Path<String>,
) -> Result<Json<Lookup>, ApiError> {
    let (block, index) = parse_extrinsic_id(&raw)
        .ok_or_else(|| ApiError::BadRequest("Invalid extrinsic ID".into()))?;
    let hash = sqlx::query_scalar!(
        r#"SELECT hash FROM sm.extrinsics WHERE block_height = $1 AND extrinsic_index = $2"#,
        block,
        index as i32
    )
    .fetch_optional(&state.db)
    .await?;
    let mut ids = vec![index.to_string(), format!("{block}-{index}")];
    if let Some(h) = hash {
        ids.push(h);
    }
    let none = Lookup {
        usd_value: None,
        source: None,
    };

    if let Some(t) = sqlx::query!(
        r#"SELECT usd_value AS "usd_value: BigDecimal", amount AS "amount!: BigDecimal", asset_id
           FROM sm.transfers WHERE block_height = $1 AND extrinsic_id = ANY($2) LIMIT 1"#,
        block,
        &ids
    )
    .fetch_optional(&state.db)
    .await?
    {
        let usd = stored(t.usd_value.as_ref());
        if usd > 0.0 {
            return Ok(Json(Lookup {
                usd_value: Some(usd),
                source: Some("transfer"),
            }));
        }
        let fb = fallback_usd(&state, &t.asset_id, &t.amount).await?;
        if fb > 0.0 {
            return Ok(Json(Lookup {
                usd_value: Some(fmt_usd(BigDecimal::try_from(fb).ok().as_ref())),
                source: Some("transfer"),
            }));
        }
    }

    if let Some(s) = sqlx::query!(
        r#"SELECT usd_value AS "in_usd: BigDecimal", output_usd_value AS "out_usd: BigDecimal",
                  input_amount AS "in_amount!: BigDecimal", output_amount AS "out_amount!: BigDecimal",
                  input_asset_id, output_asset_id
           FROM sm.swaps WHERE block_height = $1 AND extrinsic_id = ANY($2) LIMIT 1"#,
        block,
        &ids
    )
    .fetch_optional(&state.db)
    .await?
    {
        let usd = stored(s.in_usd.as_ref().filter(|v| v.to_f64().unwrap_or(0.0) != 0.0).or(s.out_usd.as_ref()));
        if usd > 0.0 {
            return Ok(Json(Lookup { usd_value: Some(usd), source: Some("swap") }));
        }
        let mut fb = fallback_usd(&state, &s.input_asset_id, &s.in_amount).await?;
        if fb <= 0.0 {
            fb = fallback_usd(&state, &s.output_asset_id, &s.out_amount).await?;
        }
        if fb > 0.0 {
            return Ok(Json(Lookup { usd_value: Some(fmt_usd(BigDecimal::try_from(fb).ok().as_ref())), source: Some("swap") }));
        }
    }

    if let Some(b) = sqlx::query!(
        r#"SELECT usd_value AS "usd_value: BigDecimal", amount AS "amount!: BigDecimal", asset_id
           FROM sm.bridges WHERE block_height = $1 AND extrinsic_id = ANY($2) LIMIT 1"#,
        block,
        &ids
    )
    .fetch_optional(&state.db)
    .await?
    {
        let usd = stored(b.usd_value.as_ref());
        if usd > 0.0 {
            return Ok(Json(Lookup {
                usd_value: Some(usd),
                source: Some("bridge"),
            }));
        }
        let fb = fallback_usd(&state, &b.asset_id, &b.amount).await?;
        if fb > 0.0 {
            return Ok(Json(Lookup {
                usd_value: Some(fmt_usd(BigDecimal::try_from(fb).ok().as_ref())),
                source: Some("bridge"),
            }));
        }
    }

    if let Some(l) = sqlx::query!(
        r#"SELECT usd_value AS "usd_value: BigDecimal"
           FROM sm.liquidity_events WHERE block_height = $1 AND extrinsic_id = ANY($2) LIMIT 1"#,
        block,
        &ids
    )
    .fetch_optional(&state.db)
    .await?
    {
        let usd = stored(l.usd_value.as_ref());
        if usd > 0.0 {
            return Ok(Json(Lookup {
                usd_value: Some(usd),
                source: Some("liquidity"),
            }));
        }
    }
    Ok(Json(none))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extrinsic_id_regex_matches_node() {
        assert_eq!(parse_extrinsic_id("27572842-1"), Some((27572842, 1)));
        assert_eq!(parse_extrinsic_id("abc"), None);
        assert_eq!(parse_extrinsic_id("1-"), None);
        assert_eq!(parse_extrinsic_id("0x12-3"), None);
    }
}
