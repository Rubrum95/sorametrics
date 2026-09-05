//! Rendering helpers for the legacy (Node) response contract.
//!
//! The frontend is unchanged in v33, so every history row must carry
//! exactly the fields and formats `db_pg.js` produced:
//!
//! - `time`: `dd/mm/yyyy HH:MM:SS` in the server's zone (`formatTimestamp`).
//! - amounts: human units as a string with 4 decimals (`toFixed(4)`).
//! - USD: a number rounded to 1 decimal (`fmtUsd`), `0` when unknown.
//! - `extrinsic_id`: `"<block>-<index>"`; `hash`: `""` when unknown.
//! - `symbol`: registry symbol, else `"0x" + last 4 hex chars upper-cased`
//!   (the `mv_*` fallback); `logo`: registry logo, else `""`.

use crate::state::Registry;
use bigdecimal::{BigDecimal, RoundingMode, Zero};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use num_bigint::BigInt;
use sorametrics_db::sm::RegistryAsset;

/// `dd/mm/yyyy HH:MM:SS` in `zone` (Node: `formatTimestamp`).
pub fn fmt_time(ts: DateTime<Utc>, zone: Tz) -> String {
    ts.with_timezone(&zone)
        .format("%d/%m/%Y %H:%M:%S")
        .to_string()
}

/// Unix milliseconds as a decimal string (legacy bridge rows carry
/// `timestamp` this way).
pub fn fmt_millis(ts: DateTime<Utc>) -> String {
    ts.timestamp_millis().to_string()
}

/// Planck → human units, 4 decimals half-up, as a string (`toFixed(4)`).
pub fn fmt_amount(planck: &BigDecimal, decimals: u32) -> String {
    let scale = BigDecimal::new(BigInt::from(1), -(decimals as i64));
    let human = planck / scale;
    let rounded = human.with_scale_round(4, RoundingMode::HalfUp);
    // BigDecimal renders exact zero without its scale; normalise.
    if rounded.is_zero() {
        return "0.0000".to_string();
    }
    rounded.to_string()
}

/// USD to 1 decimal as a JSON number (`parseFloat(n.toFixed(1))`).
/// Unknown → `0`, matching the Node (`parseFloat(null) || 0`).
pub fn fmt_usd(usd: Option<&BigDecimal>) -> f64 {
    match usd {
        Some(v) => v
            .with_scale_round(1, RoundingMode::HalfUp)
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.0),
        None => 0.0,
    }
}

/// `"<block>-<index>"`. Legacy ETL rows already carry that form (or a
/// richer `block-idx-evt`); live rows store the bare in-block index.
pub fn fmt_extrinsic_id(block: i64, stored: &str) -> String {
    if stored.contains('-') {
        stored.to_string()
    } else {
        format!("{block}-{stored}")
    }
}

/// Symbol for an asset id: registry, else the `mv_*` fallback.
pub fn symbol_for(registry: &Registry, asset_id: &str) -> String {
    match registry.get(asset_id) {
        Some(a) => a.symbol.clone(),
        None => fallback_symbol(asset_id),
    }
}

/// `'0x' || UPPER(RIGHT(asset_id, 4))` — the legacy MV fallback.
pub fn fallback_symbol(asset_id: &str) -> String {
    let tail: String = asset_id
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("0x{}", tail.to_uppercase())
}

/// Logo for an asset id, `""` when unknown (Node: `_logoCache[s] || ''`).
pub fn logo_for(registry: &Registry, asset_id: &str) -> String {
    registry
        .get(asset_id)
        .and_then(|a| a.logo.clone())
        .unwrap_or_default()
}

/// Decimals for an asset id, 18 when unlisted (Node fallback).
pub fn decimals_for(registry: &Registry, asset_id: &str) -> u32 {
    registry
        .get(asset_id)
        .map(|a: &RegistryAsset| a.decimals.max(0) as u32)
        .unwrap_or(18)
}

/// Legacy bridge `direction` label from the `sm.bridge_direction` enum.
pub fn bridge_direction_label(direction: &str) -> &'static str {
    match direction {
        "in" => "Incoming",
        _ => "Outgoing",
    }
}

/// Legacy `(sender, recipient)` from v33's `(caller, counterparty)`:
/// the caller is always the SORA side; on an outgoing bridge it sends,
/// on an incoming one it receives.
pub fn bridge_parties(
    direction: &str,
    caller: &str,
    counterparty: Option<&str>,
) -> (String, String) {
    let other = counterparty.unwrap_or("").to_string();
    match direction {
        "in" => (other, caller.to_string()),
        _ => (caller.to_string(), other),
    }
}

/// Node: `Math.ceil(total / limit)` and `max(1, min(page, totalPages))`.
pub fn page_bounds(total: i64, limit: i64, page: i64) -> (i64, i64) {
    let total_pages = if limit > 0 {
        (total + limit - 1) / limit
    } else {
        0
    };
    let safe_page = page.min(total_pages).max(1);
    (total_pages, safe_page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn time_renders_in_madrid_like_the_node() {
        // Block 27546384 (2026-09-05T15:46:12Z) rendered by prod as
        // "05/09/2026 17:46:12" (CEST).
        let ts = DateTime::from_timestamp(1_788_623_172, 0).unwrap();
        assert_eq!(
            fmt_time(ts, chrono_tz::Europe::Madrid),
            "05/09/2026 17:46:12"
        );
    }

    #[test]
    fn amount_is_four_decimals_half_up() {
        // Prod transfer 27538740: 122234690120850746540 planck → "122.2347".
        let p = BigDecimal::from_str("122234690120850746540").unwrap();
        assert_eq!(fmt_amount(&p, 18), "122.2347");
        assert_eq!(fmt_amount(&BigDecimal::from(0), 18), "0.0000");
        assert_eq!(
            fmt_amount(&BigDecimal::from(1_000_000_000_000_000_000_u64), 18),
            "1.0000"
        );
        assert_eq!(fmt_amount(&BigDecimal::from(1_500_000_u64), 6), "1.5000");
    }

    #[test]
    fn usd_is_one_decimal_number() {
        let v = BigDecimal::from_str("23.633770").unwrap();
        assert_eq!(fmt_usd(Some(&v)), 23.6);
        let v = BigDecimal::from_str("0.05").unwrap();
        assert_eq!(fmt_usd(Some(&v)), 0.1);
        assert_eq!(fmt_usd(None), 0.0);
    }

    #[test]
    fn extrinsic_id_is_block_dash_index() {
        assert_eq!(fmt_extrinsic_id(27538740, "1"), "27538740-1");
        assert_eq!(fmt_extrinsic_id(27538740, "27538740-1"), "27538740-1");
    }

    #[test]
    fn fallback_symbol_is_last_four_upper() {
        assert_eq!(
            fallback_symbol("0x00513be6f0d4dfb8ab5f2eb9b3f1a0c2d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8ab"),
            "0xF8AB"
        );
    }

    #[test]
    fn bridge_parties_follow_direction() {
        assert_eq!(
            bridge_parties("out", "cnA", Some("0xB")),
            ("cnA".to_string(), "0xB".to_string())
        );
        assert_eq!(
            bridge_parties("in", "cnA", Some("0xB")),
            ("0xB".to_string(), "cnA".to_string())
        );
        assert_eq!(bridge_direction_label("in"), "Incoming");
        assert_eq!(bridge_direction_label("out"), "Outgoing");
    }

    #[test]
    fn page_bounds_match_node() {
        assert_eq!(page_bounds(4238, 25, 1), (170, 1));
        assert_eq!(page_bounds(4238, 25, 999), (170, 170));
        assert_eq!(page_bounds(0, 25, 3), (0, 1));
    }
}
