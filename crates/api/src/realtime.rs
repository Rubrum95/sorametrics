//! Socket.IO realtime feed — the five server→client events `index.js`
//! emits (`new-block-stats`, `swaps-batch`, `transfers-batch`,
//! `extrinsics-batch`, `orderbook-batch`) with the row shapes
//! `pulse.jsx` parses.
//!
//! Source of truth is the database the ingest writes, tailed by
//! `block_height` (the Node emitted straight from its in-process
//! decoder). Every 5 s (`BATCH_INTERVAL_MS`, the Node's cadence) each
//! feed emits the rows indexed since the previous tick, at most 300 per
//! batch (`MAX_EVENTS_PER_BATCH`); a restart resumes from the current
//! head, never replaying history. Block stats follow the live cursor of
//! `sm.indexer_state` at 1 s granularity.

use crate::legacy::{decimals_for, fmt_amount, fmt_time, fmt_time_es, logo_for, symbol_for};
use crate::state::Registry;
use crate::AppState;
use bigdecimal::{BigDecimal, RoundingMode};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use socketioxide::SocketIo;
use std::collections::VecDeque;
use std::time::Duration;
use tracing::{debug, warn};

/// Batch cadence (`index.js` `BATCH_INTERVAL_MS`).
pub const BATCH_INTERVAL_MS: u64 = 5_000;
/// Rows per batch (`index.js` `MAX_EVENTS_PER_BATCH`).
pub const MAX_EVENTS_PER_BATCH: i64 = 300;
/// Block-stats poll period.
pub const BLOCK_POLL_MS: u64 = 1_000;
/// Rolling window of block intervals for `avgTime` (`BLOCK_TIMES_WINDOW`).
pub const BLOCK_TIMES_WINDOW: usize = 10;

const LIVE_CURSOR: &str = "substrate_live";

/// Starts the block-stats and batch emitters.
pub fn spawn(state: AppState, io: SocketIo) {
    tokio::spawn(block_stats_loop(state.clone(), io.clone()));
    tokio::spawn(batch_loop(state, io));
}

// ---------------------------------------------------------------------
// new-block-stats
// ---------------------------------------------------------------------

/// `{ block, finalized, avgTime }`; `avgTime` = mean of the last ten
/// block intervals in seconds with 3 decimals, `null` until two blocks.
#[derive(Serialize)]
struct BlockStats {
    block: i64,
    finalized: Option<i64>,
    #[serde(rename = "avgTime")]
    avg_time: Option<String>,
}

async fn block_stats_loop(state: AppState, io: SocketIo) {
    let mut last_block: Option<i64> = None;
    let mut last_ts: Option<DateTime<Utc>> = None;
    let mut intervals: VecDeque<i64> = VecDeque::with_capacity(BLOCK_TIMES_WINDOW + 1);
    let mut ticker = tokio::time::interval(Duration::from_millis(BLOCK_POLL_MS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let cursor = match sorametrics_db::sm::get_cursor(&state.db, LIVE_CURSOR).await {
            Ok(c) => c.map(|h| h.0 as i64),
            Err(e) => {
                warn!(error = %e, "realtime: live cursor read failed");
                continue;
            }
        };
        let Some(block) = cursor else { continue };
        if last_block.is_some_and(|lb| block <= lb) {
            continue;
        }
        if last_block.is_none() {
            // First observation: adopt the head without emitting.
            last_block = Some(block);
            last_ts = block_timestamp(&state, block).await;
            continue;
        }
        if let Some(ts) = block_timestamp(&state, block).await {
            if let Some(prev) = last_ts {
                intervals.push_back((ts - prev).num_milliseconds());
                while intervals.len() > BLOCK_TIMES_WINDOW {
                    intervals.pop_front();
                }
            }
            last_ts = Some(ts);
        }
        last_block = Some(block);
        let avg_time = (!intervals.is_empty()).then(|| {
            let mean = intervals.iter().sum::<i64>() as f64 / intervals.len() as f64 / 1000.0;
            format!("{mean:.3}")
        });
        let finalized = finalized_number(&state).await;
        let payload = BlockStats {
            block,
            finalized,
            avg_time,
        };
        if let Err(e) = io.emit("new-block-stats", &payload).await {
            debug!(error = %e, "realtime: new-block-stats emit failed");
        }
    }
}

/// On-chain time of a block from its `timestamp.set` inherent (index 0).
async fn block_timestamp(state: &AppState, block: i64) -> Option<DateTime<Utc>> {
    sqlx::query_scalar!(
        r#"SELECT block_timestamp FROM sm.extrinsics WHERE block_height = $1 AND extrinsic_index = 0"#,
        block
    )
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten()
}

/// Finalized head number via RPC (the Node: `getFinalizedHead` +
/// `getHeader`); `None` without a chain client or on RPC failure.
async fn finalized_number(state: &AppState) -> Option<i64> {
    let chain = state.chain.as_ref()?;
    let legacy = chain.legacy_rpc().await.ok()?;
    let hash = legacy.chain_get_finalized_head().await.ok()?;
    let client = chain.client().await.ok()?;
    let block = client.blocks().at(hash).await.ok()?;
    Some(i64::from(block.number()))
}

// ---------------------------------------------------------------------
// batches
// ---------------------------------------------------------------------

/// Per-feed tail position (highest `block_height` already emitted).
struct Tail {
    swaps: i64,
    transfers: i64,
    extrinsics: i64,
    order_book: i64,
}

async fn max_height(state: &AppState, table: &str) -> Result<i64, sqlx::Error> {
    let v: Option<i64> = match table {
        "swaps" => {
            sqlx::query_scalar!(r#"SELECT MAX(block_height) FROM sm.swaps"#)
                .fetch_one(&state.db)
                .await?
        }
        "transfers" => {
            sqlx::query_scalar!(r#"SELECT MAX(block_height) FROM sm.transfers"#)
                .fetch_one(&state.db)
                .await?
        }
        "extrinsics" => {
            sqlx::query_scalar!(r#"SELECT MAX(block_height) FROM sm.extrinsics"#)
                .fetch_one(&state.db)
                .await?
        }
        _ => {
            sqlx::query_scalar!(r#"SELECT MAX(block_height) FROM sm.order_book_events"#)
                .fetch_one(&state.db)
                .await?
        }
    };
    Ok(v.unwrap_or(0))
}

async fn batch_loop(state: AppState, io: SocketIo) {
    let mut tail = loop {
        let heads = tokio::try_join!(
            max_height(&state, "swaps"),
            max_height(&state, "transfers"),
            max_height(&state, "extrinsics"),
            max_height(&state, "order_book"),
        );
        match heads {
            Ok((swaps, transfers, extrinsics, order_book)) => {
                break Tail {
                    swaps,
                    transfers,
                    extrinsics,
                    order_book,
                }
            }
            Err(e) => {
                warn!(error = %e, "realtime: initial tail read failed, retrying");
                tokio::time::sleep(Duration::from_millis(BATCH_INTERVAL_MS)).await;
            }
        }
    };
    let mut ticker = tokio::time::interval(Duration::from_millis(BATCH_INTERVAL_MS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let zone = state.time_zone;
        match swaps_since(&state, tail.swaps).await {
            Ok((rows, head)) => emit(&io, "swaps-batch", &rows, &mut tail.swaps, head).await,
            Err(e) => warn!(error = %e, "realtime: swaps tail failed"),
        }
        let registry = state.registry.read().await;
        match transfers_since(&state, tail.transfers, &registry, zone).await {
            Ok((rows, head)) => {
                emit(&io, "transfers-batch", &rows, &mut tail.transfers, head).await
            }
            Err(e) => warn!(error = %e, "realtime: transfers tail failed"),
        }
        match extrinsics_since(&state, tail.extrinsics, zone).await {
            Ok((rows, head)) => {
                emit(&io, "extrinsics-batch", &rows, &mut tail.extrinsics, head).await
            }
            Err(e) => warn!(error = %e, "realtime: extrinsics tail failed"),
        }
        match order_book_since(&state, tail.order_book, &registry, zone).await {
            Ok((rows, head)) => {
                emit(&io, "orderbook-batch", &rows, &mut tail.order_book, head).await
            }
            Err(e) => warn!(error = %e, "realtime: order book tail failed"),
        }
    }
}

/// Emits a non-empty batch and advances the tail to the highest block
/// contained in it (rows of that block are complete: the ingest writes
/// a block atomically before moving the cursor).
async fn emit<T: Serialize>(
    io: &SocketIo,
    event: &str,
    rows: &[T],
    tail: &mut i64,
    head: Option<i64>,
) {
    if rows.is_empty() {
        return;
    }
    if let Err(e) = io.emit(event, &rows).await {
        debug!(error = %e, event, "realtime: emit failed");
        return;
    }
    if let Some(h) = head {
        *tail = h;
    }
}

fn usd2(v: Option<&BigDecimal>) -> String {
    v.map(|d| d.with_scale_round(2, RoundingMode::HalfUp).to_string())
        .unwrap_or_else(|| "0.00".to_string())
}

/// `BigNumber.toFormat(4)`: 4 decimals with thousands separators.
fn fmt_amount_grouped(planck: &BigDecimal, decimals: u32) -> String {
    let plain = fmt_amount(planck, decimals);
    let (int_part, frac) = plain.split_once('.').unwrap_or((plain.as_str(), ""));
    let (sign, digits) = match int_part.strip_prefix('-') {
        Some(d) => ("-", d),
        None => ("", int_part),
    };
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(c);
    }
    if frac.is_empty() {
        format!("{sign}{grouped}")
    } else {
        format!("{sign}{grouped}.{frac}")
    }
}

/// `parseOrderBookValue` for `{ inner }` balances: 6 decimals; empty
/// when the event carries no value (cancels).
fn ob_value(v: Option<&BigDecimal>) -> String {
    v.map(|d| d.with_scale_round(6, RoundingMode::HalfUp).to_string())
        .unwrap_or_default()
}

/// Live swap row (`index.js` `swapData`): `usd` legs are 2-decimal strings.
async fn swaps_since(
    state: &AppState,
    after: i64,
) -> Result<(Vec<Value>, Option<i64>), sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT block_height, extrinsic_id, hash, block_timestamp, caller,
               input_asset_id, input_amount, output_asset_id, output_amount,
               usd_value, output_usd_value
        FROM sm.swaps
        WHERE block_height > $1
        ORDER BY block_height, event_id
        LIMIT $2
        "#,
        after,
        MAX_EVENTS_PER_BATCH
    )
    .fetch_all(&state.db)
    .await?;
    let registry = state.registry.read().await;
    let zone = state.time_zone;
    let head = rows.last().map(|r| r.block_height);
    let leg = |asset: &str, amount: &BigDecimal, usd: Option<&BigDecimal>| {
        json!({
            "symbol": symbol_for(&registry, asset),
            "logo": logo_for(&registry, asset),
            "amount": fmt_amount(amount, decimals_for(&registry, asset)),
            "usd": usd2(usd),
        })
    };
    let out = rows
        .iter()
        .map(|r| {
            json!({
                "block": r.block_height,
                "wallet": r.caller,
                "time": fmt_time(r.block_timestamp, zone),
                "in": leg(&r.input_asset_id, &r.input_amount, r.usd_value.as_ref()),
                "out": leg(&r.output_asset_id, &r.output_amount, r.output_usd_value.as_ref()),
                "hash": r.hash.clone().unwrap_or_default(),
                "extrinsic_id": crate::legacy::fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
            })
        })
        .collect();
    Ok((out, head))
}

/// Live transfer row (`index.js` `transferData`): grouped amount,
/// 2-decimal `usdValue` string.
async fn transfers_since(
    state: &AppState,
    after: i64,
    registry: &Registry,
    zone: chrono_tz::Tz,
) -> Result<(Vec<Value>, Option<i64>), sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT block_height, extrinsic_id, hash, block_timestamp, from_address, to_address,
               asset_id, amount, usd_value
        FROM sm.transfers
        WHERE block_height > $1
        ORDER BY block_height, event_id
        LIMIT $2
        "#,
        after,
        MAX_EVENTS_PER_BATCH
    )
    .fetch_all(&state.db)
    .await?;
    let head = rows.last().map(|r| r.block_height);
    let out = rows
        .iter()
        .map(|r| {
            json!({
                "time": fmt_time(r.block_timestamp, zone),
                "from": r.from_address,
                "to": r.to_address,
                "amount": fmt_amount_grouped(&r.amount, decimals_for(registry, &r.asset_id)),
                "symbol": symbol_for(registry, &r.asset_id),
                "logo": logo_for(registry, &r.asset_id),
                "usdValue": usd2(r.usd_value.as_ref()),
                "assetId": r.asset_id,
                "block": r.block_height,
                "hash": r.hash.clone().unwrap_or_default(),
                "extrinsic_id": crate::legacy::fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
            })
        })
        .collect();
    Ok((out, head))
}

/// Live extrinsic row (`index.js` `pendingExtrinsicsBatch.push`): es-ES
/// time, boolean `success`, no args / events.
async fn extrinsics_since(
    state: &AppState,
    after: i64,
    zone: chrono_tz::Tz,
) -> Result<(Vec<Value>, Option<i64>), sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT block_height, extrinsic_index, block_timestamp, hash, section, method, signer, success, error_msg
        FROM sm.extrinsics
        WHERE block_height > $1
        ORDER BY block_height, extrinsic_index
        LIMIT $2
        "#,
        after,
        MAX_EVENTS_PER_BATCH
    )
    .fetch_all(&state.db)
    .await?;
    let head = rows.last().map(|r| r.block_height);
    let out = rows
        .iter()
        .map(|r| {
            json!({
                "time": fmt_time_es(r.block_timestamp, zone),
                "block": r.block_height,
                "extrinsic_index": r.extrinsic_index,
                "extrinsic_id": format!("{}-{}", r.block_height, r.extrinsic_index),
                "hash": r.hash,
                "section": r.section,
                "method": r.method,
                "signer": r.signer,
                "success": r.success,
                "error_msg": r.error_msg,
            })
        })
        .collect();
    Ok((out, head))
}

/// Live order-book row (`index.js` `pendingOrderBook.push`): es-ES time,
/// symbols, decimal strings, `usd_value: "0.00"`.
async fn order_book_since(
    state: &AppState,
    after: i64,
    registry: &Registry,
    zone: chrono_tz::Tz,
) -> Result<(Vec<Value>, Option<i64>), sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT block_height, extrinsic_id, hash, block_timestamp, event_type, wallet, order_id,
               base_asset_id, quote_asset_id, side, price, amount
        FROM sm.order_book_events
        WHERE block_height > $1
        ORDER BY block_height, event_id
        LIMIT $2
        "#,
        after,
        MAX_EVENTS_PER_BATCH
    )
    .fetch_all(&state.db)
    .await?;
    let head = rows.last().map(|r| r.block_height);
    let symbol = |id: &Option<String>| {
        id.as_deref()
            .map(|i| symbol_for(registry, i))
            .unwrap_or_default()
    };
    let out = rows
        .iter()
        .map(|r| {
            json!({
                "time": fmt_time_es(r.block_timestamp, zone),
                "block": r.block_height,
                "event_type": r.event_type,
                "wallet": r.wallet,
                "order_id": r.order_id.clone().unwrap_or_default(),
                "base_asset": symbol(&r.base_asset_id),
                "quote_asset": symbol(&r.quote_asset_id),
                "side": r.side.clone().unwrap_or_default(),
                "price": ob_value(r.price.as_ref()),
                "amount": ob_value(r.amount.as_ref()),
                "usd_value": "0.00",
                "hash": r.hash.clone().unwrap_or_default(),
                "extrinsic_id": crate::legacy::fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
            })
        })
        .collect();
    Ok((out, head))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn grouped_amount_like_bignumber_to_format() {
        let planck = BigDecimal::from_str("1234567890000000000000").unwrap();
        assert_eq!(fmt_amount_grouped(&planck, 18), "1,234.5679");
        let small = BigDecimal::from_str("5000000000000000000").unwrap();
        assert_eq!(fmt_amount_grouped(&small, 18), "5.0000");
        let big = BigDecimal::from_str("1000000000000000000000000").unwrap();
        assert_eq!(fmt_amount_grouped(&big, 18), "1,000,000.0000");
    }

    #[test]
    fn usd_and_order_book_strings() {
        assert_eq!(usd2(None), "0.00");
        assert_eq!(usd2(Some(&BigDecimal::from_str("3.2").unwrap())), "3.20");
        assert_eq!(usd2(Some(&BigDecimal::from_str("0.005").unwrap())), "0.01");
        assert_eq!(ob_value(None), "");
        assert_eq!(
            ob_value(Some(&BigDecimal::from_str("0.5").unwrap())),
            "0.500000"
        );
    }

    #[test]
    fn block_stats_shape() {
        let s = serde_json::to_string(&BlockStats {
            block: 5,
            finalized: None,
            avg_time: Some("6.012".into()),
        })
        .unwrap();
        assert_eq!(s, r#"{"block":5,"finalized":null,"avgTime":"6.012"}"#);
    }
}
