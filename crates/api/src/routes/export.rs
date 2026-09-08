//! `/export/csv` — the Node's wallet CSV export in its four formats.
//!
//! `?wallets=<ss58,…>` (≤ 50, shape-validated like the Node's
//! `VALID_SS58`, no checksum), `?types=<swaps,transfers,bridges,
//! liquidity,orderbook,extrinsics>` (unknown ones dropped), `?format=
//! sorametrics|koinly|cointracking|cointracker` (default sorametrics),
//! `?walletNames=<a,b,…>` (index-aligned with `wallets`), `?start` /
//! `?end` in unix ms (defaults 0 / now; `start ≥ end` → 400).
//!
//! Per type: rows of any of the wallets in `[start, end]`, newest
//! first, at most 50 000 (Node `getExportData`). The tax formats drop
//! `extrinsics`. Response: UTF-8 BOM + CSV, `text/csv; charset=utf-8`,
//! `Content-Disposition: attachment; filename="<name>_<YYYY-MM-DD>.csv"`.
//!
//! Cell values follow the Node's live-era stored text: amounts with 4
//! decimals (order book price/amount 6), USD with 2 decimals and `''`
//! when unknown or zero, dates in UTC (`YYYY-MM-DD HH:MM:SS`; CoinTracker
//! `MM/DD/YYYY HH:MM:SS`). Legacy-era rows in prod carried the MV's
//! 18-decimal text instead; v33 stores planck and prints them the same
//! way as live rows.

use crate::legacy::{
    bridge_direction_label, bridge_parties, decimals_for, fmt_amount, fmt_extrinsic_id, symbol_for,
};
use crate::state::Registry;
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use bigdecimal::{BigDecimal, RoundingMode, Zero};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::HashSet;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new().route("/export/csv", get(export_csv))
}

const ROW_LIMIT: i64 = 50_000;
const MAX_WALLETS: usize = 50;
const VALID_TYPES: [&str; 6] = [
    "swaps",
    "transfers",
    "bridges",
    "liquidity",
    "orderbook",
    "extrinsics",
];

/// Node `VALID_SS58 = /^[1-9A-HJ-NP-Za-km-z]{46,50}$/`.
fn is_ss58_shape(s: &str) -> bool {
    (46..=50).contains(&s.len())
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() && !matches!(c, '0' | 'O' | 'I' | 'l'))
}

/// Node `csvEsc`.
pub fn csv_esc(v: &str) -> String {
    if v.is_empty() {
        return String::new();
    }
    if v.contains(',') || v.contains('"') || v.contains('\n') {
        format!("\"{}\"", v.replace('"', "\"\""))
    } else {
        v.to_string()
    }
}

/// Node `fmtDateISO`: UTC `YYYY-MM-DD HH:MM:SS`.
pub fn fmt_date_iso(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Node `fmtDateUS`: UTC `MM/DD/YYYY HH:MM:SS`.
pub fn fmt_date_us(t: DateTime<Utc>) -> String {
    t.format("%m/%d/%Y %H:%M:%S").to_string()
}

/// Live-era USD text: 2 decimals; `''` when unknown or zero (the
/// Node's `r.usd || ''`).
pub fn fmt_usd_cell(v: Option<&BigDecimal>) -> String {
    match v {
        Some(v) if !v.is_zero() => {
            let r = v.with_scale_round(2, RoundingMode::HalfUp);
            if r.is_zero() {
                String::new()
            } else {
                r.to_string()
            }
        }
        _ => String::new(),
    }
}

/// Node: the stored 6-decimal order book text.
pub fn fmt_ob_number(v: Option<&BigDecimal>) -> String {
    match v {
        Some(v) => v.with_scale_round(6, RoundingMode::HalfUp).to_string(),
        None => "0".to_string(),
    }
}

/// `String(parseFloat(price) * parseFloat(amount))`, `''` when zero.
pub fn quote_amount_cell(price: &str, amount: &str) -> String {
    let p: f64 = price.parse().unwrap_or(0.0);
    let a: f64 = amount.parse().unwrap_or(0.0);
    let q = p * a;
    if q == 0.0 || !q.is_finite() {
        String::new()
    } else {
        js_number(q)
    }
}

/// `String(number)` for the finite values a CSV cell can hold.
fn js_number(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e21 {
        format!("{}", v as i128)
    } else {
        format!("{v}")
    }
}

// ---------------------------------------------------------------------
// Row models: the Node's export column sets, already rendered to text.
// ---------------------------------------------------------------------

/// `mv_swaps` export columns.
#[derive(Clone, Debug, PartialEq)]
struct SwapRow {
    timestamp: DateTime<Utc>,
    block: i64,
    wallet: String,
    in_symbol: String,
    in_amount: String,
    in_usd: String,
    out_symbol: String,
    out_amount: String,
    out_usd: String,
    hash: String,
    extrinsic_id: String,
}

/// `mv_transfers` export columns.
#[derive(Clone, Debug, PartialEq)]
struct TransferRow {
    timestamp: DateTime<Utc>,
    block: i64,
    from_addr: String,
    to_addr: String,
    amount: String,
    symbol: String,
    usd_value: String,
    hash: String,
    extrinsic_id: String,
}

/// `mv_bridges` export columns.
#[derive(Clone, Debug, PartialEq)]
struct BridgeRow {
    timestamp: DateTime<Utc>,
    block: i64,
    network: String,
    direction: String,
    sender: String,
    recipient: String,
    symbol: String,
    amount: String,
    usd_value: String,
    hash: String,
    extrinsic_id: String,
}

/// `mv_liquidity_events` export columns.
#[derive(Clone, Debug, PartialEq)]
struct LiquidityRow {
    timestamp: DateTime<Utc>,
    block: i64,
    wallet: String,
    pool_base: String,
    pool_target: String,
    base_amount: String,
    target_amount: String,
    usd_value: String,
    kind: String,
    hash: String,
    extrinsic_id: String,
}

/// `mv_order_book_events` export columns.
#[derive(Clone, Debug, PartialEq)]
struct OrderBookRow {
    timestamp: DateTime<Utc>,
    block: i64,
    event_type: String,
    wallet: String,
    base_asset: String,
    quote_asset: String,
    side: String,
    price: String,
    amount: String,
    usd_value: String,
    hash: String,
    extrinsic_id: String,
}

/// `mv_extrinsics` export columns.
#[derive(Clone, Debug, PartialEq)]
struct ExtrinsicRow {
    timestamp: DateTime<Utc>,
    block: i64,
    extrinsic_index: i32,
    hash: String,
    section: String,
    method: String,
    signer: String,
    success: bool,
}

/// Everything the formatters consume.
#[derive(Clone, Debug, Default)]
struct ExportData {
    swaps: Vec<SwapRow>,
    transfers: Vec<TransferRow>,
    bridges: Vec<BridgeRow>,
    liquidity: Vec<LiquidityRow>,
    orderbook: Vec<OrderBookRow>,
    extrinsics: Vec<ExtrinsicRow>,
}

impl LiquidityRow {
    /// Node: `type` contains `add` or `deposit`.
    fn is_add(&self) -> bool {
        let k = self.kind.to_lowercase();
        k.contains("add") || k.contains("deposit")
    }
    fn pool(&self) -> String {
        format!("{}/{}", self.pool_base, self.pool_target)
    }
}

impl OrderBookRow {
    fn is_buy(&self) -> bool {
        self.side.eq_ignore_ascii_case("buy")
    }
    fn quote_amount(&self) -> String {
        quote_amount_cell(&self.price, &self.amount)
    }
}

/// Node `getTxDirection`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    Out,
    In,
    Internal,
}

fn tx_direction(from: &str, to: &str, wallets: &HashSet<&str>) -> Direction {
    match (wallets.contains(from), wallets.contains(to)) {
        (true, true) => Direction::Internal,
        (true, false) => Direction::Out,
        _ => Direction::In,
    }
}

fn join(cells: &[String]) -> String {
    cells.join(",")
}

// ---------------------------------------------------------------------
// Format: SoraMetrics (per wallet, sectioned)
// ---------------------------------------------------------------------

fn wallet_label(addr: &str, name: &str) -> String {
    let head: String = addr.chars().take(8).collect();
    let tail: String = addr
        .chars()
        .rev()
        .take(6)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    if name.is_empty() {
        format!("{head}...{tail}")
    } else {
        format!("{head}...{tail} ({name})")
    }
}

fn section(title: &str, header: &str, rows: Vec<String>, out: &mut Vec<String>) {
    if rows.is_empty() {
        return;
    }
    out.push(format!("--- {title} ({}) ---", rows.len()));
    out.push(header.to_string());
    out.extend(rows);
    out.push(String::new());
}

/// Node `formatSorametrics`.
fn format_sorametrics(
    data: &ExportData,
    types: &[&str],
    wallets: &[String],
    names: &[String],
) -> String {
    let mut lines = Vec::new();
    for (i, addr) in wallets.iter().enumerate() {
        let name = names.get(i).map(String::as_str).unwrap_or("");
        lines.push(format!("=== WALLET: {} ===", wallet_label(addr, name)));
        for t in types {
            match *t {
                "swaps" => section(
                    "SWAPS",
                    "Date,Block,Wallet,In_Token,In_Amount,In_USD,Out_Token,Out_Amount,Out_USD,Hash,Extrinsic_ID",
                    data.swaps
                        .iter()
                        .filter(|r| r.wallet == *addr)
                        .map(|r| {
                            join(&[
                                fmt_date_iso(r.timestamp),
                                r.block.to_string(),
                                csv_esc(&r.wallet),
                                csv_esc(&r.in_symbol),
                                r.in_amount.clone(),
                                r.in_usd.clone(),
                                csv_esc(&r.out_symbol),
                                r.out_amount.clone(),
                                r.out_usd.clone(),
                                csv_esc(&r.hash),
                                csv_esc(&r.extrinsic_id),
                            ])
                        })
                        .collect(),
                    &mut lines,
                ),
                "transfers" => section(
                    "TRANSFERS",
                    "Date,Block,From,To,Token,Amount,USD_Value,Hash,Extrinsic_ID",
                    data.transfers
                        .iter()
                        .filter(|r| r.from_addr == *addr || r.to_addr == *addr)
                        .map(|r| {
                            join(&[
                                fmt_date_iso(r.timestamp),
                                r.block.to_string(),
                                csv_esc(&r.from_addr),
                                csv_esc(&r.to_addr),
                                csv_esc(&r.symbol),
                                r.amount.clone(),
                                r.usd_value.clone(),
                                csv_esc(&r.hash),
                                csv_esc(&r.extrinsic_id),
                            ])
                        })
                        .collect(),
                    &mut lines,
                ),
                "bridges" => section(
                    "BRIDGES",
                    "Date,Block,Network,Direction,From,To,Token,Amount,USD_Value,Hash,Extrinsic_ID",
                    data.bridges
                        .iter()
                        .filter(|r| r.sender == *addr || r.recipient == *addr)
                        .map(|r| {
                            join(&[
                                fmt_date_iso(r.timestamp),
                                r.block.to_string(),
                                csv_esc(&r.network),
                                csv_esc(&r.direction),
                                csv_esc(&r.sender),
                                csv_esc(&r.recipient),
                                csv_esc(&r.symbol),
                                r.amount.clone(),
                                r.usd_value.clone(),
                                csv_esc(&r.hash),
                                csv_esc(&r.extrinsic_id),
                            ])
                        })
                        .collect(),
                    &mut lines,
                ),
                "liquidity" => section(
                    "LIQUIDITY",
                    "Date,Block,Wallet,Pool_Base,Pool_Target,Base_Amount,Target_Amount,USD_Value,Action,Hash,Extrinsic_ID",
                    data.liquidity
                        .iter()
                        .filter(|r| r.wallet == *addr)
                        .map(|r| {
                            join(&[
                                fmt_date_iso(r.timestamp),
                                r.block.to_string(),
                                csv_esc(&r.wallet),
                                csv_esc(&r.pool_base),
                                csv_esc(&r.pool_target),
                                r.base_amount.clone(),
                                r.target_amount.clone(),
                                r.usd_value.clone(),
                                csv_esc(&r.kind),
                                csv_esc(&r.hash),
                                csv_esc(&r.extrinsic_id),
                            ])
                        })
                        .collect(),
                    &mut lines,
                ),
                "orderbook" => section(
                    "ORDER BOOK",
                    "Date,Block,Wallet,Event,Base_Asset,Quote_Asset,Side,Price,Amount,USD_Value,Hash,Extrinsic_ID",
                    data.orderbook
                        .iter()
                        .filter(|r| r.wallet == *addr)
                        .map(|r| {
                            join(&[
                                fmt_date_iso(r.timestamp),
                                r.block.to_string(),
                                csv_esc(&r.wallet),
                                csv_esc(&r.event_type),
                                csv_esc(&r.base_asset),
                                csv_esc(&r.quote_asset),
                                csv_esc(&r.side),
                                r.price.clone(),
                                r.amount.clone(),
                                r.usd_value.clone(),
                                csv_esc(&r.hash),
                                csv_esc(&r.extrinsic_id),
                            ])
                        })
                        .collect(),
                    &mut lines,
                ),
                "extrinsics" => section(
                    "EXTRINSICS",
                    "Date,Block,Extrinsic_ID,Signer,Pallet,Method,Result,Hash",
                    data.extrinsics
                        .iter()
                        .filter(|r| r.signer == *addr)
                        .map(|r| {
                            join(&[
                                fmt_date_iso(r.timestamp),
                                r.block.to_string(),
                                format!("{}-{}", r.block, r.extrinsic_index),
                                csv_esc(&r.signer),
                                csv_esc(&r.section),
                                csv_esc(&r.method),
                                if r.success { "Success" } else { "Failed" }.to_string(),
                                csv_esc(&r.hash),
                            ])
                        })
                        .collect(),
                    &mut lines,
                ),
                _ => {}
            }
        }
        lines.push(String::new());
    }
    lines.join("\n")
}

// ---------------------------------------------------------------------
// Format: Koinly
// ---------------------------------------------------------------------

/// Node `formatKoinly`.
fn format_koinly(data: &ExportData, types: &[&str], wallets: &[String]) -> String {
    let set: HashSet<&str> = wallets.iter().map(String::as_str).collect();
    let mut rows = vec!["Date,Sent Amount,Sent Currency,Received Amount,Received Currency,Fee Amount,Fee Currency,Net Worth Amount,Net Worth Currency,TxHash,Description".to_string()];
    let k = |date: String,
             sent_amt: &str,
             sent_cur: &str,
             rcv_amt: &str,
             rcv_cur: &str,
             nw_amt: &str,
             nw_cur: &str,
             hash: &str,
             desc: &str| {
        join(&[
            date,
            sent_amt.to_string(),
            csv_esc(sent_cur),
            rcv_amt.to_string(),
            csv_esc(rcv_cur),
            String::new(),
            String::new(),
            nw_amt.to_string(),
            csv_esc(nw_cur),
            csv_esc(hash),
            csv_esc(desc),
        ])
    };
    if types.contains(&"swaps") {
        for r in &data.swaps {
            rows.push(k(
                fmt_date_iso(r.timestamp),
                &r.out_amount,
                &r.out_symbol,
                &r.in_amount,
                &r.in_symbol,
                &r.in_usd,
                "USD",
                &r.hash,
                &format!("Swap {} -> {}", r.out_symbol, r.in_symbol),
            ));
        }
    }
    if types.contains(&"transfers") {
        for r in &data.transfers {
            let d = fmt_date_iso(r.timestamp);
            rows.push(match tx_direction(&r.from_addr, &r.to_addr, &set) {
                Direction::Out => k(
                    d,
                    &r.amount,
                    &r.symbol,
                    "",
                    "",
                    &r.usd_value,
                    "USD",
                    &r.hash,
                    "Transfer out",
                ),
                Direction::In => k(
                    d,
                    "",
                    "",
                    &r.amount,
                    &r.symbol,
                    &r.usd_value,
                    "USD",
                    &r.hash,
                    "Transfer in",
                ),
                Direction::Internal => k(
                    d,
                    &r.amount,
                    &r.symbol,
                    "",
                    "",
                    &r.usd_value,
                    "USD",
                    &r.hash,
                    "Internal transfer",
                ),
            });
        }
    }
    if types.contains(&"bridges") {
        for r in &data.bridges {
            let d = fmt_date_iso(r.timestamp);
            rows.push(match tx_direction(&r.sender, &r.recipient, &set) {
                Direction::Out => k(
                    d,
                    &r.amount,
                    &r.symbol,
                    "",
                    "",
                    &r.usd_value,
                    "USD",
                    &r.hash,
                    &format!("Bridge out ({})", r.network),
                ),
                Direction::In => k(
                    d,
                    "",
                    "",
                    &r.amount,
                    &r.symbol,
                    &r.usd_value,
                    "USD",
                    &r.hash,
                    &format!("Bridge in ({})", r.network),
                ),
                Direction::Internal => k(
                    d,
                    &r.amount,
                    &r.symbol,
                    "",
                    "",
                    &r.usd_value,
                    "USD",
                    &r.hash,
                    &format!("Bridge internal ({})", r.network),
                ),
            });
        }
    }
    if types.contains(&"liquidity") {
        for r in &data.liquidity {
            let d = fmt_date_iso(r.timestamp);
            let pool = r.pool();
            if r.is_add() {
                let desc = format!("Provide Liquidity {pool}");
                if !r.base_amount.is_empty() {
                    rows.push(k(
                        d.clone(),
                        &r.base_amount,
                        &r.pool_base,
                        "",
                        "",
                        "",
                        "",
                        &r.hash,
                        &desc,
                    ));
                }
                if !r.target_amount.is_empty() {
                    rows.push(k(
                        d,
                        &r.target_amount,
                        &r.pool_target,
                        "",
                        "",
                        "",
                        "",
                        &r.hash,
                        &desc,
                    ));
                }
            } else {
                let desc = format!("Remove Liquidity {pool}");
                if !r.base_amount.is_empty() {
                    rows.push(k(
                        d.clone(),
                        "",
                        "",
                        &r.base_amount,
                        &r.pool_base,
                        "",
                        "",
                        &r.hash,
                        &desc,
                    ));
                }
                if !r.target_amount.is_empty() {
                    rows.push(k(
                        d,
                        "",
                        "",
                        &r.target_amount,
                        &r.pool_target,
                        "",
                        "",
                        &r.hash,
                        &desc,
                    ));
                }
            }
        }
    }
    if types.contains(&"orderbook") {
        for r in &data.orderbook {
            let d = fmt_date_iso(r.timestamp);
            let q = r.quote_amount();
            let desc = format!("Order Book {}", r.event_type);
            rows.push(if r.is_buy() {
                k(
                    d,
                    &q,
                    &r.quote_asset,
                    &r.amount,
                    &r.base_asset,
                    &r.usd_value,
                    "USD",
                    &r.hash,
                    &desc,
                )
            } else {
                k(
                    d,
                    &r.amount,
                    &r.base_asset,
                    &q,
                    &r.quote_asset,
                    &r.usd_value,
                    "USD",
                    &r.hash,
                    &desc,
                )
            });
        }
    }
    rows.join("\n")
}

// ---------------------------------------------------------------------
// Format: CoinTracking
// ---------------------------------------------------------------------

/// Node `formatCoinTracking`.
fn format_cointracking(data: &ExportData, types: &[&str], wallets: &[String]) -> String {
    let set: HashSet<&str> = wallets.iter().map(String::as_str).collect();
    let mut rows = vec![
        "\"Type\",\"Buy\",\"Cur.\",\"Sell\",\"Cur.\",\"Fee\",\"Cur.\",\"Exchange\",\"Group\",\"Comment\",\"Date\",\"Tx-ID\"".to_string(),
    ];
    let ct = |kind: &str,
              buy_amt: &str,
              buy_cur: &str,
              sell_amt: &str,
              sell_cur: &str,
              comment: &str,
              date: String,
              tx: &str| {
        join(&[
            csv_esc(kind),
            buy_amt.to_string(),
            csv_esc(buy_cur),
            sell_amt.to_string(),
            csv_esc(sell_cur),
            String::new(),
            String::new(),
            "\"SORA DEX\"".to_string(),
            String::new(),
            csv_esc(comment),
            csv_esc(&date),
            csv_esc(tx),
        ])
    };
    if types.contains(&"swaps") {
        for r in &data.swaps {
            rows.push(ct(
                "Trade",
                &r.in_amount,
                &r.in_symbol,
                &r.out_amount,
                &r.out_symbol,
                "Swap on SORA",
                fmt_date_iso(r.timestamp),
                &r.hash,
            ));
        }
    }
    if types.contains(&"transfers") {
        for r in &data.transfers {
            let d = fmt_date_iso(r.timestamp);
            rows.push(match tx_direction(&r.from_addr, &r.to_addr, &set) {
                Direction::Out => ct(
                    "Withdrawal",
                    "",
                    "",
                    &r.amount,
                    &r.symbol,
                    "Transfer out",
                    d,
                    &r.hash,
                ),
                Direction::In => ct(
                    "Deposit",
                    &r.amount,
                    &r.symbol,
                    "",
                    "",
                    "Transfer in",
                    d,
                    &r.hash,
                ),
                Direction::Internal => ct(
                    "Withdrawal",
                    "",
                    "",
                    &r.amount,
                    &r.symbol,
                    "Internal transfer",
                    d,
                    &r.hash,
                ),
            });
        }
    }
    if types.contains(&"bridges") {
        for r in &data.bridges {
            let d = fmt_date_iso(r.timestamp);
            rows.push(match tx_direction(&r.sender, &r.recipient, &set) {
                Direction::Out => ct(
                    "Withdrawal",
                    "",
                    "",
                    &r.amount,
                    &r.symbol,
                    &format!("Bridge to {}", r.network),
                    d,
                    &r.hash,
                ),
                Direction::In => ct(
                    "Deposit",
                    &r.amount,
                    &r.symbol,
                    "",
                    "",
                    &format!("Bridge from {}", r.network),
                    d,
                    &r.hash,
                ),
                Direction::Internal => ct(
                    "Withdrawal",
                    "",
                    "",
                    &r.amount,
                    &r.symbol,
                    &format!("Bridge internal {}", r.network),
                    d,
                    &r.hash,
                ),
            });
        }
    }
    if types.contains(&"liquidity") {
        for r in &data.liquidity {
            let d = fmt_date_iso(r.timestamp);
            let pool = r.pool();
            if r.is_add() {
                let c = format!("Add LP {pool}");
                if !r.base_amount.is_empty() {
                    rows.push(ct(
                        "Provide Liquidity",
                        "",
                        "",
                        &r.base_amount,
                        &r.pool_base,
                        &c,
                        d.clone(),
                        &r.hash,
                    ));
                }
                if !r.target_amount.is_empty() {
                    rows.push(ct(
                        "Provide Liquidity",
                        "",
                        "",
                        &r.target_amount,
                        &r.pool_target,
                        &c,
                        d,
                        &r.hash,
                    ));
                }
            } else {
                let c = format!("Remove LP {pool}");
                if !r.base_amount.is_empty() {
                    rows.push(ct(
                        "Remove Liquidity",
                        &r.base_amount,
                        &r.pool_base,
                        "",
                        "",
                        &c,
                        d.clone(),
                        &r.hash,
                    ));
                }
                if !r.target_amount.is_empty() {
                    rows.push(ct(
                        "Remove Liquidity",
                        &r.target_amount,
                        &r.pool_target,
                        "",
                        "",
                        &c,
                        d,
                        &r.hash,
                    ));
                }
            }
        }
    }
    if types.contains(&"orderbook") {
        for r in &data.orderbook {
            let d = fmt_date_iso(r.timestamp);
            let q = r.quote_amount();
            let c = format!("Order Book {}", r.event_type);
            rows.push(if r.is_buy() {
                ct(
                    "Trade",
                    &r.amount,
                    &r.base_asset,
                    &q,
                    &r.quote_asset,
                    &c,
                    d,
                    &r.hash,
                )
            } else {
                ct(
                    "Trade",
                    &q,
                    &r.quote_asset,
                    &r.amount,
                    &r.base_asset,
                    &c,
                    d,
                    &r.hash,
                )
            });
        }
    }
    rows.join("\n")
}

// ---------------------------------------------------------------------
// Format: CoinTracker
// ---------------------------------------------------------------------

/// Node `formatCoinTracker`.
fn format_cointracker(data: &ExportData, types: &[&str], wallets: &[String]) -> String {
    let set: HashSet<&str> = wallets.iter().map(String::as_str).collect();
    let mut rows = vec![
        "Date,Received Quantity,Received Currency,Sent Quantity,Sent Currency,Fee Amount,Fee Currency,Tag".to_string(),
    ];
    let ck = |date: String, rcv_amt: &str, rcv_cur: &str, sent_amt: &str, sent_cur: &str| {
        join(&[
            date,
            rcv_amt.to_string(),
            csv_esc(rcv_cur),
            sent_amt.to_string(),
            csv_esc(sent_cur),
            String::new(),
            String::new(),
            String::new(),
        ])
    };
    if types.contains(&"swaps") {
        for r in &data.swaps {
            rows.push(ck(
                fmt_date_us(r.timestamp),
                &r.in_amount,
                &r.in_symbol,
                &r.out_amount,
                &r.out_symbol,
            ));
        }
    }
    if types.contains(&"transfers") {
        for r in &data.transfers {
            let d = fmt_date_us(r.timestamp);
            rows.push(match tx_direction(&r.from_addr, &r.to_addr, &set) {
                Direction::In => ck(d, &r.amount, &r.symbol, "", ""),
                _ => ck(d, "", "", &r.amount, &r.symbol),
            });
        }
    }
    if types.contains(&"bridges") {
        for r in &data.bridges {
            let d = fmt_date_us(r.timestamp);
            rows.push(match tx_direction(&r.sender, &r.recipient, &set) {
                Direction::In => ck(d, &r.amount, &r.symbol, "", ""),
                _ => ck(d, "", "", &r.amount, &r.symbol),
            });
        }
    }
    if types.contains(&"liquidity") {
        for r in &data.liquidity {
            let d = fmt_date_us(r.timestamp);
            if r.is_add() {
                if !r.base_amount.is_empty() {
                    rows.push(ck(d.clone(), "", "", &r.base_amount, &r.pool_base));
                }
                if !r.target_amount.is_empty() {
                    rows.push(ck(d, "", "", &r.target_amount, &r.pool_target));
                }
            } else {
                if !r.base_amount.is_empty() {
                    rows.push(ck(d.clone(), &r.base_amount, &r.pool_base, "", ""));
                }
                if !r.target_amount.is_empty() {
                    rows.push(ck(d, &r.target_amount, &r.pool_target, "", ""));
                }
            }
        }
    }
    if types.contains(&"orderbook") {
        for r in &data.orderbook {
            let d = fmt_date_us(r.timestamp);
            let q = r.quote_amount();
            rows.push(if r.is_buy() {
                ck(d, &r.amount, &r.base_asset, &q, &r.quote_asset)
            } else {
                ck(d, &q, &r.quote_asset, &r.amount, &r.base_asset)
            });
        }
    }
    rows.join("\n")
}

// ---------------------------------------------------------------------
// Data access
// ---------------------------------------------------------------------

struct Window {
    wallets: Vec<String>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

async fn load_swaps(
    state: &AppState,
    w: &Window,
    reg: &Registry,
) -> Result<Vec<SwapRow>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT block_height, extrinsic_id, block_timestamp, caller, hash,
               input_asset_id, input_amount AS "input_amount!: BigDecimal",
               output_asset_id, output_amount AS "output_amount!: BigDecimal",
               usd_value AS "usd_value: BigDecimal",
               output_usd_value AS "output_usd_value: BigDecimal"
        FROM sm.swaps
        WHERE caller = ANY($1::text[]) AND block_timestamp >= $2 AND block_timestamp <= $3
        ORDER BY block_timestamp DESC, block_height DESC, event_id DESC
        LIMIT $4
        "#,
        &w.wallets,
        w.start,
        w.end,
        ROW_LIMIT,
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| SwapRow {
            timestamp: r.block_timestamp,
            block: r.block_height,
            wallet: r.caller,
            in_symbol: symbol_for(reg, &r.input_asset_id),
            in_amount: fmt_amount(&r.input_amount, decimals_for(reg, &r.input_asset_id)),
            in_usd: fmt_usd_cell(r.usd_value.as_ref()),
            out_symbol: symbol_for(reg, &r.output_asset_id),
            out_amount: fmt_amount(&r.output_amount, decimals_for(reg, &r.output_asset_id)),
            out_usd: fmt_usd_cell(r.output_usd_value.as_ref()),
            hash: r.hash.unwrap_or_default(),
            extrinsic_id: fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
        })
        .collect())
}

async fn load_transfers(
    state: &AppState,
    w: &Window,
    reg: &Registry,
) -> Result<Vec<TransferRow>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT block_height, extrinsic_id, block_timestamp, from_address, to_address, hash,
               asset_id, amount AS "amount!: BigDecimal", usd_value AS "usd_value: BigDecimal"
        FROM sm.transfers
        WHERE (from_address = ANY($1::text[]) OR to_address = ANY($1::text[]))
          AND block_timestamp >= $2 AND block_timestamp <= $3
        ORDER BY block_timestamp DESC, block_height DESC, event_id DESC
        LIMIT $4
        "#,
        &w.wallets,
        w.start,
        w.end,
        ROW_LIMIT,
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| TransferRow {
            timestamp: r.block_timestamp,
            block: r.block_height,
            from_addr: r.from_address,
            to_addr: r.to_address,
            amount: fmt_amount(&r.amount, decimals_for(reg, &r.asset_id)),
            symbol: symbol_for(reg, &r.asset_id),
            usd_value: fmt_usd_cell(r.usd_value.as_ref()),
            hash: r.hash.unwrap_or_default(),
            extrinsic_id: fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
        })
        .collect())
}

async fn load_bridges(
    state: &AppState,
    w: &Window,
    reg: &Registry,
) -> Result<Vec<BridgeRow>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT block_height, extrinsic_id, block_timestamp, network, caller, counterparty, hash,
               direction::text AS "direction!", asset_id,
               amount AS "amount!: BigDecimal", usd_value AS "usd_value: BigDecimal"
        FROM sm.bridges
        WHERE (caller = ANY($1::text[]) OR counterparty = ANY($1::text[]))
          AND block_timestamp >= $2 AND block_timestamp <= $3
        ORDER BY block_timestamp DESC, block_height DESC, event_id DESC
        LIMIT $4
        "#,
        &w.wallets,
        w.start,
        w.end,
        ROW_LIMIT,
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let (sender, recipient) =
                bridge_parties(&r.direction, &r.caller, r.counterparty.as_deref());
            BridgeRow {
                timestamp: r.block_timestamp,
                block: r.block_height,
                network: r.network,
                direction: bridge_direction_label(&r.direction).to_string(),
                sender,
                recipient,
                symbol: symbol_for(reg, &r.asset_id),
                amount: fmt_amount(&r.amount, decimals_for(reg, &r.asset_id)),
                usd_value: fmt_usd_cell(r.usd_value.as_ref()),
                hash: r.hash.unwrap_or_default(),
                extrinsic_id: fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
            }
        })
        .collect())
}

async fn load_liquidity(
    state: &AppState,
    w: &Window,
    reg: &Registry,
) -> Result<Vec<LiquidityRow>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT block_height, extrinsic_id, block_timestamp, caller, hash, kind,
               base_asset_id, target_asset_id,
               base_amount AS "base_amount!: BigDecimal",
               target_amount AS "target_amount!: BigDecimal",
               usd_value AS "usd_value: BigDecimal"
        FROM sm.liquidity_events
        WHERE caller = ANY($1::text[]) AND block_timestamp >= $2 AND block_timestamp <= $3
        ORDER BY block_timestamp DESC, block_height DESC, event_id DESC
        LIMIT $4
        "#,
        &w.wallets,
        w.start,
        w.end,
        ROW_LIMIT,
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| LiquidityRow {
            timestamp: r.block_timestamp,
            block: r.block_height,
            wallet: r.caller,
            pool_base: symbol_for(reg, &r.base_asset_id),
            pool_target: symbol_for(reg, &r.target_asset_id),
            base_amount: fmt_amount(&r.base_amount, decimals_for(reg, &r.base_asset_id)),
            target_amount: fmt_amount(&r.target_amount, decimals_for(reg, &r.target_asset_id)),
            usd_value: fmt_usd_cell(r.usd_value.as_ref()),
            kind: r.kind,
            hash: r.hash.unwrap_or_default(),
            extrinsic_id: fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
        })
        .collect())
}

async fn load_orderbook(
    state: &AppState,
    w: &Window,
    reg: &Registry,
) -> Result<Vec<OrderBookRow>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT block_height, extrinsic_id, block_timestamp, event_type, wallet, hash,
               base_asset_id, quote_asset_id, side,
               price AS "price: BigDecimal", amount AS "amount: BigDecimal",
               usd_value AS "usd_value: BigDecimal"
        FROM sm.order_book_events
        WHERE wallet = ANY($1::text[]) AND block_timestamp >= $2 AND block_timestamp <= $3
        ORDER BY block_timestamp DESC, block_height DESC, event_id DESC
        LIMIT $4
        "#,
        &w.wallets,
        w.start,
        w.end,
        ROW_LIMIT,
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| OrderBookRow {
            timestamp: r.block_timestamp,
            block: r.block_height,
            event_type: r.event_type,
            wallet: r.wallet,
            base_asset: r
                .base_asset_id
                .as_deref()
                .map(|id| symbol_for(reg, id))
                .unwrap_or_default(),
            quote_asset: r
                .quote_asset_id
                .as_deref()
                .map(|id| symbol_for(reg, id))
                .unwrap_or_default(),
            side: r.side.unwrap_or_default(),
            price: fmt_ob_number(r.price.as_ref()),
            amount: fmt_ob_number(r.amount.as_ref()),
            usd_value: fmt_usd_cell(r.usd_value.as_ref()),
            hash: r.hash.unwrap_or_default(),
            extrinsic_id: fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
        })
        .collect())
}

async fn load_extrinsics(state: &AppState, w: &Window) -> Result<Vec<ExtrinsicRow>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT block_height, extrinsic_index, block_timestamp, hash, section, method, signer, success
        FROM sm.extrinsics
        WHERE signer = ANY($1::text[]) AND block_timestamp >= $2 AND block_timestamp <= $3
        ORDER BY block_timestamp DESC, block_height DESC, extrinsic_index DESC
        LIMIT $4
        "#,
        &w.wallets,
        w.start,
        w.end,
        ROW_LIMIT,
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| ExtrinsicRow {
            timestamp: r.block_timestamp,
            block: r.block_height,
            extrinsic_index: r.extrinsic_index,
            hash: r.hash,
            section: r.section,
            method: r.method,
            signer: r.signer,
            success: r.success,
        })
        .collect())
}

// ---------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct ExportQuery {
    wallets: Option<String>,
    types: Option<String>,
    format: Option<String>,
    #[serde(rename = "walletNames")]
    wallet_names: Option<String>,
    start: Option<String>,
    end: Option<String>,
}

/// Node: `parseInt(x) || default`.
fn parse_ms(raw: Option<&str>, default: i64) -> i64 {
    raw.and_then(|s| s.trim().parse::<i64>().ok())
        .filter(|v| *v != 0)
        .unwrap_or(default)
}

fn csv_filename(format: &str, today: DateTime<Utc>) -> String {
    let stem = match format {
        "koinly" => "koinly_import",
        "cointracking" => "cointracking_import",
        "cointracker" => "cointracker_import",
        _ => "sorametrics_export",
    };
    format!("{stem}_{}.csv", today.format("%Y-%m-%d"))
}

async fn export_csv(
    State(state): State<AppState>,
    Query(q): Query<ExportQuery>,
) -> Result<Response, ApiError> {
    let wallets: Vec<String> = q
        .wallets
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|w| is_ss58_shape(w))
        .map(str::to_string)
        .collect();
    let types: Vec<&str> = q
        .types
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|t| VALID_TYPES.contains(t))
        .collect();
    let format = q
        .format
        .as_deref()
        .filter(|f| matches!(*f, "koinly" | "cointracking" | "cointracker"))
        .unwrap_or("sorametrics");
    let names: Vec<String> = q
        .wallet_names
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(str::to_string)
        .collect();
    let now = Utc::now();
    let start_ms = parse_ms(q.start.as_deref(), 0);
    let end_ms = parse_ms(q.end.as_deref(), now.timestamp_millis());

    if wallets.is_empty() {
        return Err(ApiError::BadRequest("No valid wallets".into()));
    }
    if wallets.len() > MAX_WALLETS {
        return Err(ApiError::BadRequest("Max 50 wallets".into()));
    }
    if types.is_empty() {
        return Err(ApiError::BadRequest("No valid types".into()));
    }
    if start_ms >= end_ms {
        return Err(ApiError::BadRequest("Invalid date range".into()));
    }
    let start = DateTime::from_timestamp_millis(start_ms)
        .ok_or_else(|| ApiError::BadRequest("start out of range".into()))?;
    let end = DateTime::from_timestamp_millis(end_ms)
        .ok_or_else(|| ApiError::BadRequest("end out of range".into()))?;

    // Tax formats ignore extrinsics (no financial value).
    let types: Vec<&str> = if format == "sorametrics" {
        types
    } else {
        types.into_iter().filter(|t| *t != "extrinsics").collect()
    };

    let w = Window {
        wallets: wallets.clone(),
        start,
        end,
    };
    let mut data = ExportData::default();
    {
        let reg = state.registry.read().await;
        for t in &types {
            match *t {
                "swaps" => data.swaps = load_swaps(&state, &w, &reg).await?,
                "transfers" => data.transfers = load_transfers(&state, &w, &reg).await?,
                "bridges" => data.bridges = load_bridges(&state, &w, &reg).await?,
                "liquidity" => data.liquidity = load_liquidity(&state, &w, &reg).await?,
                "orderbook" => data.orderbook = load_orderbook(&state, &w, &reg).await?,
                "extrinsics" => data.extrinsics = load_extrinsics(&state, &w).await?,
                _ => {}
            }
        }
    }

    let csv = match format {
        "koinly" => format_koinly(&data, &types, &wallets),
        "cointracking" => format_cointracking(&data, &types, &wallets),
        "cointracker" => format_cointracker(&data, &types, &wallets),
        _ => format_sorametrics(&data, &types, &wallets, &names),
    };
    let filename = csv_filename(format, now);
    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
        .map_err(|e| ApiError::Internal(format!("content-disposition: {e}")))?;
    let body = format!("\u{FEFF}{csv}");
    Ok((
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/csv; charset=utf-8"),
            ),
            (header::CONTENT_DISPOSITION, disposition),
        ],
        body,
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    const BOT: &str = "cnSBot6vTtpckdo7HQhjk2hiAAKfDJX34DZsyLVs7B5Y2Gv6T";
    const HASH1: &str = "0x38989790407134fbf8859a44354d95b944753e8e779be6077b90ed55384e8e04";
    const HASH2: &str = "0x93f4e4193d3336721d67e9fb591e5c195006876329ad3efe394987ed38a87f8b";

    /// The two prod swaps of 2026-09-08 (golden export captured live).
    fn golden() -> ExportData {
        ExportData {
            swaps: vec![
                SwapRow {
                    timestamp: ts("2026-09-08T03:47:18Z"),
                    block: 27574036,
                    wallet: BOT.into(),
                    in_symbol: "DAI".into(),
                    in_amount: "8.7144".into(),
                    in_usd: "8.71".into(),
                    out_symbol: "XOR".into(),
                    out_amount: "2.1000".into(),
                    out_usd: "9.18".into(),
                    hash: HASH1.into(),
                    extrinsic_id: "27574036-1".into(),
                },
                SwapRow {
                    timestamp: ts("2026-09-08T03:46:54Z"),
                    block: 27574034,
                    wallet: BOT.into(),
                    in_symbol: "XOR".into(),
                    in_amount: "4.0000".into(),
                    in_usd: "17.48".into(),
                    out_symbol: "DAI".into(),
                    out_amount: "16.7394".into(),
                    out_usd: "16.74".into(),
                    hash: HASH2.into(),
                    extrinsic_id: "27574034-1".into(),
                },
            ],
            extrinsics: vec![
                ExtrinsicRow {
                    timestamp: ts("2026-09-08T03:47:18Z"),
                    block: 27574036,
                    extrinsic_index: 1,
                    hash: HASH1.into(),
                    section: "liquidityProxy".into(),
                    method: "swap".into(),
                    signer: BOT.into(),
                    success: true,
                },
                ExtrinsicRow {
                    timestamp: ts("2026-09-08T03:46:54Z"),
                    block: 27574034,
                    extrinsic_index: 1,
                    hash: HASH2.into(),
                    section: "liquidityProxy".into(),
                    method: "swap".into(),
                    signer: BOT.into(),
                    success: true,
                },
            ],
            ..Default::default()
        }
    }

    #[test]
    fn sorametrics_matches_prod_golden() {
        let out = format_sorametrics(
            &golden(),
            &["swaps", "transfers", "extrinsics"],
            &[BOT.into()],
            &[],
        );
        let expected = format!(
            "=== WALLET: cnSBot6v...Y2Gv6T ===\n\
             --- SWAPS (2) ---\n\
             Date,Block,Wallet,In_Token,In_Amount,In_USD,Out_Token,Out_Amount,Out_USD,Hash,Extrinsic_ID\n\
             2026-09-08 03:47:18,27574036,{BOT},DAI,8.7144,8.71,XOR,2.1000,9.18,{HASH1},27574036-1\n\
             2026-09-08 03:46:54,27574034,{BOT},XOR,4.0000,17.48,DAI,16.7394,16.74,{HASH2},27574034-1\n\
             \n\
             --- EXTRINSICS (2) ---\n\
             Date,Block,Extrinsic_ID,Signer,Pallet,Method,Result,Hash\n\
             2026-09-08 03:47:18,27574036,27574036-1,{BOT},liquidityProxy,swap,Success,{HASH1}\n\
             2026-09-08 03:46:54,27574034,27574034-1,{BOT},liquidityProxy,swap,Success,{HASH2}\n\
             \n\
             "
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn koinly_matches_prod_golden() {
        let out = format_koinly(&golden(), &["swaps", "extrinsics"], &[BOT.into()]);
        let expected = format!(
            "Date,Sent Amount,Sent Currency,Received Amount,Received Currency,Fee Amount,Fee Currency,Net Worth Amount,Net Worth Currency,TxHash,Description\n\
             2026-09-08 03:47:18,2.1000,XOR,8.7144,DAI,,,8.71,USD,{HASH1},Swap XOR -> DAI\n\
             2026-09-08 03:46:54,16.7394,DAI,4.0000,XOR,,,17.48,USD,{HASH2},Swap DAI -> XOR"
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn cointracking_matches_prod_golden() {
        let out = format_cointracking(&golden(), &["swaps"], &[BOT.into()]);
        let expected = format!(
            "\"Type\",\"Buy\",\"Cur.\",\"Sell\",\"Cur.\",\"Fee\",\"Cur.\",\"Exchange\",\"Group\",\"Comment\",\"Date\",\"Tx-ID\"\n\
             Trade,8.7144,DAI,2.1000,XOR,,,\"SORA DEX\",,Swap on SORA,2026-09-08 03:47:18,{HASH1}\n\
             Trade,4.0000,XOR,16.7394,DAI,,,\"SORA DEX\",,Swap on SORA,2026-09-08 03:46:54,{HASH2}"
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn cointracker_matches_prod_golden() {
        let out = format_cointracker(&golden(), &["swaps"], &[BOT.into()]);
        let expected = "Date,Received Quantity,Received Currency,Sent Quantity,Sent Currency,Fee Amount,Fee Currency,Tag\n\
             09/08/2026 03:47:18,8.7144,DAI,2.1000,XOR,,,\n\
             09/08/2026 03:46:54,4.0000,XOR,16.7394,DAI,,,";
        assert_eq!(out, expected);
    }

    #[test]
    fn wallet_label_with_name_matches_prod() {
        assert_eq!(
            wallet_label("cnSMPnA1v4R4uciDxgkzgYYraipNfNwwWSdMxjQ9JfbGmPe56", "Bot"),
            "cnSMPnA1...GmPe56 (Bot)"
        );
    }

    #[test]
    fn transfer_directions_follow_the_wallet_set() {
        let set: HashSet<&str> = ["a", "b"].into_iter().collect();
        assert_eq!(tx_direction("a", "b", &set), Direction::Internal);
        assert_eq!(tx_direction("a", "x", &set), Direction::Out);
        assert_eq!(tx_direction("x", "a", &set), Direction::In);
        assert_eq!(tx_direction("x", "y", &set), Direction::In);
    }

    #[test]
    fn liquidity_rows_split_per_leg_and_direction() {
        let d = ExportData {
            liquidity: vec![LiquidityRow {
                timestamp: ts("2026-02-22T17:16:30Z"),
                block: 24969475,
                wallet: "w".into(),
                pool_base: "VXOR".into(),
                pool_target: "APOLLO".into(),
                base_amount: "3.3951".into(),
                target_amount: "3.3951".into(),
                usd_value: "0.05".into(),
                kind: "deposit".into(),
                hash: "0xh".into(),
                extrinsic_id: "24969475-1".into(),
            }],
            ..Default::default()
        };
        let k = format_koinly(&d, &["liquidity"], &["w".into()]);
        let lines: Vec<&str> = k.lines().collect();
        assert_eq!(
            lines[1],
            "2026-02-22 17:16:30,3.3951,VXOR,,,,,,,0xh,Provide Liquidity VXOR/APOLLO"
        );
        assert_eq!(
            lines[2],
            "2026-02-22 17:16:30,3.3951,APOLLO,,,,,,,0xh,Provide Liquidity VXOR/APOLLO"
        );
        let c = format_cointracking(&d, &["liquidity"], &["w".into()]);
        assert_eq!(c.lines().nth(1).unwrap(), "Provide Liquidity,,,3.3951,VXOR,,,\"SORA DEX\",,Add LP VXOR/APOLLO,2026-02-22 17:16:30,0xh");
    }

    #[test]
    fn order_book_quote_amount_is_js_float_product() {
        assert_eq!(
            quote_amount_cell("0.001000", "100000000.000000"),
            js_number(0.001f64 * 100_000_000.0)
        );
        assert_eq!(quote_amount_cell("0", "0"), "");
        assert_eq!(quote_amount_cell("1.000000", "95000000.000000"), "95000000");
    }

    #[test]
    fn cells_follow_the_live_era_text() {
        assert_eq!(fmt_usd_cell(None), "");
        assert_eq!(fmt_usd_cell(Some(&BigDecimal::zero())), "");
        assert_eq!(
            fmt_usd_cell(Some(&BigDecimal::from_str("8.714402").unwrap())),
            "8.71"
        );
        assert_eq!(
            fmt_usd_cell(Some(&BigDecimal::from_str("0.001").unwrap())),
            ""
        );
        assert_eq!(fmt_ob_number(None), "0");
        assert_eq!(
            fmt_ob_number(Some(&BigDecimal::from_str("0.001").unwrap())),
            "0.001000"
        );
        assert_eq!(csv_esc("a,b"), "\"a,b\"");
        assert_eq!(csv_esc("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(csv_esc("plain"), "plain");
    }

    #[test]
    fn query_parsing_matches_node() {
        assert!(is_ss58_shape(BOT));
        assert!(!is_ss58_shape("0x1234"));
        assert!(!is_ss58_shape(
            "cnSBot6vTtpckdo7HQhjk2hiAAKfDJX34DZsyLVs7B5Y2Gv60"
        ));
        assert_eq!(parse_ms(Some("5"), 9), 5);
        assert_eq!(parse_ms(Some("x"), 9), 9);
        assert_eq!(parse_ms(Some("0"), 9), 9);
        assert_eq!(
            csv_filename("koinly", ts("2026-09-08T03:00:00Z")),
            "koinly_import_2026-09-08.csv"
        );
        assert_eq!(
            csv_filename("junk", ts("2026-09-08T03:00:00Z")),
            "sorametrics_export_2026-09-08.csv"
        );
    }
}
