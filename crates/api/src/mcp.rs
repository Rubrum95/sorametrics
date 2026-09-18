//! Remote MCP server (`POST /mcp`), read-only, no authentication.
//!
//! Protocol: Model Context Protocol, Streamable HTTP transport.
//! - **Modern** clients (revision `2026-07-28`): stateless. Every request
//!   carries its version in `_meta` and in the `MCP-Protocol-Version`
//!   header; `Mcp-Method` / `Mcp-Name` mirror the body and are validated
//!   (`HeaderMismatch`, `-32020`). `server/discover`, `tools/list`
//!   (cacheable: `ttlMs`, `cacheScope`), `tools/call`.
//! - **Legacy** clients (`2025-03-26` … `2025-11-25`): the `initialize`
//!   handshake is answered, without minting a session (sessions were
//!   optional there), so both eras share the endpoint ("dual-era" server).
//!
//! Tools do not reimplement anything: each one maps its arguments to an
//! existing REST route and calls it in-process through the same router, so
//! validation, caches and data semantics are the API's own. Results carry
//! `structuredContent` (`{data, source, notes}`) plus the same JSON as
//! text; `logo` / `sparkline` blobs are stripped to spare the model's
//! context.

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Map, Value};
use tower::ServiceExt;

/// The modern revision served statelessly.
const MODERN_VERSION: &str = "2026-07-28";
/// Handshake-based revisions answered through `initialize`.
const LEGACY_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26"];

const META_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
const META_SERVER_INFO: &str = "io.modelcontextprotocol/serverInfo";

const ERR_PARSE: i64 = -32700;
const ERR_INVALID_REQUEST: i64 = -32600;
const ERR_METHOD_NOT_FOUND: i64 = -32601;
const ERR_INVALID_PARAMS: i64 = -32602;
const ERR_HEADER_MISMATCH: i64 = -32020;
const ERR_UNSUPPORTED_VERSION: i64 = -32022;

/// Freshness hint of `tools/list` and `server/discover` (the set is static).
const LIST_TTL_MS: u64 = 3_600_000;
/// Largest REST response a tool will relay.
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

const INSTRUCTIONS: &str = "SoraMetrics: read-only analytics of the SORA v2 blockchain \
(Substrate) indexed from the chain itself. Amounts are in whole tokens unless a field says raw. \
PRICES ARE MARGINAL QUOTES: a token carrying `illiquid: true` sells for under 0.50 USD per 1 USD \
quoted, so never compute a market cap or a holding's value from its price; wallet `usdValue` is \
already 0 for such tokens. Holder counts are accounts above a threshold (>1 XOR, >0.1 others), \
not every account. Token supply is the official SORA circulating figure, not on-chain total \
issuance. Get asset ids from `list_tokens` before calling tools that take `asset_id`.";

#[derive(Clone)]
struct McpState {
    /// The REST router (no rate limiting, no compression): tools call it
    /// in-process.
    inner: Router,
}

const OPENAPI_JSON: &str = include_str!("../assets/openapi.json");
const LLMS_TXT: &str = include_str!("../assets/llms.txt");

async fn openapi() -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/json; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        OPENAPI_JSON,
    )
        .into_response()
}

async fn llms_txt() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        LLMS_TXT,
    )
        .into_response()
}

/// `/mcp` over the REST router it delegates to, plus the two discovery
/// documents (`/openapi.json`, `/llms.txt`).
pub fn router(inner: Router) -> Router {
    Router::new()
        .route("/openapi.json", get(openapi))
        .route("/llms.txt", get(llms_txt))
        .route(
            "/mcp",
            post(handle)
                .get(method_not_allowed)
                .delete(method_not_allowed),
        )
        .with_state(McpState { inner })
}

/// GET (the legacy standalone SSE stream) and DELETE (session teardown)
/// are not part of a stateless server.
async fn method_not_allowed() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [(header::ALLOW, "POST")],
        "MCP endpoint: POST only",
    )
        .into_response()
}

fn rpc_error(id: &Value, code: i64, message: impl Into<String>, data: Option<Value>) -> Value {
    let mut err = json!({ "code": code, "message": message.into() });
    if let (Some(d), Some(obj)) = (data, err.as_object_mut()) {
        obj.insert("data".into(), d);
    }
    json!({ "jsonrpc": "2.0", "id": id, "error": err })
}

fn rpc_result(id: &Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn server_info() -> Value {
    json!({
        "name": "sorametrics",
        "title": "SoraMetrics",
        "version": env!("CARGO_PKG_VERSION"),
        "websiteUrl": "https://sorametrics.org",
    })
}

/// A result of the modern era: `resultType` plus the server identity.
fn modern(mut result: Value) -> Value {
    if let Some(obj) = result.as_object_mut() {
        obj.insert("resultType".into(), json!("complete"));
        obj.insert("_meta".into(), json!({ META_SERVER_INFO: server_info() }));
    }
    result
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// DNS-rebinding guard: a browser `Origin` must be this host or listed in
/// `MCP_ALLOWED_ORIGINS` (comma-separated). No `Origin` = not a browser.
fn origin_allowed(headers: &HeaderMap) -> bool {
    let Some(origin) = header_str(headers, "origin") else {
        return true;
    };
    let origin_host = url::Url::parse(origin)
        .ok()
        .and_then(|u| u.host_str().map(str::to_ascii_lowercase));
    let Some(origin_host) = origin_host else {
        return false;
    };
    let request_host =
        header_str(headers, "host").map(|h| h.split(':').next().unwrap_or(h).to_ascii_lowercase());
    if request_host.as_deref() == Some(origin_host.as_str()) {
        return true;
    }
    std::env::var("MCP_ALLOWED_ORIGINS")
        .map(|list| {
            list.split(',')
                .map(str::trim)
                .any(|allowed| allowed.eq_ignore_ascii_case(origin))
        })
        .unwrap_or(false)
}

/// `=?base64?…?=` sentinel of `Mcp-Name` (standard alphabet, padded).
fn decode_header_value(raw: &str) -> Option<String> {
    let Some(b64) = raw
        .strip_prefix("=?base64?")
        .and_then(|r| r.strip_suffix("?="))
    else {
        return Some(raw.to_string());
    };
    let mut bits: u32 = 0;
    let mut n = 0u8;
    let mut out = Vec::with_capacity(b64.len() * 3 / 4);
    for c in b64.bytes().filter(|c| *c != b'=') {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        };
        bits = (bits << 6) | u32::from(v);
        n += 6;
        if n >= 8 {
            n -= 8;
            out.push((bits >> n) as u8);
            bits &= (1 << n) - 1;
        }
    }
    String::from_utf8(out).ok()
}

async fn handle(State(state): State<McpState>, headers: HeaderMap, body: String) -> Response {
    if !origin_allowed(&headers) {
        return (
            StatusCode::FORBIDDEN,
            Json(rpc_error(
                &Value::Null,
                ERR_INVALID_REQUEST,
                "Origin not allowed",
                None,
            )),
        )
            .into_response();
    }
    let msg: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(rpc_error(&Value::Null, ERR_PARSE, "Parse error", None)),
            )
                .into_response();
        }
    };
    let Some(method) = msg
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        return (
            StatusCode::BAD_REQUEST,
            Json(rpc_error(
                &Value::Null,
                ERR_INVALID_REQUEST,
                "Missing method",
                None,
            )),
        )
            .into_response();
    };
    let params = msg.get("params").cloned().unwrap_or_else(|| json!({}));
    let Some(id) = msg.get("id").cloned().filter(|v| !v.is_null()) else {
        // A notification (`notifications/initialized` from legacy clients).
        return StatusCode::ACCEPTED.into_response();
    };

    let meta_version = params
        .get("_meta")
        .and_then(|m| m.get(META_VERSION))
        .and_then(Value::as_str)
        .map(str::to_string);
    match meta_version {
        Some(version) => modern_request(&state, &headers, &id, &method, &params, &version).await,
        None => legacy_request(&state, &id, &method, &params).await,
    }
}

async fn modern_request(
    state: &McpState,
    headers: &HeaderMap,
    id: &Value,
    method: &str,
    params: &Value,
    version: &str,
) -> Response {
    let bad = |code: i64, message: String, data: Option<Value>| {
        (
            StatusCode::BAD_REQUEST,
            Json(rpc_error(id, code, message, data)),
        )
            .into_response()
    };
    match header_str(headers, "mcp-protocol-version") {
        Some(h) if h == version => {}
        Some(h) => {
            return bad(
                ERR_HEADER_MISMATCH,
                format!("Header mismatch: MCP-Protocol-Version header value '{h}' does not match body value '{version}'"),
                None,
            );
        }
        None => {
            return bad(
                ERR_HEADER_MISMATCH,
                "Header mismatch: MCP-Protocol-Version header is missing".into(),
                None,
            );
        }
    }
    if version != MODERN_VERSION {
        let mut supported = vec![MODERN_VERSION];
        supported.extend_from_slice(LEGACY_VERSIONS);
        return bad(
            ERR_UNSUPPORTED_VERSION,
            "Unsupported protocol version".into(),
            Some(json!({ "supported": supported, "requested": version })),
        );
    }
    match header_str(headers, "mcp-method") {
        Some(h) if h == method => {}
        Some(h) => {
            return bad(
                ERR_HEADER_MISMATCH,
                format!("Header mismatch: Mcp-Method header value '{h}' does not match body value '{method}'"),
                None,
            );
        }
        None => {
            return bad(
                ERR_HEADER_MISMATCH,
                "Header mismatch: Mcp-Method header is missing".into(),
                None,
            );
        }
    }
    if method == "tools/call" {
        let body_name = params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let header_name = header_str(headers, "mcp-name").and_then(decode_header_value);
        match header_name {
            Some(h) if h == body_name => {}
            Some(h) => {
                return bad(
                    ERR_HEADER_MISMATCH,
                    format!("Header mismatch: Mcp-Name header value '{h}' does not match body value '{body_name}'"),
                    None,
                );
            }
            None => {
                return bad(
                    ERR_HEADER_MISMATCH,
                    "Header mismatch: Mcp-Name header is missing or malformed".into(),
                    None,
                );
            }
        }
    }

    match method {
        "server/discover" => Json(rpc_result(
            id,
            modern(json!({
                "supportedVersions": [MODERN_VERSION],
                "capabilities": { "tools": {} },
                "instructions": INSTRUCTIONS,
                "ttlMs": LIST_TTL_MS,
                "cacheScope": "public",
            })),
        ))
        .into_response(),
        "tools/list" => Json(rpc_result(
            id,
            modern(json!({
                "tools": tool_definitions(),
                "ttlMs": LIST_TTL_MS,
                "cacheScope": "public",
            })),
        ))
        .into_response(),
        "tools/call" => match call_tool(state, params).await {
            Ok(result) => Json(rpc_result(id, modern(result))).into_response(),
            Err(message) => bad(ERR_INVALID_PARAMS, message, None),
        },
        other => (
            StatusCode::NOT_FOUND,
            Json(rpc_error(
                id,
                ERR_METHOD_NOT_FOUND,
                format!("Method not found: {other}"),
                None,
            )),
        )
            .into_response(),
    }
}

/// Handshake-era clients: JSON-RPC errors travel with HTTP 200, as those
/// revisions did.
async fn legacy_request(state: &McpState, id: &Value, method: &str, params: &Value) -> Response {
    let body = match method {
        "initialize" => {
            let requested = params
                .get("protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let version = LEGACY_VERSIONS
                .iter()
                .find(|v| **v == requested)
                .copied()
                .unwrap_or(LEGACY_VERSIONS[0]);
            rpc_result(
                id,
                json!({
                    "protocolVersion": version,
                    "capabilities": { "tools": { "listChanged": false } },
                    "serverInfo": server_info(),
                    "instructions": INSTRUCTIONS,
                }),
            )
        }
        "ping" => rpc_result(id, json!({})),
        "tools/list" => rpc_result(id, json!({ "tools": tool_definitions() })),
        "tools/call" => match call_tool(state, params).await {
            Ok(result) => rpc_result(id, result),
            Err(message) => rpc_error(id, ERR_INVALID_PARAMS, message, None),
        },
        other => rpc_error(
            id,
            ERR_METHOD_NOT_FOUND,
            format!("Method not found: {other}"),
            None,
        ),
    };
    Json(body).into_response()
}

// ---------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------

/// What a tool resolves to: one REST call on the inner router.
struct RestCall {
    method: Method,
    path: String,
    body: Option<Value>,
}

impl RestCall {
    fn get(path: String) -> Self {
        Self {
            method: Method::GET,
            path,
            body: None,
        }
    }
}

struct ToolSpec {
    name: &'static str,
    title: &'static str,
    description: &'static str,
    input_schema: fn() -> Value,
    build: fn(&Map<String, Value>) -> Result<RestCall, String>,
    /// Caveats returned with every result of this tool.
    notes: &'static [&'static str],
}

const NOTE_PRICES: &str = "Prices are marginal on-chain quotes. `illiquid: true` = selling 1 USD worth returns under 0.50 USD: do not value anything with that price.";
const NOTE_USD: &str = "`usdValue` is 0 for tokens that failed the liquidity check (`illiquid: true`). For the rest it is amount x marginal price, which overstates what a large holding would sell for.";
const NOTE_HOLDERS: &str = "`totalHolders` counts accounts whose free balance is above 1 (XOR) or 0.1 (other assets), not every account holding the asset.";
const NOTE_TIME: &str = "`time` is dd/mm/yyyy HH:MM:SS in Europe/Madrid.";

fn paging_props() -> Value {
    json!({
        "page": { "type": "integer", "minimum": 1, "description": "1-based page (default 1)" },
        "limit": { "type": "integer", "minimum": 1, "maximum": 100, "description": "Rows per page (default 25, max 100)" }
    })
}

fn schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}

fn merge(mut a: Value, b: Value) -> Value {
    if let (Some(x), Some(y)) = (a.as_object_mut(), b.as_object()) {
        for (k, v) in y {
            x.insert(k.clone(), v.clone());
        }
    }
    a
}

fn str_arg<'a>(args: &'a Map<String, Value>, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("Missing required string argument `{key}`"))
}

fn int_arg(args: &Map<String, Value>, key: &str) -> Result<Option<i64>, String> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .map(Some)
            .ok_or_else(|| format!("Argument `{key}` must be an integer")),
    }
}

fn paging_query(args: &Map<String, Value>) -> Result<String, String> {
    let page = int_arg(args, "page")?.unwrap_or(1).max(1);
    let limit = int_arg(args, "limit")?.unwrap_or(25).clamp(1, 100);
    Ok(format!("page={page}&limit={limit}"))
}

fn address_arg(args: &Map<String, Value>, key: &str) -> Result<String, String> {
    let a = str_arg(args, key)?;
    let ok = (46..=50).contains(&a.len()) && a.bytes().all(|c| c.is_ascii_alphanumeric());
    if ok {
        Ok(a.to_string())
    } else {
        Err(format!(
            "`{key}` must be an SS58 address (SORA addresses start with `cn`)"
        ))
    }
}

fn asset_id_arg(args: &Map<String, Value>, key: &str) -> Result<String, String> {
    let a = str_arg(args, key)?;
    let ok =
        a.len() == 66 && a.starts_with("0x") && a.bytes().skip(2).all(|c| c.is_ascii_hexdigit());
    if ok {
        Ok(a.to_ascii_lowercase())
    } else {
        Err(format!(
            "`{key}` must be a 0x-prefixed 32-byte asset id (get it from list_tokens)"
        ))
    }
}

fn enum_arg<'a>(
    args: &'a Map<String, Value>,
    key: &str,
    allowed: &[&str],
) -> Result<&'a str, String> {
    let v = str_arg(args, key)?;
    if allowed.contains(&v) {
        Ok(v)
    } else {
        Err(format!("`{key}` must be one of: {}", allowed.join(", ")))
    }
}

fn encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

const ACTIVITY_KINDS: &[&str] = &[
    "swaps",
    "transfers",
    "bridges",
    "extrinsics",
    "liquidity",
    "orderbook",
];
const WALLET_KINDS: &[&str] = &["swaps", "transfers", "bridges", "extrinsics"];
const BURN_SYMBOLS: &[&str] = &["XOR", "VAL", "PSWAP", "TBCD", "KUSD"];
const GOVERNANCE_SECTIONS: &[&str] = &["motions", "council", "preimages"];

fn tools() -> &'static [ToolSpec] {
    &[
        ToolSpec {
            name: "network_status",
            title: "Network status",
            description: "Current SORA v2 network state: best and finalized block, active era and its progress, validator counts, total XOR staked and total issuance.",
            input_schema: || schema(json!({}), &[]),
            build: |_| Ok(RestCall::get("/staking/network".into())),
            notes: &[],
        },
        ToolSpec {
            name: "list_tokens",
            title: "Tokens and prices",
            description: "Registered SORA assets with symbol, name, decimals, asset id, USD price, 24 h change and the `illiquid` flag. Use `search` to find one by symbol or name. This is where asset ids come from.",
            input_schema: || {
                schema(
                    merge(
                        paging_props(),
                        json!({ "search": { "type": "string", "description": "Substring of the symbol or name" } }),
                    ),
                    &[],
                )
            },
            build: |a| {
                let mut q = format!("/tokens?sparkline=false&{}", paging_query(a)?);
                if let Some(s) = a.get("search").and_then(Value::as_str).filter(|s| !s.trim().is_empty()) {
                    q.push_str(&format!("&search={}", encode(s.trim())));
                }
                Ok(RestCall::get(q))
            },
            notes: &[NOTE_PRICES],
        },
        ToolSpec {
            name: "wallet_balances",
            title: "Wallet balances",
            description: "Every token an address holds on SORA v2 (free balance read from the chain) with its USD value.",
            input_schema: || schema(json!({ "address": { "type": "string", "description": "SS58 address (cn…)" } }), &["address"]),
            build: |a| Ok(RestCall::get(format!("/balance/{}", address_arg(a, "address")?))),
            notes: &[NOTE_USD],
        },
        ToolSpec {
            name: "wallet_history",
            title: "Wallet history",
            description: "Indexed history of one address: swaps, transfers, bridge operations or extrinsics, newest first.",
            input_schema: || {
                schema(
                    merge(
                        paging_props(),
                        json!({
                            "address": { "type": "string", "description": "SS58 address (cn…)" },
                            "kind": { "type": "string", "enum": WALLET_KINDS }
                        }),
                    ),
                    &["address", "kind"],
                )
            },
            build: |a| {
                let kind = enum_arg(a, "kind", WALLET_KINDS)?;
                Ok(RestCall::get(format!(
                    "/history/{kind}/{}?{}",
                    address_arg(a, "address")?,
                    paging_query(a)?
                )))
            },
            notes: &[NOTE_TIME],
        },
        ToolSpec {
            name: "recent_activity",
            title: "Recent network activity",
            description: "Latest network-wide swaps, transfers, bridge operations, extrinsics, liquidity or order-book events, newest first. `token` filters by symbol where the list supports it.",
            input_schema: || {
                schema(
                    merge(
                        paging_props(),
                        json!({
                            "kind": { "type": "string", "enum": ACTIVITY_KINDS },
                            "token": { "type": "string", "description": "Symbol filter, e.g. XOR" }
                        }),
                    ),
                    &["kind"],
                )
            },
            build: |a| {
                let kind = enum_arg(a, "kind", ACTIVITY_KINDS)?;
                let mut q = format!("/history/global/{kind}?{}", paging_query(a)?);
                if let Some(t) = a.get("token").and_then(Value::as_str).filter(|s| !s.trim().is_empty()) {
                    q.push_str(&format!("&token={}", encode(t.trim())));
                }
                Ok(RestCall::get(q))
            },
            notes: &[NOTE_TIME],
        },
        ToolSpec {
            name: "top_holders",
            title: "Top holders of an asset",
            description: "Accounts ranked by free balance of one asset, read from the chain (25 per page). The first call for an asset may take several seconds while the chain is scanned.",
            input_schema: || {
                schema(
                    json!({
                        "asset_id": { "type": "string", "description": "0x… asset id from list_tokens" },
                        "page": { "type": "integer", "minimum": 1 }
                    }),
                    &["asset_id"],
                )
            },
            build: |a| {
                let page = int_arg(a, "page")?.unwrap_or(1).max(1);
                Ok(RestCall::get(format!("/holders/{}?page={page}", asset_id_arg(a, "asset_id")?)))
            },
            notes: &[NOTE_HOLDERS],
        },
        ToolSpec {
            name: "list_pools",
            title: "Liquidity pools",
            description: "XYK pools with both reserves (raw, 18 decimals) and the USD prices of each side, ordered by value locked. Illiquid assets are priced 0, so dust pools do not rank.",
            input_schema: || {
                schema(
                    merge(
                        paging_props(),
                        json!({ "base": { "type": "string", "description": "Base asset symbol filter: XOR, XSTUSD, KUSD…" } }),
                    ),
                    &[],
                )
            },
            build: |a| {
                let mut q = format!("/pools?{}", paging_query(a)?);
                if let Some(b) = a.get("base").and_then(Value::as_str).filter(|s| !s.trim().is_empty()) {
                    q.push_str(&format!("&base={}", encode(b.trim())));
                }
                Ok(RestCall::get(q))
            },
            notes: &[NOTE_PRICES],
        },
        ToolSpec {
            name: "burn_stats",
            title: "Burn statistics",
            description: "Supply and burn figures of XOR, VAL, PSWAP, TBCD or KUSD over 24 h, 7 d, 30 d and all time. For XOR the windows give the measured supply drop (`totalBurned`), the fee-mechanism burn (`feeBased`) and explicit burns (`explicitBurned`).",
            input_schema: || schema(json!({ "symbol": { "type": "string", "enum": BURN_SYMBOLS } }), &["symbol"]),
            build: |a| Ok(RestCall::get(format!("/burns/stats/{}", enum_arg(a, "symbol", BURN_SYMBOLS)?))),
            notes: &["The `all` window spans the February 2026 XOR redenomination and mixes units: treat it as indicative only."],
        },
        ToolSpec {
            name: "get_block",
            title: "Block by number",
            description: "One block with its extrinsics and their events, read from an archive node.",
            input_schema: || schema(json!({ "number": { "type": "integer", "minimum": 1 } }), &["number"]),
            build: |a| {
                let n = int_arg(a, "number")?.filter(|n| *n > 0).ok_or("`number` must be a positive integer")?;
                Ok(RestCall::get(format!("/block/{n}")))
            },
            notes: &[],
        },
        ToolSpec {
            name: "get_extrinsic",
            title: "Extrinsic detail",
            description: "One extrinsic (block number + index inside the block) with its call arguments and emitted events.",
            input_schema: || {
                schema(
                    json!({
                        "block": { "type": "integer", "minimum": 1 },
                        "index": { "type": "integer", "minimum": 0 }
                    }),
                    &["block", "index"],
                )
            },
            build: |a| {
                let b = int_arg(a, "block")?.filter(|n| *n > 0).ok_or("`block` must be a positive integer")?;
                let i = int_arg(a, "index")?.filter(|n| *n >= 0).ok_or("`index` must be zero or positive")?;
                Ok(RestCall::get(format!("/history/extrinsic/{b}/{i}")))
            },
            notes: &[],
        },
        ToolSpec {
            name: "search",
            title: "Search",
            description: "Resolve free text: an address, a block number, an extrinsic id (`block-index`), a transaction hash or a token symbol.",
            input_schema: || schema(json!({ "q": { "type": "string", "minLength": 3, "maxLength": 128 } }), &["q"]),
            build: |a| {
                let q = str_arg(a, "q")?;
                if !(3..=128).contains(&q.len()) {
                    return Err("`q` must be 3 to 128 characters".into());
                }
                Ok(RestCall::get(format!("/search?q={}", encode(q))))
            },
            notes: &[],
        },
        ToolSpec {
            name: "governance",
            title: "Governance",
            description: "On-chain governance: open council and technical-committee motions (decoded calls and votes), council members, or stored preimages.",
            input_schema: || schema(json!({ "section": { "type": "string", "enum": GOVERNANCE_SECTIONS } }), &["section"]),
            build: |a| Ok(RestCall::get(format!("/governance/{}", enum_arg(a, "section", GOVERNANCE_SECTIONS)?))),
            notes: &[],
        },
        ToolSpec {
            name: "staking_validators",
            title: "Validators",
            description: "The active validator set with commission, own and nominated stake, nominator count and eras since the last payout.",
            input_schema: || schema(json!({}), &[]),
            build: |_| Ok(RestCall::get("/staking/validators".into())),
            notes: &[],
        },
        ToolSpec {
            name: "prediction_markets",
            title: "Polkamarkt prediction markets",
            description: "Polkamarkt markets indexed from the chain: question, status, outcomes, volume and resolution.",
            input_schema: || schema(paging_props(), &[]),
            build: |a| Ok(RestCall::get(format!("/polkamarkt/markets?{}", paging_query(a)?))),
            notes: &["Volume is gross collateral of trades; the chain's own `marketVolume` is net of the 0.5 % fee."],
        },
        ToolSpec {
            name: "resolve_identities",
            title: "On-chain identities",
            description: "Display names registered in the chain's Identity pallet for up to 100 addresses. Addresses without an identity are simply absent: never invent a label for them.",
            input_schema: || {
                schema(
                    json!({
                        "addresses": {
                            "type": "array",
                            "items": { "type": "string" },
                            "minItems": 1,
                            "maxItems": 100
                        }
                    }),
                    &["addresses"],
                )
            },
            build: |a| {
                let list = a
                    .get("addresses")
                    .and_then(Value::as_array)
                    .filter(|l| !l.is_empty() && l.len() <= 100)
                    .ok_or("`addresses` must be an array of 1 to 100 addresses")?;
                let addresses: Vec<&str> = list.iter().filter_map(Value::as_str).collect();
                if addresses.len() != list.len() {
                    return Err("`addresses` must contain strings only".into());
                }
                Ok(RestCall {
                    method: Method::POST,
                    path: "/api/identities".into(),
                    body: Some(json!({ "addresses": addresses })),
                })
            },
            notes: &[],
        },
    ]
}

fn output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "data": { "description": "The REST response, without logo / sparkline blobs" },
            "source": { "type": "string", "description": "REST path that produced `data`" },
            "notes": { "type": "array", "items": { "type": "string" }, "description": "Caveats that apply to `data`" }
        },
        "required": ["data", "source", "notes"]
    })
}

/// Deterministic order (declaration order), as the spec asks for caching.
fn tool_definitions() -> Vec<Value> {
    tools()
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "title": t.title,
                "description": t.description,
                "inputSchema": (t.input_schema)(),
                "outputSchema": output_schema(),
                "annotations": {
                    "title": t.title,
                    "readOnlyHint": true,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": true
                }
            })
        })
        .collect()
}

/// Drops the blobs an agent has no use for (`logo` data URIs weigh tens of
/// kilobytes each).
fn strip_blobs(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|k, _| k != "logo" && k != "sparkline" && k != "icon");
            for v in map.values_mut() {
                strip_blobs(v);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(strip_blobs),
        _ => {}
    }
}

fn tool_error(message: String) -> Value {
    json!({ "content": [{ "type": "text", "text": message }], "isError": true })
}

/// `Err` = protocol error (unknown tool, malformed call); an execution
/// problem is an `Ok` result with `isError: true`, which the model can act on.
async fn call_tool(state: &McpState, params: &Value) -> Result<Value, String> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or("Missing tool name")?;
    let spec = tools()
        .iter()
        .find(|t| t.name == name)
        .ok_or_else(|| format!("Unknown tool: {name}"))?;
    let empty = Map::new();
    let args = match params.get("arguments") {
        None | Some(Value::Null) => &empty,
        Some(Value::Object(m)) => m,
        Some(_) => return Err("`arguments` must be an object".into()),
    };
    let call = match (spec.build)(args) {
        Ok(c) => c,
        Err(message) => return Ok(tool_error(message)),
    };

    let source = call.path.clone();
    let mut request = Request::builder().method(call.method).uri(&call.path);
    let body = match &call.body {
        Some(v) => {
            request = request.header(header::CONTENT_TYPE, "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let request = match request.body(body) {
        Ok(r) => r,
        Err(e) => return Ok(tool_error(format!("Could not build the request: {e}"))),
    };
    let response = match state.inner.clone().oneshot(request).await {
        Ok(r) => r,
        Err(e) => return Ok(tool_error(format!("Internal call failed: {e}"))),
    };
    let status = response.status();
    let bytes = match axum::body::to_bytes(response.into_body(), MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            return Ok(tool_error(
                "The response is too large; narrow the request (smaller `limit`).".into(),
            ))
        }
    };
    let mut data: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if !status.is_success() {
        let reason = data
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| status.to_string());
        let hint = if status == StatusCode::SERVICE_UNAVAILABLE {
            " (the chain scan is still running: retry in ~15 s)"
        } else {
            ""
        };
        return Ok(tool_error(format!(
            "{source} answered {}: {reason}{hint}",
            status.as_u16()
        )));
    }
    strip_blobs(&mut data);
    let structured = json!({ "data": data, "source": source, "notes": spec.notes });
    Ok(json!({
        "content": [{ "type": "text", "text": structured.to_string() }],
        "structuredContent": structured,
        "isError": false
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_names_are_unique_and_header_safe() {
        let mut seen = std::collections::HashSet::new();
        for t in tools() {
            assert!(seen.insert(t.name), "duplicate tool {}", t.name);
            assert!(t.name.len() <= 128);
            assert!(t
                .name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.')));
            let s = (t.input_schema)();
            assert_eq!(s["type"], "object");
        }
    }

    #[test]
    fn mcp_name_sentinel_decodes() {
        assert_eq!(
            decode_header_value("list_tokens").as_deref(),
            Some("list_tokens")
        );
        assert_eq!(
            decode_header_value("=?base64?SGVsbG8sIOS4lueVjA==?=").as_deref(),
            Some("Hello, 世界")
        );
        assert_eq!(decode_header_value("=?base64?***?="), None);
    }

    #[test]
    fn arguments_are_validated_before_any_call() {
        let args = |v: Value| v.as_object().cloned().unwrap_or_default();
        let wallet = tools()
            .iter()
            .find(|t| t.name == "wallet_balances")
            .unwrap();
        assert!((wallet.build)(&args(json!({ "address": "../etc/passwd" }))).is_err());
        let ok = (wallet.build)(&args(json!({
            "address": "cnSMPnA1v4R4uciDxgkzgYYraipNfNwwWSdMxjQ9JfbGmPe56"
        })))
        .unwrap();
        assert_eq!(
            ok.path,
            "/balance/cnSMPnA1v4R4uciDxgkzgYYraipNfNwwWSdMxjQ9JfbGmPe56"
        );
        let holders = tools().iter().find(|t| t.name == "top_holders").unwrap();
        assert!((holders.build)(&args(json!({ "asset_id": "XOR" }))).is_err());
        let act = tools()
            .iter()
            .find(|t| t.name == "recent_activity")
            .unwrap();
        assert!((act.build)(&args(json!({ "kind": "drop table" }))).is_err());
        let q = (act.build)(&args(
            json!({ "kind": "swaps", "limit": 5000, "token": "X O&R" }),
        ))
        .unwrap();
        assert_eq!(
            q.path,
            "/history/global/swaps?page=1&limit=100&token=X+O%26R"
        );
    }

    #[test]
    fn openapi_document_is_valid_json_with_unique_operation_ids() {
        let doc: Value = serde_json::from_str(OPENAPI_JSON).unwrap();
        assert_eq!(doc["openapi"], "3.1.0");
        let mut ids = std::collections::HashSet::new();
        for (path, item) in doc["paths"].as_object().unwrap() {
            for (_, operation) in item.as_object().unwrap() {
                let id = operation["operationId"].as_str().unwrap();
                assert!(ids.insert(id.to_string()), "duplicate operationId {id}");
                let declared = operation["parameters"]
                    .as_array()
                    .map(|p| p.iter().filter(|x| x["in"] == "path").count())
                    .unwrap_or(0);
                assert_eq!(declared, path.matches('{').count(), "path params of {path}");
            }
        }
    }

    #[test]
    fn blobs_are_stripped_recursively() {
        let mut v = json!({ "data": [{ "symbol": "XOR", "logo": "data:…", "sparkline": [1, 2] }] });
        strip_blobs(&mut v);
        assert_eq!(v, json!({ "data": [{ "symbol": "XOR" }] }));
    }
}
