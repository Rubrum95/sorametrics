//! Small proxies (`index.js`):
//!
//! - `/currency-rates` — USD→EUR rate from
//!   `https://open.er-api.com/v6/latest/USD`, cached 1 h, `0.92` when
//!   the upstream fails (the Node's seed value).
//! - `/mof/qty/:symbol` — `https://mof.sora.org/qty/<symbol>` as
//!   `text/plain` (symbol lower-cased and stripped to `[a-z0-9_-]`;
//!   empty → 400 `bad_symbol`; upstream status and body passed through;
//!   unreachable → 502 `mof_unavailable`; `Cache-Control: max-age=300`).
//! - `/proxy-image?url=` — image proxy for the token logos: only
//!   http(s) URLs on `raw.githubusercontent.com`, `github.com` or
//!   `avatars.githubusercontent.com` (private hosts and anything else
//!   get a 1×1 GIF placeholder); at most 2 concurrent downloads, 5 s
//!   timeout, 2000-entry in-memory cache, `Cache-Control: max-age=86400`.

use crate::{error::ApiError, AppState};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Semaphore;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/currency-rates", get(currency_rates))
        .route("/mof/qty/:symbol", get(mof_qty))
        .route("/proxy-image", get(proxy_image))
        .route("/api/tech-accounts", get(tech_accounts))
}

// ---------------------------------------------------------------------
// /api/tech-accounts
// ---------------------------------------------------------------------

const TECH_ACCOUNTS_TTL: Duration = Duration::from_secs(6 * 3600);

fn label_bytes(b: &[u8]) -> String {
    if !b.is_empty() && b.iter().all(|c| (0x20..0x7f).contains(c)) {
        String::from_utf8_lossy(b).to_string()
    } else {
        format!("0x{}", hex::encode(b))
    }
}

/// Node `buildTechAccountsMap` label of a `TechAccountId`.
pub fn tech_label(
    id: &sorametrics_substrate::runtime::sora::runtime_types::common::primitives::TechAccountId<
        subxt::utils::AccountId32,
        sorametrics_substrate::runtime::sora::runtime_types::common::primitives::TechAssetId<
            sorametrics_substrate::runtime::sora::runtime_types::common::primitives::_allowed_deprecated::PredefinedAssetId,
        >,
        u32,
    >,
) -> String {
    use sorametrics_substrate::runtime::sora::runtime_types::common::primitives::{
        TechAccountId as T, TechPurpose as P,
    };
    match id {
        T::Generic(a, b) => {
            let l = format!("{}/{}", label_bytes(a), label_bytes(b));
            if l == "/" {
                "Generic".into()
            } else {
                l
            }
        }
        T::Pure(_, purpose) => match purpose {
            P::FeeCollector => "FeeCollector",
            P::FeeCollectorForPair(_) => "FeeCollectorForPair",
            P::XykLiquidityKeeper(_) => "XykLiquidityKeeper",
            P::Identifier(_) => "Identifier",
            P::OrderBookLiquidityKeeper(_) => "OrderBookLiquidityKeeper",
        }
        .into(),
        T::Wrapped(_) => "Wrapped".into(),
        T::WrappedRepr(_) => "WrappedRepr".into(),
        T::None => "None".into(),
    }
}

async fn tech_accounts(State(state): State<AppState>) -> Result<Response, ApiError> {
    let map = match state.cached_scan("tech-accounts", TECH_ACCOUNTS_TTL).await {
        Some(v) => v,
        None => {
            let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
            let out: std::collections::BTreeMap<String, String> = chain
                .with_client(|client| async move {
                    let at = client.storage().at_latest().await?;
                    let mut stream = at
                        .iter(
                            sorametrics_substrate::runtime::sora::storage()
                                .technical()
                                .tech_accounts_iter(),
                        )
                        .await?;
                    let mut out = std::collections::BTreeMap::new();
                    while let Some(kv) = stream.next().await {
                        let kv = kv?;
                        let n = kv.key_bytes.len();
                        let acc: [u8; 32] = kv.key_bytes[n - 32..].try_into().unwrap_or([0; 32]);
                        out.insert(
                            sorametrics_core::chain::ss58_encode_sora(&acc),
                            tech_label(&kv.value),
                        );
                    }
                    Ok(out)
                })
                .await?;
            let v = serde_json::to_value(&out).map_err(|e| ApiError::Internal(e.to_string()))?;
            state.store_scan("tech-accounts", v.clone()).await;
            v
        }
    };
    let mut r = Json(map).into_response();
    r.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("public, max-age=3600"),
    );
    Ok(r)
}

// ---------------------------------------------------------------------
// /mof/qty/:symbol
// ---------------------------------------------------------------------

/// Node: `toLowerCase().replace(/[^a-z0-9_\-]/g, '')`.
pub fn clean_symbol(raw: &str) -> String {
    raw.to_lowercase()
        .chars()
        .filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_' || *c == '-')
        .collect()
}

fn text(status: StatusCode, body: String, cache: Option<&str>) -> Response {
    let mut r = (status, body).into_response();
    let h = r.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        "text/plain; charset=utf-8"
            .parse()
            .unwrap_or(header::HeaderValue::from_static("text/plain")),
    );
    if let Some(c) = cache {
        if let Ok(v) = c.parse() {
            h.insert(header::CACHE_CONTROL, v);
        }
    }
    r
}

async fn mof_qty(Path(symbol): Path<String>) -> Response {
    let sym = clean_symbol(&symbol);
    if sym.is_empty() {
        return text(StatusCode::BAD_REQUEST, "bad_symbol".into(), None);
    }
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(_) => return text(StatusCode::BAD_GATEWAY, "mof_unavailable".into(), None),
    };
    match client
        .get(format!("https://mof.sora.org/qty/{sym}"))
        .send()
        .await
    {
        Ok(r) => {
            let status =
                StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body = r.text().await.unwrap_or_default();
            if status.is_success() {
                text(
                    StatusCode::OK,
                    body.trim().to_string(),
                    Some("public, max-age=300"),
                )
            } else {
                text(status, body, None)
            }
        }
        Err(_) => text(StatusCode::BAD_GATEWAY, "mof_unavailable".into(), None),
    }
}

// ---------------------------------------------------------------------
// /proxy-image
// ---------------------------------------------------------------------

/// Node `PLACEHOLDER_GIF` (1×1 transparent GIF).
const PLACEHOLDER_GIF: [u8; 34] = [
    0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
    0xff, 0xff, 0xff, 0x2c, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x01, 0x4c,
    0x00, 0x3b,
];
const ALLOWED_HOSTS: [&str; 3] = [
    "raw.githubusercontent.com",
    "github.com",
    "avatars.githubusercontent.com",
];
const MAX_CONCURRENT_DOWNLOADS: usize = 2;
const MAX_QUEUE: usize = 500;
const IMAGE_CACHE_MAX: usize = 2000;
const IMAGE_CACHE_EVICT: usize = 500;

#[derive(Deserialize)]
struct ProxyQuery {
    url: Option<String>,
}

/// Node's anti-SSRF host block list.
pub fn is_private_host(host: &str) -> bool {
    let h = host.to_lowercase();
    h == "localhost"
        || h.ends_with(".local")
        || h == "0.0.0.0"
        || h.starts_with("127.")
        || h.starts_with("10.")
        || h.starts_with("192.168.")
        || h.strip_prefix("172.")
            .and_then(|r| r.split('.').next())
            .and_then(|o| o.parse::<u8>().ok())
            .is_some_and(|o| (16..=31).contains(&o))
        || h == "169.254.169.254"
        || h.starts_with('[')
        || h == "::1"
        || h.starts_with("::ffff:")
        || h.starts_with("fd")
        || h.starts_with("fc")
        || h.starts_with("fe80")
}

/// Whether the host is one of the logo hosts (or a subdomain).
pub fn is_allowed_host(host: &str) -> bool {
    let h = host.to_lowercase();
    ALLOWED_HOSTS
        .iter()
        .any(|a| h == *a || h.ends_with(&format!(".{a}")))
}

/// Node's per-request outcome before any download.
pub enum ProxyDecision {
    /// 400 with this text.
    Bad(&'static str),
    /// The placeholder GIF.
    Placeholder,
    /// Fetch this normalised URL.
    Fetch(String),
}

/// Validate the `url` query the way `/proxy-image` does.
pub fn decide(url: Option<&str>) -> ProxyDecision {
    let Some(raw) = url.filter(|u| !u.is_empty()) else {
        return ProxyDecision::Bad("No URL");
    };
    let Ok(u) = url::Url::parse(raw) else {
        return ProxyDecision::Bad("Bad URL");
    };
    if !matches!(u.scheme(), "http" | "https") {
        return ProxyDecision::Bad("Bad protocol");
    }
    let host = u.host_str().unwrap_or("");
    if is_private_host(host) || !is_allowed_host(host) {
        return ProxyDecision::Placeholder;
    }
    ProxyDecision::Fetch(u.to_string())
}

struct CachedImage {
    content_type: String,
    bytes: Vec<u8>,
}

struct ImageProxy {
    cache: Mutex<HashMap<String, CachedImage>>,
    order: Mutex<Vec<String>>,
    slots: Semaphore,
    pending: AtomicUsize,
}

fn proxy() -> &'static ImageProxy {
    static P: OnceLock<ImageProxy> = OnceLock::new();
    P.get_or_init(|| ImageProxy {
        cache: Mutex::new(HashMap::new()),
        order: Mutex::new(Vec::new()),
        slots: Semaphore::new(MAX_CONCURRENT_DOWNLOADS),
        pending: AtomicUsize::new(0),
    })
}

fn placeholder() -> Response {
    let mut r = (StatusCode::OK, Body::from(PLACEHOLDER_GIF.to_vec())).into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("image/gif"),
    );
    r.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("public, max-age=86400"),
    );
    r
}

fn image(content_type: &str, bytes: Vec<u8>) -> Response {
    let mut r = (StatusCode::OK, Body::from(bytes)).into_response();
    if let Ok(v) = content_type.parse() {
        r.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    r.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("public, max-age=86400"),
    );
    r
}

fn cached(url: &str) -> Option<(String, Vec<u8>)> {
    let cache = proxy().cache.lock().ok()?;
    cache
        .get(url)
        .map(|c| (c.content_type.clone(), c.bytes.clone()))
}

fn remember(url: String, content_type: String, bytes: Vec<u8>) {
    let p = proxy();
    let (Ok(mut cache), Ok(mut order)) = (p.cache.lock(), p.order.lock()) else {
        return;
    };
    if cache.len() > IMAGE_CACHE_MAX {
        let n = IMAGE_CACHE_EVICT.min(order.len());
        let evict: Vec<String> = order.drain(..n).collect();
        for k in evict {
            cache.remove(&k);
        }
    }
    order.push(url.clone());
    cache.insert(
        url,
        CachedImage {
            content_type,
            bytes,
        },
    );
}

async fn download(url: &str) -> Option<(String, Vec<u8>)> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;
    let r = client.get(url).send().await.ok()?;
    if r.status() != reqwest::StatusCode::OK {
        return None;
    }
    let content_type = r
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("image/png")
        .to_string();
    let bytes = r.bytes().await.ok()?.to_vec();
    Some((content_type, bytes))
}

async fn proxy_image(Query(q): Query<ProxyQuery>) -> Response {
    let url = match decide(q.url.as_deref()) {
        ProxyDecision::Bad(msg) => return (StatusCode::BAD_REQUEST, msg).into_response(),
        ProxyDecision::Placeholder => return placeholder(),
        ProxyDecision::Fetch(u) => u,
    };
    if let Some((ct, bytes)) = cached(&url) {
        return image(&ct, bytes);
    }
    let p = proxy();
    if p.pending.load(Ordering::Relaxed) > MAX_QUEUE {
        return placeholder();
    }
    p.pending.fetch_add(1, Ordering::Relaxed);
    let permit = p.slots.acquire().await;
    p.pending.fetch_sub(1, Ordering::Relaxed);
    let Ok(_permit) = permit else {
        return placeholder();
    };
    match download(&url).await {
        Some((ct, bytes)) => {
            remember(url, ct.clone(), bytes.clone());
            image(&ct, bytes)
        }
        None => placeholder(),
    }
}

// ---------------------------------------------------------------------
// /currency-rates
// ---------------------------------------------------------------------

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mof_symbol_is_cleaned_like_the_node() {
        assert_eq!(clean_symbol("XOR"), "xor");
        assert_eq!(clean_symbol("xst usd/../"), "xstusd");
        assert_eq!(clean_symbol("@@"), "");
    }

    #[test]
    fn proxy_decisions_follow_the_node_rules() {
        assert!(matches!(decide(None), ProxyDecision::Bad("No URL")));
        assert!(matches!(
            decide(Some("nope")),
            ProxyDecision::Bad("Bad URL")
        ));
        assert!(matches!(
            decide(Some("ftp://github.com/x")),
            ProxyDecision::Bad("Bad protocol")
        ));
        assert!(matches!(
            decide(Some("http://127.0.0.1/x")),
            ProxyDecision::Placeholder
        ));
        assert!(matches!(
            decide(Some("http://172.20.1.1/x")),
            ProxyDecision::Placeholder
        ));
        assert!(matches!(
            decide(Some("https://example.com/x.png")),
            ProxyDecision::Placeholder
        ));
        assert!(matches!(
            decide(Some("https://raw.githubusercontent.com/a/b.png")),
            ProxyDecision::Fetch(_)
        ));
        assert!(is_allowed_host("sub.github.com"));
        assert!(!is_allowed_host("evilgithub.com"));
        assert_eq!(PLACEHOLDER_GIF.len(), 34);
    }
}
