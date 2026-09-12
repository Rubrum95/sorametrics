//! Per-client, per-route fixed-window rate limiting — the Node's
//! `rateLimit(maxReqs, 60000)` middleware: one bucket per `(ip, route
//! pattern)`, a 60 s window that restarts when it expires, `429
//! {"error":"Too many requests"}` past the maximum. Maxima are the
//! Node's, keyed by route pattern; routes it did not limit stay
//! unlimited.
//!
//! Deviation: the Node never set `trust proxy`, so behind nginx its
//! `req.ip` was the proxy address and every client shared one bucket.
//! Here the client is the first `X-Forwarded-For` entry, then
//! `X-Real-IP`, then the socket peer.

use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::{header::HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Window length (`windowMs`).
pub const WINDOW: Duration = Duration::from_secs(60);
/// Entries idle longer than this are swept (the Node's 120 s cleanup).
const SWEEP_AFTER: Duration = Duration::from_secs(120);
const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// `(route pattern with parameters as `:p`, requests per window)` — the
/// Node's table (`index.js` + `minamoto/routes.js`).
const LIMITS: &[(&str, u32)] = &[
    ("/", 60),
    ("/sorav2", 60),
    ("/minamoto", 60),
    ("/favicon.svg", 60),
    ("/api/version", 30),
    ("/health", 30),
    ("/health/rpc-source", 60),
    ("/proxy-image", 30),
    ("/tokens", 30),
    ("/pools", 20),
    ("/pool/providers", 10),
    ("/pool/activity", 20),
    ("/stats/network/trend", 15),
    ("/stats/stablecoins", 20),
    ("/stats/trending-tokens", 20),
    ("/stats/accumulation", 15),
    ("/stats/extrinsics-24h", 30),
    ("/stats/fee-config", 30),
    ("/stats/fees", 20),
    ("/stats/fees/trend", 20),
    ("/stats/header", 30),
    ("/stats/network", 20),
    ("/stats/overview", 60),
    ("/holders/:p", 15),
    ("/wallet/liquidity/:p", 300),
    ("/wallet/staking/:p", 300),
    ("/wallet/info/:p", 300),
    ("/identity/:p", 60),
    ("/currency-rates", 10),
    ("/balance/:p", 300),
    ("/balances", 20),
    ("/history/global/transfers", 30),
    ("/history/global/swaps", 30),
    ("/history/global/bridges", 30),
    ("/history/global/liquidity", 30),
    ("/history/global/orderbook", 30),
    ("/history/global/extrinsics", 60),
    ("/history/transfers/:p", 30),
    ("/history/swaps/:p", 30),
    ("/history/bridges/:p", 30),
    ("/history/orderbook/:p", 30),
    ("/history/extrinsics/:p", 30),
    ("/history/extrinsic/:p/:p", 30),
    ("/history/extrinsic-fees", 60),
    ("/history/extrinsic-sections", 10),
    ("/chart/:p", 30),
    ("/export/csv", 10),
    ("/search", 20),
    ("/tools/price-series", 60),
    ("/lookup/usd-value/:p", 30),
    ("/mof/qty/:p", 60),
    ("/news/episodes", 60),
    ("/burns/fee-flow", 20),
    ("/burns/series/:p", 30),
    ("/burns/stats/:p", 20),
    ("/burns/supply-history/:p", 15),
    ("/burns/supply/:p", 20),
    ("/staking/rewards/live", 120),
    ("/polkamarkt/buybacks", 30),
    ("/polkamarkt/market/:p", 30),
    ("/polkamarkt/markets", 30),
    ("/polkamarkt/positions/:p", 30),
    ("/polkamarkt/state", 30),
    ("/governance/preimage/:p/decode-pretty", 60),
    ("/api/identities", 30),
    // /api/minamoto (its router's own limits)
    ("/api/minamoto/health", 60),
    ("/api/minamoto/torii/health", 60),
    ("/api/minamoto/status", 60),
    ("/api/minamoto/network-state", 60),
    ("/api/minamoto/network-state/live", 30),
    ("/api/minamoto/blocks", 60),
    ("/api/minamoto/blocks/stats", 60),
    ("/api/minamoto/transactions", 60),
    ("/api/minamoto/transactions/stats", 60),
    ("/api/minamoto/transactions/fee-sponsorship", 60),
    ("/api/minamoto/wallet/:p/info", 30),
    ("/api/minamoto/accounts", 60),
    ("/api/minamoto/accounts/stats", 60),
    ("/api/minamoto/accounts/:p/assets", 60),
    ("/api/minamoto/accounts/:p/transactions", 60),
    ("/api/minamoto/accounts/:p/permissions", 60),
    ("/api/minamoto/domains", 60),
    ("/api/minamoto/domains/stats", 60),
    ("/api/minamoto/assets", 60),
    ("/api/minamoto/asset-definitions", 60),
    ("/api/minamoto/asset-definitions/stats", 60),
    ("/api/minamoto/asset/:p", 60),
    ("/api/minamoto/asset/:p/holders", 60),
    ("/api/minamoto/block/:p", 60),
    ("/api/minamoto/tx/:p", 60),
    ("/api/minamoto/instructions", 60),
    ("/api/minamoto/instructions/kinds", 60),
    ("/api/minamoto/transfers/stats", 60),
    ("/api/minamoto/permissions/stats", 60),
    ("/api/minamoto/permissions/grants", 60),
    ("/api/minamoto/lane-staking/lifecycle", 60),
    ("/api/minamoto/telemetry/peers-info", 60),
    ("/api/minamoto/telemetry/propagation", 60),
    ("/api/minamoto/telemetry/sumeragi", 60),
    ("/api/minamoto/sumeragi/roles", 30),
    ("/api/minamoto/gov/council", 60),
    ("/api/minamoto/gov/unlocks", 60),
    ("/api/minamoto/kaigi/relays", 60),
    ("/api/minamoto/explorer/nfts", 60),
    ("/api/minamoto/explorer/rwas", 60),
    ("/api/minamoto/cross-chain/stats", 60),
    ("/api/minamoto/cross-chain/timeseries", 60),
    ("/api/minamoto/cross-chain/claims", 60),
    ("/api/minamoto/cross-chain/mint-history", 60),
    ("/api/minamoto/cross-chain/pending-burns", 60),
    ("/api/minamoto/peers", 60),
    ("/api/minamoto/prometheus/raw", 10),
    ("/api/minamoto/prometheus/parsed", 20),
    ("/api/minamoto/prometheus/metric/:p", 60),
    ("/api/minamoto/indexer/state", 30),
];

/// Route pattern with every `:param` segment replaced by `:p`.
pub fn normalize(pattern: &str) -> String {
    pattern
        .split('/')
        .map(|seg| if seg.starts_with(':') { ":p" } else { seg })
        .collect::<Vec<_>>()
        .join("/")
}

/// Requests per window for an axum route pattern; `None` = unlimited.
pub fn limit_for(pattern: &str) -> Option<u32> {
    let key = normalize(pattern);
    // `/api/sorav2/xor-migration` is the Node's second mount of the
    // Minamoto router: same per-route limits.
    let key = match key.strip_prefix("/api/sorav2/xor-migration") {
        Some(rest) => format!("/api/minamoto{rest}"),
        None => key,
    };
    LIMITS.iter().find(|(p, _)| *p == key).map(|(_, max)| *max)
}

struct Bucket {
    start: Instant,
    count: u32,
}

/// Shared limiter state.
#[derive(Clone, Default)]
pub struct RateLimiter {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    buckets: HashMap<(IpAddr, String), Bucket>,
    last_sweep: Option<Instant>,
}

impl RateLimiter {
    /// Registers one request; `true` when it exceeds `max` in the window.
    pub fn exceeds(&self, ip: IpAddr, route: &str, max: u32, now: Instant) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner
            .last_sweep
            .is_none_or(|t| now.duration_since(t) >= SWEEP_EVERY)
        {
            inner
                .buckets
                .retain(|_, b| now.duration_since(b.start) <= SWEEP_AFTER);
            inner.last_sweep = Some(now);
        }
        let bucket = inner
            .buckets
            .entry((ip, route.to_string()))
            .or_insert(Bucket {
                start: now,
                count: 0,
            });
        if now.duration_since(bucket.start) > WINDOW {
            bucket.start = now;
            bucket.count = 0;
        }
        bucket.count += 1;
        bucket.count > max
    }
}

/// Client address: `X-Forwarded-For` (first hop), `X-Real-IP`, socket peer.
pub fn client_ip(headers: &HeaderMap, peer: Option<SocketAddr>) -> IpAddr {
    let from_header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .and_then(|v| v.trim().parse::<IpAddr>().ok())
    };
    from_header("x-forwarded-for")
        .or_else(|| from_header("x-real-ip"))
        .or_else(|| peer.map(|p| p.ip()))
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))
}

/// axum middleware (`middleware::from_fn_with_state`).
pub async fn middleware(State(limiter): State<RateLimiter>, req: Request, next: Next) -> Response {
    let Some(pattern) = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
    else {
        return next.run(req).await;
    };
    let Some(max) = limit_for(&pattern) else {
        return next.run(req).await;
    };
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0);
    let ip = client_ip(req.headers(), peer);
    if limiter.exceeds(ip, &pattern, max, Instant::now()) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({ "error": "Too many requests" })),
        )
            .into_response();
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns_normalize_to_node_table() {
        assert_eq!(limit_for("/history/transfers/:address"), Some(30));
        assert_eq!(limit_for("/holders/:asset_id"), Some(15));
        assert_eq!(limit_for("/history/extrinsic/:block/:index"), Some(30));
        assert_eq!(limit_for("/api/minamoto/wallet/:addr/info"), Some(30));
        assert_eq!(limit_for("/api/minamoto/prometheus/raw"), Some(10));
        assert_eq!(limit_for("/music/list"), None);
        assert_eq!(limit_for("/health/freshness"), None);
    }

    #[test]
    fn fixed_window_per_ip_and_route() {
        let l = RateLimiter::default();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let other: IpAddr = "10.0.0.2".parse().unwrap();
        let t0 = Instant::now();
        for _ in 0..3 {
            assert!(!l.exceeds(ip, "/x", 3, t0));
        }
        assert!(l.exceeds(ip, "/x", 3, t0));
        assert!(!l.exceeds(other, "/x", 3, t0));
        assert!(!l.exceeds(ip, "/y", 3, t0));
        // Window restarts after 60 s (strictly greater, as the Node's `>`).
        assert!(l.exceeds(ip, "/x", 3, t0 + Duration::from_secs(60)));
        assert!(!l.exceeds(ip, "/x", 3, t0 + Duration::from_secs(61)));
    }

    #[test]
    fn client_ip_precedence() {
        let mut h = HeaderMap::new();
        let peer: SocketAddr = "127.0.0.1:9".parse().unwrap();
        assert_eq!(client_ip(&h, Some(peer)).to_string(), "127.0.0.1");
        h.insert("x-real-ip", "203.0.113.5".parse().unwrap());
        assert_eq!(client_ip(&h, Some(peer)).to_string(), "203.0.113.5");
        h.insert("x-forwarded-for", "198.51.100.7, 10.0.0.1".parse().unwrap());
        assert_eq!(client_ip(&h, Some(peer)).to_string(), "198.51.100.7");
        assert_eq!(client_ip(&HeaderMap::new(), None).to_string(), "0.0.0.0");
    }

    #[test]
    fn every_table_pattern_is_normalized() {
        for (p, _) in LIMITS {
            assert_eq!(normalize(p), *p, "{p}");
        }
    }
}
