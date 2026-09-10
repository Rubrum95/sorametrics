//! Torii REST client.
//!
//! Semantics mirror `minamoto/torii_client.js`: one request timeout,
//! retries with exponential backoff on transport errors and 5xx (4xx
//! bubble up at once), and a short per-URL cache so the API's
//! passthrough routes do not hammer the node. Paths are those of the
//! `optimizations@cfa5e8ce77` route catalogue; anything the Node
//! proxied verbatim is returned as `serde_json::Value`.

use crate::dto::{
    AccountDto, AppPage, AssetDefinitionDto, AssetDto, BlockDto, CursorPage, DomainDto,
    HistoryPage, InstructionDto, PeerInfoDto, StatusSnapshot, TransactionDto,
};
use serde::de::DeserializeOwned;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::debug;
use url::Url;

/// Explorer page-size bounds (`EXPLORER_CURSOR_DEFAULT_LIMIT` / `_MAX_LIMIT`).
pub const EXPLORER_DEFAULT_LIMIT: u32 = 25;
/// Largest `limit` the explorer accepts.
pub const EXPLORER_MAX_LIMIT: u32 = 100;

/// Errors from the Torii client.
#[derive(Debug, Error)]
pub enum ToriiError {
    /// Configuration problem (bad URL, unparsable env var).
    #[error("torii config: {0}")]
    Config(String),
    /// Torii answered with a non-2xx status.
    #[error("torii {status} on {url}: {body}")]
    Http {
        /// HTTP status code.
        status: u16,
        /// Requested URL.
        url: String,
        /// First 200 bytes of the body.
        body: String,
    },
    /// Connection / timeout / protocol failure after retries.
    #[error("torii transport on {url}: {source}")]
    Transport {
        /// Requested URL.
        url: String,
        /// Underlying error.
        #[source]
        source: reqwest::Error,
    },
    /// Body was not the expected JSON shape.
    #[error("torii invalid JSON from {url}: {reason}")]
    InvalidJson {
        /// Requested URL.
        url: String,
        /// Parse error.
        reason: String,
    },
}

impl ToriiError {
    /// HTTP status when the error is an HTTP one.
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Http { status, .. } => Some(*status),
            _ => None,
        }
    }
}

/// Client configuration (env: `MINAMOTO_TORII`, `MINAMOTO_HTTP_TIMEOUT_MS`,
/// `MINAMOTO_HTTP_RETRY_MAX`, `MINAMOTO_HTTP_RETRY_DELAY_MS`,
/// `MINAMOTO_HTTP_CACHE_TTL_MS`; defaults = the Node's `config.js`).
#[derive(Clone, Debug)]
pub struct ToriiConfig {
    /// Base URL without trailing slash.
    pub base: Url,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Retries after the first attempt.
    pub retry_max: u32,
    /// Base delay; attempt `n` waits `delay * 2^n`.
    pub retry_delay: Duration,
    /// Cache TTL for identical URLs.
    pub cache_ttl: Duration,
}

impl ToriiConfig {
    /// Config for a base URL with the Node's defaults.
    pub fn new(base: &str) -> Result<Self, ToriiError> {
        let base = Url::parse(base.trim_end_matches('/'))
            .map_err(|e| ToriiError::Config(format!("MINAMOTO_TORII '{base}': {e}")))?;
        if !matches!(base.scheme(), "http" | "https") {
            return Err(ToriiError::Config(format!(
                "MINAMOTO_TORII must be http(s), got '{}'",
                base.scheme()
            )));
        }
        Ok(Self {
            base,
            timeout: Duration::from_millis(10_000),
            retry_max: 2,
            retry_delay: Duration::from_millis(500),
            cache_ttl: Duration::from_millis(5_000),
        })
    }

    /// Reads the env vars; `MINAMOTO_TORII` defaults to `https://minamoto.sora.org`.
    pub fn from_env() -> Result<Self, ToriiError> {
        let base = std::env::var("MINAMOTO_TORII")
            .unwrap_or_else(|_| "https://minamoto.sora.org".to_string());
        let mut cfg = Self::new(&base)?;
        cfg.timeout = env_ms("MINAMOTO_HTTP_TIMEOUT_MS", cfg.timeout)?;
        cfg.retry_max = env_u32("MINAMOTO_HTTP_RETRY_MAX", cfg.retry_max)?;
        cfg.retry_delay = env_ms("MINAMOTO_HTTP_RETRY_DELAY_MS", cfg.retry_delay)?;
        cfg.cache_ttl = env_ms("MINAMOTO_HTTP_CACHE_TTL_MS", cfg.cache_ttl)?;
        Ok(cfg)
    }
}

fn env_ms(name: &str, default: Duration) -> Result<Duration, ToriiError> {
    match std::env::var(name) {
        Ok(v) => v
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| ToriiError::Config(format!("{name}='{v}': {e}"))),
        Err(_) => Ok(default),
    }
}

fn env_u32(name: &str, default: u32) -> Result<u32, ToriiError> {
    match std::env::var(name) {
        Ok(v) => v
            .parse::<u32>()
            .map_err(|e| ToriiError::Config(format!("{name}='{v}': {e}"))),
        Err(_) => Ok(default),
    }
}

#[derive(Clone)]
enum Cached {
    Json(serde_json::Value),
    Text(String),
}

struct Cache {
    entries: HashMap<String, (Instant, Cached)>,
    order: VecDeque<String>,
}

const CACHE_CAP: usize = 256;

/// Torii client. Cheap to clone (shared HTTP pool + cache).
#[derive(Clone)]
pub struct ToriiClient {
    cfg: ToriiConfig,
    http: reqwest::Client,
    cache: Arc<Mutex<Cache>>,
}

/// Filters of `/v1/explorer/transactions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxFilters {
    /// Signing account.
    pub authority: Option<String>,
    /// Block height.
    pub block: Option<u64>,
    /// `Committed` | `Rejected`.
    pub status: Option<String>,
}

/// Filters of `/v1/explorer/instructions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IsiFilters {
    /// Signing account.
    pub authority: Option<String>,
    /// Parent transaction hash.
    pub transaction_hash: Option<String>,
    /// Block height.
    pub block: Option<u64>,
}

impl ToriiClient {
    /// Builds the HTTP client.
    pub fn new(cfg: ToriiConfig) -> Result<Self, ToriiError> {
        let http = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .user_agent(concat!("sorametrics-v33/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| ToriiError::Config(format!("building HTTP client: {e}")))?;
        Ok(Self {
            cfg,
            http,
            cache: Arc::new(Mutex::new(Cache {
                entries: HashMap::new(),
                order: VecDeque::new(),
            })),
        })
    }

    /// Base URL as configured (for `/api/minamoto/health.torii`).
    pub fn base_url(&self) -> String {
        self.cfg.base.as_str().trim_end_matches('/').to_string()
    }

    fn url(&self, path: &str, query: &[(&str, String)]) -> Result<Url, ToriiError> {
        let mut u = self
            .cfg
            .base
            .join(path.trim_start_matches('/'))
            .map_err(|e| ToriiError::Config(format!("joining '{path}': {e}")))?;
        if !query.is_empty() {
            let mut q = u.query_pairs_mut();
            for (k, v) in query {
                q.append_pair(k, v);
            }
        }
        Ok(u)
    }

    async fn cached(&self, key: &str) -> Option<Cached> {
        let mut cache = self.cache.lock().await;
        let hit = cache
            .entries
            .get(key)
            .and_then(|(at, v)| (at.elapsed() <= self.cfg.cache_ttl).then(|| v.clone()));
        if hit.is_none() {
            cache.entries.remove(key);
        }
        hit
    }

    async fn store(&self, key: String, value: Cached) {
        let mut cache = self.cache.lock().await;
        if cache
            .entries
            .insert(key.clone(), (Instant::now(), value))
            .is_none()
        {
            cache.order.push_back(key);
        }
        while cache.order.len() > CACHE_CAP {
            if let Some(oldest) = cache.order.pop_front() {
                cache.entries.remove(&oldest);
            }
        }
    }

    async fn fetch_with_retry(&self, url: &Url, accept: &str) -> Result<String, ToriiError> {
        let mut attempt = 0u32;
        loop {
            let result = self
                .http
                .get(url.clone())
                .header(reqwest::header::ACCEPT, accept)
                .send()
                .await;
            let retry_after = self.cfg.retry_delay * 2u32.pow(attempt);
            match result {
                Ok(resp) => {
                    let status = resp.status();
                    let text = resp.text().await.map_err(|source| ToriiError::Transport {
                        url: url.to_string(),
                        source,
                    })?;
                    if status.is_success() {
                        return Ok(text);
                    }
                    if status.is_server_error() && attempt < self.cfg.retry_max {
                        debug!(%url, status = status.as_u16(), attempt, "torii 5xx, retrying");
                        tokio::time::sleep(retry_after).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(ToriiError::Http {
                        status: status.as_u16(),
                        url: url.to_string(),
                        body: text.chars().take(200).collect(),
                    });
                }
                Err(source) => {
                    if attempt < self.cfg.retry_max {
                        debug!(%url, error = %source, attempt, "torii transport error, retrying");
                        tokio::time::sleep(retry_after).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(ToriiError::Transport {
                        url: url.to_string(),
                        source,
                    });
                }
            }
        }
    }

    /// GET JSON (cached by URL).
    pub async fn get_json(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<serde_json::Value, ToriiError> {
        let url = self.url(path, query)?;
        let key = url.to_string();
        if let Some(Cached::Json(v)) = self.cached(&key).await {
            return Ok(v);
        }
        let text = self.fetch_with_retry(&url, "application/json").await?;
        let value: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| ToriiError::InvalidJson {
                url: key.clone(),
                reason: e.to_string(),
            })?;
        self.store(key, Cached::Json(value.clone())).await;
        Ok(value)
    }

    /// GET JSON decoded into `T` (cached as raw JSON by URL).
    pub async fn get_typed<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, ToriiError> {
        let value = self.get_json(path, query).await?;
        serde_json::from_value(value).map_err(|e| ToriiError::InvalidJson {
            url: self
                .url(path, query)
                .map(|u| u.to_string())
                .unwrap_or_default(),
            reason: e.to_string(),
        })
    }

    /// GET text (cached by URL).
    pub async fn get_text(&self, path: &str) -> Result<String, ToriiError> {
        let url = self.url(path, &[])?;
        let key = url.to_string();
        if let Some(Cached::Text(t)) = self.cached(&key).await {
            return Ok(t);
        }
        let text = self.fetch_with_retry(&url, "text/plain").await?;
        self.store(key, Cached::Text(text.clone())).await;
        Ok(text)
    }

    // ---- public, unauthenticated routes ------------------------------

    /// `/health` body.
    pub async fn health(&self) -> Result<String, ToriiError> {
        self.get_text("/health").await
    }

    /// `/status` as JSON (the Node proxied it verbatim).
    pub async fn status(&self) -> Result<serde_json::Value, ToriiError> {
        self.get_json("/status", &[]).await
    }

    /// Typed subset of `/status`.
    pub async fn status_snapshot(&self) -> Result<StatusSnapshot, ToriiError> {
        self.get_typed("/status", &[]).await
    }

    /// `/metrics` Prometheus text.
    pub async fn metrics(&self) -> Result<String, ToriiError> {
        self.get_text("/metrics").await
    }

    /// `/v1/telemetry/peers-info`, typed.
    pub async fn peers_info(&self) -> Result<Vec<PeerInfoDto>, ToriiError> {
        self.get_typed("/v1/telemetry/peers-info", &[]).await
    }

    /// `/v1/telemetry/peers-info`, raw.
    pub async fn peers_info_raw(&self) -> Result<serde_json::Value, ToriiError> {
        self.get_json("/v1/telemetry/peers-info", &[]).await
    }

    /// `/v1/telemetry/propagation`, raw.
    pub async fn propagation(&self) -> Result<serde_json::Value, ToriiError> {
        self.get_json("/v1/telemetry/propagation", &[]).await
    }

    fn cursor_query(cursor: Option<&str>, limit: u32) -> Vec<(&'static str, String)> {
        let mut q = vec![("limit", limit.clamp(1, EXPLORER_MAX_LIMIT).to_string())];
        if let Some(c) = cursor {
            q.push(("cursor", c.to_string()));
        }
        q
    }

    /// `/v1/explorer/blocks`, newest first.
    pub async fn explorer_blocks(
        &self,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<HistoryPage<BlockDto>, ToriiError> {
        self.get_typed("/v1/explorer/blocks", &Self::cursor_query(cursor, limit))
            .await
    }

    /// `/v1/explorer/transactions`, newest first.
    pub async fn explorer_transactions(
        &self,
        f: &TxFilters,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<HistoryPage<TransactionDto>, ToriiError> {
        let mut q = Self::cursor_query(cursor, limit);
        if let Some(a) = &f.authority {
            q.push(("authority", a.clone()));
        }
        if let Some(b) = f.block {
            q.push(("block", b.to_string()));
        }
        if let Some(s) = &f.status {
            q.push(("status", s.clone()));
        }
        self.get_typed("/v1/explorer/transactions", &q).await
    }

    /// `/v1/explorer/instructions`, newest first.
    pub async fn explorer_instructions(
        &self,
        f: &IsiFilters,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<HistoryPage<InstructionDto>, ToriiError> {
        let mut q = Self::cursor_query(cursor, limit);
        if let Some(a) = &f.authority {
            q.push(("authority", a.clone()));
        }
        if let Some(h) = &f.transaction_hash {
            q.push(("transaction_hash", h.clone()));
        }
        if let Some(b) = f.block {
            q.push(("block", b.to_string()));
        }
        self.get_typed("/v1/explorer/instructions", &q).await
    }

    /// `/v1/explorer/accounts`.
    pub async fn explorer_accounts(
        &self,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<CursorPage<AccountDto>, ToriiError> {
        self.get_typed("/v1/explorer/accounts", &Self::cursor_query(cursor, limit))
            .await
    }

    /// `/v1/explorer/domains`.
    pub async fn explorer_domains(
        &self,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<CursorPage<DomainDto>, ToriiError> {
        self.get_typed("/v1/explorer/domains", &Self::cursor_query(cursor, limit))
            .await
    }

    /// `/v1/explorer/assets`.
    pub async fn explorer_assets(
        &self,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<CursorPage<AssetDto>, ToriiError> {
        self.get_typed("/v1/explorer/assets", &Self::cursor_query(cursor, limit))
            .await
    }

    /// `/v1/explorer/nfts`, raw (API passthrough).
    pub async fn explorer_nfts(
        &self,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<serde_json::Value, ToriiError> {
        self.get_json("/v1/explorer/nfts", &Self::cursor_query(cursor, limit))
            .await
    }

    /// `/v1/explorer/rwas`, raw (API passthrough).
    pub async fn explorer_rwas(
        &self,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<serde_json::Value, ToriiError> {
        self.get_json("/v1/explorer/rwas", &Self::cursor_query(cursor, limit))
            .await
    }

    /// `/v1/explorer/blocks/{identifier}` (height or hash), raw.
    pub async fn block(&self, identifier: &str) -> Result<serde_json::Value, ToriiError> {
        self.get_json(&format!("/v1/explorer/blocks/{}", enc(identifier)), &[])
            .await
    }

    /// `/v1/explorer/transactions/{hash}`, raw (carries `metadata`).
    pub async fn transaction(&self, hash: &str) -> Result<serde_json::Value, ToriiError> {
        self.get_json(&format!("/v1/explorer/transactions/{}", enc(hash)), &[])
            .await
    }

    /// `/v1/assets/definitions?limit&offset` (exact totals).
    pub async fn asset_definitions(
        &self,
        limit: u32,
        offset: u64,
    ) -> Result<AppPage<AssetDefinitionDto>, ToriiError> {
        self.get_typed(
            "/v1/assets/definitions",
            &[
                ("limit", limit.to_string()),
                ("offset", offset.to_string()),
                ("count_mode", "exact".to_string()),
            ],
        )
        .await
    }

    /// `/v1/assets/definitions/{asset}`, raw.
    pub async fn asset_definition(&self, id: &str) -> Result<serde_json::Value, ToriiError> {
        self.get_json(&format!("/v1/assets/definitions/{}", enc(id)), &[])
            .await
    }

    /// `/v1/accounts/{id}/assets`, raw.
    pub async fn account_assets(&self, account: &str) -> Result<serde_json::Value, ToriiError> {
        self.get_json(&format!("/v1/accounts/{}/assets", enc(account)), &[])
            .await
    }

    /// `/v1/accounts/{id}/transactions`, raw.
    pub async fn account_transactions(
        &self,
        account: &str,
    ) -> Result<serde_json::Value, ToriiError> {
        self.get_json(&format!("/v1/accounts/{}/transactions", enc(account)), &[])
            .await
    }

    /// `/v1/accounts/{id}/permissions`, raw.
    pub async fn account_permissions(
        &self,
        account: &str,
    ) -> Result<serde_json::Value, ToriiError> {
        self.get_json(&format!("/v1/accounts/{}/permissions", enc(account)), &[])
            .await
    }
}

/// Percent-encodes one path segment (I105 ids carry katakana; `#` in
/// asset ids must not start a fragment).
fn enc(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        let keep = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if keep {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> ToriiClient {
        ToriiClient::new(ToriiConfig::new("https://minamoto.sora.org/").unwrap()).unwrap()
    }

    #[test]
    fn base_url_has_no_trailing_slash() {
        assert_eq!(client().base_url(), "https://minamoto.sora.org");
    }

    #[test]
    fn rejects_non_http_base() {
        assert!(ToriiConfig::new("wss://x").is_err());
        assert!(ToriiConfig::new("not a url").is_err());
    }

    #[test]
    fn cursor_query_clamps_limit_and_adds_cursor() {
        let q = ToriiClient::cursor_query(Some("abc"), 500);
        assert_eq!(
            q,
            vec![("limit", "100".to_string()), ("cursor", "abc".to_string())]
        );
        let q = ToriiClient::cursor_query(None, 0);
        assert_eq!(q, vec![("limit", "1".to_string())]);
    }

    #[test]
    fn urls_encode_path_segments() {
        let c = client();
        let u = c
            .url(&format!("/v1/accounts/{}/assets", enc("sorauﾛ1#x")), &[])
            .unwrap();
        assert_eq!(
            u.as_str(),
            "https://minamoto.sora.org/v1/accounts/sorau%EF%BE%9B1%23x/assets"
        );
        let u = c
            .url(
                "/v1/explorer/blocks",
                &[("limit", "25".into()), ("cursor", "a b".into())],
            )
            .unwrap();
        assert_eq!(
            u.as_str(),
            "https://minamoto.sora.org/v1/explorer/blocks?limit=25&cursor=a+b"
        );
    }

    #[tokio::test]
    async fn cache_expires_and_caps() {
        let mut cfg = ToriiConfig::new("http://127.0.0.1:1").unwrap();
        cfg.cache_ttl = Duration::from_millis(20);
        let c = ToriiClient::new(cfg).unwrap();
        c.store("k".into(), Cached::Text("v".into())).await;
        assert!(matches!(c.cached("k").await, Some(Cached::Text(ref t)) if t == "v"));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(c.cached("k").await.is_none());
        for i in 0..(CACHE_CAP + 5) {
            c.store(format!("k{i}"), Cached::Text(String::new())).await;
        }
        assert_eq!(c.cache.lock().await.entries.len(), CACHE_CAP);
        assert!(c.cached("k0").await.is_none());
    }

    #[tokio::test]
    async fn transport_error_after_retries() {
        let mut cfg = ToriiConfig::new("http://127.0.0.1:9").unwrap();
        cfg.retry_max = 1;
        cfg.retry_delay = Duration::from_millis(1);
        cfg.timeout = Duration::from_millis(500);
        let c = ToriiClient::new(cfg).unwrap();
        let err = c.health().await.unwrap_err();
        assert!(matches!(err, ToriiError::Transport { .. }), "{err}");
    }
}
