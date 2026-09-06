//! Read-only chain access for the query API (group D endpoints).
//!
//! The Node kept one `@polkadot/api` connection per process and every
//! chain-state route read through it. v33's API is DB-first, so the
//! chain client is optional: with `WS_ENDPOINTS` unset the API still
//! serves every DB-backed route and the chain routes answer 503. When
//! set, a subxt `OnlineClient` is opened lazily on first use, trying
//! the endpoints in order, and dropped on any RPC error so the next
//! request reconnects (possibly on the next endpoint).

use std::sync::Arc;
use subxt::{OnlineClient, SubstrateConfig};
use tokio::sync::Mutex;
use tracing::{info, warn};
use url::Url;

/// Lazily connected, self-healing subxt client.
#[derive(Clone)]
pub struct ChainClient {
    endpoints: Arc<Vec<Url>>,
    inner: Arc<Mutex<Option<Connected>>>,
}

#[derive(Clone)]
struct Connected {
    client: OnlineClient<SubstrateConfig>,
    endpoint: Url,
}

/// Errors from [`ChainClient`].
#[derive(Debug, thiserror::Error)]
pub enum ChainError {
    /// No endpoint accepted a connection.
    #[error("no substrate endpoint reachable (tried {tried})")]
    Unreachable {
        /// Endpoints attempted.
        tried: usize,
    },
    /// An RPC / decode error from subxt (boxed: it is large).
    #[error("chain rpc: {0}")]
    Rpc(#[source] Box<subxt::Error>),
}

impl From<subxt::Error> for ChainError {
    fn from(e: subxt::Error) -> Self {
        Self::Rpc(Box::new(e))
    }
}

impl ChainClient {
    /// Build from a non-empty endpoint list (primary first).
    pub fn new(endpoints: Vec<Url>) -> Self {
        Self {
            endpoints: Arc::new(endpoints),
            inner: Arc::new(Mutex::new(None)),
        }
    }

    /// Parse `WS_ENDPOINTS` (comma-separated). `None` when unset/empty.
    pub fn from_env() -> Result<Option<Self>, String> {
        let raw = match std::env::var("WS_ENDPOINTS") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => return Ok(None),
        };
        let mut urls = Vec::new();
        for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let url = Url::parse(part).map_err(|e| format!("WS_ENDPOINTS '{part}': {e}"))?;
            if !matches!(url.scheme(), "ws" | "wss") {
                return Err(format!("WS_ENDPOINTS '{part}' must be ws:// or wss://"));
            }
            urls.push(url);
        }
        Ok((!urls.is_empty()).then(|| Self::new(urls)))
    }

    /// The connected client, connecting on first use.
    pub async fn client(&self) -> Result<OnlineClient<SubstrateConfig>, ChainError> {
        let mut guard = self.inner.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.client.clone());
        }
        for url in self.endpoints.iter() {
            match OnlineClient::<SubstrateConfig>::from_url(url.as_str()).await {
                Ok(client) => {
                    info!(endpoint = %url, "api chain client connected");
                    *guard = Some(Connected {
                        client: client.clone(),
                        endpoint: url.clone(),
                    });
                    return Ok(client);
                }
                Err(e) => warn!(endpoint = %url, error = %e, "api chain connect failed"),
            }
        }
        Err(ChainError::Unreachable {
            tried: self.endpoints.len(),
        })
    }

    /// Forget the current connection so the next call reconnects.
    pub async fn invalidate(&self) {
        *self.inner.lock().await = None;
    }

    /// Endpoint currently in use, if connected.
    pub async fn active_endpoint(&self) -> Option<Url> {
        self.inner.lock().await.as_ref().map(|c| c.endpoint.clone())
    }

    /// Configured endpoints, primary first.
    pub fn endpoints(&self) -> &[Url] {
        &self.endpoints
    }

    /// Run `f` with the client; on an RPC error drop the connection so
    /// the next request reconnects.
    pub async fn with_client<T, F, Fut>(&self, f: F) -> Result<T, ChainError>
    where
        F: FnOnce(OnlineClient<SubstrateConfig>) -> Fut,
        Fut: std::future::Future<Output = Result<T, subxt::Error>>,
    {
        let client = self.client().await?;
        match f(client).await {
            Ok(v) => Ok(v),
            Err(e) => {
                warn!(error = %e, "chain call failed; dropping connection");
                self.invalidate().await;
                Err(e.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_parsing_rejects_http_and_accepts_lists() {
        std::env::set_var("WS_ENDPOINTS", "wss://a.example, ws://127.0.0.1:9944");
        let c = ChainClient::from_env().unwrap().unwrap();
        assert_eq!(c.endpoints().len(), 2);
        std::env::set_var("WS_ENDPOINTS", "https://a.example");
        assert!(ChainClient::from_env().is_err());
        std::env::set_var("WS_ENDPOINTS", "  ");
        assert!(ChainClient::from_env().unwrap().is_none());
        std::env::remove_var("WS_ENDPOINTS");
    }
}
