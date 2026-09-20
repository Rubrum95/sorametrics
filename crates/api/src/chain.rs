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
use subxt::backend::rpc::RpcClient;
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
    /// Raw RPC on the same connection (batched `state_queryStorageAt`).
    rpc: RpcClient,
    /// Same connection with SORA's raw `AccountId32` address type, for
    /// building extrinsics (fee samples).
    sora: OnlineClient<SoraConfig>,
    endpoint: Url,
}

/// subxt `Config` matching the SORA runtime's extrinsic format: the
/// `Address` is the bare `AccountId32` (not `MultiAddress`). Storage and
/// block reads are identical to `SubstrateConfig`; only transaction
/// building depends on this.
pub enum SoraConfig {}

impl subxt::Config for SoraConfig {
    type Hash = subxt::utils::H256;
    type AccountId = subxt::utils::AccountId32;
    type Address = subxt::utils::AccountId32;
    type Signature = subxt::utils::MultiSignature;
    type Hasher = subxt::config::substrate::BlakeTwo256;
    type Header = subxt::config::substrate::SubstrateHeader<u32, Self::Hasher>;
    type ExtrinsicParams = subxt::config::DefaultExtrinsicParams<Self>;
    type AssetId = u32;
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

    /// The archive node for reads at historical blocks (`/block/:n`, the
    /// governance preimage scans): `ARCHIVE_WS_ENDPOINT`, defaulting to
    /// `wss://mof2.sora.org` like the Node. Validators keep only recent
    /// state, so those reads cannot go to `WS_ENDPOINTS`.
    pub fn archive_from_env() -> Result<Self, String> {
        let raw = std::env::var("ARCHIVE_WS_ENDPOINT")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "wss://mof2.sora.org".to_string());
        let url = Url::parse(&raw).map_err(|e| format!("ARCHIVE_WS_ENDPOINT '{raw}': {e}"))?;
        if !matches!(url.scheme(), "ws" | "wss") {
            return Err(format!(
                "ARCHIVE_WS_ENDPOINT '{raw}' must be ws:// or wss://"
            ));
        }
        Ok(Self::new(vec![url]))
    }

    /// The connected client, connecting on first use.
    pub async fn client(&self) -> Result<OnlineClient<SubstrateConfig>, ChainError> {
        let mut guard = self.inner.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.client.clone());
        }
        for url in self.endpoints.iter() {
            match Self::connect(url).await {
                Ok(c) => {
                    info!(endpoint = %url, "api chain client connected");
                    let client = c.client.clone();
                    *guard = Some(c);
                    return Ok(client);
                }
                Err(e) => warn!(endpoint = %url, error = %e, "api chain connect failed"),
            }
        }
        Err(ChainError::Unreachable {
            tried: self.endpoints.len(),
        })
    }

    /// When a failover left the client on a secondary endpoint, try the
    /// primary again and move back if it answers. Returns `true` on a move.
    /// The probe connects before taking the lock, so requests keep being
    /// served by the secondary meanwhile.
    pub async fn recover_primary(&self) -> bool {
        let Some(primary) = self.endpoints.first() else {
            return false;
        };
        match self.active_endpoint().await {
            Some(active) if active != *primary => {}
            _ => return false,
        }
        match Self::connect(primary).await {
            Ok(c) => {
                info!(endpoint = %primary, "api chain client back on the primary endpoint");
                *self.inner.lock().await = Some(c);
                true
            }
            Err(_) => false,
        }
    }

    /// Probe for the primary every `every` (the Node did it every 2 min).
    pub fn spawn_primary_recovery(&self, every: std::time::Duration) {
        if self.endpoints.len() < 2 {
            return;
        }
        let chain = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                chain.recover_primary().await;
            }
        });
    }

    async fn connect(url: &Url) -> Result<Connected, subxt::Error> {
        let rpc = RpcClient::from_url(url.as_str()).await?;
        let client = sorametrics_substrate::online_client::<SubstrateConfig>(rpc.clone()).await?;
        let sora = OnlineClient::<SoraConfig>::from_backend_with(
            client.genesis_hash(),
            client.runtime_version(),
            client.metadata(),
            sorametrics_substrate::legacy_backend::<SoraConfig>(rpc.clone()),
        )?;
        Ok(Connected {
            client,
            rpc,
            sora,
            endpoint: url.clone(),
        })
    }

    /// The SORA-address-typed client (transaction building).
    pub async fn sora_client(&self) -> Result<OnlineClient<SoraConfig>, ChainError> {
        self.client().await?;
        let guard = self.inner.lock().await;
        guard
            .as_ref()
            .map(|c| c.sora.clone())
            .ok_or(ChainError::Unreachable {
                tried: self.endpoints.len(),
            })
    }

    /// The raw RPC client of the current connection (custom SORA RPCs
    /// such as `liquidityProxy_quote`).
    pub async fn rpc(&self) -> Result<RpcClient, ChainError> {
        self.client().await?;
        let guard = self.inner.lock().await;
        guard
            .as_ref()
            .map(|c| c.rpc.clone())
            .ok_or(ChainError::Unreachable {
                tried: self.endpoints.len(),
            })
    }

    /// Legacy RPC methods on the current connection (`state_queryStorageAt`
    /// batches — the polkadot-js `.multi` equivalent).
    pub async fn legacy_rpc(
        &self,
    ) -> Result<subxt::backend::legacy::LegacyRpcMethods<SubstrateConfig>, ChainError> {
        self.client().await?;
        let guard = self.inner.lock().await;
        guard
            .as_ref()
            .map(|c| subxt::backend::legacy::LegacyRpcMethods::new(c.rpc.clone()))
            .ok_or(ChainError::Unreachable {
                tried: self.endpoints.len(),
            })
    }

    /// Fetch many storage entries in ONE `state_queryStorageAt` (the
    /// polkadot-js `.multi` equivalent); each result is `None` when the
    /// key is absent. Values are SCALE-decoded as `T`.
    pub async fn fetch_many<T: subxt::ext::codec::Decode>(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<T>>, ChainError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let legacy = self.legacy_rpc().await?;
        let sets = legacy
            .state_query_storage_at(keys.iter().map(Vec::as_slice), None)
            .await?;
        let mut by_key: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        for set in sets {
            for (k, v) in set.changes {
                if let Some(bytes) = v {
                    by_key.insert(k.0, bytes.0);
                }
            }
        }
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            match by_key.get(k) {
                Some(bytes) => {
                    let v = T::decode(&mut &bytes[..])
                        .map_err(|e| subxt::Error::Decode(subxt::error::DecodeError::from(e)))?;
                    out.push(Some(v));
                }
                None => out.push(None),
            }
        }
        Ok(out)
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
