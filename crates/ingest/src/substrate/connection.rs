//! Failover-aware WebSocket connection to a SORA Substrate node.
//!
//! Owns an ordered list of WS endpoints (primary first). Tries each in
//! order; on connection failure or RPC error, rotates to the next.
//! Reconnection is lazy: the next call after a marked-dead state forces
//! a rotation.
//!
//! All access is async-safe via `tokio::sync::Mutex`. The mutex is held
//! only across short critical sections (swap the active client); RPC
//! calls themselves use a cloned `Arc<WsClient>` so they don't block
//! other callers.

use jsonrpsee::core::client::ClientT;
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use url::Url;

/// Substrate `system_health` RPC response (only the fields we use).
///
/// `#[serde(rename_all = "camelCase")]` because Substrate emits camelCase
/// over the wire. Unknown fields are ignored by default.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemHealth {
    /// Number of connected peers.
    pub peers: u32,
    /// Whether the node is currently syncing.
    pub is_syncing: bool,
}

/// Errors surfaced by [`WsConnection`].
#[derive(Debug, Error)]
pub enum ConnectionError {
    /// All endpoints in the failover list failed to connect.
    #[error("all WS endpoints failed to connect ({0} tried)")]
    AllEndpointsFailed(usize),

    /// An RPC call failed (e.g., disconnect mid-call, server error).
    #[error("rpc call '{method}': {source}")]
    Rpc {
        /// The method name we attempted.
        method: String,
        /// Underlying jsonrpsee client error.
        #[source]
        source: jsonrpsee::core::client::Error,
    },
}

/// A WebSocket connection with multi-endpoint failover.
///
/// Cloning is cheap (`Arc` internally) — share across tasks freely.
#[derive(Clone)]
pub struct WsConnection {
    endpoints: Arc<Vec<Url>>,
    connect_timeout: Duration,
    state: Arc<Mutex<State>>,
}

// Manual `Debug` impl: the inner `WsClient` doesn't implement `Debug`, and
// neither does `Arc<Mutex<...>>` containing it. Print only the parts that
// don't require locking.
impl std::fmt::Debug for WsConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsConnection")
            .field("endpoints", &self.endpoints.len())
            .field("connect_timeout", &self.connect_timeout)
            .finish_non_exhaustive()
    }
}

struct State {
    /// Index into `endpoints` of the currently-active connection.
    active_index: usize,
    /// `None` if no live client (will reconnect on next call).
    client: Option<Arc<WsClient>>,
}

impl WsConnection {
    /// Connect to the first reachable endpoint, starting from index 0.
    ///
    /// Returns an error only if **all** endpoints failed.
    pub async fn connect(
        endpoints: Vec<Url>,
        connect_timeout: Duration,
    ) -> Result<Self, ConnectionError> {
        assert!(
            !endpoints.is_empty(),
            "WsConnection::connect called with empty endpoints — guard at config layer"
        );
        let endpoints = Arc::new(endpoints);
        let (active_index, client) = try_each(&endpoints, 0, connect_timeout).await?;

        info!(
            endpoint = %endpoints[active_index],
            index = active_index,
            "connected to substrate WS"
        );

        Ok(Self {
            endpoints,
            connect_timeout,
            state: Arc::new(Mutex::new(State {
                active_index,
                client: Some(client),
            })),
        })
    }

    /// All endpoints, in failover priority order.
    pub fn endpoints(&self) -> &[Url] {
        &self.endpoints
    }

    /// Index (into [`endpoints`]) of the currently-active endpoint.
    pub async fn active_index(&self) -> usize {
        self.state.lock().await.active_index
    }

    /// `true` if the active endpoint is the primary (index 0).
    pub async fn is_on_primary(&self) -> bool {
        self.active_index().await == 0
    }

    /// Force rotation to the next endpoint, regardless of current state.
    ///
    /// Used by the healthcheck loop when the active endpoint stops responding.
    /// Re-establishes the connection (or returns `AllEndpointsFailed`).
    pub async fn rotate(&self) -> Result<usize, ConnectionError> {
        let mut s = self.state.lock().await;
        let start = (s.active_index + 1) % self.endpoints.len();
        let (new_index, new_client) =
            try_each(&self.endpoints, start, self.connect_timeout).await?;
        warn!(
            from_index = s.active_index,
            to_index = new_index,
            from = %self.endpoints[s.active_index],
            to = %self.endpoints[new_index],
            "rotated WS endpoint"
        );
        s.active_index = new_index;
        s.client = Some(new_client);
        Ok(new_index)
    }

    /// Try to reconnect using the current endpoint first; fall back via failover.
    ///
    /// Used when a call detects the connection is dead.
    pub async fn reconnect(&self) -> Result<(), ConnectionError> {
        let mut s = self.state.lock().await;
        let (new_index, new_client) =
            try_each(&self.endpoints, s.active_index, self.connect_timeout).await?;
        if new_index != s.active_index {
            warn!(
                from = %self.endpoints[s.active_index],
                to = %self.endpoints[new_index],
                "reconnect rotated endpoint"
            );
        } else {
            debug!(endpoint = %self.endpoints[new_index], "reconnected to same endpoint");
        }
        s.active_index = new_index;
        s.client = Some(new_client);
        Ok(())
    }

    /// Returns a clone of the live client (forces a reconnect if marked dead).
    async fn client_or_reconnect(&self) -> Result<Arc<WsClient>, ConnectionError> {
        {
            let s = self.state.lock().await;
            if let Some(c) = &s.client {
                return Ok(c.clone());
            }
        }
        self.reconnect().await?;
        let s = self.state.lock().await;
        s.client
            .clone()
            .ok_or(ConnectionError::AllEndpointsFailed(self.endpoints.len()))
    }

    /// Marks the active client as dead. Next call will trigger a reconnect.
    async fn mark_dead(&self) {
        let mut s = self.state.lock().await;
        s.client = None;
    }

    /// Calls Substrate's `system_health` RPC. Used both as liveness check
    /// and as smoke test post-connect.
    pub async fn system_health(&self) -> Result<SystemHealth, ConnectionError> {
        let client = self.client_or_reconnect().await?;
        match client.request("system_health", rpc_params![]).await {
            Ok(h) => Ok(h),
            Err(e) => {
                self.mark_dead().await;
                Err(ConnectionError::Rpc {
                    method: "system_health".into(),
                    source: e,
                })
            }
        }
    }
}

/// Try connecting to endpoints starting at `start_index`, rotating forward.
async fn try_each(
    endpoints: &[Url],
    start_index: usize,
    timeout: Duration,
) -> Result<(usize, Arc<WsClient>), ConnectionError> {
    let n = endpoints.len();
    for offset in 0..n {
        let i = (start_index + offset) % n;
        let url = &endpoints[i];
        debug!(endpoint = %url, index = i, "attempting connect");
        match WsClientBuilder::default()
            .connection_timeout(timeout)
            .build(url.as_str())
            .await
        {
            Ok(c) => return Ok((i, Arc::new(c))),
            Err(e) => {
                warn!(endpoint = %url, index = i, error = %e, "connect failed");
                continue;
            }
        }
    }
    Err(ConnectionError::AllEndpointsFailed(n))
}

#[cfg(test)]
mod tests {
    //! Tests use deliberately-bad URLs (closed ports on localhost) to
    //! exercise failover without depending on external network. Each
    //! attempt fails fast (sub-second).

    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).expect("valid test URL")
    }

    #[tokio::test]
    async fn connect_fails_when_all_endpoints_unreachable() {
        // Two definitely-closed ports on localhost.
        let endpoints = vec![url("ws://127.0.0.1:1"), url("ws://127.0.0.1:2")];
        let err = WsConnection::connect(endpoints, Duration::from_millis(500))
            .await
            .unwrap_err();
        match err {
            ConnectionError::AllEndpointsFailed(n) => assert_eq!(n, 2),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn try_each_visits_all_endpoints_when_all_fail() {
        let endpoints = vec![
            url("ws://127.0.0.1:1"),
            url("ws://127.0.0.1:2"),
            url("ws://127.0.0.1:3"),
        ];
        // start_index = 1 should still try all three before failing.
        let err = try_each(&endpoints, 1, Duration::from_millis(500))
            .await
            .unwrap_err();
        assert!(matches!(err, ConnectionError::AllEndpointsFailed(3)));
    }

    #[test]
    #[should_panic(expected = "empty endpoints")]
    fn connect_empty_endpoints_panics_in_debug() {
        // We assert that the caller (config layer) guards this.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let _ = WsConnection::connect(vec![], Duration::from_millis(100)).await;
        });
    }
}
