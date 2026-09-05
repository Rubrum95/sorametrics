//! Socket.IO compatibility spike — answers ONE question before Phase 3
//! commits to `socketioxide`: does the EXACT client build the production
//! frontend pins (socket.io 4.7.5 from the CDN, `transports:
//! ['websocket', 'polling']`) handshake with a socketioxide layer on our
//! axum stack, and receive the legacy event contract?
//!
//! Legacy contract (from `sorametrics v6/js/pulse.jsx` + `index.js`):
//! server → client only, no rooms, no auth, 5 events:
//!   new-block-stats · swaps-batch · transfers-batch ·
//!   extrinsics-batch · orderbook-batch
//!
//! Run:  cargo run -p sorametrics-api --example socket_spike
//! Then open http://127.0.0.1:8321 — the page loads the same CDN client
//! as production, connects, and prints every event received.

use axum::response::Html;
use axum::routing::get;
use serde_json::json;
use socketioxide::extract::SocketRef;
use socketioxide::SocketIo;
use std::time::Duration;
use tracing::info;

/// Test page: same CDN build + connect options as the production
/// frontend (`index.html` line 43 + `pulse.jsx` getPulseSocket()).
const PAGE: &str = r#"<!doctype html>
<html><head><meta charset="utf-8"><title>socketioxide spike</title>
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"
  integrity="sha384-2huaZvOR9iDzHqslqwpR87isEmrfxqyWOF7hr7BY6KG0+hVKLoEXMPUJw3ynWuhO"
  crossorigin="anonymous"></script>
</head><body>
<h3>socketioxide ⟷ socket.io-client 4.7.5</h3>
<div id="status">connecting…</div>
<pre id="log"></pre>
<script>
  const log = (m) => { document.getElementById('log').textContent += m + "\n"; };
  // Same options as pulse.jsx getPulseSocket().
  const sock = io('/', { transports: ['websocket', 'polling'], reconnection: true, reconnectionDelay: 1500 });
  sock.on('connect', () => {
    document.getElementById('status').textContent =
      'CONNECTED id=' + sock.id + ' transport=' + sock.io.engine.transport.name;
  });
  sock.on('disconnect', (r) => { document.getElementById('status').textContent = 'DISCONNECTED: ' + r; });
  for (const ev of ['new-block-stats','swaps-batch','transfers-batch','extrinsics-batch','orderbook-batch']) {
    sock.on(ev, (data) => log(ev + ' → ' + JSON.stringify(data).slice(0, 140)));
  }
</script>
</body></html>"#;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    sorametrics_telemetry::init(sorametrics_telemetry::LogFormat::Pretty)?;

    let (layer, io) = SocketIo::new_layer();
    io.ns("/", async |socket: SocketRef| {
        info!(id = %socket.id, "socket.io client connected");
    });

    // Fake emitter: one new-block-stats + one swaps-batch every 2s, with
    // the payload shapes the legacy backend sends (pulse.jsx parses
    // exactly these fields).
    let emitter = io.clone();
    tokio::spawn(async move {
        let mut block: u64 = 27_250_000;
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            block += 1;
            let stats = json!({ "block": block, "extrinsics": 2, "events": 14 });
            let swaps = json!([{
                "time": "2026-08-11T10:00:00Z",
                "block": block,
                "wallet": "cnVcgVYJqhyuQohhYrZraVs85dujMDCBsBhMj5z8QPHq91C84",
                "in":  { "amount": "11.91", "symbol": "PSWAP", "usd": 3.2 },
                "out": { "amount": "600.0", "symbol": "KUSD" },
                "hash": "0xfe082bb3e5474fc9a3e308e40fa5d9470c9e0be43cd1379999cc7fbe2ed56855",
                "extrinsic_id": format!("{block}-2"),
            }]);
            if emitter.emit("new-block-stats", &stats).await.is_err() {
                break;
            }
            if emitter.emit("swaps-batch", &swaps).await.is_err() {
                break;
            }
            info!(block, "emitted new-block-stats + swaps-batch");
        }
    });

    let app = axum::Router::new()
        .route("/", get(|| async { Html(PAGE) }))
        .layer(layer);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8321").await?;
    info!("spike listening on http://127.0.0.1:8321");
    axum::serve(listener, app).await?;
    Ok(())
}
