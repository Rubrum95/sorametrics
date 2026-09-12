//! `/analytics/*` — the Node's site meta-analytics
//! (`analytics/{db,presence,routes}.js`), same contract:
//! - `POST /analytics/hit`: beacon ingestion, always 204; events are
//!   queued in memory and flushed in batches (5 s / 500 rows); a
//!   `heartbeat` only refreshes presence and is never stored.
//! - `GET /analytics/stats`: dashboard payload cached 60 s + live
//!   `online` / `peak`.
//! - `GET /analytics/advanced`: navigation flow, heatmap, bounce/depth,
//!   traffic vs XOR price, web vitals; cached 60 s.
//!
//! Presence: a session is online if seen within 45 s; silent past 90 s
//! it is flushed as one `session_end` with its duration. Every 6 h the
//! complete past days are rolled into `sm.site_daily` and raw rows
//! older than the retention are pruned. No raw IP is ever stored: the
//! visitor id is `sha256(ip|ua|day|salt)[..32]`.

use crate::error::ApiError;
use crate::AppState;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const STATS_TTL: Duration = Duration::from_secs(60);
const ALLOWED_TYPES: &[&str] = &[
    "pageview",
    "section",
    "search",
    "interaction",
    "error",
    "vitals",
    "heartbeat",
];
const MAX_META_BYTES: usize = 2048;
const SWEEP_EVERY: Duration = Duration::from_secs(30);
const ROLLUP_EVERY: Duration = Duration::from_secs(6 * 3600);
const XOR_ASSET: &str = "0x0200000000000000000000000000000000000000000000000000000000000000";

/// One queued beacon (a `sm.site_events` row).
#[derive(Debug, Clone)]
pub struct SiteEvent {
    ts_ms: i64,
    kind: String,
    section: Option<String>,
    visitor: Option<String>,
    session_id: Option<String>,
    path: Option<String>,
    referrer: Option<String>,
    country: Option<String>,
    device: Option<String>,
    duration_ms: Option<i64>,
    meta: Option<Value>,
}

struct Session {
    first_seen: Instant,
    last_seen: Instant,
    visitor: Option<String>,
}

/// In-memory side of the analytics: the beacon queue, presence and the
/// two response caches. Shared through [`AppState`].
pub struct Analytics {
    salt: String,
    retention_days: i64,
    flush_every: Duration,
    flush_max: usize,
    buffer_cap: usize,
    online_window: Duration,
    session_gap: Duration,
    buffer: Mutex<Vec<SiteEvent>>,
    sessions: Mutex<HashMap<String, Session>>,
    peak: AtomicUsize,
    stats_cache: Mutex<Option<(Instant, Value)>>,
    adv_cache: Mutex<Option<(Instant, Value)>>,
}

impl Default for Analytics {
    fn default() -> Self {
        Self::from_env()
    }
}

impl Analytics {
    /// Reads the Node's `ANALYTICS_*` variables (same defaults).
    pub fn from_env() -> Self {
        let num = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(default)
        };
        Self {
            salt: std::env::var("ANALYTICS_SALT")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "sorametrics-default-salt".to_string()),
            retention_days: num("ANALYTICS_RAW_RETENTION_DAYS", 30) as i64,
            flush_every: Duration::from_millis(num("ANALYTICS_FLUSH_MS", 5000)),
            flush_max: num("ANALYTICS_FLUSH_MAX", 500) as usize,
            buffer_cap: num("ANALYTICS_BUFFER_CAP", 50_000) as usize,
            online_window: Duration::from_millis(num("ANALYTICS_ONLINE_WINDOW_MS", 45_000)),
            session_gap: Duration::from_millis(num("ANALYTICS_SESSION_GAP_MS", 90_000)),
            buffer: Mutex::new(Vec::new()),
            sessions: Mutex::new(HashMap::new()),
            peak: AtomicUsize::new(0),
            stats_cache: Mutex::new(None),
            adv_cache: Mutex::new(None),
        }
    }

    /// `sha256(ip|ua|day|salt)` hex, first 32 chars; the salt rotates
    /// daily so an id cannot be linked across days or reversed.
    pub fn visitor_hash(&self, ip: &str, ua: &str) -> String {
        let day = Utc::now().format("%Y-%m-%d").to_string();
        let digest = Sha256::digest(format!("{ip}|{ua}|{day}|{}", self.salt).as_bytes());
        hex::encode(digest)[..32].to_string()
    }

    /// Queue an event; dropped silently at the hard cap.
    fn enqueue(&self, ev: SiteEvent) -> bool {
        let mut buf = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
        if buf.len() >= self.buffer_cap {
            return false;
        }
        buf.push(ev);
        buf.len() >= self.flush_max
    }

    fn take_buffer(&self) -> Vec<SiteEvent> {
        std::mem::take(&mut *self.buffer.lock().unwrap_or_else(|e| e.into_inner()))
    }

    fn touch(&self, session_id: &str, visitor: Option<&str>) {
        let now = Instant::now();
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        match sessions.get_mut(session_id) {
            Some(s) => {
                s.last_seen = now;
                if s.visitor.is_none() {
                    s.visitor = visitor.map(str::to_string);
                }
            }
            None => {
                sessions.insert(
                    session_id.to_string(),
                    Session {
                        first_seen: now,
                        last_seen: now,
                        visitor: visitor.map(str::to_string),
                    },
                );
            }
        }
    }

    /// Sessions seen within the online window.
    pub fn online(&self) -> usize {
        let cutoff = Instant::now() - self.online_window;
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|s| s.last_seen >= cutoff)
            .count()
    }

    /// Highest `online` seen by the sweep.
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Sessions silent past the gap → one `session_end` each.
    fn sweep(&self) {
        let now = Instant::now();
        let ended: Vec<(String, Session)> = {
            let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            let gone: Vec<String> = sessions
                .iter()
                .filter(|(_, s)| now.duration_since(s.last_seen) > self.session_gap)
                .map(|(id, _)| id.clone())
                .collect();
            gone.into_iter()
                .filter_map(|id| sessions.remove(&id).map(|s| (id, s)))
                .collect()
        };
        for (id, s) in ended {
            let duration = s.last_seen.duration_since(s.first_seen).as_millis() as i64;
            if duration >= 1000 {
                self.enqueue(SiteEvent {
                    ts_ms: Utc::now().timestamp_millis(),
                    kind: "session_end".to_string(),
                    section: None,
                    visitor: s.visitor,
                    session_id: Some(id),
                    path: None,
                    referrer: None,
                    country: None,
                    device: None,
                    duration_ms: Some(duration),
                    meta: None,
                });
            }
        }
        let cur = self.online();
        self.peak.fetch_max(cur, Ordering::Relaxed);
    }
}

/// The three `/analytics` routes.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/analytics/hit", post(hit))
        .route("/analytics/stats", get(stats))
        .route("/analytics/advanced", get(advanced))
}

/// Flush loop, presence sweep and the 6-hourly rollup (the Node's
/// `startFlushLoop`, `startSweepLoop`, `rollupAndPrune`).
pub fn spawn(state: AppState) {
    let a = state.analytics.clone();
    let db = state.db.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(a.flush_every);
        loop {
            tick.tick().await;
            flush(&db, &a).await;
        }
    });
    let a = state.analytics.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SWEEP_EVERY);
        tick.tick().await;
        loop {
            tick.tick().await;
            a.sweep();
        }
    });
    let a = state.analytics.clone();
    let db = state.db.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(ROLLUP_EVERY);
        tick.tick().await;
        loop {
            tick.tick().await;
            match rollup_and_prune(&db, a.retention_days).await {
                Ok(pruned) => info!(pruned, "site analytics rollup done"),
                Err(e) => warn!(error = %e, "site analytics rollup failed"),
            }
        }
    });
}

/// Batch insert of the queued events; a failed batch is dropped and
/// logged (the Node did the same: tracking never blocks or retries).
async fn flush(db: &PgPool, a: &Analytics) {
    let batch = a.take_buffer();
    if batch.is_empty() {
        return;
    }
    let n = batch.len();
    let ts: Vec<i64> = batch.iter().map(|e| e.ts_ms).collect();
    let kinds: Vec<String> = batch.iter().map(|e| e.kind.clone()).collect();
    let sections: Vec<Option<String>> = batch.iter().map(|e| e.section.clone()).collect();
    let visitors: Vec<Option<String>> = batch.iter().map(|e| e.visitor.clone()).collect();
    let sessions: Vec<Option<String>> = batch.iter().map(|e| e.session_id.clone()).collect();
    let paths: Vec<Option<String>> = batch.iter().map(|e| e.path.clone()).collect();
    let referrers: Vec<Option<String>> = batch.iter().map(|e| e.referrer.clone()).collect();
    let countries: Vec<Option<String>> = batch.iter().map(|e| e.country.clone()).collect();
    let devices: Vec<Option<String>> = batch.iter().map(|e| e.device.clone()).collect();
    let durations: Vec<Option<i64>> = batch.iter().map(|e| e.duration_ms).collect();
    let metas: Vec<Option<String>> = batch
        .iter()
        .map(|e| e.meta.as_ref().map(|m| m.to_string()))
        .collect();
    let res = sqlx::query!(
        r#"
        INSERT INTO sm.site_events
            (ts, type, section, visitor, session_id, path, referrer, country, device, duration_ms, meta)
        SELECT to_timestamp(t / 1000.0), k, s, v, sid, p, r, c, d, dur, m::jsonb
        FROM UNNEST($1::bigint[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
                    $7::text[], $8::text[], $9::text[], $10::bigint[], $11::text[])
             AS x(t, k, s, v, sid, p, r, c, d, dur, m)
        "#,
        &ts,
        &kinds,
        &sections as &[Option<String>],
        &visitors as &[Option<String>],
        &sessions as &[Option<String>],
        &paths as &[Option<String>],
        &referrers as &[Option<String>],
        &countries as &[Option<String>],
        &devices as &[Option<String>],
        &durations as &[Option<i64>],
        &metas as &[Option<String>],
    )
    .execute(db)
    .await;
    if let Err(e) = res {
        warn!(dropped = n, error = %e, "site analytics flush failed");
    }
}

/// Complete past days → `sm.site_daily` (site-wide row `section = ''`
/// and one row per section), then raw rows past the retention deleted.
pub async fn rollup_and_prune(db: &PgPool, retention_days: i64) -> Result<u64, sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO sm.site_daily (day, section, pageviews, section_views, sessions, uniques, avg_session_ms)
        SELECT
            (ts AT TIME ZONE 'UTC')::date,
            '',
            COUNT(*) FILTER (WHERE type='pageview'),
            COUNT(*) FILTER (WHERE type='section'),
            COUNT(DISTINCT session_id),
            COUNT(DISTINCT visitor),
            COALESCE(AVG(duration_ms) FILTER (WHERE type='session_end'), 0)::bigint
        FROM sm.site_events
        WHERE ts < date_trunc('day', now())
        GROUP BY 1
        ON CONFLICT (day, section) DO UPDATE SET
            pageviews=EXCLUDED.pageviews, section_views=EXCLUDED.section_views,
            sessions=EXCLUDED.sessions, uniques=EXCLUDED.uniques, avg_session_ms=EXCLUDED.avg_session_ms
        "#
    )
    .execute(db)
    .await?;
    sqlx::query!(
        r#"
        INSERT INTO sm.site_daily (day, section, section_views, uniques)
        SELECT (ts AT TIME ZONE 'UTC')::date, section, COUNT(*), COUNT(DISTINCT visitor)
        FROM sm.site_events
        WHERE ts < date_trunc('day', now()) AND type='section' AND section IS NOT NULL
        GROUP BY 1, 2
        ON CONFLICT (day, section) DO UPDATE SET
            section_views=EXCLUDED.section_views, uniques=EXCLUDED.uniques
        "#
    )
    .execute(db)
    .await?;
    let days = retention_days.to_string();
    let del = sqlx::query!(
        r#"DELETE FROM sm.site_events WHERE ts < now() - ($1 || ' days')::interval"#,
        days
    )
    .execute(db)
    .await?;
    Ok(del.rows_affected())
}

// ---------------------------------------------------------------- /hit

/// Real client IP behind Cloudflare, never stored (only hashed).
fn client_ip(headers: &HeaderMap, peer: Option<SocketAddr>) -> String {
    let h = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.split(',').next().unwrap_or("").trim().to_string())
            .filter(|v| !v.is_empty())
    };
    h("cf-connecting-ip")
        .or_else(|| h("x-forwarded-for"))
        .or_else(|| peer.map(|p| p.ip().to_string()))
        .unwrap_or_default()
}

/// Coarse device / browser / OS from the user agent (the Node's
/// `parseUA`: device tests are case-insensitive, the rest are not).
pub fn parse_ua(ua: &str) -> (&'static str, &'static str, &'static str) {
    let lower = ua.to_ascii_lowercase();
    let has_ci = |needle: &str| lower.contains(&needle.to_ascii_lowercase());
    let android = has_ci("Android");
    let android_mobile = android && has_ci("Mobile");
    let tablet = has_ci("iPad")
        || has_ci("Tablet")
        || has_ci("PlayBook")
        || has_ci("Silk")
        || (android && !android_mobile);
    let mobile = has_ci("Mobi")
        || has_ci("iPhone")
        || has_ci("iPod")
        || android_mobile
        || has_ci("Windows Phone")
        || has_ci("webOS")
        || has_ci("BlackBerry");
    let device = if tablet {
        "tablet"
    } else if mobile {
        "mobile"
    } else {
        "desktop"
    };
    let browser = if ua.contains("Edg/") {
        "Edge"
    } else if ua.contains("OPR/") || ua.contains("Opera") {
        "Opera"
    } else if ua.contains("Chrome/") {
        "Chrome"
    } else if ua.contains("Firefox/") {
        "Firefox"
    } else if ua.contains("Safari/") {
        "Safari"
    } else {
        "other"
    };
    let os = if ua.contains("Windows") {
        "Windows"
    } else if ua.contains("Mac OS X") || ua.contains("Macintosh") {
        "macOS"
    } else if ua.contains("Android") {
        "Android"
    } else if ua.contains("iPhone") || ua.contains("iPad") || ua.contains("iOS") {
        "iOS"
    } else if ua.contains("Linux") {
        "Linux"
    } else {
        "other"
    };
    (device, browser, os)
}

/// Referrer → bare hostname; internal navigation collapses to `None`.
pub fn ref_host(referrer: Option<&str>) -> Option<String> {
    let url = url::Url::parse(referrer?).ok()?;
    let host = url.host_str()?;
    let host = host.strip_prefix("www.").unwrap_or(host);
    if host.is_empty() || host.ends_with("sorametrics.org") {
        return None;
    }
    Some(host.chars().take(120).collect())
}

fn str_field(b: &Value, key: &str, max: usize) -> Option<String> {
    b.get(key)
        .and_then(Value::as_str)
        .map(|s| s.chars().take(max).collect())
}

async fn hit(
    State(state): State<AppState>,
    headers: HeaderMap,
    peer: Option<ConnectInfo<SocketAddr>>,
    body: Bytes,
) -> impl IntoResponse {
    let Ok(b) = serde_json::from_slice::<Value>(&body) else {
        return StatusCode::BAD_REQUEST;
    };
    let kind = b.get("type").and_then(Value::as_str).unwrap_or("");
    if !ALLOWED_TYPES.contains(&kind) {
        return StatusCode::NO_CONTENT;
    }
    let a = &state.analytics;
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let sid = str_field(&b, "sid", 64);
    let visitor = a.visitor_hash(&client_ip(&headers, peer.map(|c| c.0)), ua);
    if let Some(sid) = &sid {
        a.touch(sid, Some(&visitor));
    }
    if kind == "heartbeat" {
        return StatusCode::NO_CONTENT;
    }
    let mut meta = b.get("meta").filter(|m| !m.is_null()).cloned();
    if let Some(m) = &meta {
        if m.to_string().len() > MAX_META_BYTES {
            meta = Some(json!({ "truncated": true }));
        }
    }
    let (device, browser, os) = parse_ua(ua);
    if kind == "pageview" {
        let mut merged = Map::new();
        merged.insert("browser".into(), Value::String(browser.into()));
        merged.insert("os".into(), Value::String(os.into()));
        if let Some(Value::Object(m)) = meta {
            for (k, v) in m {
                merged.insert(k, v);
            }
        }
        meta = Some(Value::Object(merged));
    }
    a.enqueue(SiteEvent {
        ts_ms: Utc::now().timestamp_millis(),
        kind: kind.to_string(),
        section: str_field(&b, "section", 64),
        visitor: Some(visitor),
        session_id: sid,
        path: str_field(&b, "path", 256),
        referrer: ref_host(b.get("ref").and_then(Value::as_str)),
        country: headers
            .get("cf-ipcountry")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        device: Some(device.to_string()),
        duration_ms: None,
        meta,
    });
    StatusCode::NO_CONTENT
}

// ------------------------------------------------------------- /stats

async fn stats(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let a = &state.analytics;
    let now = Instant::now();
    let cached = a
        .stats_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .filter(|(at, _)| now.duration_since(*at) < STATS_TTL);
    let (at, mut payload) = match cached {
        Some(c) => c,
        None => {
            let p = site_stats(&state.db).await?;
            *a.stats_cache.lock().unwrap_or_else(|e| e.into_inner()) = Some((now, p.clone()));
            (now, p)
        }
    };
    if let Value::Object(m) = &mut payload {
        m.insert("online".into(), json!(a.online()));
        m.insert("peak".into(), json!(a.peak()));
        m.insert("cached".into(), json!(now > at));
    }
    Ok(Json(payload))
}

async fn advanced(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let a = &state.analytics;
    let now = Instant::now();
    let cached = a
        .adv_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .filter(|(at, _)| now.duration_since(*at) < STATS_TTL);
    let (at, mut payload) = match cached {
        Some(c) => c,
        None => {
            let p = site_advanced(&state).await?;
            *a.adv_cache.lock().unwrap_or_else(|e| e.into_inner()) = Some((now, p.clone()));
            (now, p)
        }
    };
    if let Value::Object(m) = &mut payload {
        m.insert("cached".into(), json!(now > at));
    }
    Ok(Json(payload))
}

struct Window {
    v: i64,
    u: i64,
    s: i64,
}

async fn window(db: &PgPool, interval: &str) -> Result<Window, sqlx::Error> {
    let r = sqlx::query!(
        r#"SELECT COUNT(*) FILTER (WHERE type='pageview') AS "v!", COUNT(DISTINCT visitor) AS "u!",
                  COUNT(DISTINCT session_id) AS "s!"
           FROM sm.site_events WHERE ts >= now() - $1::interval"#,
        interval as _
    )
    .fetch_one(db)
    .await?;
    Ok(Window {
        v: r.v,
        u: r.u,
        s: r.s,
    })
}

/// The Node's `getSiteStats` (without `online`/`peak`/`cached`).
async fn site_stats(db: &PgPool) -> Result<Value, ApiError> {
    let w24 = window(db, "24 hours").await?;
    let w7 = window(db, "7 days").await?;
    let w30 = window(db, "30 days").await?;
    let year = sqlx::query!(
        r#"SELECT COALESCE(SUM(pageviews),0)::bigint AS "v!", COALESCE(SUM(uniques),0)::bigint AS "u!",
                  COALESCE(SUM(sessions),0)::bigint AS "s!"
           FROM sm.site_daily WHERE section='' AND day >= current_date - 365 AND day < current_date"#
    )
    .fetch_one(db)
    .await?;
    let today = sqlx::query!(
        r#"SELECT COUNT(*) FILTER (WHERE type='pageview') AS "v!", COUNT(DISTINCT visitor) AS "u!",
                  COUNT(DISTINCT session_id) AS "s!"
           FROM sm.site_events WHERE ts >= date_trunc('day', now())"#
    )
    .fetch_one(db)
    .await?;
    let secs = sqlx::query!(
        r#"SELECT section AS "section!", COUNT(*) AS "views!", COUNT(DISTINCT visitor) AS "uniques!"
           FROM sm.site_events
           WHERE type='section' AND section IS NOT NULL AND ts >= now() - interval '30 days'
           GROUP BY section ORDER BY 2 DESC LIMIT 30"#
    )
    .fetch_all(db)
    .await?;
    let avg = sqlx::query_scalar!(
        r#"SELECT COALESCE(AVG(duration_ms),0)::bigint AS "a!"
           FROM sm.site_events WHERE type='session_end' AND ts >= now() - interval '30 days'"#
    )
    .fetch_one(db)
    .await?;
    let searches = sqlx::query!(
        r#"SELECT meta->>'q' AS "q!", COUNT(*) AS "c!" FROM sm.site_events
           WHERE type='search' AND meta->>'q' IS NOT NULL AND ts >= now() - interval '30 days'
           GROUP BY 1 ORDER BY 2 DESC LIMIT 15"#
    )
    .fetch_all(db)
    .await?;
    let inters = sqlx::query!(
        r#"SELECT meta->>'name' AS "name!", COUNT(*) AS "c!" FROM sm.site_events
           WHERE type='interaction' AND meta->>'name' IS NOT NULL AND ts >= now() - interval '30 days'
           GROUP BY 1 ORDER BY 2 DESC LIMIT 15"#
    )
    .fetch_all(db)
    .await?;
    let devices = sqlx::query!(
        r#"SELECT COALESCE(device,'unknown') AS "d!", COUNT(*) AS "c!" FROM sm.site_events
           WHERE type='pageview' AND ts >= now() - interval '30 days' GROUP BY 1 ORDER BY 2 DESC"#
    )
    .fetch_all(db)
    .await?;
    let countries = sqlx::query!(
        r#"SELECT COALESCE(country,'??') AS "k!", COUNT(*) AS "c!" FROM sm.site_events
           WHERE type='pageview' AND ts >= now() - interval '30 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 15"#
    )
    .fetch_all(db)
    .await?;
    let referrers = sqlx::query!(
        r#"SELECT COALESCE(NULLIF(referrer,''),'direct') AS "r!", COUNT(*) AS "c!" FROM sm.site_events
           WHERE type='pageview' AND ts >= now() - interval '30 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 12"#
    )
    .fetch_all(db)
    .await?;
    let errs = sqlx::query_scalar!(
        r#"SELECT COUNT(*) AS "c!" FROM sm.site_events WHERE type='error' AND ts >= now() - interval '7 days'"#
    )
    .fetch_one(db)
    .await?;
    let wallets = sqlx::query!(
        r#"SELECT meta->>'addr' AS "addr!", COUNT(*) AS "c!" FROM sm.site_events
           WHERE type='interaction' AND meta->>'name'='wallet_view' AND meta->>'addr' IS NOT NULL
             AND ts >= now() - interval '30 days'
           GROUP BY 1 ORDER BY 2 DESC LIMIT 12"#
    )
    .fetch_all(db)
    .await?;
    Ok(json!({
        "visits":   { "24h": w24.v, "7d": w7.v, "30d": w30.v, "1y": year.v + today.v },
        "uniques":  { "24h": w24.u, "7d": w7.u, "30d": w30.u, "1y": year.u + today.u },
        "sessions": { "24h": w24.s, "7d": w7.s, "30d": w30.s, "1y": year.s + today.s },
        "sections": secs.iter().map(|r| json!({ "section": r.section, "views": r.views, "uniques": r.uniques })).collect::<Vec<_>>(),
        "avgSessionMs": avg,
        "searches": searches.iter().map(|r| json!({ "q": r.q, "count": r.c })).collect::<Vec<_>>(),
        "interactions": inters.iter().map(|r| json!({ "name": r.name, "count": r.c })).collect::<Vec<_>>(),
        "devices": devices.iter().map(|r| json!({ "device": r.d, "count": r.c })).collect::<Vec<_>>(),
        "countries": countries.iter().map(|r| json!({ "country": r.k, "count": r.c })).collect::<Vec<_>>(),
        "referrers": referrers.iter().map(|r| json!({ "ref": r.r, "count": r.c })).collect::<Vec<_>>(),
        "topWallets": wallets.iter().map(|r| json!({ "addr": r.addr, "count": r.c })).collect::<Vec<_>>(),
        "errors7d": errs,
        "generatedAt": Utc::now().timestamp_millis(),
    }))
}

/// A `date` column as node-pg serialises it: local midnight in the
/// API time zone, printed in UTC with milliseconds.
fn day_iso(day: NaiveDate, zone: chrono_tz::Tz) -> String {
    let local = zone
        .from_local_datetime(&day.and_hms_opt(0, 0, 0).unwrap_or_default())
        .single()
        .or_else(|| {
            zone.from_local_datetime(&day.and_hms_opt(0, 0, 0).unwrap_or_default())
                .earliest()
        });
    let utc: DateTime<Utc> = local.map(|l| l.with_timezone(&Utc)).unwrap_or_default();
    utc.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// The Node's `getSiteAdvanced` (without `cached`).
async fn site_advanced(state: &AppState) -> Result<Value, ApiError> {
    let db = &state.db;
    let flow = sqlx::query!(
        r#"SELECT prev || ' → ' || section AS "transition!", COUNT(*) AS "c!"
           FROM (SELECT section, LAG(section) OVER (PARTITION BY session_id ORDER BY ts) prev
                 FROM sm.site_events
                 WHERE type='section' AND section IS NOT NULL AND session_id IS NOT NULL
                   AND ts >= now() - interval '30 days') x
           WHERE prev IS NOT NULL AND prev <> section
           GROUP BY 1 ORDER BY 2 DESC LIMIT 15"#
    )
    .fetch_all(db)
    .await?;
    let heat = sqlx::query!(
        r#"SELECT EXTRACT(DOW FROM ts)::int AS "dow!", EXTRACT(HOUR FROM ts)::int AS "hr!", COUNT(*) AS "c!"
           FROM sm.site_events
           WHERE type IN ('pageview','section') AND ts >= now() - interval '30 days'
           GROUP BY 1, 2"#
    )
    .fetch_all(db)
    .await?;
    let eng = sqlx::query!(
        r#"SELECT COALESCE(AVG(cnt), 0)::numeric(10,2) AS "depth!",
                  COALESCE(COUNT(*) FILTER (WHERE cnt <= 1)::float8 / NULLIF(COUNT(*), 0), 0) AS "bounce!",
                  COUNT(*) AS "sessions!"
           FROM (SELECT session_id, COUNT(*) FILTER (WHERE type='section') cnt
                 FROM sm.site_events
                 WHERE session_id IS NOT NULL AND ts >= now() - interval '30 days'
                 GROUP BY session_id) s"#
    )
    .fetch_one(db)
    .await?;
    let price_traffic = sqlx::query!(
        r#"SELECT g::date AS "dt!", COALESCE(v.visits,0)::bigint AS "visits!", x.price AS "price?"
           FROM generate_series(date_trunc('day', now()) - interval '29 days', date_trunc('day', now()), interval '1 day') g
           LEFT JOIN (SELECT ts::date vd, COUNT(*) FILTER (WHERE type='pageview') visits
                      FROM sm.site_events WHERE ts >= now() - interval '30 days' GROUP BY 1) v ON v.vd = g::date
           LEFT JOIN (SELECT to_timestamp(hour_bucket)::date xd, AVG(price_usd) price
                      FROM ts.price_history
                      WHERE asset_id = $1 AND hour_bucket >= EXTRACT(EPOCH FROM now() - interval '30 days')
                      GROUP BY 1) x ON x.xd = g::date
           ORDER BY 1"#,
        XOR_ASSET
    )
    .fetch_all(db)
    .await?;
    let vitals = sqlx::query!(
        r#"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (meta->>'ttfb')::float8) AS "ttfb?",
                  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (meta->>'fcp')::float8)  AS "fcp?",
                  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (meta->>'lcp')::float8)  AS "lcp?",
                  COUNT(*) AS "n!"
           FROM sm.site_events
           WHERE type='vitals' AND ts >= now() - interval '7 days' AND meta ? 'lcp'"#
    )
    .fetch_one(db)
    .await?;
    let depth: f64 = eng.depth.to_string().parse().unwrap_or(0.0);
    let round = |v: Option<f64>| v.map(|x| x.round() as i64);
    Ok(json!({
        "navFlow": flow.iter().map(|r| json!({ "transition": r.transition, "count": r.c })).collect::<Vec<_>>(),
        "heatmap": heat.iter().map(|r| json!({ "dow": r.dow, "hr": r.hr, "count": r.c })).collect::<Vec<_>>(),
        "bounce": eng.bounce,
        "depth": depth,
        "engagedSessions": eng.sessions,
        "priceTraffic": price_traffic.iter().map(|r| json!({
            "day": day_iso(r.dt, state.time_zone), "visits": r.visits, "price": r.price
        })).collect::<Vec<_>>(),
        "vitals": { "ttfb": round(vitals.ttfb), "fcp": round(vitals.fcp), "lcp": round(vitals.lcp), "samples": vitals.n },
        "generatedAt": Utc::now().timestamp_millis(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ua_parsing_matches_the_node() {
        let iphone = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1";
        assert_eq!(parse_ua(iphone), ("mobile", "Safari", "macOS"));
        let android_tab = "Mozilla/5.0 (Linux; Android 13; SM-X700) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36";
        assert_eq!(parse_ua(android_tab), ("tablet", "Chrome", "Android"));
        let edge = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36 Edg/120.0";
        assert_eq!(parse_ua(edge), ("desktop", "Edge", "Windows"));
        assert_eq!(parse_ua(""), ("desktop", "other", "other"));
    }

    #[test]
    fn referrer_host_rules() {
        assert_eq!(
            ref_host(Some("https://www.google.com/search?q=x")),
            Some("google.com".into())
        );
        assert_eq!(ref_host(Some("https://sorametrics.org/sorav2")), None);
        assert_eq!(ref_host(Some("https://app.sorametrics.org/")), None);
        assert_eq!(ref_host(Some("not a url")), None);
        assert_eq!(ref_host(None), None);
    }

    #[test]
    fn day_iso_is_local_midnight_in_utc() {
        let d = NaiveDate::from_ymd_opt(2026, 8, 14).unwrap();
        assert_eq!(
            day_iso(d, chrono_tz::Europe::Madrid),
            "2026-08-13T22:00:00.000Z"
        );
        assert_eq!(day_iso(d, chrono_tz::UTC), "2026-08-14T00:00:00.000Z");
    }
}
