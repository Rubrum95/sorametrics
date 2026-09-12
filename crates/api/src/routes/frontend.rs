//! The SPA files the Node served itself (`express.static` behind an
//! allow-list, plus the three clean routes): `/` → `landing.html`,
//! `/sorav2` → `index.html`, `/minamoto` → `minamoto.html`, the listed
//! root files, and `/js/<name>.jsx` / `/js/minamoto/<name>.jsx`.
//! Nothing else under `STATIC_DIR` is reachable (the Node's directory
//! also holds `.env`, state JSON and `.bak` copies).
//!
//! `STATIC_DIR` unset → no route is registered.

use crate::AppState;
use axum::extract::{Path, Request};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::path::PathBuf;
use std::sync::OnceLock;
use tower::ServiceExt;
use tower_http::services::ServeFile;

/// Root files the Node's `ALLOWED_STATIC` set exposes by name.
const ROOT_FILES: &[&str] = &[
    "index.html",
    "script.js",
    "sw.js",
    "manifest.json",
    "favicon.svg",
    "header-banner.jpg",
    "styles.css",
    "landing.html",
    "minamoto.html",
];

static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();

/// `STATIC_DIR`, if configured.
pub fn static_dir() -> Option<&'static PathBuf> {
    DIR.get_or_init(|| {
        std::env::var("STATIC_DIR")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from)
    })
    .as_ref()
}

/// The clean routes and the allow-listed files; empty without `STATIC_DIR`.
pub fn router() -> Router<AppState> {
    if static_dir().is_none() {
        return Router::new();
    }
    let mut r = Router::new()
        .route("/", get(|req: Request| file("landing.html", req)))
        .route("/sorav2", get(|req: Request| file("index.html", req)))
        .route("/minamoto", get(|req: Request| file("minamoto.html", req)))
        .route("/js/:name", get(jsx_root))
        .route("/js/minamoto/:name", get(jsx_minamoto));
    for name in ROOT_FILES {
        r = r.route(
            &format!("/{name}"),
            get(move |req: Request| file(name, req)),
        );
    }
    r
}

async fn jsx_root(Path(name): Path<String>, req: Request) -> Response {
    if !is_jsx_name(&name) {
        return StatusCode::NOT_FOUND.into_response();
    }
    file(&format!("js/{name}"), req).await
}

async fn jsx_minamoto(Path(name): Path<String>, req: Request) -> Response {
    if !is_jsx_name(&name) {
        return StatusCode::NOT_FOUND.into_response();
    }
    file(&format!("js/minamoto/{name}"), req).await
}

/// The Node's `^[a-zA-Z0-9_.-]+\.jsx$`.
pub fn is_jsx_name(name: &str) -> bool {
    name.ends_with(".jsx")
        && name.len() > 4
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-')
}

/// Serves one file of `STATIC_DIR` (GET and HEAD, conditional requests)
/// with express.static's `Cache-Control` and content types.
async fn file(rel: &str, req: Request) -> Response {
    let Some(dir) = static_dir() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let path = dir.join(rel);
    let mut resp = match ServeFile::new(&path).oneshot(req).await {
        Ok(r) => r.into_response(),
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    if resp.status() == StatusCode::OK || resp.status() == StatusCode::NOT_MODIFIED {
        let headers = resp.headers_mut();
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("public, max-age=0"));
        if let Some(ct) = content_type(rel) {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static(ct));
        }
    } else if resp.status() == StatusCode::NOT_FOUND {
        return StatusCode::NOT_FOUND.into_response();
    }
    resp
}

/// express.static's `Content-Type` per extension (verified against the
/// Node: `.jsx` is served as `text/jsx`).
fn content_type(rel: &str) -> Option<&'static str> {
    let ext = rel.rsplit('.').next()?;
    Some(match ext {
        "html" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" => "application/javascript; charset=utf-8",
        "jsx" => "text/jsx; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "svg" => "image/svg+xml",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsx_names_follow_the_node_regex() {
        assert!(is_jsx_name("common.jsx"));
        assert!(is_jsx_name("site_metrics.jsx"));
        assert!(is_jsx_name("i18n-v2.jsx"));
        assert!(!is_jsx_name("common.jsx.bak.20260523_120641"));
        assert!(!is_jsx_name("../index.js"));
        assert!(!is_jsx_name("a/b.jsx"));
        assert!(!is_jsx_name(".jsx"));
        assert!(!is_jsx_name("x.js"));
    }

    #[test]
    fn content_types_match_express() {
        assert_eq!(content_type("js/a.jsx"), Some("text/jsx; charset=utf-8"));
        assert_eq!(content_type("styles.css"), Some("text/css; charset=utf-8"));
        assert_eq!(content_type("favicon.svg"), Some("image/svg+xml"));
        assert_eq!(content_type("noext"), None);
    }
}
