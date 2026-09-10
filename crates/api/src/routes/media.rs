//! Radio / news reads (`index.js`): `/music/list` and `/news/episodes`.
//! The audio, cover and video files themselves are static: the API
//! mounts `MUSIC_DIR` at `/music` and `NEWS_DIR` at `/news` when set
//! (the Node served its own directory with `express.static`).
//!
//! - `/music/list`: every `*.mp3` of `MUSIC_DIR` sorted by name, with
//!   `manifest.json` metadata when present; a file without an entry
//!   gets a title from its name (leading `NN_` stripped, `_` → space),
//!   artist `SoraMetrics Radio`, a cover with the same stem
//!   (`.webp/.png/.jpg/.jpeg`) and `dur: null`. Unreadable dir → `[]`.
//! - `/news/episodes?limit&offset` (limit 1–100, default 50): rows of
//!   `sm.news_episodes` newest first as `{ episodes, total }` where
//!   `total` is the page length (the Node's quirk).

use crate::{error::ApiError, AppState};
use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/music/list", get(music_list))
        .route("/news/episodes", get(news_episodes))
}

/// Directory of the radio tracks (`MUSIC_DIR`), if configured.
pub fn music_dir() -> Option<String> {
    std::env::var("MUSIC_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Directory of the news media (`NEWS_DIR`), if configured.
pub fn news_dir() -> Option<String> {
    std::env::var("NEWS_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

#[derive(Deserialize, Default)]
struct ManifestEntry {
    title: Option<String>,
    artist: Option<String>,
    cover: Option<String>,
    dur: Option<f64>,
}

#[derive(Serialize, Debug, PartialEq)]
struct Track {
    title: String,
    artist: String,
    src: String,
    cover: Option<String>,
    dur: Option<f64>,
}

fn url_encode(name: &str) -> String {
    // `encodeURIComponent`: everything but A-Z a-z 0-9 - _ . ! ~ * ' ( )
    let mut out = String::with_capacity(name.len());
    for b in name.bytes() {
        match b {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'!'
            | b'~'
            | b'*'
            | b'\''
            | b'('
            | b')' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn is_cover(name: &str) -> bool {
    let lower = name.to_lowercase();
    [".webp", ".png", ".jpg", ".jpeg"]
        .iter()
        .any(|e| lower.ends_with(e))
}

/// Node `/music/list` over a file list and manifest.
fn playlist(files: &[String], manifest: &BTreeMap<String, ManifestEntry>) -> Vec<Track> {
    let covers: HashSet<&str> = files
        .iter()
        .filter(|f| is_cover(f))
        .map(String::as_str)
        .collect();
    let mut mp3s: Vec<&String> = files
        .iter()
        .filter(|f| f.to_lowercase().ends_with(".mp3"))
        .collect();
    mp3s.sort();
    mp3s.into_iter()
        .map(|f| {
            let meta = manifest.get(f);
            let stem = &f[..f.len() - 4];
            let fallback_title = {
                let t = stem
                    .strip_prefix(|c: char| c.is_ascii_digit())
                    .and_then(|r| r.strip_prefix(|c: char| c.is_ascii_digit()))
                    .and_then(|r| r.strip_prefix('_'))
                    .unwrap_or(stem);
                t.replace('_', " ").trim().to_string()
            };
            let cover = meta
                .and_then(|m| m.cover.as_deref())
                .filter(|c| covers.contains(c))
                .map(str::to_string)
                .or_else(|| {
                    [".webp", ".png", ".jpg", ".jpeg"]
                        .iter()
                        .map(|e| format!("{stem}{e}"))
                        .find(|c| covers.contains(c.as_str()))
                });
            Track {
                title: meta.and_then(|m| m.title.clone()).unwrap_or(fallback_title),
                artist: meta
                    .and_then(|m| m.artist.clone())
                    .unwrap_or_else(|| "SoraMetrics Radio".into()),
                src: format!("/music/{}", url_encode(f)),
                cover: cover.map(|c| format!("/music/{}", url_encode(&c))),
                dur: meta.and_then(|m| m.dur).filter(|d| d.is_finite()),
            }
        })
        .collect()
}

fn read_playlist(dir: &Path) -> Option<Vec<Track>> {
    let manifest: BTreeMap<String, ManifestEntry> =
        std::fs::read_to_string(dir.join("manifest.json"))
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
    let files: Vec<String> = std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    Some(playlist(&files, &manifest))
}

async fn music_list() -> Json<Vec<Track>> {
    let dir = music_dir().unwrap_or_else(|| "music".into());
    Json(read_playlist(Path::new(&dir)).unwrap_or_default())
}

#[derive(Deserialize)]
struct NewsQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Serialize)]
struct Episode {
    slug: String,
    published_at: String,
    title_es: String,
    title_en: String,
    summary_es: Option<String>,
    summary_en: Option<String>,
    cover_path: String,
    audio_path_es: String,
    audio_path_en: String,
    video_path_es: Option<String>,
    video_path_en: Option<String>,
    duration_s: Option<i32>,
    source_url: Option<String>,
    tags: Option<Vec<String>>,
}

#[derive(Serialize)]
struct Episodes {
    episodes: Vec<Episode>,
    total: usize,
}

fn iso_millis(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

async fn news_episodes(
    State(state): State<AppState>,
    Query(q): Query<NewsQuery>,
) -> Result<Json<Episodes>, ApiError> {
    let limit = q.limit.unwrap_or(50).clamp(1, 100);
    let offset = q.offset.unwrap_or(0).max(0);
    let rows = sqlx::query!(
        r#"SELECT slug, published_at, title_es, title_en, summary_es, summary_en,
                  cover_path, audio_path_es, audio_path_en, video_path_es, video_path_en,
                  duration_s, source_url, tags
           FROM sm.news_episodes ORDER BY published_at DESC LIMIT $1 OFFSET $2"#,
        limit,
        offset
    )
    .fetch_all(&state.db)
    .await?;
    let episodes: Vec<Episode> = rows
        .into_iter()
        .map(|r| Episode {
            slug: r.slug,
            published_at: iso_millis(r.published_at),
            title_es: r.title_es,
            title_en: r.title_en,
            summary_es: r.summary_es,
            summary_en: r.summary_en,
            cover_path: r.cover_path,
            audio_path_es: r.audio_path_es,
            audio_path_en: r.audio_path_en,
            video_path_es: r.video_path_es,
            video_path_en: r.video_path_en,
            duration_s: r.duration_s,
            source_url: r.source_url,
            tags: r.tags,
        })
        .collect();
    let total = episodes.len();
    Ok(Json(Episodes { episodes, total }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn playlist_matches_node_fallbacks_and_manifest() {
        let files: Vec<String> = [
            "02_20_Twenty_What.mp3",
            "01_10100111001.mp3",
            "01_10100111001.webp",
            "notes.txt",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let mut manifest = BTreeMap::new();
        manifest.insert(
            "02_20_Twenty_What.mp3".to_string(),
            ManifestEntry {
                title: Some("20-Twenty-What".into()),
                artist: Some("sorametrics.org".into()),
                cover: None,
                dur: Some(384.9),
            },
        );
        let p = playlist(&files, &manifest);
        assert_eq!(p.len(), 2);
        assert_eq!(p[0].title, "10100111001");
        assert_eq!(p[0].artist, "SoraMetrics Radio");
        assert_eq!(p[0].cover.as_deref(), Some("/music/01_10100111001.webp"));
        assert_eq!(p[0].dur, None);
        assert_eq!(p[1].title, "20-Twenty-What");
        assert_eq!(p[1].src, "/music/02_20_Twenty_What.mp3");
        assert_eq!(p[1].cover, None);
        assert_eq!(p[1].dur, Some(384.9));
        assert_eq!(url_encode("a b.mp3"), "a%20b.mp3");
    }
}
