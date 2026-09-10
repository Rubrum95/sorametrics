-- ============================================================
-- 0018: sm.news_episodes — the Node's Sora News table, verbatim
-- (`initNewsSchema`). Written by the radio production tooling, read by
-- `/news/episodes`; the ETL copies the legacy rows as they are.
-- ============================================================
CREATE TABLE IF NOT EXISTS sm.news_episodes (
    slug           TEXT         PRIMARY KEY,
    published_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    title_es       TEXT         NOT NULL,
    title_en       TEXT         NOT NULL,
    summary_es     TEXT,
    summary_en     TEXT,
    cover_path     TEXT         NOT NULL,
    audio_path_es  TEXT         NOT NULL,
    audio_path_en  TEXT         NOT NULL,
    video_path_es  TEXT,
    video_path_en  TEXT,
    duration_s     INTEGER,
    source_url     TEXT,
    tags           TEXT[]
);
CREATE INDEX IF NOT EXISTS idx_news_published ON sm.news_episodes (published_at DESC);
