-- ============================================================
-- 0022: site meta-analytics (the Node's analytics/db.js tables).
--
-- sm.site_events: raw beacons (pageview/section/search/interaction/
-- error/vitals + session_end from the presence sweep), kept
-- ANALYTICS_RAW_RETENTION_DAYS; sm.site_daily: the rollup that keeps
-- the 1-year window. `origin` marks rows copied from the Node.
-- ============================================================
CREATE TABLE IF NOT EXISTS sm.site_events (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    type        TEXT NOT NULL,
    section     TEXT,
    visitor     TEXT,
    session_id  TEXT,
    path        TEXT,
    referrer    TEXT,
    country     TEXT,
    device      TEXT,
    duration_ms BIGINT,
    meta        JSONB,
    origin      TEXT NOT NULL DEFAULT 'live' CHECK (origin IN ('live', 'legacy'))
);
CREATE INDEX IF NOT EXISTS idx_site_events_ts      ON sm.site_events (ts DESC);
CREATE INDEX IF NOT EXISTS idx_site_events_type_ts ON sm.site_events (type, ts DESC);
CREATE INDEX IF NOT EXISTS idx_site_events_section ON sm.site_events (section, ts DESC) WHERE section IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_site_events_visitor ON sm.site_events (visitor, ts DESC) WHERE visitor IS NOT NULL;

CREATE TABLE IF NOT EXISTS sm.site_daily (
    day            DATE NOT NULL,
    section        TEXT NOT NULL DEFAULT '',
    pageviews      BIGINT NOT NULL DEFAULT 0,
    section_views  BIGINT NOT NULL DEFAULT 0,
    sessions       BIGINT NOT NULL DEFAULT 0,
    uniques        BIGINT NOT NULL DEFAULT 0,
    avg_session_ms BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, section)
);
