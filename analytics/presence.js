'use strict';
// analytics/presence.js — live "online now" + session duration from heartbeat
// beacons. The frontend socket.io is lazy (only opens in Burns/Pulse), so
// socket-based presence would undercount; heartbeats cover every visit.
//
// The client sends a heartbeat on load and every ~30s while the tab is visible.
// A session counts as online if seen within ONLINE_WINDOW_MS. When a session
// goes silent past SESSION_GAP_MS, its duration (first→last beat) is flushed as
// one session_end event — so avg session time is recorded without persisting
// every heartbeat.

const db = require('./db');

const ONLINE_WINDOW_MS = parseInt(process.env.ANALYTICS_ONLINE_WINDOW_MS, 10) || 45000;
const SESSION_GAP_MS = parseInt(process.env.ANALYTICS_SESSION_GAP_MS, 10) || 90000;
const SWEEP_MS = 30000;

// session_id -> { firstSeen, lastSeen, visitor }
const sessions = new Map();
let peak = 0;
let sweepTimer = null;

function touch(sessionId, visitor) {
    if (!sessionId) return;
    const now = Date.now();
    const s = sessions.get(sessionId);
    if (s) {
        s.lastSeen = now;
        if (visitor && !s.visitor) s.visitor = visitor;
    } else {
        sessions.set(sessionId, { firstSeen: now, lastSeen: now, visitor: visitor || null });
    }
}

function getOnline() {
    const cutoff = Date.now() - ONLINE_WINDOW_MS;
    let n = 0;
    for (const s of sessions.values()) if (s.lastSeen >= cutoff) n += 1;
    return n;
}

function getPeak() { return peak; }

// Flush sessions silent past the gap → persist duration as one session_end.
function sweep() {
    const now = Date.now();
    for (const [id, s] of sessions) {
        if (now - s.lastSeen > SESSION_GAP_MS) {
            const duration = s.lastSeen - s.firstSeen;
            if (duration >= 1000) {
                db.enqueue({ type: 'session_end', session_id: id, visitor: s.visitor, duration_ms: duration });
            }
            sessions.delete(id);
        }
    }
    const cur = getOnline();
    if (cur > peak) peak = cur;
}

function startSweepLoop() {
    if (sweepTimer) return;
    sweepTimer = setInterval(sweep, SWEEP_MS);
    if (sweepTimer.unref) sweepTimer.unref();
}

module.exports = { touch, getOnline, getPeak, startSweepLoop };
