'use strict';
// ============================================================
// minamoto/prom_parser.js — Prometheus text format → JSON array
// Spec: https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md
// We only need the subset emitted by Iroha 3 (/metrics): gauges, counters, histograms.
// Returns: Array<{ name, labels: Record<string,string>, value: number }>.
// HELP / TYPE lines are ignored (we keep only samples).
// ============================================================

function parseLabels(raw) {
    // raw is the body inside `{...}`. Empty string → {}.
    if (!raw) return {};
    const out = {};
    let i = 0;
    while (i < raw.length) {
        // Skip whitespace and commas
        while (i < raw.length && (raw[i] === ',' || raw[i] === ' ')) i++;
        if (i >= raw.length) break;
        // key
        const keyStart = i;
        while (i < raw.length && raw[i] !== '=') i++;
        const key = raw.slice(keyStart, i).trim();
        if (i >= raw.length || raw[i] !== '=') break;
        i++; // skip '='
        if (raw[i] !== '"') break;
        i++; // skip opening quote
        // value (may contain escaped quotes \" and backslashes \\)
        let val = '';
        while (i < raw.length && raw[i] !== '"') {
            if (raw[i] === '\\' && i + 1 < raw.length) {
                const next = raw[i + 1];
                if (next === '"' || next === '\\') { val += next; i += 2; continue; }
                if (next === 'n') { val += '\n'; i += 2; continue; }
            }
            val += raw[i];
            i++;
        }
        i++; // skip closing quote
        if (key) out[key] = val;
    }
    return out;
}

function parseSampleValue(raw) {
    const t = raw.trim();
    if (t === 'NaN' || t === '+Inf' || t === '-Inf') return Number.NaN;
    const n = Number(t);
    return Number.isFinite(n) ? n : Number.NaN;
}

function parse(text) {
    if (!text || typeof text !== 'string') return [];
    const out = [];
    const lines = text.split('\n');
    for (let raw of lines) {
        const line = raw.trim();
        if (!line) continue;
        if (line.startsWith('#')) continue; // skip HELP / TYPE / EOF

        // metric_name{labels} value [timestamp]
        // metric_name value
        let braceOpen = line.indexOf('{');
        let name, labelsRaw, rest;
        if (braceOpen !== -1) {
            name = line.slice(0, braceOpen).trim();
            const braceClose = line.lastIndexOf('}');
            if (braceClose === -1 || braceClose < braceOpen) continue;
            labelsRaw = line.slice(braceOpen + 1, braceClose);
            rest = line.slice(braceClose + 1).trim();
        } else {
            const sp = line.indexOf(' ');
            if (sp === -1) continue;
            name = line.slice(0, sp).trim();
            labelsRaw = '';
            rest = line.slice(sp + 1).trim();
        }
        if (!name) continue;
        // rest = "value [timestamp]" → take first token only
        const valTok = rest.split(/\s+/, 1)[0];
        const value = parseSampleValue(valTok);
        if (Number.isNaN(value)) continue;
        const labels = parseLabels(labelsRaw);
        out.push({ name, labels, value });
    }
    return out;
}

// Group samples by metric_name → useful for dashboard widgets.
function groupByName(samples) {
    const out = {};
    for (const s of samples) {
        if (!out[s.name]) out[s.name] = [];
        out[s.name].push({ labels: s.labels, value: s.value });
    }
    return out;
}

// Pick a single value (no labels OR one entry only) for a scalar metric.
function scalar(samples, name) {
    const matches = samples.filter(s => s.name === name);
    if (matches.length === 0) return null;
    return matches[0].value;
}

module.exports = { parse, groupByName, scalar };
