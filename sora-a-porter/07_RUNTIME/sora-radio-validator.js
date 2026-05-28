/**
 * SORA-a-porter RADIO Station Home v1 payload validator
 * Closes JSD F1 Gate 9 (Validation Gate).
 *
 * Verifies:
 *  - all 7 canonical families present (station, tracks, queue, stationMetrics,
 *    relatedObjects, assetAvailability, modeAccess)
 *  - required fields per family
 *  - unique track ids and modeAccess ids
 *  - all references between families resolve (queue.trackId → tracks.id,
 *    track.relatedObjectIds → relatedObjects.id, assetAvailability.trackId
 *    → tracks.id, currentTrackId → tracks.id)
 *  - asset states are one of the 4 canonical values (metadata_only,
 *    available_later, available, not_available)
 *  - metric kinds are 'symbolic' or 'factual'
 *  - modeAccess routes are non-empty
 *  - local asset paths can be HEAD-fetched when state is 'available'
 *
 * Usage in the browser:
 *   const result = await window.SORA_RADIO_VALIDATOR.validate(payload);
 *   if (!result.ok) console.error(result.errors);
 *
 * Usage in Node:
 *   node sora-radio-validator.js path/to/payload.json
 */
(function (root, factory) {
  const api = factory();
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
    if (require.main === module) {
      const fs = require('fs');
      const path = process.argv[2];
      if (!path) { console.error('usage: node sora-radio-validator.js <payload.json>'); process.exit(2); }
      const payload = JSON.parse(fs.readFileSync(path, 'utf8'));
      api.validate(payload, { checkPaths: false }).then((r) => {
        if (r.ok) { console.log('OK · payload validates · ' + r.summary); process.exit(0); }
        console.error('FAIL · ' + r.errors.length + ' error(s):');
        r.errors.forEach((e) => console.error('  - ' + e));
        if (r.warnings.length) { console.error(r.warnings.length + ' warning(s):'); r.warnings.forEach((w) => console.error('  ! ' + w)); }
        process.exit(1);
      });
    }
  } else {
    root.SORA_RADIO_VALIDATOR = api;
  }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const REQUIRED_FAMILIES = ['station', 'tracks', 'queue', 'stationMetrics', 'relatedObjects', 'assetAvailability', 'modeAccess'];
  const ASSET_STATES = ['metadata_only', 'available_later', 'available', 'not_available'];
  const METRIC_KINDS = ['symbolic', 'factual'];
  const COVER_STATES = ['cover_review', 'artwork_final', 'needs_rebuild', 'none'];
  const AUDIO_STATES = ['available', 'available_later', 'metadata_only', 'not_available'];

  function isString(v)  { return typeof v === 'string' && v.length > 0; }
  function isObject(v)  { return v !== null && typeof v === 'object' && !Array.isArray(v); }
  function isArray(v)   { return Array.isArray(v); }
  function dedup(arr)   { return Array.from(new Set(arr)); }
  function unique(arr)  { return arr.length === dedup(arr).length; }

  function validateStation(station, errors) {
    if (!isObject(station)) { errors.push('station: must be an object'); return; }
    ['id', 'name', 'state', 'frequencyDisplay'].forEach((f) => {
      if (!isString(station[f])) errors.push('station.' + f + ': required non-empty string');
    });
    if (station.memo) {
      if (!isString(station.memo.title)) errors.push('station.memo.title: required when memo is present');
      if (!isString(station.memo.body))  errors.push('station.memo.body: required when memo is present');
    }
  }

  function validateTracks(tracks, errors) {
    if (!isArray(tracks)) { errors.push('tracks: must be an array'); return new Set(); }
    const ids = new Set();
    tracks.forEach((t, i) => {
      const tag = 'tracks[' + i + ']';
      if (!isObject(t)) { errors.push(tag + ': must be an object'); return; }
      ['id', 'title', 'slug', 'family'].forEach((f) => {
        if (!isString(t[f])) errors.push(tag + '.' + f + ': required non-empty string');
      });
      if (t.id) {
        if (ids.has(t.id)) errors.push(tag + '.id: duplicate "' + t.id + '"');
        ids.add(t.id);
      }
      if (t.audioStatus && !AUDIO_STATES.includes(t.audioStatus)) {
        errors.push(tag + '.audioStatus: "' + t.audioStatus + '" not one of ' + AUDIO_STATES.join('|'));
      }
      if (t.coverStatus && !COVER_STATES.includes(t.coverStatus)) {
        errors.push(tag + '.coverStatus: "' + t.coverStatus + '" not one of ' + COVER_STATES.join('|'));
      }
      if (t.relatedObjectIds && !isArray(t.relatedObjectIds)) {
        errors.push(tag + '.relatedObjectIds: must be an array');
      }
    });
    return ids;
  }

  function validateQueue(queue, trackIds, errors) {
    if (!isArray(queue)) { errors.push('queue: must be an array'); return; }
    queue.forEach((q, i) => {
      const tag = 'queue[' + i + ']';
      if (!isObject(q)) { errors.push(tag + ': must be an object'); return; }
      if (typeof q.position !== 'number') errors.push(tag + '.position: required number');
      if (!isString(q.trackId)) { errors.push(tag + '.trackId: required'); return; }
      if (!trackIds.has(q.trackId)) errors.push(tag + '.trackId: "' + q.trackId + '" not found in tracks');
    });
  }

  function validateMetrics(metrics, errors) {
    if (!isArray(metrics)) { errors.push('stationMetrics: must be an array'); return; }
    metrics.forEach((m, i) => {
      const tag = 'stationMetrics[' + i + ']';
      ['key', 'label', 'valueDisplay'].forEach((f) => {
        if (!isString(m[f])) errors.push(tag + '.' + f + ': required non-empty string');
      });
      if (!METRIC_KINDS.includes(m.kind)) {
        errors.push(tag + '.kind: "' + m.kind + '" not one of ' + METRIC_KINDS.join('|'));
      }
      if (m.kind === 'symbolic' && !isString(m.note)) {
        errors.push(tag + '.note: required when kind=symbolic (Metrics Gate)');
      }
    });
  }

  function validateRelatedObjects(objs, errors) {
    if (!isArray(objs)) { errors.push('relatedObjects: must be an array'); return new Set(); }
    const ids = new Set();
    objs.forEach((o, i) => {
      const tag = 'relatedObjects[' + i + ']';
      if (!isObject(o)) { errors.push(tag + ': must be an object'); return; }
      ['id', 'kind', 'title'].forEach((f) => {
        if (!isString(o[f])) errors.push(tag + '.' + f + ': required non-empty string');
      });
      if (o.id) {
        if (ids.has(o.id)) errors.push(tag + '.id: duplicate "' + o.id + '"');
        ids.add(o.id);
      }
      if (!isString(o.route) && !o.linkOnly) {
        errors.push(tag + '.route or linkOnly: required (Commerce Gate)');
      }
    });
    return ids;
  }

  function validateTrackRelatedRefs(tracks, relatedObjectIds, errors) {
    tracks.forEach((t, i) => {
      if (!isArray(t.relatedObjectIds)) return;
      t.relatedObjectIds.forEach((rid, j) => {
        if (!relatedObjectIds.has(rid)) {
          errors.push('tracks[' + i + '].relatedObjectIds[' + j + ']: "' + rid + '" not found in relatedObjects');
        }
      });
    });
  }

  function validateAssetAvailability(availability, trackIds, errors) {
    if (!isArray(availability)) { errors.push('assetAvailability: must be an array'); return; }
    availability.forEach((a, i) => {
      const tag = 'assetAvailability[' + i + ']';
      if (!isObject(a)) { errors.push(tag + ': must be an object'); return; }
      if (!isString(a.kind))  errors.push(tag + '.kind: required');
      if (!ASSET_STATES.includes(a.state)) {
        errors.push(tag + '.state: "' + a.state + '" not one of ' + ASSET_STATES.join('|'));
      }
      if (a.trackId !== null && a.trackId !== undefined && !trackIds.has(a.trackId)) {
        errors.push(tag + '.trackId: "' + a.trackId + '" not found in tracks (use null for non-track assets)');
      }
      if ((a.state === 'available' || a.state === 'available_later') && !isString(a.path)) {
        errors.push(tag + '.path: required when state=' + a.state);
      }
    });
  }

  function validateModeAccess(modes, errors) {
    if (!isArray(modes)) { errors.push('modeAccess: must be an array'); return; }
    const ids = new Set();
    modes.forEach((m, i) => {
      const tag = 'modeAccess[' + i + ']';
      ['modeId', 'label', 'kind', 'route'].forEach((f) => {
        if (!isString(m[f])) errors.push(tag + '.' + f + ': required non-empty string');
      });
      if (m.modeId) {
        if (ids.has(m.modeId)) errors.push(tag + '.modeId: duplicate "' + m.modeId + '"');
        ids.add(m.modeId);
      }
    });
  }

  function validateCurrentTrack(currentTrackId, trackIds, errors) {
    if (currentTrackId === undefined || currentTrackId === null) return;
    if (!isString(currentTrackId)) {
      errors.push('currentTrackId: must be a string when present');
      return;
    }
    if (!trackIds.has(currentTrackId)) {
      errors.push('currentTrackId: "' + currentTrackId + '" not found in tracks');
    }
  }

  async function checkLocalPaths(availability, baseURL, warnings) {
    if (typeof fetch === 'undefined') return;
    for (let i = 0; i < availability.length; i++) {
      const a = availability[i];
      if (a.state !== 'available' || !isString(a.path)) continue;
      try {
        const url = new URL(a.path, baseURL).toString();
        const res = await fetch(url, { method: 'HEAD' });
        if (!res.ok) warnings.push('assetAvailability[' + i + '].path: HEAD ' + res.status + ' for ' + a.path);
      } catch (e) {
        warnings.push('assetAvailability[' + i + '].path: fetch failed for ' + a.path + ' (' + e.message + ')');
      }
    }
  }

  async function validate(payload, opts) {
    opts = opts || {};
    const errors = [];
    const warnings = [];

    if (!isObject(payload)) {
      return { ok: false, errors: ['payload: must be an object'], warnings: [], summary: '' };
    }

    REQUIRED_FAMILIES.forEach((f) => {
      if (!(f in payload)) errors.push('payload.' + f + ': required family missing');
    });

    if (errors.length) return { ok: false, errors, warnings, summary: 'missing families' };

    validateStation(payload.station, errors);
    const trackIds = validateTracks(payload.tracks, errors);
    validateCurrentTrack(payload.currentTrackId, trackIds, errors);
    validateQueue(payload.queue, trackIds, errors);
    validateMetrics(payload.stationMetrics, errors);
    const relatedIds = validateRelatedObjects(payload.relatedObjects, errors);
    validateTrackRelatedRefs(payload.tracks, relatedIds, errors);
    validateAssetAvailability(payload.assetAvailability, trackIds, errors);
    validateModeAccess(payload.modeAccess, errors);

    if (opts.checkPaths !== false && typeof window !== 'undefined') {
      await checkLocalPaths(payload.assetAvailability, window.location.href, warnings);
    }

    const summary = [
      payload.tracks.length + ' tracks',
      payload.queue.length + ' queue items',
      payload.stationMetrics.length + ' metrics',
      payload.relatedObjects.length + ' related objects',
      payload.assetAvailability.length + ' asset records',
      payload.modeAccess.length + ' mode accesses',
    ].join(' · ');

    return { ok: errors.length === 0, errors, warnings, summary };
  }

  return { validate, REQUIRED_FAMILIES, ASSET_STATES, METRIC_KINDS, COVER_STATES, AUDIO_STATES };
});
