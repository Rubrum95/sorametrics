#!/usr/bin/env node
'use strict';
const { Pool } = require('pg');
const pool = new Pool({ host:'localhost', port:23798, database:'squid', user:'postgres', password:'squid', max:3 });
const DAI_ID = '0x0200060000000000000000000000000000000000000000000000000000000000';

(async () => {
  // Check DAI price for 15/03/2026 14:00 UTC
  const ts = Math.floor(new Date('2026-03-15T14:00:00Z').getTime() / 1000);
  console.log('Hour bucket:', ts);
  const r = await pool.query('SELECT * FROM sm.price_history WHERE asset_id = $1 AND hour_bucket = $2', [DAI_ID, ts]);
  console.log('DAI price for that hour:', r.rows);

  // Check the DAI transfers in MV
  const t = await pool.query("SELECT timestamp, usd_value, asset_id FROM sm.mv_transfers WHERE symbol = 'DAI' ORDER BY timestamp DESC LIMIT 5");
  console.log('\nRecent DAI transfers in MV:');
  t.rows.forEach(r => console.log('  ts=' + r.timestamp, 'bucket=' + (Math.floor(r.timestamp/3600)*3600), 'usd=' + r.usd_value));

  // Check live_transfers count
  const l = await pool.query('SELECT COUNT(*) as cnt FROM sm.live_transfers');
  console.log('\nLive transfers:', l.rows[0].cnt, 'rows');

  // Check max DAI hour
  const mx = await pool.query('SELECT MAX(hour_bucket) as mx FROM sm.price_history WHERE asset_id = $1', [DAI_ID]);
  console.log('Max DAI hour bucket:', mx.rows[0].mx, new Date(mx.rows[0].mx * 1000).toISOString());

  await pool.end();
})();
