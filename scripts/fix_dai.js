#!/usr/bin/env node
'use strict';
const { Pool } = require('pg');
const pool = new Pool({ host:'localhost', port:23798, database:'squid', user:'postgres', password:'squid', max:3 });
const DAI_ID = '0x0200060000000000000000000000000000000000000000000000000000000000';

(async () => {
  // Extend DAI to all hours in the price_history range
  const r = await pool.query(`
    INSERT INTO sm.price_history (asset_id, hour_bucket, price_usd, sample_count)
    SELECT $1, h, 1.0, 1
    FROM generate_series(
      (SELECT MIN(hour_bucket) FROM sm.price_history),
      (SELECT MAX(hour_bucket) FROM sm.price_history),
      3600
    ) AS h
    ON CONFLICT (asset_id, hour_bucket) DO NOTHING
  `, [DAI_ID]);
  console.log('Extended DAI by', r.rowCount, 'hours');

  // Check coverage now
  const cnt = await pool.query('SELECT COUNT(*) as cnt FROM sm.price_history WHERE asset_id = $1', [DAI_ID]);
  console.log('DAI total hours:', cnt.rows[0].cnt);

  await pool.end();
})();
