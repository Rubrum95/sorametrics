#!/usr/bin/env node
'use strict';
const { Pool } = require('pg');
const pool = new Pool({ host:'localhost', port:23798, database:'squid', user:'postgres', password:'squid', max:3 });

(async () => {
  const range = await pool.query('SELECT MIN(hour_bucket) AS mn, MAX(hour_bucket) AS mx FROM sm.price_history');
  const totalHours = Math.floor((range.rows[0].mx - range.rows[0].mn) / 3600);
  console.log('Total hours span:', totalHours);

  const assets = [
    ['XOR', '0x0200000000000000000000000000000000000000000000000000000000000000'],
    ['DAI', '0x0200060000000000000000000000000000000000000000000000000000000000'],
    ['PSWAP','0x0200050000000000000000000000000000000000000000000000000000000000'],
    ['VAL',  '0x0200040000000000000000000000000000000000000000000000000000000000'],
    ['TBCD', '0x02000a0000000000000000000000000000000000000000000000000000000000'],
    ['ETH',  '0x0200080000000000000000000000000000000000000000000000000000000000'],
  ];
  for (const [sym, id] of assets) {
    const r = await pool.query('SELECT COUNT(*) AS cnt FROM sm.price_history WHERE asset_id = $1', [id]);
    const pct = ((parseInt(r.rows[0].cnt) / totalHours) * 100).toFixed(1);
    console.log(sym + ': ' + r.rows[0].cnt + ' hours (' + pct + '%)');
  }

  const total = await pool.query('SELECT COUNT(DISTINCT asset_id) AS cnt FROM sm.price_history');
  console.log('Total assets with prices:', total.rows[0].cnt);

  const rows = await pool.query('SELECT COUNT(*) AS cnt FROM sm.price_history');
  console.log('Total price_history rows:', parseInt(rows.rows[0].cnt).toLocaleString());

  await pool.end();
})();
