const { ApiPromise, WsProvider } = require("@polkadot/api");
const { options } = require("@sora-substrate/api");
const { Pool } = require("pg");

const ASSET = {
    XOR:  "0x0200000000000000000000000000000000000000000000000000000000000000",
    VAL:  "0x0200040000000000000000000000000000000000000000000000000000000000",
    KUSD: "0x02000c0000000000000000000000000000000000000000000000000000000000",
    TBCD: "0x02000d0000000000000000000000000000000000000000000000000000000000",
};
const log = (m) => process.stdout.write(m + "\n");
const rawToHuman = (raw) => {
    if (raw == null) return 0;
    const s = String(raw);
    if (s === "0") return 0;
    try { return Number(BigInt(s)) / 1e18; } catch { return parseFloat(s) / 1e18; }
};

(async () => {
  const ws = new WsProvider("wss://mof2.sora.org");
  const api = await ApiPromise.create(options({ provider: ws }));
  const head = await api.rpc.chain.getHeader();
  const headN = head.number.toNumber();
  log("archive head: " + headN);

  const pg = new Pool({ host: "127.0.0.1", port: 23798, user: "postgres", password: "squid", database: "squid" });

  const HOURS = parseInt(process.argv[2]) || 4;
  const startN = headN - HOURS * 600;
  log("scanning " + startN + ".." + headN + " (" + (headN - startN) + " blocks, " + HOURS + "h)");

  // Phase 1: parallel fetch of xorToVal for all blocks in batches
  const BATCH = 50;
  const xorToValByBlock = {};
  let progress = 0;

  for (let i = startN; i <= headN; i += BATCH) {
    const blocks = [];
    for (let j = i; j < Math.min(i + BATCH, headN + 1); j++) blocks.push(j);
    const results = await Promise.all(blocks.map(async (n) => {
      try {
        const h = await api.rpc.chain.getBlockHash(n);
        const xtv = await api.query.xorFee.xorToVal.at(h);
        return [n, rawToHuman(xtv.toString()), h.toHex()];
      } catch (e) { return [n, null, null]; }
    }));
    for (const [n, v, h] of results) xorToValByBlock[n] = { val: v, hash: h };
    progress += blocks.length;
    log("  fetched " + progress + "/" + (headN - startN));
  }

  log("\nphase 2 — detect remints");
  const remintBlocks = [];
  let prev = null;
  for (let n = startN; n <= headN; n++) {
    const cur = xorToValByBlock[n]?.val;
    if (cur != null && prev != null && prev - cur > 0.001) {
      remintBlocks.push({ block: n, hash: xorToValByBlock[n].hash });
    }
    if (cur != null) prev = cur;
  }
  log("found " + remintBlocks.length + " candidate remint blocks");

  // Phase 3: parallel fetch events + ts for each remint, in chunks of 10
  let inserted = 0;
  for (let i = 0; i < remintBlocks.length; i += 10) {
    const chunk = remintBlocks.slice(i, i + 10);
    await Promise.all(chunk.map(async ({ block, hash }) => {
      try {
        const [events, ts] = await Promise.all([
          api.query.system.events.at(hash),
          api.query.timestamp.now.at(hash),
        ]);
        const tsMs = Number(ts.toString());
        const burns = {};
        for (const r of events) {
          const e = r.event;
          if (e.section === "tokens" && e.method === "Withdrawn") {
            const data = e.data.toJSON();
            const aid = data[0]?.code ? String(data[0].code).toLowerCase() : null;
            const amt = data[2];
            if (aid && amt != null) burns[aid] = (burns[aid] || 0n) + BigInt(amt);
          }
        }
        const xorB  = rawToHuman(burns[ASSET.XOR.toLowerCase()]?.toString()  || "0");
        const valB  = rawToHuman(burns[ASSET.VAL.toLowerCase()]?.toString()  || "0");
        const kusdB = rawToHuman(burns[ASSET.KUSD.toLowerCase()]?.toString() || "0");
        const tbcdB = rawToHuman(burns[ASSET.TBCD.toLowerCase()]?.toString() || "0");

        if (valB > 0 || kusdB > 0 || xorB > 0) {
          await pg.query(
            "INSERT INTO sm.fee_burns_live (block_height, ts, fees_paid_xor, ref_paid_xor, ref_redirected_xor, remint_xor_burned, remint_val_burned, remint_kusd_burned, remint_tbcd_burned) VALUES ($1, $2, 0, 0, 0, $3, $4, $5, $6) ON CONFLICT (block_height) DO UPDATE SET remint_xor_burned = EXCLUDED.remint_xor_burned, remint_val_burned = EXCLUDED.remint_val_burned, remint_kusd_burned = EXCLUDED.remint_kusd_burned, remint_tbcd_burned = EXCLUDED.remint_tbcd_burned",
            [block, tsMs, xorB, valB, kusdB, tbcdB]
          );
          inserted++;
          log("  [" + inserted + "] @" + block + " VAL=" + valB.toFixed(2) + " KUSD=" + kusdB.toFixed(4));
        }
      } catch (e) { log("  err @" + block + ": " + e.message); }
    }));
  }

  log("\nDONE: " + inserted + " remints inserted");
  await pg.end();
  await api.disconnect();
  process.exit(0);
})();
