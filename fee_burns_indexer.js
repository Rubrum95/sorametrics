// Fee-burns live indexer — counts XOR-fee remint burns from HEAD onwards.
//
// Runs as a side-effect of the main API process: subscribes to finalized
// heads via the same Polkadot API that everything else uses, and on each
// new block:
//
//   1. Reads xorFee.xorToVal and xorFee.xorToBuyBack storage.
//   2. Compares against the previous block's cached values.
//   3. Walks the block's events, summing:
//        · xorFee.FeeWithdrawn(who, amount)        → fees_paid_xor
//        · xorFee.ReferrerRewarded(who, ref, amt)  → ref_paid_xor
//   4. If a bucket dropped by ≥ MIN_REMINT_DROP_XOR, treats this as a
//      remint and reads assets.Burn events in the same block to derive
//      VAL / KUSD / TBCD / XOR amounts that were really burned.
//   5. Computes ref_redirected_xor = max(0, fees*refW/total − ref_paid).
//   6. Inserts a row into sm.fee_burns_live ONLY when at least one
//      column is non-zero (most blocks have no fee activity, no point
//      polluting the table).
//
// Asset IDs (SORA mainnet, native v6):
//   XOR  : 0x0200000000000000000000000000000000000000000000000000000000000000
//   VAL  : 0x0200040000000000000000000000000000000000000000000000000000000000
//   KUSD : 0x02000c0000000000000000000000000000000000000000000000000000000000
//   TBCD : 0x02000d0000000000000000000000000000000000000000000000000000000000
//   PSWAP: 0x0200050000000000000000000000000000000000000000000000000000000000
//   XSTUSD:0x0200080000000000000000000000000000000000000000000000000000000000
//
// We track XOR/VAL/KUSD/TBCD because those are the four targets of the
// xor-fee buy-burn cycle. PSWAP burns live in pswap-distribution and are
// out of scope for this widget.

'use strict';

const ASSET = {
    XOR:  '0x0200000000000000000000000000000000000000000000000000000000000000',
    VAL:  '0x0200040000000000000000000000000000000000000000000000000000000000',
    KUSD: '0x02000c0000000000000000000000000000000000000000000000000000000000',
    TBCD: '0x02000d0000000000000000000000000000000000000000000000000000000000',
};

const TOKEN_DECIMALS = 18; // all four are 18-decimal natives

// A "remint" is detected when a bucket drops by at least this many XOR
// in a single block. Random noise (e.g. someone sending 0.00001 XOR fee)
// can't cause a bucket to drop, only on_initialize remint logic does, so
// any decrement is real — but we still threshold to skip floating-point
// rounding artefacts.
const MIN_REMINT_DROP_XOR = 0.001;

// Convert a raw u128 string ("12345...") to a number in human XOR units.
// We go via BigInt to keep precision for amounts > 2^53.
function rawToHuman(raw) {
    if (raw == null) return 0;
    const s = String(raw);
    if (s === '0') return 0;
    try {
        // Number(BigInt) loses precision past 2^53; for display this is fine
        // since burns will never exceed billions of tokens.
        return Number(BigInt(s)) / Math.pow(10, TOKEN_DECIMALS);
    } catch {
        return parseFloat(s) / Math.pow(10, TOKEN_DECIMALS);
    }
}

function startFeeBurnsIndexer(api, db, log = console) {
    // In-memory state — persists across blocks within this process. After
    // a restart we lose the previous bucket reading, so the very first
    // block after restart skips remint detection (treats prev = current).
    const state = {
        prev: { xorToVal: null, xorToBuyBack: null },
        startBlock: null,
        processed: 0,
        remintsDetected: 0,
        lastError: null,
    };

    const refWeight = () => {
        const sv = api?.runtimeVersion?.specVersion?.toNumber?.() ?? 0;
        if (sv >= 128) return { ref: 10, xor: 35, val: 40, kusd: 0,  total: 85 };  // 4.8.6
        if (sv >= 120) return { ref: 10, xor: 20, val: 50, kusd: 5,  total: 85 };  // 4.8.2-4.8.4
        return { ref: 10, xor: 20, val: 50, kusd: 20, total: 100 };                 // 4.7.x
    };

    async function processBlock(header) {
        const blockNumber = header.number.toNumber();
        const blockHash = await api.rpc.chain.getBlockHash(blockNumber);

        // Read everything in parallel to keep latency low.
        const [xtv, xtb, events, timestamp] = await Promise.all([
            api.query.xorFee.xorToVal.at(blockHash),
            api.query.xorFee.xorToBuyBack.at(blockHash),
            api.query.system.events.at(blockHash),
            api.query.timestamp.now.at(blockHash),
        ]);

        const xtvHuman = rawToHuman(xtv.toString());
        const xtbHuman = rawToHuman(xtb.toString());
        const tsMs = Number(timestamp.toString());

        // Per-block accumulators we'll insert (most stay 0).
        const row = {
            block_height: blockNumber,
            ts: tsMs,
            fees_paid_xor: 0,
            ref_paid_xor: 0,
            ref_redirected_xor: 0,
            remint_xor_burned: 0,
            remint_val_burned: 0,
            remint_kusd_burned: 0,
            remint_tbcd_burned: 0,
        };

        // Walk events once, dispatching by section/method.
        const burnsByAsset = {}; // asset_id_hex → sum of raw amounts (this block only)
        let xorBurnedFromFees = 0; // each fee burns part directly; we sum the events

        for (const record of events) {
            const { event } = record;
            const section = event.section;
            const method  = event.method;

            if (section === 'xorFee' && method === 'FeeWithdrawn') {
                const data = event.data.toJSON();
                // [who, amount]
                const amt = rawToHuman(data[1]);
                row.fees_paid_xor += amt;
            }
            else if (section === 'xorFee' && method === 'ReferrerRewarded') {
                const data = event.data.toJSON();
                // 4.7.x: [who, referrer, amount]
                // 4.8.x: [who, referrer, asset_id, amount]
                // Amount lives at the LAST element either way.
                const amt = rawToHuman(data[data.length - 1]);
                row.ref_paid_xor += amt;
            }
            // SORA does NOT emit assets.Burn / assets.Burned events. The
            // xor-fee remint flow burns by withdrawing from the pallet tech
            // account, which fires tokens.Withdrawn for VAL/KUSD/TBCD.
            // Direct XOR burns (the 20%/85 weight) fire balances.Withdraw on
            // the tech account. We capture both shapes here.
            //
            // tokens.Withdrawn data: [{code: assetIdHex}, account, amount]
            else if (section === 'tokens' && method === 'Withdrawn') {
                const data = event.data.toJSON();
                const assetIdRaw = data[0];
                const assetId = (assetIdRaw && typeof assetIdRaw === 'object')
                    ? String(assetIdRaw.code || '')
                    : String(assetIdRaw || '');
                const amount = data[2];
                if (assetId && amount != null) {
                    const k = assetId.toLowerCase();
                    burnsByAsset[k] = (burnsByAsset[k] || 0n) + safeBig(amount);
                }
            }

        }

        // ref_redirected_xor: ideally fees*refW/total goes to referrers,
        // anything not paid out got redirected to the kusd buy-back bucket.
        const w = refWeight();
        const refIdeal = row.fees_paid_xor * (w.ref / w.total);
        row.ref_redirected_xor = Math.max(0, refIdeal - row.ref_paid_xor);

        // XOR direct burn is implicit — the pallet drops a NegativeImbalance
        // (`_xor_burned` in pallets/xor-fee/src/lib.rs:400) without emitting
        // any event. The amount is feeWithdrawn × xorBurnedWeight/totalWeight,
        // and it happens ON EACH FEE (not at remint time like VAL/KUSD/TBCD).
        // We attribute it directly to the block that produced the fee.
        if (row.fees_paid_xor > 0) {
            row.remint_xor_burned = row.fees_paid_xor * (w.xor / w.total);
        }

        // Detect remint via bucket drop.
        const isRemint =
            state.prev.xorToVal != null &&
            (state.prev.xorToVal - xtvHuman > MIN_REMINT_DROP_XOR ||
             state.prev.xorToBuyBack - xtbHuman > MIN_REMINT_DROP_XOR);

        if (isRemint) {
            state.remintsDetected++;
            // Real burn amounts from on-chain tokens.Withdrawn events.
            // XOR is computed above (implicit burn, no event).
            row.remint_val_burned  = rawToHuman(burnsByAsset[ASSET.VAL]?.toString()  || '0');
            row.remint_kusd_burned = rawToHuman(burnsByAsset[ASSET.KUSD]?.toString() || '0');
            row.remint_tbcd_burned = rawToHuman(burnsByAsset[ASSET.TBCD]?.toString() || '0');
        }

        // Update state for the next block's diff.
        state.prev.xorToVal = xtvHuman;
        state.prev.xorToBuyBack = xtbHuman;
        if (state.startBlock == null) state.startBlock = blockNumber;
        state.processed++;
        if (state.processed % 50 === 0) log.info("[fee-burns-indexer] alive @" + blockNumber + " processed=" + state.processed + " remints=" + state.remintsDetected);

        // Only persist if there's anything to record. ~99% of blocks have
        // zero fees, no point inflating the table.
        const hasActivity =
            row.fees_paid_xor > 0 ||
            row.ref_paid_xor  > 0 ||
            row.ref_redirected_xor > 0 ||
            row.remint_xor_burned  > 0 ||
            row.remint_val_burned  > 0 ||
            row.remint_kusd_burned > 0 ||
            row.remint_tbcd_burned > 0;

        if (hasActivity) {
            try {
                await db.insertFeeBurnRow(row);
            } catch (e) {
                state.lastError = e.message;
                log.warn('[fee-burns-indexer] insert failed:', e.message);
            }
        }

        if (isRemint) {
            log.info(
                `[fee-burns-indexer] remint @${blockNumber} | ` +
                `XOR=${row.remint_xor_burned.toFixed(4)} ` +
                `VAL=${row.remint_val_burned.toFixed(4)} ` +
                `KUSD=${row.remint_kusd_burned.toFixed(4)} ` +
                `TBCD=${row.remint_tbcd_burned.toFixed(6)}`
            );
        }
    }

    // Subscribe to finalized heads (already finalized = no reorg risk).
    let unsubscribe = null;
    api.rpc.chain.subscribeNewHeads(async header => {
        try {
            await processBlock(header);
        } catch (e) {
            state.lastError = e.message;
            log.warn('[fee-burns-indexer] block processing failed:',
                     header.number.toNumber(), e.message);
        }
    }).then(unsub => { unsubscribe = unsub; })
      .catch(e => log.error('[fee-burns-indexer] subscribe failed:', e.message));

    log.info('[fee-burns-indexer] started, will count from next finalized head onwards');

    return {
        state,
        stop() { if (unsubscribe) unsubscribe(); },
    };
}

function safeBig(v) {
    try { return BigInt(v); }
    catch { try { return BigInt(String(v)); } catch { return 0n; } }
}

module.exports = { startFeeBurnsIndexer };
