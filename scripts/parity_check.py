#!/usr/bin/env python3
"""Contract parity check: every Node API route, prod vs v33.

Usage:
  scripts/parity_check.py [--prod URL] [--v33 URL] [--only SUBSTR] [--report FILE]
                          [--save-golden DIR] [--golden DIR] [--max-diffs N]

Modes per route (see ROUTES):
  full       every field must match (after the ignore / volatile rules)
  structure  keys and scalar types must match (rows depend on the DB history)
  unordered  lists keyed by `key` are compared as sets
Global rules: block-relative and clock fields are ignored everywhere; price
fields are compared by type only (`volatile`). Exit code 1 when any route
reports unexpected differences or a transport error.
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

ADDR = "cnSMPnA1v4R4uciDxgkzgYYraipNfNwwWSdMxjQ9JfbGmPe56"
VALIDATOR = "cnVS46aLyfRHTossU1ZEXaw6Eok1Lk9NeMdhJsSNzp7ywJLEq"
LP_WALLET = "cnSMPnA1v4R4uciDxgkzgYYraipNfNwwWSdMxjQ9JfbGmPe56"
XOR = "0x0200000000000000000000000000000000000000000000000000000000000000"
VAL = "0x0200040000000000000000000000000000000000000000000000000000000000"
PREIMAGE = "0xad10b3d7a9e6becaf3f3c70f3c23757c3af282e0a26b6a3bd4bcf01dad15aea3"
BLOCK = 27572842

# Fields whose value depends on the moment (head block, clock, live prices).
# `next_before` is the documented additive keyset cursor of the v33 history routes.
GLOBAL_IGNORE = {
    "next_before", "currentBlock", "blocksRemaining", "timeRemaining", "bestBlock", "finalizedBlock", "tip",
    "epochProgress", "eraStartedAgo", "sessionProgress", "eraProgress", "lastUpdate", "startTime",
    "endTime", "blocksUntilElection", "timeUntilElection", "uptime", "timestamp", "scanned",
    "identities", "lastTs", "firstTs", "blocks",
}
GLOBAL_VOLATILE = {
    "xorPrice", "valPrice", "price", "usdValue", "marketCap", "totalStakeUsd", "valToXorRate",
    "stakedUsd", "unbondingUsd", "usd_value", "value", "basePrice", "targetPrice", "EUR",
    "totalSupply", "pendingKusd", "valStakingEraReward", "valBucketPrevEra", "xorToVal",
    "xorToBuyBack", "unassignedValStakingReward", "activeEra", "history", "valBucketCurrentEra",
    "valBucketUnassigned", "xorTotalSupply", "totalStaked", "in_usd", "out_usd", "wsConnected",
}

from urllib.parse import quote as _q
MN_AUTH = _q("sorauﾛ1NﾖFjﾇDbhｦﾐﾉﾘkﾎｿPｵﾐﾒ7czﾃｴｦｶｻRﾋrcﾚeﾚｵｲDycFPRDKK")
MN_XOR = "6TEAJqbb8oEPmLncoNiMRbLEK6tw"
# --- /api/minamoto (mn.* frozen in prod since 2026-06-18: full parity expected on DB-backed routes;
#     Torii passthroughs are not listed while minamoto.sora.org is down) ---
MN_ROUTES = [
    dict(path="/api/minamoto/health", mode="full", ignore=["db"]),
    dict(path="/api/minamoto/network-state", mode="full"),
    dict(path="/api/minamoto/blocks?per_page=5", mode="full"),
    dict(path="/api/minamoto/blocks?page=3&per_page=50", mode="full"),
    dict(path="/api/minamoto/blocks/stats", mode="full"),
    dict(path="/api/minamoto/transactions?per_page=5", mode="full"),
    dict(path="/api/minamoto/transactions?page=2&per_page=100", mode="full"),
    dict(path="/api/minamoto/transactions?status=Rejected&per_page=5", mode="full"),
    dict(path=f"/api/minamoto/transactions?authority={MN_AUTH}&per_page=3", mode="full"),
    dict(path="/api/minamoto/transactions/stats", mode="full"),
    dict(path="/api/minamoto/transactions/fee-sponsorship", mode="full"),
    dict(path=f"/api/minamoto/wallet/{MN_AUTH}/info", mode="full"),
    dict(path="/api/minamoto/accounts?per_page=100", mode="unordered", key="id"),
    dict(path="/api/minamoto/accounts/stats", mode="full"),
    dict(path="/api/minamoto/domains", mode="full"),
    dict(path="/api/minamoto/domains/stats", mode="full"),
    dict(path="/api/minamoto/assets?per_page=100", mode="unordered", key="definition_id+account_id"),
    dict(path="/api/minamoto/asset-definitions", mode="full"),
    dict(path="/api/minamoto/asset-definitions/stats", mode="full"),
    dict(path="/api/minamoto/asset/XOR", mode="full"),
    dict(path=f"/api/minamoto/asset/{MN_XOR}/holders", mode="full"),
    dict(path="/api/minamoto/instructions?per_page=5", mode="full"),
    dict(path="/api/minamoto/instructions?kind=Mint&per_page=100", mode="full"),
    dict(path="/api/minamoto/instructions?block=429", mode="full"),
    dict(path="/api/minamoto/instructions/kinds", mode="full"),
    dict(path="/api/minamoto/transfers/stats", mode="full"),
    dict(path="/api/minamoto/permissions/stats", mode="full"),
    dict(path="/api/minamoto/permissions/grants?per_page=100", mode="full"),
    dict(path="/api/minamoto/lane-staking/lifecycle", mode="full"),
    dict(path="/api/minamoto/cross-chain/stats", mode="full"),
    dict(path="/api/minamoto/cross-chain/timeseries?hours=720", mode="full"),
    dict(path="/api/minamoto/cross-chain/claims?per_page=100", mode="full"),
    dict(path="/api/minamoto/cross-chain/mint-history", mode="full"),
    dict(path="/api/minamoto/cross-chain/pending-burns?status=all", mode="structure"),
    dict(path="/api/minamoto/peers", mode="full"),
    dict(path="/api/minamoto/indexer/state", mode="structure"),
    dict(path="/api/minamoto/prometheus/metric/blocks?hours=24", mode="full"),
]
ROUTES = [
    # --- health / meta ---
    dict(path="/health", mode="structure"),
    dict(path="/health/rpc-source", mode="structure"),
    dict(path="/api/version", mode="full"),
    dict(path="/currency-rates", mode="full"),
    # --- tokens / prices ---
    dict(path="/tokens", mode="full", ignore=["price", "priceChange24h", "change24h", "sparkline", "marketCap", "volume24h"]),
    # v33-only routes (not in index.js, so nothing to compare): /asset/:id,
    # /health/freshness, /history/global/fee_events, /history/fee_events/:address.
    dict(path="/chart/XOR?res=60", mode="structure"),
    dict(path=f"/tools/price-series?assets={XOR},{VAL}&window=7d", mode="structure"),
    # --- history (rows depend on the indexed history) ---
    dict(path="/history/global/swaps?limit=3", mode="structure"),
    dict(path="/history/global/transfers?limit=3", mode="structure"),
    dict(path="/history/global/bridges?limit=3", mode="structure"),
    dict(path="/history/global/liquidity?limit=3", mode="structure"),
    dict(path="/history/global/orderbook?limit=3", mode="structure"),
    dict(path="/history/global/extrinsics?limit=3", mode="structure"),
    dict(path=f"/history/swaps/{ADDR}?limit=3", mode="structure"),
    dict(path=f"/history/transfers/{ADDR}?limit=3", mode="structure"),
    dict(path=f"/history/bridges/{ADDR}?limit=3", mode="structure"),
    dict(path=f"/history/orderbook/{ADDR}?limit=3", mode="structure"),
    dict(path=f"/history/extrinsics/{ADDR}?limit=3", mode="structure"),
    dict(path=f"/history/extrinsic/{BLOCK}/1", mode="full", ignore=["args_json", "timestamp", "formatted_time", "time"]),
    dict(path="/history/extrinsic-sections", mode="structure"),
    dict(path=f"/history/extrinsic-fees?blocks={BLOCK}", mode="full", volatile=["totalUsd"]),
    dict(path="/pool/activity?base=XOR&target=VAL&limit=3", mode="structure"),
    dict(path="/search?q=XOR", mode="structure"),
    dict(path=f"/lookup/usd-value/{BLOCK}-1", mode="structure"),
    dict(path="/export/csv?wallets=" + ADDR + "&types=swaps&format=sorametrics", mode="structure", raw=True),
    # --- stats ---
    dict(path="/stats/network", mode="structure"),
    dict(path="/stats/overview", mode="structure"),
    dict(path="/stats/header", mode="structure"),
    dict(path="/stats/fees?timeframe=1d", mode="structure"),
    dict(path="/stats/fees/trend?timeframe=7d", mode="structure"),
    dict(path="/stats/network/trend?timeframe=7d", mode="structure"),
    dict(path="/stats/stablecoins", mode="structure"),
    dict(path="/stats/trending-tokens", mode="structure"),
    dict(path="/stats/accumulation", mode="structure"),
    dict(path="/stats/extrinsics-24h", mode="structure"),
    dict(path="/stats/fee-config", mode="structure"),
    dict(path="/stats/fee-burns-live?window=24h", mode="structure"),
    # --- chain state ---
    dict(path="/pools?limit=5", mode="full", ignore=["basePrice", "targetPrice", "reserves"]),
    dict(path=f"/holders/{VAL}?page=1", mode="full", ignore=["balance", "balanceStr", "totalHolders", "totalPages"]),
    dict(path=f"/pool/providers?base={XOR}&target={VAL}", mode="full", ignore=["balance", "balanceStr", "total", "totalPages", "value", "share", "amountBase", "amountTarget"]),
    dict(path=f"/wallet/liquidity/{LP_WALLET}", mode="unordered", key="pool", ignore=["value", "amountBase", "amountTarget", "share"]),
    dict(path=f"/balance/{ADDR}", mode="structure"),
    dict(path="/balances", method="POST", body={"addresses": [ADDR]}, mode="structure"),
    dict(path=f"/identity/{VALIDATOR}", mode="full"),
    dict(path="/api/identities", method="POST", body={"addresses": [VALIDATOR, ADDR]}, mode="full"),
    dict(path="/api/tech-accounts", mode="full"),
    dict(path=f"/block/{BLOCK}", mode="full", ignore=["type", "source"]),
    dict(path=f"/wallet/info/{ADDR}", mode="structure"),
    # --- staking ---
    dict(path=f"/wallet/staking/{ADDR}", mode="full"),
    dict(path="/staking/validators", mode="full", ignore=["erasSincePayout"]),
    dict(path="/staking/network", mode="full", ignore=["sessionIndex", "eraStart", "activeEra", "currentEra", "era"]),
    dict(path="/staking/recent-blocks", mode="structure"),
    dict(path="/staking/rewards", mode="full", ignore=["indexedTotalValReceived", "indexedPayoutCount", "indexedErasCovered", "lastClaimEra", "lastClaimTs", "networkTotals", "topDestinations", "valToXorRateWindows", "era", "avgRewardPointsPerEra", "erasProducedRecent", "valOutstanding", "ownOutstanding", "pendingErasCount", "yieldRateNominatorPerXorPerEra", "own", "total", "nominators"]),
    dict(path="/staking/rewards/live", mode="structure"),
    # --- burns ---
    dict(path="/burns/supply/VAL", mode="full"),
    dict(path="/burns/stats/VAL", mode="structure"),
    dict(path="/burns/series/XOR?days=3", mode="structure"),
    dict(path="/burns/fee-flow", mode="structure"),
    dict(path="/burns/supply-history/VAL?timeframe=7d", mode="structure"),
    dict(path="/burns/holders/VAL?page=1", mode="full", ignore=["balance", "balanceStr", "totalHolders", "totalPages", "name"]),
    # --- polkamarkt ---
    dict(path="/polkamarkt/state", mode="structure"),
    dict(path="/polkamarkt/markets?limit=2", mode="structure"),
    dict(path="/polkamarkt/market/41", mode="full", ignore=["created_at_ts", "recentTrades", "topPositions", "probHistory", "positions", "volume", "yes_shares", "no_shares", "coll_yes", "coll_no"]),
    dict(path=f"/polkamarkt/positions/{ADDR}", mode="structure"),
    dict(path="/polkamarkt/buybacks?limit=2", mode="structure"),
    # --- governance ---
    dict(path="/governance/council", mode="full"),
    dict(path="/governance/technical-committee", mode="full"),
    dict(path="/governance/elections", mode="full"),
    dict(path="/governance/motions", mode="full"),
    dict(path="/governance/democracy", mode="full"),
    dict(path=f"/governance/votes/{VALIDATOR}", mode="full"),
    dict(path="/governance/scheduler/agenda", mode="full"),
    dict(path="/governance/preimages", mode="unordered", key="hash", ignore=["firstSeenBlock", "firstSeenTimestamp"]),
    dict(path=f"/governance/preimage/{PREIMAGE}?len=947", mode="full"),
    dict(path=f"/governance/preimage/{PREIMAGE}/referendums?limit=20", mode="full"),
    dict(path=f"/governance/preimage/{PREIMAGE}/events-fast", mode="full", ignore=["indexer"]),
    dict(path=f"/governance/preimage/{PREIMAGE}/decode-pretty?len=947", mode="full"),
    dict(path=f"/governance/preimage/recover/{PREIMAGE}", mode="full", ignore=["source", "timestamp"]),
    # --- media / proxies ---
    dict(path="/music/list", mode="structure"),
    dict(path="/news/episodes?limit=1", mode="structure"),
    dict(path="/mof/qty/xor", mode="structure", raw=True),
    dict(path="/proxy-image?url=https://example.com/x.png", mode="full", raw=True),
    # --- frontend files the Node serves itself (STATIC_DIR) ---
    dict(path="/", mode="full", raw=True),
    dict(path="/sorav2", mode="full", raw=True),
    dict(path="/minamoto", mode="full", raw=True),
    dict(path="/styles.css", mode="full", raw=True),
    dict(path="/manifest.json", mode="full", raw=True),
    dict(path="/js/common.jsx", mode="full", raw=True),
    dict(path="/js/minamoto/main.jsx", mode="full", raw=True),
    # --- site analytics ---
    dict(path="/analytics/stats", mode="structure"),
    dict(path="/analytics/advanced", mode="structure"),
    # --- second mount of the Minamoto router ---
    dict(path="/api/sorav2/xor-migration/cross-chain/stats", mode="full"),
]


WARNINGS = []


ROUTES = ROUTES + MN_ROUTES

def fetch(base, route, timeout, retries=0):
    url = base + route["path"]
    data = None
    # Cloudflare in front of prod rejects the default urllib agent with 403.
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0 (sorametrics-v33 parity check)"}
    if route.get("method") == "POST":
        data = json.dumps(route.get("body") or {}).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=route.get("method", "GET"))
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            status = r.status
    except urllib.error.HTTPError as e:
        body = e.read()
        status = e.code
    except Exception as e:  # transport
        return None, str(e), time.time() - started
    if status == 503 and retries > 0:
        # v33 answers 503 + Retry-After while a full storage scan runs in the background.
        time.sleep(30)
        return fetch(base, route, timeout, retries - 1)
    if route.get("raw"):
        return status, body, time.time() - started
    try:
        return status, json.loads(body.decode()), time.time() - started
    except Exception:
        return status, body.decode(errors="replace")[:200], time.time() - started


def typ(v):
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, (int, float)):
        return "number"
    if v is None:
        return "null"
    return type(v).__name__


JSON_STRING_FIELDS = {"events_json", "args_json"}


def parse_json_field(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return v
    return v


def diff(a, b, mode, ignore, volatile, path="", out=None, key=None, strict=False):
    out = [] if out is None else out
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            p = f"{path}.{k}"
            if k in ignore or k in GLOBAL_IGNORE:
                continue
            if k not in a:
                out.append(f"{p}: missing in v33")
                continue
            if k not in b:
                out.append(f"{p}: extra in v33")
                continue
            if k in volatile or k in GLOBAL_VOLATILE:
                if typ(a[k]) != typ(b[k]) and not (a[k] is None or b[k] is None):
                    out.append(f"{p}: type {typ(a[k])} vs prod {typ(b[k])}")
                continue
            if k in JSON_STRING_FIELDS:
                diff(parse_json_field(a[k]), parse_json_field(b[k]), mode, ignore, volatile, p, out, key, strict)
                continue
            diff(a[k], b[k], mode, ignore, volatile, p, out, key, strict)
    elif isinstance(a, list) and isinstance(b, list):
        if mode == "structure":
            if a and b:
                diff(a[0], b[0], mode, ignore, volatile, f"{path}[0]", out, key, strict)
            elif bool(a) != bool(b):
                (out if strict else WARNINGS).append(f"{path}: v33 has {len(a)} rows, prod {len(b)} (row structure not comparable)")
            return out
        if key and a and isinstance(a[0], dict) and (key in a[0] or key == "pool" or "+" in key):
            def k_of(x):
                if key == "pool":
                    return f"{x.get('base', {}).get('symbol')}/{x.get('target', {}).get('symbol')}"
                if "+" in key:
                    return "+".join(str(x.get(part)) for part in key.split("+"))
                return x.get(key)
            am = {k_of(x): x for x in a}
            bm = {k_of(x): x for x in b}
            for k in sorted(set(am) | set(bm), key=str):
                if k not in am:
                    out.append(f"{path}[{key}={k}]: missing in v33")
                elif k not in bm:
                    out.append(f"{path}[{key}={k}]: extra in v33")
                else:
                    diff(am[k], bm[k], mode, ignore, volatile, f"{path}[{key}={k}]", out, None, strict)
            return out
        if len(a) != len(b):
            out.append(f"{path}: {len(a)} items vs prod {len(b)}")
        for i, (x, y) in enumerate(zip(a, b)):
            diff(x, y, mode, ignore, volatile, f"{path}[{i}]", out, key, strict)
    else:
        if mode == "structure":
            if typ(a) != typ(b) and a is not None and b is not None:
                out.append(f"{path}: type {typ(a)} vs prod {typ(b)}")
        elif a != b:
            out.append(f"{path}: v33={json.dumps(a)[:80]} prod={json.dumps(b)[:80]}")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prod", default="https://sorametrics.org")
    ap.add_argument("--v33", default="http://127.0.0.1:3311")
    ap.add_argument("--only", default=None, help="substring filter on the route path")
    ap.add_argument("--report", default=None, help="write the full JSON report here")
    ap.add_argument("--save-golden", default=None, help="store prod responses in this dir")
    ap.add_argument("--golden", default=None, help="compare against stored prod responses instead of live prod")
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--max-diffs", type=int, default=6)
    ap.add_argument("--strict", action="store_true", help="empty-vs-populated lists in structure mode fail (full ETL expected)")
    args = ap.parse_args()

    routes = [r for r in ROUTES if not args.only or args.only in r["path"]]
    report = []
    failures = 0
    for r in routes:
        name = r["path"]
        slug = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")[:120]
        if args.golden:
            try:
                with open(os.path.join(args.golden, slug + ".json")) as f:
                    saved = json.load(f)
                ps, pb, pt = saved["status"], saved["body"], 0.0
                if r.get("raw"):
                    pb = bytes.fromhex(pb)
            except FileNotFoundError:
                ps, pb, pt = None, "no golden", 0.0
        else:
            ps, pb, pt = fetch(args.prod, r, args.timeout)
            if args.save_golden and ps is not None:
                os.makedirs(args.save_golden, exist_ok=True)
                with open(os.path.join(args.save_golden, slug + ".json"), "w") as f:
                    json.dump({"status": ps, "body": pb.hex() if r.get("raw") else pb}, f)
        WARNINGS.clear()
        vs, vb, vt = fetch(args.v33, r, args.timeout, retries=10)
        entry = {"path": name, "mode": r["mode"], "prod_status": ps, "v33_status": vs,
                 "prod_s": round(pt, 2), "v33_s": round(vt, 2), "diffs": []}
        if ps is None or vs is None:
            entry["error"] = f"transport: prod={pb if ps is None else ps} v33={vb if vs is None else vs}"
            failures += 1
        elif ps != vs:
            entry["diffs"] = [f"status {vs} vs prod {ps}"]
            failures += 1
        elif r.get("raw"):
            if r["mode"] == "full" and vb != pb:
                entry["diffs"] = [f"bytes differ ({len(vb)} vs {len(pb)})"]
                failures += 1
        else:
            d = diff(vb, pb, r["mode"], set(r.get("ignore", [])), set(r.get("volatile", [])), key=r.get("key"), strict=args.strict)
            entry["diffs"] = d
            entry["warnings"] = list(WARNINGS)
            if d:
                failures += 1
        report.append(entry)
        status = "OK " if not entry["diffs"] and "error" not in entry else "DIFF" if entry["diffs"] else "ERR "
        if status == "OK " and entry.get("warnings"):
            status = "WARN"
        print(f"{status} {name}  [{r['mode']}] prod {entry['prod_s']}s v33 {entry['v33_s']}s")
        for line in entry.get("warnings", [])[: args.max_diffs]:
            print("       (data)", line)
        for line in entry["diffs"][: args.max_diffs]:
            print("      ", line)
        if len(entry["diffs"]) > args.max_diffs:
            print(f"       … {len(entry['diffs']) - args.max_diffs} more")
        if "error" in entry:
            print("      ", entry["error"])
    print(f"\n{len(routes) - failures}/{len(routes)} routes in parity")
    if args.report:
        with open(args.report, "w") as f:
            json.dump(report, f, indent=1)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
