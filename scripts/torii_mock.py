#!/usr/bin/env python3
"""Local stand-in for the Minamoto Torii while minamoto.sora.org is down.

Serves the `optimizations@cfa5e8ce77` response shapes (cursor pagination,
`/status` with `build.git_commit_sha`, `/v1/assets/definitions` offset
pages, `/v1/telemetry/peers-info`) from the production fixtures captured
with the `/api/minamoto/*` routes, so `sorametrics-ingest --source=iroha`
can be exercised end to end.

Usage:
  python3 scripts/torii_mock.py FIXTURE_DIR [PORT]
  MINAMOTO_TORII=http://127.0.0.1:8085 sorametrics-ingest --source iroha
"""
import base64
import glob
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

FIX = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8085


def items(pattern):
    out = []
    for f in sorted(glob.glob(os.path.join(FIX, pattern))):
        d = json.load(open(f))
        out.extend(d["items"] if isinstance(d, dict) else d)
    return out


BLOCKS = sorted({b["height"]: b for b in items("blocks_p*.json")}.values(), key=lambda b: -b["height"])
TXS = sorted({t["hash"]: t for t in items("transactions_p*.json")}.values(), key=lambda t: (-t["block"], t["hash"]))
ISIS = sorted({(i["transaction_hash"], i["instruction_index"]): i for i in items("instructions_p*.json")}.values(),
              key=lambda i: (-i["block"], i["transaction_hash"], i["instruction_index"]))
ACCOUNTS = items("accounts.json")
DOMAINS = items("domains.json")
ASSETS = items("assets.json")
DEFS = items("asset_definitions.json")
CLAIMS = {c["mn_tx_hash"]: c for c in items("cross-chain_claims_per_page_100.json")}
NS = json.load(open(os.path.join(FIX, "network_state.json")))["state"]
TOP = BLOCKS[0]["height"] if BLOCKS else 0


def block_dto(b):
    return {"hash": b["hash"], "height": b["height"], "created_at": b["created_at"],
            "prev_block_hash": b["prev_hash"], "transactions_hash": b["transactions_hash"],
            "transactions_rejected": b["transactions_rejected"], "transactions_total": b["transactions_committed"]}


def tx_dto(t):
    return {"authority": t["authority"], "hash": t["hash"], "block": t["block"], "created_at": t["created_at"],
            "executable": t["executable"], "status": t["status"]}


def isi_dto(i):
    return {"authority": i["authority"], "created_at": i["created_at"], "kind": i["kind"],
            "box": {"encoded": "00", "framed_sha256": "00",
                    "json": {"kind": i["kind"], "payload": i["payload"], "wire_id": "iroha." + i["kind"].lower(), "encoded": "00"}},
            "transaction_hash": i["transaction_hash"], "transaction_status": i["transaction_status"],
            "block": i["block"], "index": i["instruction_index"]}


def account_dto(a):
    return {"id": a["id"], "network_prefix": a["network_prefix"], "metadata": a["metadata"],
            "owned_domains": 0, "owned_assets": 0, "owned_nfts": 0}


def domain_dto(d):
    return {"id": d["id"], "logo": None, "metadata": d["metadata"], "owned_by": d["owned_by"],
            "accounts": d["accounts_count"], "assets": d["assets_count"], "nfts": d["nfts_count"]}


def asset_dto(a):
    return {"id": f"{a['definition_id']}#{a['account_id']}", "definition_id": a["definition_id"],
            "account_id": a["account_id"], "value": a["value"]}


def def_dto(d):
    return {"id": d["id"], "name": d["name"], "description": d["description"], "alias": d["alias"],
            "spec": {"scale": 6}, "mintable": d["mintable"], "logo": None, "metadata": d["metadata"],
            "balance_scope_policy": d["balance_scope_policy"],
            "confidential_policy": {"mode": d["confidential_mode"]} if d["confidential_mode"] else None,
            "total_quantity": d["total_quantity"], "owned_by": d["owned_by"]}


def history_page(rows, dto, q, snapshot):
    limit = int(q.get("limit", ["25"])[0])
    if "page" in q or "per_page" in q:
        return 400, {"error": "unknown query fields: page/per_page"}
    start = int(base64.b64decode(q["cursor"][0]).decode()) if q.get("cursor") else 0
    chunk = rows[start:start + limit]
    nxt = start + limit
    has_more = nxt < len(rows)
    meta = {"limit": limit, "next_cursor": base64.b64encode(str(nxt).encode()).decode() if has_more else None,
            "has_more": has_more}
    if snapshot:
        meta = {"limit": limit, "snapshot_height": TOP, "snapshot_hash": BLOCKS[0]["hash"] if BLOCKS else None,
                **{k: meta[k] for k in ("next_cursor", "has_more")}}
    return 200, {"pagination": meta, "items": [dto(r) for r in chunk]}


class H(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def send(self, code, body, ctype="application/json"):
        data = body.encode() if isinstance(body, str) else json.dumps(body, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        p = u.path
        if p == "/health":
            return self.send(200, "Healthy", "text/plain")
        if p == "/status":
            return self.send(200, {
                "build": {"version": NS["iroha_version"], "git_commit_sha": "cfa5e8ce77mock", "dpn_validator_release_commit": "", "cargo_features": "telemetry", "target_triple": "mock"},
                "observed_at_ms": 0, "peers": NS["peers"], "blocks": TOP, "blocks_non_empty": TOP, "commit_time_ms": NS["avg_commit_time_ms"],
                "txs_approved": int(NS["transactions_accepted"]), "txs_rejected": int(NS["transactions_rejected"]),
                "uptime": {"secs": 1, "nanos": 0}, "view_changes": 0, "queue_size": 0, "queue_queued": 0, "queue_inflight": 0,
                "last_block_committed_at_ms": 0, "last_non_empty_block_committed_at_ms": 0, "time_since_last_block_ms": 0,
                "time_since_last_non_empty_block_ms": 0, "sumeragi": None, "nexus": None})
        if p == "/metrics":
            return self.send(200, f"# TYPE blocks gauge\nblocks {TOP}\ntxs_total{{status=\"approved\"}} {NS['transactions_accepted']}\n", "text/plain")
        if p == "/v1/telemetry/peers-info":
            pk = [a["multiaddr"] for a in items("peers.json")]
            return self.send(200, [{"url": "https://mock", "connected": True, "telemetry_unsupported": False,
                                    "config": {"public_key": None}, "location": None, "connected_peers": pk}])
        if p == "/v1/explorer/blocks":
            return self.send(*history_page(BLOCKS, block_dto, q, True))
        if p == "/v1/explorer/transactions":
            return self.send(*history_page(TXS, tx_dto, q, True))
        if p == "/v1/explorer/instructions":
            return self.send(*history_page(ISIS, isi_dto, q, True))
        if p == "/v1/explorer/accounts":
            return self.send(*history_page(ACCOUNTS, account_dto, q, False))
        if p == "/v1/explorer/domains":
            return self.send(*history_page(DOMAINS, domain_dto, q, False))
        if p == "/v1/explorer/assets":
            return self.send(*history_page(ASSETS, asset_dto, q, False))
        if p == "/v1/assets/definitions":
            limit = int(q.get("limit", ["25"])[0]); offset = int(q.get("offset", ["0"])[0])
            chunk = DEFS[offset:offset + limit]
            return self.send(200, {"items": [def_dto(d) for d in chunk], "total": len(DEFS),
                                   "has_more": offset + limit < len(DEFS), "count_mode": "exact"})
        if p.startswith("/v1/explorer/transactions/"):
            h = p.rsplit("/", 1)[1]
            t = next((t for t in TXS if t["hash"] == h), None)
            if not t:
                return self.send(404, {"error": "not found"})
            md = {}
            if h in CLAIMS:
                c = CLAIMS[h]
                md = {"sora_v2_claim_tx_hash": c["v2_tx_hash"], "sora_nexus_claim_recipient": c["mn_recipient"], "fee_sponsor": c["fee_sponsor"]}
            return self.send(200, {**tx_dto(t), "rejection_reason": None, "executable_payload": {}, "metadata": md,
                                   "nonce": None, "signature": "", "time_to_live": None})
        return self.send(404, {"error": f"no mock for {p}"})


if __name__ == "__main__":
    print(f"torii mock on 127.0.0.1:{PORT} — {len(BLOCKS)} blocks, {len(TXS)} txs, {len(ISIS)} instructions", flush=True)
    HTTPServer(("127.0.0.1", PORT), H).serve_forever()
