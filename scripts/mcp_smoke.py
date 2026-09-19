"""Smoke test of the MCP endpoint: protocol errors, both eras, every tool.

Usage: python3 scripts/mcp_smoke.py [base_url] [wallet]
"""
import json, sys, urllib.request, urllib.error
BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:3399"
V = "2026-07-28"
def post(body, headers=None, raw=None):
    h = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream", "User-Agent": "Mozilla/5.0 mcp-test"}
    h.update(headers or {})
    data = raw if raw is not None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + "/mcp", data=data, headers=h, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            t = r.read().decode()
            return r.status, (json.loads(t) if t else None)
    except urllib.error.HTTPError as e:
        t = e.read().decode()
        try: return e.code, json.loads(t)
        except Exception: return e.code, t
def modern(method, params=None, name=None, hdr=None):
    p = dict(params or {})
    p["_meta"] = {"io.modelcontextprotocol/protocolVersion": V,
                  "io.modelcontextprotocol/clientInfo": {"name": "t", "version": "0"},
                  "io.modelcontextprotocol/clientCapabilities": {}}
    h = {"MCP-Protocol-Version": V, "Mcp-Method": method}
    if name: h["Mcp-Name"] = name
    h.update(hdr or {})
    return post({"jsonrpc": "2.0", "id": 1, "method": method, "params": p}, h)
ok = True
def check(label, cond, extra=""):
    global ok
    ok &= bool(cond)
    print(("PASS " if cond else "FAIL ") + label, extra)

s, r = modern("server/discover")
check("discover", s == 200 and r["result"]["supportedVersions"] == [V] and r["result"]["resultType"] == "complete", s)
s, r = modern("tools/list")
tools = r["result"]["tools"]
check("tools/list", s == 200 and len(tools) == 22 and r["result"]["cacheScope"] == "public", len(tools))
s, r = modern("tools/list", hdr={"MCP-Protocol-Version": "2025-11-25"})
check("version header mismatch -> 400/-32020", s == 400 and r["error"]["code"] == -32020, s)
s, r = modern("tools/list", hdr={"Mcp-Method": "tools/call"})
check("method header mismatch -> 400/-32020", s == 400 and r["error"]["code"] == -32020, s)
s, r = post({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {"_meta": {"io.modelcontextprotocol/protocolVersion": "2030-01-01"}}},
            {"MCP-Protocol-Version": "2030-01-01", "Mcp-Method": "tools/list"})
check("unsupported version -> 400/-32022", s == 400 and r["error"]["code"] == -32022 and V in r["error"]["data"]["supported"], s)
s, r = modern("sampling/createMessage")
check("unknown method -> 404/-32601", s == 404 and r["error"]["code"] == -32601, s)
s, r = modern("tools/call", {"name": "nope", "arguments": {}}, name="nope")
check("unknown tool -> -32602", r["error"]["code"] == -32602, s)
s, r = modern("tools/call", {"name": "list_tokens", "arguments": {}}, name="other")
check("Mcp-Name mismatch -> -32020", s == 400 and r["error"]["code"] == -32020, s)
s, r = post(None, raw=b"{nope")
check("parse error -> 400/-32700", s == 400 and r["error"]["code"] == -32700, s)
s, r = post({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, {"Origin": "https://evil.example"})
check("foreign Origin -> 403", s == 403, s)
for m in ("GET", "DELETE"):
    try:
        urllib.request.urlopen(urllib.request.Request(BASE + "/mcp", method=m, headers={"User-Agent": "Mozilla/5.0"}), timeout=20); c = 200
    except urllib.error.HTTPError as e: c = e.code
    check(m + " -> 405", c == 405, c)
# legacy
s, r = post({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}})
check("legacy initialize", s == 200 and r["result"]["protocolVersion"] == "2025-06-18", s)
s, r = post({"jsonrpc": "2.0", "method": "notifications/initialized"})
check("notification -> 202", s == 202, s)
s, r = post({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
check("legacy tools/list", s == 200 and len(r["result"]["tools"]) == 22 and "resultType" not in r["result"], s)

WALLET = sys.argv[2] if len(sys.argv) > 2 else "cnRus2m2Rn776v88H5RUtyiaXtr3daN6ePn6yenLKepx1SqYo"
XOR = "0x0200000000000000000000000000000000000000000000000000000000000000"
calls = [
 ("network_status", {}), ("list_tokens", {"search": "xor", "limit": 3}), ("wallet_balances", {"address": WALLET}),
 ("wallet_history", {"address": WALLET, "kind": "transfers", "limit": 2}), ("recent_activity", {"kind": "swaps", "limit": 2}),
 ("recent_activity", {"kind": "bridges", "limit": 2, "token": "VAL"}), ("list_pools", {"limit": 2}), ("burn_stats", {"symbol": "XOR"}),
 ("get_block", {"number": 27683422}), ("get_extrinsic", {"block": 27683422, "index": 1}), ("search", {"q": "PSWAP"}),
 ("governance", {"section": "council"}), ("staking_validators", {}), ("prediction_markets", {"limit": 2}),
 ("resolve_identities", {"addresses": [WALLET]}), ("top_holders", {"asset_id": XOR}),
 ("price_history", {"asset_ids": [XOR], "labels": ["XOR"], "window": "7d"}), ("wallet_staking", {"address": WALLET}),
 ("wallet_liquidity", {"address": WALLET}), ("wallet_realizable_value", {"address": WALLET, "pct": 10}), ("prediction_market", {"id": 0}), ("network_overview", {}), ("data_freshness", {}),
]
for name, args in calls:
    s, r = modern("tools/call", {"name": name, "arguments": args}, name=name)
    res = r.get("result", {}) if isinstance(r, dict) else {}
    txt = (res.get("content") or [{}])[0].get("text", "")
    good = s == 200 and res.get("isError") is False and "structuredContent" in res
    check(f"call {name}", good, f"{s} bytes={len(txt)} " + ("" if good else txt[:200] + str(r)[:200]))
    if good and "logo" in txt: check(f"  {name} has no logo blobs", '"logo"' not in txt)
s, r = modern("server/discover")
check("discover advertises prompts, resources and the UI extension", all(k in r["result"]["capabilities"] for k in ("prompts", "resources")) and "io.modelcontextprotocol/ui" in r["result"]["capabilities"]["extensions"])
s, r = modern("prompts/list")
check("prompts/list", s == 200 and len(r["result"]["prompts"]) == 4 and r["result"]["cacheScope"] == "public", s)
s, r = modern("prompts/get", {"name": "wallet_report", "arguments": {"address": WALLET}}, name="wallet_report")
check("prompts/get", s == 200 and WALLET in r["result"]["messages"][0]["content"]["text"], s)
s, r = modern("prompts/get", {"name": "wallet_report", "arguments": {"address": "x"}}, name="wallet_report")
check("prompts/get bad argument -> -32602", s == 400 and r["error"]["code"] == -32602, s)
s, r = modern("resources/list")
uris = [x["uri"] for x in r["result"]["resources"]]
check("resources/list", s == 200 and "ui://sorametrics/price-chart" in uris, uris)
s, r = modern("resources/read", {"uri": "ui://sorametrics/price-chart"}, name="ui://sorametrics/price-chart")
c = r["result"]["contents"][0]
check("resources/read chart", c["mimeType"] == "text/html;profile=mcp-app" and "ui/initialize" in c["text"], s)
s, r = modern("resources/read", {"uri": "sorametrics://nope"}, name="sorametrics://nope")
check("unknown resource -> -32602", s == 400 and r["error"]["code"] == -32602, s)
pt = [t for t in tools if t["name"] == "price_history"][0]
check("price_history links the chart", pt["_meta"]["ui"]["resourceUri"] == "ui://sorametrics/price-chart")
card = json.load(urllib.request.urlopen(urllib.request.Request(BASE + "/.well-known/mcp/server-card.json", headers={"User-Agent": "Mozilla/5.0"}), timeout=30))
check("server card", card["transport"]["endpoint"] == "/mcp" and len(card["tools"]) == 22)
s, r = modern("tools/call", {"name": "wallet_balances", "arguments": {"address": "bad"}}, name="wallet_balances")
check("bad argument -> isError result", s == 200 and r["result"]["isError"] is True, s)
print("ALL OK" if ok else "SOME FAILED")
