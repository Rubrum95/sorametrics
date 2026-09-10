#!/usr/bin/env python3
"""Rebuild the legacy `mn.*` schema in a mock database from the JSON the
production API serves (`/api/minamoto/*`), so the ETL and the parity
check can run without access to the VPS PostgreSQL.

Usage:
  python3 scripts/minamoto_mock_from_prod.py FIXTURE_DIR NODE_SCHEMA_SQL \
      | docker exec -i sorametrics-v33-postgres psql -U sorametrics -d legacy_mock -v ON_ERROR_STOP=1

FIXTURE_DIR holds the files written by the capture loop (blocks_p*.json,
transactions_p*.json, instructions_p*.json, accounts.json, domains.json,
assets.json, asset_definitions.json, peers.json, network_state.json,
indexer_state.json, cross-chain_claims_per_page_100.json).
"""
import glob
import json
import os
import sys


def q(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, (dict, list)):
        return "'" + json.dumps(v, ensure_ascii=False).replace("'", "''") + "'::jsonb"
    return "'" + str(v).replace("'", "''") + "'"


def hexb(v):
    return "NULL" if v is None else f"decode('{v}', 'hex')"


def items(path_glob):
    out = []
    for f in sorted(glob.glob(path_glob)):
        d = json.load(open(f))
        out.extend(d["items"] if isinstance(d, dict) else d)
    return out


def main(fixtures, schema_sql):
    print(open(schema_sql).read())
    print("TRUNCATE mn.instructions, mn.transactions, mn.blocks, mn.assets, mn.accounts, mn.domains, "
          "mn.asset_definitions, mn.peers, mn.network_state, mn.indexer_state, mn.metrics_snapshots;")

    seen = set()
    for b in items(os.path.join(fixtures, "blocks_p*.json")):
        if b["height"] in seen:
            continue
        seen.add(b["height"])
        print("INSERT INTO mn.blocks (height, hash, prev_hash, transactions_hash, created_at, "
              "transactions_committed, transactions_rejected, indexed_at) VALUES "
              f"({b['height']}, {hexb(b['hash'])}, {hexb(b['prev_hash'])}, {hexb(b['transactions_hash'])}, "
              f"{q(b['created_at'])}, {b['transactions_committed']}, {b['transactions_rejected']}, {q(b['indexed_at'])});")

    acc_ids = set()
    for a in items(os.path.join(fixtures, "accounts.json")):
        acc_ids.add(a["id"])
        print("INSERT INTO mn.accounts (id, network_prefix, has_primary_alias, primary_alias, primary_alias_dataspace, "
              "primary_alias_domain, primary_alias_name, multisig_quorum, multisig_signatories_count, metadata, "
              "first_seen_at, last_seen_at) VALUES "
              f"({q(a['id'])}, {a['network_prefix']}, {q(a['has_primary_alias'])}, {q(a['primary_alias'])}, "
              f"{q(a['primary_alias_dataspace'])}, {q(a['primary_alias_domain'])}, {q(a['primary_alias_name'])}, "
              f"{q(a['multisig_quorum'])}, {q(a['multisig_signatories_count'])}, {q(a['metadata'])}, "
              f"{q(a['first_seen_at'])}, {q(a['last_seen_at'])});")

    seen = set()
    for t in items(os.path.join(fixtures, "transactions_p*.json")):
        if t["hash"] in seen:
            continue
        seen.add(t["hash"])
        print("INSERT INTO mn.transactions (hash, block_height, authority, created_at, executable_kind, status, indexed_at) "
              f"VALUES ({hexb(t['hash'])}, {t['block']}, {q(t['authority'])}, {q(t['created_at'])}, "
              f"{q(t['executable'])}, {q(t['status'])}, {q(t['indexed_at'])});")

    for c in items(os.path.join(fixtures, "cross-chain_claims_per_page_100.json")):
        print("UPDATE mn.transactions SET sora_v2_claim_tx_hash = %s, sora_nexus_claim_recipient = %s, fee_sponsor = %s, "
              "sora_v2_block = %s, sora_v2_signer = %s WHERE hash = %s;" % (
                  q(c["v2_tx_hash"]), q(c["mn_recipient"]), q(c["fee_sponsor"]), q(c["v2_block"]),
                  q(c["v2_signer"]), hexb(c["mn_tx_hash"])))

    seen = set()
    for i in items(os.path.join(fixtures, "instructions_p*.json")):
        key = (i["transaction_hash"], i["instruction_index"])
        if key in seen:
            continue
        seen.add(key)
        print("INSERT INTO mn.instructions (transaction_hash, instruction_index, block_height, authority, kind, payload, "
              "transaction_status, created_at) VALUES "
              f"({hexb(i['transaction_hash'])}, {i['instruction_index']}, {i['block']}, {q(i['authority'])}, "
              f"{q(i['kind'])}, {q(i['payload'])}, {q(i['transaction_status'])}, {q(i['created_at'])});")

    for d in items(os.path.join(fixtures, "domains.json")):
        if d["owned_by"] not in acc_ids:
            acc_ids.add(d["owned_by"])
            print(f"INSERT INTO mn.accounts (id) VALUES ({q(d['owned_by'])}) ON CONFLICT DO NOTHING;")
        print("INSERT INTO mn.domains (id, owned_by, accounts_count, assets_count, nfts_count, metadata, updated_at) VALUES "
              f"({q(d['id'])}, {q(d['owned_by'])}, {d['accounts_count']}, {d['assets_count']}, {d['nfts_count']}, "
              f"{q(d['metadata'])}, {q(d['updated_at'])});")

    for d in items(os.path.join(fixtures, "asset_definitions.json")):
        if d["owned_by"] not in acc_ids:
            acc_ids.add(d["owned_by"])
            print(f"INSERT INTO mn.accounts (id) VALUES ({q(d['owned_by'])}) ON CONFLICT DO NOTHING;")
        print("INSERT INTO mn.asset_definitions (id, alias, name, description, owned_by, mintable, confidential_mode, "
              "balance_scope_policy, total_quantity, metadata, updated_at) VALUES "
              f"({q(d['id'])}, {q(d['alias'])}, {q(d['name'])}, {q(d['description'])}, {q(d['owned_by'])}, "
              f"{q(d['mintable'])}, {q(d['confidential_mode'])}, {q(d['balance_scope_policy'])}, "
              f"{q(d['total_quantity'])}::numeric, {q(d['metadata'])}, {q(d['updated_at'])});")

    for a in items(os.path.join(fixtures, "assets.json")):
        if a["account_id"] not in acc_ids:
            acc_ids.add(a["account_id"])
            print(f"INSERT INTO mn.accounts (id) VALUES ({q(a['account_id'])}) ON CONFLICT DO NOTHING;")
        print("INSERT INTO mn.assets (definition_id, account_id, value, updated_at) VALUES "
              f"({q(a['definition_id'])}, {q(a['account_id'])}, {q(a['value'])}::numeric, {q(a['updated_at'])});")

    for p in items(os.path.join(fixtures, "peers.json")):
        print("INSERT INTO mn.peers (multiaddr, public_key, ip_address, port, first_seen_at, last_seen_at, is_active) VALUES "
              f"({q(p['multiaddr'])}, {q(p['public_key'])}, {q(p['ip_address'])}, {q(p['port'])}, "
              f"{q(p['first_seen_at'])}, {q(p['last_seen_at'])}, {q(p['is_active'])});")

    ns = json.load(open(os.path.join(fixtures, "network_state.json")))["state"]
    print("INSERT INTO mn.network_state (id, peers, domains, accounts, assets, transactions_accepted, transactions_rejected, "
          "block_height, finalized_block, avg_commit_time_ms, avg_block_time_ms, last_block_at, iroha_version, updated_at) VALUES "
          f"(1, {ns['peers']}, {ns['domains']}, {ns['accounts']}, {ns['assets']}, {ns['transactions_accepted']}, "
          f"{ns['transactions_rejected']}, {ns['block_height']}, {ns['finalized_block']}, {ns['avg_commit_time_ms']}, "
          f"{ns['avg_block_time_ms']}, {q(ns['last_block_at'])}, {q(ns['iroha_version'])}, {q(ns['updated_at'])});")

    for s in items(os.path.join(fixtures, "indexer_state.json")):
        print("INSERT INTO mn.indexer_state (name, last_value, last_run_at, last_run_status, error_count, last_error) VALUES "
              f"({q(s['name'])}, {q(s['last_value'])}, {q(s['last_run_at'])}, {q(s['last_run_status'])}, "
              f"{s['error_count']}, {q(s['last_error'])});")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
