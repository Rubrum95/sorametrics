#!/usr/bin/env bash
# Copy the Node's tables the ETL reads from the legacy Postgres (squid) into a
# throwaway `legacy_copy` database next to v33, streaming through this machine:
# no server-to-server credential, no temp file on the (full) legacy disk, and
# every chunk is resumable. Run from a host that can reach both sides.
#
#   SRC_SQL="ssh sorametrics docker exec -i sora_subsquid_db psql -U postgres -d squid" \
#   DST_SQL="ssh sora-node docker exec -i sorametrics-v33-postgres psql -U sorametrics" \
#   deploy/transfer_legacy.sh [--only table,table] [--chunk 100000]
#
# SRC_SQL / DST_SQL: commands that run psql on each side reading SQL on stdin.
# DST_SQL must connect as a role allowed to CREATE DATABASE; the copy lands in
# `legacy_copy` (created if missing). Progress markers in $STATE_DIR let a
# broken run continue where it stopped.
set -euo pipefail

: "${SRC_SQL:?SRC_SQL required}"
: "${DST_SQL:?DST_SQL required}"
DST_DB="${DST_DB:-legacy_copy}"
CHUNK="${CHUNK:-100000}"
STATE_DIR="${STATE_DIR:-$HOME/.sorametrics-transfer}"
ONLY=""
while [ $# -gt 0 ]; do
  case "$1" in
    --only) ONLY="$2"; shift 2 ;;
    --chunk) CHUNK="$2"; shift 2 ;;
    *) echo "unknown arg $1" >&2; exit 2 ;;
  esac
done
mkdir -p "$STATE_DIR"

# Whole tables streamed in one COPY (small: minutes at most). `rel|where`
# copies only the rows the ETL reads (asset_snapshot: the DAY points).
WHOLE=(
  sm.asset_registry sm.mv_swaps sm.mv_transfers sm.mv_bridges sm.mv_fees sm.mv_extrinsics
  sm.mv_order_book_events sm.mv_liquidity_events sm.price_history sm.fee_burns_live
  sm.news_episodes sm.polkamarkt_markets sm.polkamarkt_trades sm.polkamarkt_claims
  sm.polkamarkt_buybacks sm.polkamarkt_burns sm.supply_snapshots sm.supply_history
  sm.val_staking_rewards sm.site_daily sm.site_events sm.identity_cache
  "public.asset_snapshot|type = 'DAY'"
  mn.blocks mn.transactions mn.instructions mn.accounts mn.domains mn.assets
  mn.asset_definitions mn.peers mn.network_state mn.indexer_state mn.metrics_snapshots
)
# Big tables streamed by block ranges of $CHUNK blocks (gzip -1 ≈ 11×).
# extrinsic_events is dense only from block ~10 M; history_element CALL rows
# (the args) span the whole chain.
CHUNKED_EVENTS="sm.extrinsic_events"       # block_height 10000000..25300000
CHUNKED_CALLS="public.history_element"     # block_height 0..26500000, type = 'CALL'

src() { eval "$SRC_SQL" -At -v ON_ERROR_STOP=1; }
dst() { eval "$DST_SQL" -d "$DST_DB" -v ON_ERROR_STOP=1 -q; }
dst_admin() { eval "$DST_SQL" -d postgres -v ON_ERROR_STOP=1 -q; }

wanted() { [ -z "$ONLY" ] || [[ ",$ONLY," == *",$1,"* ]]; }

# DDL of a table or materialized view as a plain table (types via format_type).
ddl_of() {
  local rel="$1"; local schema="${rel%%.*}"; local name="${rel#*.}"
  printf "SELECT 'CREATE TABLE IF NOT EXISTS %s (' || string_agg(quote_ident(a.attname) || ' ' || format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum) || ');' FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '%s' AND c.relname = '%s' AND a.attnum > 0 AND NOT a.attisdropped;\n" "$rel" "$schema" "$name" | src
}

# Pre-flight: the data crosses this machine twice. Through a VPN the SSH legs
# measured ~15 KB/s (11 GB = days) and the dead client left a COPY holding a
# 40 min transaction on production. Refuse to start on a slow path.
if [ "${SKIP_SPEED_CHECK:-0}" != "1" ]; then
  src_host=$(printf '%s' "$SRC_SQL" | awk '{print $2}'); dst_host=$(printf '%s' "$DST_SQL" | awk '{print $2}')
  if [ "$(printf '%s' "$SRC_SQL" | awk '{print $1}')" = "ssh" ]; then
    t0=$(date +%s); ssh "$src_host" 'head -c 4000000 /dev/urandom' | ssh "$dst_host" 'cat > /dev/null'; t1=$(date +%s)
    secs=$(( t1 - t0 )); [ "$secs" -lt 1 ] && secs=1
    kbps=$(( 4000 / secs ))
    echo "== path speed through this host: ~${kbps} KB/s"
    if [ "$kbps" -lt 500 ]; then
      echo "too slow (< 500 KB/s): disconnect the VPN or exclude the two servers from it, then retry (SKIP_SPEED_CHECK=1 overrides)" >&2
      exit 3
    fi
  fi
fi

echo "== destination database $DST_DB"
printf "SELECT 1 FROM pg_database WHERE datname = '%s';\n" "$DST_DB" | eval "$DST_SQL" -d postgres -At | grep -q 1 \
  || printf "CREATE DATABASE %s;\n" "$DST_DB" | dst_admin
printf "CREATE SCHEMA IF NOT EXISTS sm; CREATE SCHEMA IF NOT EXISTS mn;\n" | dst

for entry in "${WHOLE[@]}"; do
  rel="${entry%%|*}"; where=""
  [ "$entry" != "$rel" ] && where=" WHERE ${entry#*|}"
  wanted "$rel" || continue
  marker="$STATE_DIR/$rel.done"
  if [ -f "$marker" ]; then echo "-- $rel: done earlier, skipped"; continue; fi
  echo "== $rel$where"
  ddl_of "$rel" | dst
  printf "TRUNCATE %s;\n" "$rel" | dst
  printf "COPY (SELECT * FROM %s%s) TO STDOUT;\n" "$rel" "$where" | src | gzip -1 \
    | { printf "COPY %s FROM STDIN;\n" "$rel"; gunzip -c; } | dst
  n=$(printf "SELECT count(*) FROM %s;\n" "$rel" | eval "$DST_SQL" -d "$DST_DB" -At)
  echo "   rows in copy: $n"
  touch "$marker"
done

copy_range() {
  local rel="$1" from="$2" to="$3" where="$4"
  local marker="$STATE_DIR/$rel.$from-$to.done"
  if [ -f "$marker" ]; then return; fi
  printf "DELETE FROM %s WHERE block_height BETWEEN %s AND %s;\n" "$rel" "$from" "$to" | dst
  printf "COPY (SELECT * FROM %s WHERE block_height BETWEEN %s AND %s%s) TO STDOUT;\n" "$rel" "$from" "$to" "$where" | src | gzip -1 \
    | { printf "COPY %s FROM STDIN;\n" "$rel"; gunzip -c; } | dst
  touch "$marker"
  echo "   $rel $from-$to ok"
}

if wanted "$CHUNKED_EVENTS"; then
  echo "== $CHUNKED_EVENTS (chunks of $CHUNK blocks)"
  ddl_of "$CHUNKED_EVENTS" | dst
  lo=$(printf "SELECT min(block_height) FROM %s;\n" "$CHUNKED_EVENTS" | src)
  hi=$(printf "SELECT max(block_height) FROM %s;\n" "$CHUNKED_EVENTS" | src)
  for ((from=lo; from<=hi; from+=CHUNK)); do copy_range "$CHUNKED_EVENTS" "$from" $((from+CHUNK-1)) ""; done
  printf "CREATE INDEX IF NOT EXISTS ee_pkey_like ON %s (block_height, extrinsic_index, event_index);\n" "$CHUNKED_EVENTS" | dst
fi

if wanted "$CHUNKED_CALLS"; then
  echo "== $CHUNKED_CALLS CALL rows (chunks of $((CHUNK*10)) blocks)"
  printf "CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, block_height integer NOT NULL, type varchar(5) NOT NULL, data jsonb);\n" "$CHUNKED_CALLS" | dst
  lo=$(printf "SELECT min(block_height) FROM %s WHERE type = 'CALL';\n" "$CHUNKED_CALLS" | src)
  hi=$(printf "SELECT max(block_height) FROM %s WHERE type = 'CALL';\n" "$CHUNKED_CALLS" | src)
  big=$((CHUNK*10))
  for ((from=lo; from<=hi; from+=big)); do
    marker="$STATE_DIR/$CHUNKED_CALLS.$from-$((from+big-1)).done"
    [ -f "$marker" ] && continue
    printf "DELETE FROM %s WHERE block_height BETWEEN %s AND %s;\n" "$CHUNKED_CALLS" "$from" $((from+big-1)) | dst
    printf "COPY (SELECT id, block_height, type, data FROM %s WHERE type = 'CALL' AND block_height BETWEEN %s AND %s) TO STDOUT;\n" "$CHUNKED_CALLS" "$from" $((from+big-1)) | src | gzip -1 \
      | { printf "COPY %s (id, block_height, type, data) FROM STDIN;\n" "$CHUNKED_CALLS"; gunzip -c; } | dst
    touch "$marker"; echo "   $CHUNKED_CALLS $from-$((from+big-1)) ok"
  done
fi

# The copy carries no indexes; the ETL reads by keyset cursor and the
# extrinsics detail/reconciliation look up events by (block, index).
echo "== indexes (deploy/legacy_indexes.sql)"
dst < "$(dirname "$0")/legacy_indexes.sql"

echo "== done. Next: LEGACY_DATABASE_URL=<$DST_DB> sorametrics-ops migrate-legacy ..."
