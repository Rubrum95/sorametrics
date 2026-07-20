#!/bin/bash
# Watchdog: alerta si el subsquid processor deja de avanzar mientras la cadena sí avanza.
# No-invasivo: solo lee (psql/RPC) y escribe su propio log+estado. Cron cada 10 min.
set -euo pipefail
STATE=/root/sorametrics/.squid_watchdog_state
LOG=/root/sorametrics/squid_watchdog.log
STALL_SECS=1800   # 30 min sin avanzar = sospechoso
NOW=$(date +%s)

H=$(docker exec sora_subsquid_db psql -U postgres -d squid -At -c "SELECT height FROM squid_processor.status;" 2>/dev/null | tr -dc '0-9')
[ -z "${H:-}" ] && { echo "$(date -Is) WARN squid status no consultable" >> "$LOG"; exit 0; }

HEAD=$(curl -s --max-time 6 -H 'Content-Type: application/json' \
  -d '{"id":1,"jsonrpc":"2.0","method":"chain_getHeader","params":[]}' \
  http://127.0.0.1:9944 2>/dev/null | grep -oE '"number":"0x[0-9a-fA-F]+"' | grep -oE '0x[0-9a-fA-F]+' | head -1)
HEAD_DEC=$([ -n "${HEAD:-}" ] && printf '%d' "$HEAD" || echo 0)

LAST_H=0; LAST_T=$NOW
[ -f "$STATE" ] && { LAST_H=$(cut -d' ' -f1 "$STATE"); LAST_T=$(cut -d' ' -f2 "$STATE"); }

if [ "$H" -gt "$LAST_H" ]; then
  echo "$H $NOW" > "$STATE"   # avanzó: resetea reloj
else
  STALLED=$(( NOW - LAST_T ))
  GAP=$(( HEAD_DEC - H ))
  if [ "$STALLED" -ge "$STALL_SECS" ] && [ "$HEAD_DEC" -gt 0 ] && [ "$GAP" -gt 50 ]; then
    echo "$(date -Is) ALERT subsquid ESTANCADO height=$H sin avanzar ${STALLED}s | chain_head=$HEAD_DEC gap=$GAP bloques" >> "$LOG"
  fi
fi
