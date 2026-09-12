#!/bin/bash
# Health check for the three v33 processes (the Node's
# /root/health-check-sorametrics.sh, extended to the ingests).
# Cron: */15 * * * * /root/sorametrics-v33/deploy/health-check-v33.sh >> /var/log/health-check-v33.log 2>&1

API_URL="${API_URL:-http://127.0.0.1:3311}"
APPS="sorametrics-v33-api sorametrics-v33-ingest-substrate sorametrics-v33-ingest-iroha"

echo "--- v33 health check $(date) ---"

status_of() {
    pm2 jlist 2>/dev/null | python3 -c "
import sys, json
try:
    apps = json.load(sys.stdin)
    for a in apps:
        if a.get('name') == '$1':
            print(a.get('pm2_env', {}).get('status', 'unknown'))
            break
    else:
        print('not_found')
except Exception:
    print('error')
" 2>/dev/null
}

rc=0
for app in $APPS; do
    st=$(status_of "$app")
    if [ "$st" != "online" ]; then
        echo "ALERTA: $app status=$st. Reiniciando..."
        pm2 restart "$app"
        rc=1
    fi
done

# The API must answer /health.
if ! timeout 10 curl -sf "$API_URL/health" > /dev/null; then
    echo "ALERTA: la API no responde /health. Reiniciando..."
    pm2 restart sorametrics-v33-api
    rc=1
fi
# Stale tables are reported, not restarted: the ingest already exits for
# a PM2 restart when its cursor stalls (SUBSTRATE_LAG_ALERT_BLOCKS), and a
# quiet chain must not trigger a restart loop.
stale=$(timeout 15 curl -sf "$API_URL/health/freshness" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    bad = [t.get('table') for t in d.get('tables', []) if str(t.get('status') or '').lower() == 'stale']
    print(','.join(x for x in bad if x))
except Exception:
    print('')
" 2>/dev/null)
[ -n "$stale" ] && echo "AVISO: tablas stale en /health/freshness: $stale"

[ $rc -eq 0 ] && echo "OK - api + ingests online, freshness sin stale"
exit $rc
