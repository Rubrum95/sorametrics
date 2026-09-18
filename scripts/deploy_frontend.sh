#!/usr/bin/env bash
# Deploy frontend/ to the host's STATIC_DIR.
#
# Cloudflare rewrites Cache-Control on .css to max-age=14400, so browsers keep
# an old stylesheet for 4 h after a deploy while HTML and JSX (not cached)
# are already new. The stylesheet URL is therefore stamped with its content
# hash: `styles.css?v=dev` in the sources becomes `styles.css?v=<md5>` here.
#
#   scripts/deploy_frontend.sh [ssh-host] [remote-dir]
set -euo pipefail

HOST="${1:-sora-node}"
DEST="${2:-/root/sorametrics-static}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

rsync -a --exclude '._*' --exclude '.DS_Store' "$ROOT/frontend/" "$STAGE/"
if command -v md5sum >/dev/null 2>&1; then
  V="$(md5sum "$STAGE/styles.css" | cut -c1-10)"
else
  V="$(md5 -q "$STAGE/styles.css" | cut -c1-10)"
fi
for f in index.html minamoto.html; do
  grep -q 'styles.css?v=dev' "$STAGE/$f" || { echo "missing styles.css?v=dev in $f" >&2; exit 1; }
  sed -i.bak "s/styles\.css?v=dev/styles.css?v=$V/" "$STAGE/$f" && rm -f "$STAGE/$f.bak"
done
echo "stylesheet version: $V"
rsync -az --delete "$STAGE/" "$HOST:$DEST/"
echo "deployed to $HOST:$DEST"
