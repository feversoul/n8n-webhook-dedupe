#!/bin/sh
# Entrypoint of the n8n container in docker-compose.yml.
# First start only: create the credentials from the environment, import and
# publish the workflows, then call the two setup webhooks once n8n is up so
# the tables exist. Later starts leave your edits alone.
set -eu

MARK=/home/node/.n8n/.webhook-dedupe-imported

if [ -f "$MARK" ]; then
  exec n8n start
fi

STAGE=$(mktemp -d)
node /bootstrap/prepare.mjs /bootstrap/workflows "$STAGE"
n8n import:credentials --input="$STAGE/credentials.json"
n8n import:workflow --separate --input="$STAGE/workflows"
for id in whDedupeSetupDt1 whDedupeSetupPg1 whDedupeRecvPg01 whDedupeRecvDt01 whDedupeLab00001; do
  n8n publish:workflow --id="$id"
done
rm -rf "$STAGE"

n8n start &
N8N=$!
trap 'kill -TERM "$N8N" 2>/dev/null' TERM INT

# Data tables can only be created by a running n8n, so both setups go through
# their webhook.
setup() {
  wget -q -O /dev/null --header="X-Webhook-Key: $WEBHOOK_KEY" --header="Content-Type: application/json" \
    --post-data='{}' "http://127.0.0.1:5678/webhook/dedupe/setup/$1" 2>/dev/null
}
tries=0
until setup data-tables && setup postgres; do
  tries=$((tries + 1))
  if [ "$tries" -gt 90 ] || ! kill -0 "$N8N" 2>/dev/null; then
    echo "webhook-dedupe: could not create the tables" >&2
    kill -TERM "$N8N" 2>/dev/null || true
    exit 1
  fi
  sleep 2
done
touch "$MARK"
echo "webhook-dedupe: workflows published and tables created"

wait "$N8N"
