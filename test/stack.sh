#!/bin/sh
# test/stack.sh up|down|logs [postgres]   the test stack by hand
cd "$(dirname "$0")/.."
action=$1; shift
files="-f docker-compose.yml -f test/docker-compose.yml"
[ "${1:-}" = postgres ] && files="$files -f test/docker-compose.postgres.yml"
case "$action" in
  up) docker compose --env-file test/test.env $files -p dedupe-test up -d ;;
  down) docker compose --env-file test/test.env $files -p dedupe-test down -v ;;
  logs) docker compose --env-file test/test.env $files -p dedupe-test logs --tail "${TAIL:-40}" n8n ;;
esac
