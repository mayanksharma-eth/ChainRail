#!/usr/bin/env bash
# Bring up dependencies and run the API + worker from source (not in Docker), so
# you get fast rebuilds and a debugger.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> starting dependencies"
docker compose up -d postgres redis redpanda redpanda-init
echo "==> waiting for postgres"
until docker compose exec -T postgres pg_isready -U chainrail -d chainrail >/dev/null 2>&1; do
    sleep 1
done

export APP_ENV=local
export CHAINRAIL__DATABASE__URL="${CHAINRAIL__DATABASE__URL:-postgres://chainrail:chainrail@127.0.0.1:55432/chainrail}"
export CHAINRAIL__KAFKA__BROKERS="${CHAINRAIL__KAFKA__BROKERS:-127.0.0.1:19092}"
export CHAINRAIL__REDIS__URL="${CHAINRAIL__REDIS__URL:-redis://127.0.0.1:56379}"
export CHAINRAIL__HTTP__BIND="${CHAINRAIL__HTTP__BIND:-0.0.0.0:8088}"

echo "==> building"
cargo build --bin chainrail-server --bin chainrail-worker

echo "==> starting server on $CHAINRAIL__HTTP__BIND"
./target/debug/chainrail-server &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true; kill $WORKER_PID 2>/dev/null || true' EXIT

sleep 3
echo "==> starting worker"
CHAINRAIL__OBSERVABILITY__METRICS_BIND=0.0.0.0:9091 \
CHAINRAIL__DATABASE__RUN_MIGRATIONS_ON_BOOT=false \
    ./target/debug/chainrail-worker &
WORKER_PID=$!

echo
echo "API:      http://127.0.0.1:8088"
echo "health:   curl -s http://127.0.0.1:8088/health | jq"
echo "metrics:  curl -s http://127.0.0.1:9090/metrics | head"
echo "seed:     ./scripts/seed.sh"
echo
wait
