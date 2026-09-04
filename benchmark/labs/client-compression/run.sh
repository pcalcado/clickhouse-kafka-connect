#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/benchmark/labs/client-compression/docker-compose.yml"
ROWS="${ROWS:-100000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-4096}"
CLIENT_VERSION="${CLIENT_VERSION:-V2}"
INSERT_FORMAT="${INSERT_FORMAT:-json}"

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

wait_for_clickhouse() {
  for _ in $(seq 1 60); do
    if compose exec -T clickhouse wget -qO- http://localhost:8123/ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "clickhouse did not become healthy" >&2
  exit 1
}

start_capture() {
  compose exec -T packet-capture sh -lc "rm -f /tmp/bench.pcap /tmp/bench.pid /tmp/bench.log; nohup tcpdump -i any -U -w /tmp/bench.pcap 'tcp dst port 8123' >/tmp/bench.log 2>&1 & echo \$! >/tmp/bench.pid"
  sleep 1
}

stop_capture_sum_bytes() {
  compose exec -T packet-capture sh -lc "kill \$(cat /tmp/bench.pid) >/dev/null 2>&1 || true"
  sleep 1
  compose exec -T packet-capture sh -lc "tcpdump -nn -tt -r /tmp/bench.pcap 2>/dev/null | sed -n 's/.* length \\([0-9][0-9]*\\)$/\\1/p' | awk '{sum += \$1} END {print sum+0}'"
}

run_case() {
  local compression="$1"
  start_capture
  CLICKHOUSE_HOST=localhost \
  CLICKHOUSE_PORT=38123 \
  CLICKHOUSE_USER=default \
  CLICKHOUSE_PASSWORD=password \
  CLICKHOUSE_SSL=false \
  "$ROOT_DIR/gradlew" -p "$ROOT_DIR/benchmark" compressionLab \
    --args="--clientVersion=${CLIENT_VERSION} --insertFormat=${INSERT_FORMAT} --rows=${ROWS} --payloadBytes=${PAYLOAD_BYTES} --clientCompression=${compression}"
  local wire_bytes
  wire_bytes="$(stop_capture_sum_bytes)"
  printf '\nwireBytes(clientCompression=%s)=%s\n\n' "$compression" "$wire_bytes"
}

compose up -d clickhouse packet-capture
trap 'compose down -v' EXIT
wait_for_clickhouse

cd "$ROOT_DIR"
./gradlew publishToMavenLocal >/dev/null

run_case false
run_case true
