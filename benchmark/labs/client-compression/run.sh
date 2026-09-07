#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
COMPOSE_SOURCE_FILE="$ROOT_DIR/benchmark/labs/client-compression/docker-compose.yml"
COMPOSE_FILE="${TMPDIR:-/tmp}/client-compression-docker-compose.$$.yml"
COMPOSE_PROJECT_NAME="client-compression-lab"
LOCAL_M2="${TMPDIR:-/tmp}/client-compression-m2.$$"
ROWS="${ROWS:-100000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-4096}"
PAYLOAD_MODE="${PAYLOAD_MODE:-repeated}"
PAYLOAD_SEED="${PAYLOAD_SEED:-528}"
CLIENT_VERSION="${CLIENT_VERSION:-V2}"
INSERT_FORMAT="${INSERT_FORMAT:-json}"
REPEATS="${REPEATS:-3}"

for arg in "$@"; do
  case "$arg" in
    --rows=*) ROWS="${arg#*=}" ;;
    --payloadBytes=*) PAYLOAD_BYTES="${arg#*=}" ;;
    --payloadMode=*) PAYLOAD_MODE="${arg#*=}" ;;
    --payloadSeed=*) PAYLOAD_SEED="${arg#*=}" ;;
    --clientVersion=*) CLIENT_VERSION="${arg#*=}" ;;
    --insertFormat=*) INSERT_FORMAT="${arg#*=}" ;;
    --repeats=*) REPEATS="${arg#*=}" ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "$COMPOSE_SOURCE_FILE" ]]; then
  echo "missing compose file: $COMPOSE_SOURCE_FILE" >&2
  exit 1
fi

cp "$COMPOSE_SOURCE_FILE" "$COMPOSE_FILE"

compose() {
  docker compose -p "$COMPOSE_PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
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
  local dropped
  dropped="$(compose exec -T packet-capture sh -lc "sed -n 's/^\\([0-9][0-9]*\\) packets dropped by kernel/\\1/p' /tmp/bench.log | tail -n1")"
  if [[ -n "${dropped}" && "${dropped}" != "0" ]]; then
    echo "packet capture dropped ${dropped} packet(s); refusing to report a partial measurement" >&2
    return 1
  fi
  local wire_bytes
  wire_bytes="$(compose exec -T packet-capture sh -lc "tcpdump -nn -tt -r /tmp/bench.pcap 2>/dev/null | sed -n 's/.* length \\([0-9][0-9]*\\)$/\\1/p' | awk '{sum += \$1} END {print sum+0}'")"
  if [[ -z "${wire_bytes}" || "${wire_bytes}" == "0" ]]; then
    echo "packet capture produced no ClickHouse-bound bytes" >&2
    return 1
  fi
  printf '%s\n' "$wire_bytes"
}

run_case() {
  local compression="$1"
  local repeat="$2"
  start_capture
  local output
  if ! output="$({
    CLICKHOUSE_HOST=localhost \
    CLICKHOUSE_PORT=38123 \
    CLICKHOUSE_USER=default \
    CLICKHOUSE_PASSWORD=password \
    CLICKHOUSE_SSL=false \
    "$ROOT_DIR/gradlew" -Dmaven.repo.local="$LOCAL_M2" -p "$ROOT_DIR/benchmark" compressionLab \
      --args="--clientVersion=${CLIENT_VERSION} --insertFormat=${INSERT_FORMAT} --rows=${ROWS} --payloadBytes=${PAYLOAD_BYTES} --payloadMode=${PAYLOAD_MODE} --payloadSeed=${PAYLOAD_SEED} --clientCompression=${compression}"
  } 2>&1)"; then
    printf '%s\n' "$output"
    return 1
  fi
  printf '%s\n' "$output"
  local wire_bytes
  wire_bytes="$(stop_capture_sum_bytes)"
  local elapsed_ms
  elapsed_ms="$(printf '%s\n' "$output" | sed -n 's/^elapsedMs=//p' | tail -n1)"
  printf 'result repeat=%s clientCompression=%s rows=%s payloadBytes=%s payloadMode=%s payloadSeed=%s elapsedMs=%s wireBytes=%s\n' \
    "$repeat" "$compression" "$ROWS" "$PAYLOAD_BYTES" "$PAYLOAD_MODE" "$PAYLOAD_SEED" "${elapsed_ms:-unknown}" "$wire_bytes"
}

if [[ "$CLIENT_VERSION" != "V2" ]]; then
  echo "client-compression lab requires clientVersion=V2 because clientCompression=true is V2-only" >&2
  exit 1
fi

compose down -v >/dev/null 2>&1 || true
compose up -d clickhouse packet-capture
trap 'compose down -v >/dev/null 2>&1 || true; rm -f "$COMPOSE_FILE"; rm -rf "$LOCAL_M2"' EXIT
wait_for_clickhouse

cd "$ROOT_DIR"
./gradlew -Dmaven.repo.local="$LOCAL_M2" publishToMavenLocal >/dev/null

echo "client-compression lab: clientVersion=${CLIENT_VERSION} insertFormat=${INSERT_FORMAT} rows=${ROWS} payloadBytes=${PAYLOAD_BYTES} payloadMode=${PAYLOAD_MODE} payloadSeed=${PAYLOAD_SEED} repeats=${REPEATS}"
for repeat in $(seq 1 "$REPEATS"); do
  echo
  echo "=== repeat ${repeat}/${REPEATS} clientCompression=false rows=${ROWS} payloadBytes=${PAYLOAD_BYTES} ==="
  run_case false "$repeat"
  echo
  echo "=== repeat ${repeat}/${REPEATS} clientCompression=true rows=${ROWS} payloadBytes=${PAYLOAD_BYTES} ==="
  run_case true "$repeat"
done
