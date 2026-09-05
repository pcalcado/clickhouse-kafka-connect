# Client compression lab

Manual lab for comparing ClickHouse wire bytes with `clientCompression=false` vs `true`.

## What it does

- starts ClickHouse with Docker Compose
- starts a `netshoot` sidecar sharing the ClickHouse network namespace
- captures ClickHouse-bound HTTP traffic with `tcpdump`
- runs `kafka_connector.CompressionLab` against the same external ClickHouse instance
- runs `clientCompression=false` and then `clientCompression=true` for each repeat
- prints Java-side elapsed time and packet-captured wire bytes for each run

This is intentionally a manual lab, not CI coverage.

## Run

From the repository root:

```bash
benchmark/labs/client-compression/run.sh
```

With arguments:

```bash
benchmark/labs/client-compression/run.sh --repeats=2 --rows=1000 --payloadBytes=256 --payloadMode=profile --payloadSeed=528 --clientVersion=V2 --insertFormat=json
```

Quick in-memory preflight without ClickHouse. This uses the same ClickHouse native LZ4 stream framing as the V2 client and prints `estimated...` field names so it is not confused with packet-captured lab output:

```bash
./gradlew -p benchmark compressionEstimate --args="--rows=100000 --payloadBytes=4096 --payloadMode=profile --payloadSeed=528 --insertFormat=json"
```

Optional lab overrides can also use environment variables:

```bash
ROWS=100000 \
PAYLOAD_BYTES=4096 \
PAYLOAD_MODE=profile \
PAYLOAD_SEED=528 \
CLIENT_VERSION=V2 \
INSERT_FORMAT=json \
REPEATS=3 \
benchmark/labs/client-compression/run.sh
```

Variables / arguments:

- `ROWS`: number of messages / rows inserted per run
- `PAYLOAD_BYTES`: payload size per row
- `PAYLOAD_MODE`: `repeated`, `seeded`, or `profile`
- `PAYLOAD_SEED`: seed used for deterministic per-row payload generation in `seeded` mode
- `CLIENT_VERSION` / `--clientVersion`: must be `V2`; this lab compares `clientCompression=false` and `true`, and `clientCompression=true` is V2-only
- `INSERT_FORMAT`: `json` or `string`
- `REPEATS`: number of times to run each compression mode

## Notes

- `PAYLOAD_MODE=repeated` uses a highly compressible payload (`"x"` repeated `PAYLOAD_BYTES` times).
- `PAYLOAD_MODE=seeded` generates deterministic per-row payloads from `PAYLOAD_SEED` so runs are repeatable with more entropy.
- `PAYLOAD_MODE=profile` generates a simple five-column shape: high-cardinality `user_id`, `session_id`, `request_id`, plus lower-cardinality `region` (10 values) and `event_type` (5 values).
- After each insert, the lab checks `SELECT count()` to verify the expected number of rows landed.
- Wire bytes are summed from `tcpdump` packet lengths for `tcp dst port 8123`.
- Because the script runs one compression mode per capture, the packet totals are much more trustworthy than the Docker `NetIO` approximation printed only by the bare `compressionLab` Java entry point.
- The script publishes the current checkout to a temporary Maven local repository and points benchmark Gradle invocations at that directory, so it does not overwrite the release coordinate in `~/.m2`.
- If you want artifacts, you can copy `/tmp/bench.pcap` from the `packet-capture` container before the script exits.
