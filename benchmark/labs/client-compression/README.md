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

Optional overrides:

```bash
ROWS=100000 \
PAYLOAD_BYTES=4096 \
PAYLOAD_MODE=seeded \
PAYLOAD_SEED=528 \
CLIENT_VERSION=V2 \
INSERT_FORMAT=json \
REPEATS=3 \
benchmark/labs/client-compression/run.sh
```

Variables:

- `ROWS`: number of messages / rows inserted per run
- `PAYLOAD_BYTES`: payload size per row
- `PAYLOAD_MODE`: `repeated` or `seeded`
- `PAYLOAD_SEED`: seed used for deterministic per-row payload generation in `seeded` mode
- `CLIENT_VERSION`: `V1` or `V2`
- `INSERT_FORMAT`: `json` or `string`
- `REPEATS`: number of times to run each compression mode

## Notes

- `PAYLOAD_MODE=repeated` uses a highly compressible payload (`"x"` repeated `PAYLOAD_BYTES` times).
- `PAYLOAD_MODE=seeded` generates deterministic per-row payloads from `PAYLOAD_SEED` so runs are repeatable with more entropy.
- After each insert, the lab checks `SELECT count()` to verify the expected number of rows landed.
- Wire bytes are summed from `tcpdump` packet lengths for `tcp dst port 8123`.
- Because the script runs one compression mode per capture, the packet totals are much more trustworthy than the earlier Docker `NetIO` approximation.
- If you want artifacts, you can copy `/tmp/bench.pcap` from the `packet-capture` container before the script exits.
