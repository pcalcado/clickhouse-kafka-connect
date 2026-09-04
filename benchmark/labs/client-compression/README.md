# Client compression lab

Manual lab for comparing ClickHouse wire bytes with `clientCompression=false` vs `true`.

## What it does

- starts ClickHouse with Docker Compose
- starts a `netshoot` sidecar sharing the ClickHouse network namespace
- captures ClickHouse-bound HTTP traffic with `tcpdump`
- runs `kafka_connector.CompressionLab` against the same external ClickHouse instance
- repeats each compression mode a configurable number of times
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
CLIENT_VERSION=V2 \
INSERT_FORMAT=json \
REPEATS=3 \
benchmark/labs/client-compression/run.sh
```

Variables:

- `ROWS`: number of messages / rows inserted per run
- `PAYLOAD_BYTES`: repeated payload size per row
- `CLIENT_VERSION`: `V1` or `V2`
- `INSERT_FORMAT`: `json` or `string`
- `REPEATS`: number of times to run each compression mode

## Notes

- The current lab defaults to a highly compressible payload (`"x"` repeated `PAYLOAD_BYTES` times).
- Wire bytes are summed from `tcpdump` packet lengths for `tcp dst port 8123`.
- Because the script runs one compression mode per capture, the packet totals are much more trustworthy than the earlier Docker `NetIO` approximation.
- If you want artifacts, you can copy `/tmp/bench.pcap` from the `packet-capture` container before the script exits.
