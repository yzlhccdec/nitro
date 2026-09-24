# rpc-trace-proxy

Pure standard-library Go HTTP-over-Unix → Nitro IPC allowlist. No Nitro restart,
no HTTP debug namespace, no additional Go dependencies. Client wire protocol is
**HTTP POST /** with a single JSON-RPC 2.0 object, not raw IPC JSON lines.

Only these calls are admitted:

* `debug_traceBlockByHash`: exactly two positional arguments: a `0x` + 64 hex
  block hash and `{ "tracer":"callTracer", "tracerConfig":{"withLog":true} }`.
  Optional `timeout` is a positive Go duration ≤15s (also ≤75% of proxy timeout).
  Omitted timeout is injected as 15s at the default proxy timeout.
* `eth_chainId` / `eth_blockNumber`: no params or empty array.

All batches (including all-valid batches) are rejected as a whole before IPC.
Notifications, null IDs, duplicate keys at any level, unexpected fields, other
tracers/config options, wrong key casing and trailing JSON are rejected. IDs can
be JSON strings or numbers. Fields are validated then re-encoded, never blindly
forwarded. Nitro's response, including JSON-RPC errors, is otherwise returned
unchanged. HTTP 200 does **not** imply trace success: inspect JSON-RPC `error`
and per-transaction errors. Failed EVM calls inside a successful trace are data.

Defaults: concurrency 4 (includes health checks), no admission queue, 20s HTTP
request deadline, 16 KiB request body, 64 MiB response including IPC newline,
5s HTTP read/header deadline, 25s write deadline, 30s idle timeout, 8 KiB header
setting. Unix socket is 0600; create its private parent directory first. Never
unlink an existing socket automatically. systemd RuntimeDirectory handles
cleanup on stop/restart. Only validated method, outcome, duration and byte count
are logged; no bodies, hashes, IDs or upstream error text are logged.

Proxy-generated errors:

| HTTP | JSON-RPC code | Meaning |
|---|---|---|
| 400 | -32700 / -32600 / -32601 / -32602 | parse / envelope / method / params rejected |
| 413 / 415 | -32600 | body too large/unreadable / encoding unsupported |
| 503 | -32000 | all slots busy; retry with backoff |
| 504 | -32001 | proxy deadline reached |
| 502 | -32002 / -32003 | IPC unavailable/invalid / response too large |

Each call opens a separate IPC connection. geth emits one newline-terminated
JSON response (`rpc/json.go`, Encoder.Encode); replies are buffered within the
limit, checked as JSON, then returned, so no partial oversized trace escapes.
Oversized replies are drained/discarded. No traces are persisted.

**Nitro cancellation limitation:** the deployed geth handler waits for active
calls before canceling its root context (`rpc/handler.go`, handler.close), and
tracer timeout is per transaction. Therefore closing IPC is not a reliable
whole-block cancellation mechanism. After an HTTP timeout or client disconnect,
the worker retains its admission slot and drains/discards Nitro's response until
completion/EOF. At most four submitted calls remain outstanding in a running
proxy; repeated client retries cannot accumulate orphan work. If all four hang,
new calls fail closed with 503. Investigate Nitro before restarting the proxy;
a restart cannot cancel old Nitro calls and resets local admission accounting.
The HTTP deadline does not claim to bound Nitro's CPU time or state reconstruction.

Memory is bounded by admitted calls and response caps (including buffer growth
and Go GC overhead), not just one response cap. The supplied unit uses concurrency
4, 64 MiB, GOMEMLIMIT=768MiB and MemoryMax=1G. Larger flags need separate sizing.
These limits do not bound Nitro's own allocations while tracing/serializing.

```sh
GOWORK=off GOPROXY=off GOTOOLCHAIN=local \
  /home/ec2-user/.local/go1.25.9/bin/go test -race ./cmd/rpc-trace-proxy
GOWORK=off GOPROXY=off GOTOOLCHAIN=local \
  /home/ec2-user/.local/go1.25.9/bin/go vet ./cmd/rpc-trace-proxy
CGO_ENABLED=0 GOWORK=off GOPROXY=off GOTOOLCHAIN=local \
  /home/ec2-user/.local/go1.25.9/bin/go build -buildvcs=false -trimpath \
  -ldflags='-s -w' -o rpc-trace-proxy ./cmd/rpc-trace-proxy
```

`-buildvcs=false` avoids this checkout's broken geth submodule Git metadata;
record the source commit alongside the release checksum. Go 1.25.9 builds a
static linux/amd64 executable without linking Nitro/cgo. Deployment templates,
read-only measurement script and the review checklist live in
[`deploy/trace-access`](../../deploy/trace-access/VERIFY.md).
