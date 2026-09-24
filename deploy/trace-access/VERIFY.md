# Trace access option A — staged, NOT deployed

Protocol: HTTP POST `/` over Unix, one JSON-RPC 2.0 object per request. See
`cmd/rpc-trace-proxy/README.md` for strict validation, errors and timeout semantics.
Token-flow must use an HTTP client whose dialer opens the Unix socket, inspect
HTTP status plus JSON-RPC/per-transaction errors, and back off on 503/504.

## Prepared topology and permissions

`rh-arbitrage → /run/rh-fullnode-trace/trace.sock → stunnel trace-client →
100.76.129.27:19445 mTLS → stunnel trace-server →
/run/rh-trace-proxy/trace.sock → /data/nitro/ipc/nitro-debug.ipc`.

Read-only checks on 2026-09-24 confirmed:

* Fullnode Nitro IPC: ec2-user:ec2-user 0600; both IPv4 and IPv6 listen tables
  have no TCP 19445 listener. Recheck at deployment; absence is not a reservation.
* Fullnode TLS files: root:ec2-user 0640; directory 0750. Proxy and server use
  ec2-user. Proxy is denied access to the TLS directory in its mount namespace.
* Local TLS files: root:rh-arbitrage 0640; directory 0750. Client runs as
  rh-arbitrage; RuntimeDirectory is 0700 and its socket is forced to 0600.
* Both sides reuse existing certificate, private key and peer-trust paths.
  `verifyChain=yes`, `verifyPeer=yes`, `requireCert=yes`, TLS minimum 1.3.
  No certificate/private-key copies are included in this package.

## Review, then deploy manually

The bundle in `/home/ec2-user/nitro-ntx2-release-20260924/trace-access/` contains
binary, checksum/source provenance, unit/drop-ins, two stunnel configs, scripts
and measurements. Templates are versioned under `deploy/trace-access/`.
Existing `rh-storage-delta-tunnel@.service` is reused, never replaced.

1. Review files and `SHA256SUMS`; run `sha256sum -c SHA256SUMS` in the bundle.
   Transfer the complete bundle to a chosen fullnode staging directory via the
   existing SSH identity. No automatic remote deployment is included.
2. On fullnode: `sudo ./deploy.sh fullnode --check`. This verifies host, IPC
   ownership, cert readability, port and absence of pre-existing trace files.
   `--check` performs no installation and starts no processes.
3. After review: `sudo ./deploy.sh fullnode --apply`. Installs only new trace
   files, daemon-reloads, enables/starts proxy then trace-server.
4. On ip-172-31-16-183: `sudo ./deploy.sh client --check`, then after review
   `sudo ./deploy.sh client --apply`. Enables/starts trace-client only.
5. Complete the following checks before configuring token-flow to use the socket.

These are initial-install scripts, not an upgrade framework. Existing target
files/drop-in directories, enabled instances or deployment state cause a refusal.
Rollback metadata in `/var/lib/rh-trace-access/<role>/installed.sha256` is created
before copying so a partial installation can be removed safely. Shared existing
directory modes, Nitro unit, native-stream/storage tunnels and TLS files are not
changed. Nitro is neither required/restarted nor modified by these scripts.

## Post-deployment checks

On fullnode:

```sh
systemctl --no-pager status rh-trace-proxy rh-storage-delta-tunnel@trace-server
stat -c '%U %G %a %n' /run/rh-trace-proxy /run/rh-trace-proxy/trace.sock
# Expected ec2-user, directory 700, socket 600; TCP listener only 100.76.129.27:19445.
```

On this host:

```sh
systemctl --no-pager status rh-storage-delta-tunnel@trace-client
stat -c '%U %G %a %n' /run/rh-fullnode-trace /run/rh-fullnode-trace/trace.sock
# Expected rh-arbitrage, directory 700, socket 600.
sudo -u rh-arbitrage curl --fail-with-body --max-time 25 \
  --unix-socket /run/rh-fullnode-trace/trace.sock http://localhost/ \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}'
sudo -u rh-arbitrage curl -i --max-time 25 \
  --unix-socket /run/rh-fullnode-trace/trace.sock http://localhost/ \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":2,"method":"admin_peers","params":[]}'
# Expected HTTP 400 / -32601. All batches also return HTTP 400 / -32600.
```

Check both journals for certificate verification/handshake errors; if existing
Tailscale policy blocks the new port, report it for review rather than opening
it broadly. Verify a connection without a client certificate fails:

```sh
timeout 8 openssl s_client -connect 100.76.129.27:19445 -tls1_3 -brief </dev/null
# Expect a TLS certificate-required alert/connection failure; no HTTP response.
```

With a readable copy of `measure.py` on each host, measure recent blocks and
larger candidates (default scans 128 eth block headers; at most 11 sequential
traces). Fullnode direct baseline:

```sh
python3 measure.py --socket /run/rh-trace-proxy/trace.sock
# Client through both deployed stunnels (eth HTTP used only for block hashes):
sudo -u rh-arbitrage python3 /path/readable-by-rh-arbitrage/measure.py \
  --socket /run/rh-fullnode-trace/trace.sock --rpc http://100.76.129.27:8547
```

Compare mean/max `duration_ms` and `response_bytes`; capture block hashes and
measurement times, not trace bodies. A cross-run comparison includes block
selection/cache differences; it is not an isolated measurement of TLS overhead.
Check JSON-RPC result count equals block transaction count and no top-level or
per-transaction trace errors occur (the script asserts these). An EVM revert
inside a trace is distinct from failure to produce the trace.

## Build and pre-deployment evidence (2026-09-24)

Go 1.25.9, standard library only, `CGO_ENABLED=0`, static linux/amd64. Mock IPC
suite covers the allow/reject matrix, batch zero-forwarding, HTTP Unix transport,
response/request bounds, deadline/disconnect with retained admission, concurrent
busy rejection and slot reuse, and no request-body logging. Run commands are in
the proxy README. `measurement-final.txt` contains raw metadata and binary SHA-256 from temporary
remote execution of the release binary at 12:42 UTC; proxy exited and its /tmp
directory was removed. `measurement.txt` preserves the earlier 12:37 sample.

| Sample | Count | Mean latency | Max latency | Mean bytes | Max bytes |
|---|---:|---:|---:|---:|---:|
| Latest blocks 71378288–71378292 | 5 | 36.131 ms | 44.291 ms | 55,918.4 | 73,489 |
| Largest measured candidate, block 71378191 | 1 | 138.373 ms | 138.373 ms | 1,177,535 | 1,177,535 |

The extra candidates are the union of the top 3 gas-used and top 3 transaction
counts among 128 recent headers, excluding duplicates. Total 10 traces succeeded;
health checks succeeded; admin_peers was rejected. Largest observed response is
~1.12 MiB; the 64 MiB cap gives ~57× headroom over this sample. This is **not**
a worst-case bound on future/historical blocks; monitor oversize errors and
memory before raising the cap. Test latency includes local HTTP connection,
request/response transfer and trace execution, excludes mTLS/Tailscale.

Validation passed: `go test -race`, `go vet`, Bash syntax and Python AST checks.
Merged systemd templates/drop-ins passed `systemd-analyze verify` in a temporary
directory (proxy ExecStart pointed at the staged binary solely for that check).
No stunnel runtime/handshake test was performed. Independent review could not run
due to the reviewer session restrictions; final review was self-review.

No systemd service was installed, no /etc file changed, no stunnel was started or
changed, and no existing service restarted during these checks. Live mTLS/unit
hardening behavior remains to be verified after deployment. Timeout/size stress
was performed only against mock IPC, never against production Nitro.

## Rollback (manual after review)

Stop token-flow's trace backfill usage first; missing evidence must remain
fail-closed. On client: `sudo ./rollback.sh client --check`, then `--apply`.
On fullnode: `sudo ./rollback.sh fullnode --check`, then `--apply`.
Rollback checks installed file hashes, stops/disables only the new instances
(server before proxy), removes only managed files, then daemon-reloads. If any
file was edited later, it refuses and preserves it for manual review. Existing
TLS, Nitro and native-stream services are retained. Keep the release bundle.
