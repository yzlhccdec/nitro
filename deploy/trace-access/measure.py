#!/usr/bin/env python3
"""Read-only trace check. Run on fullnode or with --rpc pointing at eth HTTP.
Print metadata only; never persist trace bodies. No concurrency/load test.
"""
import argparse
import datetime
import http.client
import json
import socket
import statistics
import time
import urllib.request


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=25)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True)
    parser.add_argument('--rpc', default='http://127.0.0.1:8547')
    parser.add_argument('--scan-blocks', type=int, default=128)
    args = parser.parse_args()
    if not 5 <= args.scan_blocks <= 256:
        parser.error('--scan-blocks must be 5..256')

    def rpc(method, params):
        body = json.dumps(dict(jsonrpc='2.0', id=1, method=method, params=params)).encode()
        req = urllib.request.Request(args.rpc, body, {'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=20) as r:
            result = json.load(r)
        assert 'error' not in result, result.get('error')
        return result['result']

    def proxy(method, params):
        body = json.dumps(dict(jsonrpc='2.0', id=1, method=method, params=params))
        conn = UnixHTTP(args.socket)
        try:
            start = time.perf_counter()
            conn.request('POST', '/', body, {'Content-Type': 'application/json'})
            response = conn.getresponse()
            raw = response.read(64 * 1024 * 1024 + 1)
            elapsed = (time.perf_counter() - start) * 1000
            return response.status, json.loads(raw), elapsed, len(raw)
        finally:
            conn.close()

    head = int(rpc('eth_blockNumber', []), 16)
    headers = [rpc('eth_getBlockByNumber', [hex(n), False])
               for n in range(head, head - args.scan_blocks, -1)]
    assert all(headers), 'missing block'
    for method in ('eth_chainId', 'eth_blockNumber'):
        status, result, _, _ = proxy(method, [])
        assert status == 200 and 'result' in result, (method, status, result.get('error'))
    status, result, _, _ = proxy('admin_peers', [])
    assert status == 400 and result['error']['code'] == -32601, (status, result)
    print(json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                         head=head, scanned=args.scan_blocks, reject_admin_peers=True)), flush=True)
    recent = headers[:5]
    # Gas and transaction count are only proxies for trace size. Measure the union
    # of each top-three; do not claim a chain-wide worst-case maximum.
    large = sorted(headers, key=lambda h: int(h['gasUsed'], 16), reverse=True)[:3]
    large += sorted(headers, key=lambda h: len(h['transactions']), reverse=True)[:3]
    seen = set()
    results = []
    for h in recent + large:
        if h['hash'] in seen:
            continue
        seen.add(h['hash'])
        status, result, elapsed, size = proxy('debug_traceBlockByHash', [h['hash'],
            dict(tracer='callTracer', tracerConfig=dict(withLog=True))])
        assert status == 200 and isinstance(result.get('result'), list), (status, result.get('error'))
        traces = result['result']
        assert len(traces) == len(h['transactions']), 'trace transaction count differs'
        errors = [x.get('error') for x in traces if x.get('error')]
        assert not errors, errors
        row = dict(block=int(h['number'], 16), hash=h['hash'],
                   group='latest5' if h in recent else 'large_candidate',
                   gas_used=int(h['gasUsed'], 16), transactions=len(traces),
                   duration_ms=round(elapsed, 3), response_bytes=size)
        results.append(row)
        print(json.dumps(row), flush=True)
    latest = [r for r in results if r['group'] == 'latest5']
    assert len(latest) == 5
    print(json.dumps(dict(summary=True, latest5_mean_ms=round(statistics.mean(r['duration_ms'] for r in latest), 3),
        latest5_max_ms=max(r['duration_ms'] for r in latest),
        latest5_mean_bytes=statistics.mean(r['response_bytes'] for r in latest),
        latest5_max_bytes=max(r['response_bytes'] for r in latest),
        largest_sample=max(results, key=lambda r: r['response_bytes']))), flush=True)


if __name__ == '__main__':
    main()
