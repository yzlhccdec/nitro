#!/usr/bin/env bash
# Sourced by deploy/rollback. Fixed paths only; never reads certificate contents.
set -euo pipefail
bundle=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
role=${1:-}
case "$role" in
  fullnode)
    expected_ip=100.76.129.27
    owner=ec2-user
    units=(rh-trace-proxy.service rh-storage-delta-tunnel@trace-server.service)
    sources=(rpc-trace-proxy rh-trace-proxy.service trace-server.conf trace-server.override.conf)
    targets=(/usr/local/libexec/rpc-trace-proxy /etc/systemd/system/rh-trace-proxy.service /etc/rh-storage-delta/trace-server.conf /etc/systemd/system/rh-storage-delta-tunnel@trace-server.service.d/10-trace.conf)
    modes=(0755 0644 0640 0644)
    ;;
  client)
    expected_ip=100.75.228.105
    owner=rh-arbitrage
    units=(rh-storage-delta-tunnel@trace-client.service)
    sources=(trace-client.conf trace-client.override.conf)
    targets=(/etc/rh-storage-delta/trace-client.conf /etc/systemd/system/rh-storage-delta-tunnel@trace-client.service.d/10-trace.conf)
    modes=(0640 0644)
    ;;
  *) echo 'Usage: deploy.sh|rollback.sh fullnode|client [--check|--apply]' >&2; exit 2 ;;
esac
dropdir=$(dirname -- "${targets[${#targets[@]}-1]}")
state=/var/lib/rh-trace-access/$role
mode=${2:---check}
[[ "$mode" == --check || "$mode" == --apply ]] || { echo 'Expected --check or --apply' >&2; exit 2; }
[[ $EUID == 0 ]] || { echo 'Run with sudo on the selected host (even --check verifies permissions).' >&2; exit 1; }
[[ $(tailscale ip -4) == "$expected_ip" ]] || { echo 'Wrong host for selected role' >&2; exit 1; }
