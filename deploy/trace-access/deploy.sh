#!/usr/bin/env bash
# No remote operations. --check is the default; --apply explicitly installs only
# the new trace units. Existing Nitro and tunnel units are never restarted.
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
[[ ! -e "$state" ]] || { echo "Existing deployment state: $state; inspect/rollback first" >&2; exit 1; }
[[ ! -e "$dropdir" && ! -L "$dropdir" ]] || { echo "Existing drop-in directory: $dropdir" >&2; exit 1; }
for i in "${!targets[@]}"; do
  [[ -f "$bundle/${sources[i]}" ]] || { echo "Missing ${sources[i]}" >&2; exit 1; }
  [[ ! -e "${targets[i]}" && ! -L "${targets[i]}" ]] || { echo "Refusing overwrite: ${targets[i]}" >&2; exit 1; }
done
for unit in "${units[@]}"; do
  [[ $(systemctl show "$unit" -p LoadState --value) == not-found || "$unit" == rh-storage-delta-tunnel@* ]] || { echo "Existing unit: $unit" >&2; exit 1; }
  ! systemctl is-active --quiet "$unit" || { echo "Already active: $unit" >&2; exit 1; }
  ! systemctl is-enabled --quiet "$unit" || { echo "Already enabled: $unit" >&2; exit 1; }
done
[[ -f /etc/systemd/system/rh-storage-delta-tunnel@.service ]] || { echo 'Missing existing tunnel template' >&2; exit 1; }
for name in own-cert.pem own-key.pem peer-trust.pem; do
  runuser -u "$owner" -- test -r "/etc/rh-storage-delta/tls/$name"
done
if [[ "$role" == fullnode ]]; then
  [[ $(stat -c '%U %a' /data/nitro/ipc/nitro-debug.ipc) == 'ec2-user 600' ]]
  runuser -u ec2-user -- test -S /data/nitro/ipc/nitro-debug.ipc
  python3 - <<'PY'
from pathlib import Path
for table in ('tcp', 'tcp6'):
    for line in Path('/proc/net/' + table).read_text().splitlines()[1:]:
        fields = line.split()
        if int(fields[1].rsplit(':', 1)[1], 16) == 19445 and fields[3] == '0A':
            raise SystemExit('TCP 19445 is occupied; abort')
PY
fi
printf 'Install on %s (%s):\n' "$role" "$expected_ip"
printf '  %s\n' "${targets[@]}"
printf 'Enable/start only: %s\n' "${units[*]}"
[[ "$mode" == --apply ]] || exit 0
# State is written first so a partial install can be rolled back. Initial install
# only: refusing overwrites means there is no prior trace configuration to restore.
install -d -m 0700 "$state"
for i in "${!targets[@]}"; do
  digest=$(sha256sum "$bundle/${sources[i]}")
  printf '%s  %s\n' "${digest%% *}" "${targets[i]}" >> "$state/installed.sha256"
done
for i in "${!targets[@]}"; do
  parent=$(dirname -- "${targets[i]}")
  if [[ ! -d "$parent" ]]; then install -d -m 0755 "$parent"; fi
  install -o root -g "$owner" -m "${modes[i]}" "$bundle/${sources[i]}" "${targets[i]}"
done
systemctl daemon-reload
for unit in "${units[@]}"; do systemctl enable --now "$unit"; done
systemctl --no-pager --full status "${units[@]}"
echo 'Run VERIFY.md checks before enabling token-flow trace backfill.'
