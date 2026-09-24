#!/usr/bin/env bash
# Roll back this initial installation only; refuses to delete subsequently edited
# files. Token-flow must stop using the socket before removing the client.
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
[[ -f "$state/installed.sha256" ]] || { echo "No deployment state: $state" >&2; exit 1; }
# Check every managed existing file before stopping anything. A partial install
# can have absent files, but symlinks or changed contents require manual review.
for target in "${targets[@]}"; do
  [[ ! -L "$target" ]] || { echo "Refusing symlink: $target" >&2; exit 1; }
  if [[ -e "$target" ]]; then
    expected=$(awk -v path="$target" '$2 == path {print $1}' "$state/installed.sha256")
    actual=$(sha256sum "$target")
    [[ -n "$expected" && "${actual%% *}" == "$expected" ]] || { echo "Changed file; preserve and review: $target" >&2; exit 1; }
  fi
done
printf 'Stop/disable only: %s\n' "${units[*]}"
printf 'Remove managed file: %s\n' "${targets[@]}"
[[ "$mode" == --apply ]] || exit 0
# Reverse startup order (server before proxy).
for ((i=${#units[@]}-1; i>=0; i--)); do
  if [[ $(systemctl show "${units[i]}" -p LoadState --value) != not-found ]]; then
    systemctl disable --now "${units[i]}"
  fi
done
for target in "${targets[@]}"; do rm -f -- "$target"; done
if [[ -d "$dropdir" ]]; then rmdir -- "$dropdir"; fi
systemctl daemon-reload
rm -- "$state/installed.sha256"
rmdir -- "$state"
echo 'Trace access removed; existing Nitro, NTX2 and TLS files untouched.'
