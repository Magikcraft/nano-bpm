#!/usr/bin/env bash
# Sparse-fetch the Zeebe sources the coverage matrix is extracted from, at the
# commit pinned in zeebe-pin.json, and print the checkout directory.
#
# Usage: formal/parity/fetch-zeebe.sh [dest]   (default: ~/.cache/nanobpm-parity/<sha>)
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
pin="$here/zeebe-pin.json"
field() { node -e "process.stdout.write(String(require(process.argv[1])[process.argv[2]]))" "$pin" "$1"; }
repo="$(field repository)"
sha="$(field sha)"
dest="${1:-${XDG_CACHE_HOME:-$HOME/.cache}/nanobpm-parity/$sha}"

paths="$(node -e "for (const p of require(process.argv[1]).paths) console.log('/' + p)" "$pin")"

# The marker records the sparse paths, so editing them in the pin refetches.
if [ -f "$dest/.complete" ] && [ "$(cat "$dest/.complete")" = "$paths" ]; then
  echo "$dest"
  exit 0
fi

rm -rf "$dest"
mkdir -p "$dest"
git -C "$dest" init -q
git -C "$dest" remote add origin "$repo"
printf '%s\n' "$paths" | git -C "$dest" sparse-checkout set --no-cone --stdin
git -C "$dest" fetch -q --depth 1 --filter=blob:none origin "$sha"
git -C "$dest" checkout -q FETCH_HEAD
printf '%s' "$paths" > "$dest/.complete"
echo "$dest"
