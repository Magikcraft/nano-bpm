#!/usr/bin/env bash
# Copies the FFI wasm + manifest emitted by `make engine-wasm-ffi-dist` from
# the top-level dist/engine-wasm-ffi/ into this Maven module's resources dir so
# they end up on the classpath.
#
# We deliberately do NOT invoke `make` from here — the wasm build has a Rust +
# node + wasm-opt toolchain of its own and this script stays toolchain-agnostic.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
module_dir="$(cd "$here/.." && pwd)"
repo_root="$(cd "$module_dir/../.." && pwd)"
dist_dir="$repo_root/dist/engine-wasm-ffi"
target_dir="$module_dir/src/main/resources/nano-bernd"

if [[ ! -f "$dist_dir/nano_engine.wasm" ]] || [[ ! -f "$dist_dir/manifest.json" ]]; then
  echo "sync-wasm: dist artefacts not found under $dist_dir" >&2
  echo "           run 'make engine-wasm-ffi-dist' at the repo root first." >&2
  exit 1
fi

mkdir -p "$target_dir"
cp "$dist_dir/nano_engine.wasm" "$target_dir/"
cp "$dist_dir/manifest.json" "$target_dir/"
echo "sync-wasm: synced nano_engine.wasm + manifest.json into $target_dir"
