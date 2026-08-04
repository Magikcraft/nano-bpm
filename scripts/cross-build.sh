#!/usr/bin/env bash
# cross-build.sh — cross-compile the gateway (nanobpm-gateway-rest-server) for a
# Linux target locally, mirroring the CI recipe in
# .github/workflows/publish-c8ctl-binaries.yml (cargo-zigbuild + a glibc floor).
#
# This lets you produce dev builds for e.g. a Linux x86-64 box or a Raspberry Pi
# (ARMv7) from any host (macOS/Linux) without Docker or a per-target gcc — zig
# provides the cross C compiler/linker for the bundled C deps (rusqlite,
# jemalloc-sys) and pins a minimum glibc so the `-gnu` binaries run on older
# distros.
#
# It does NOT regenerate the REST layer or the web console: the git-ignored
# generated/ + server/src/stub_impls.rs must already exist (plus generated-console/
# and console/dist for the default console build; --no-console needs neither). The
# Makefile `cross-*` targets share `release`'s prerequisites, so
# `make cross-linux-x64` builds those for you first — prefer the make targets.
#
# Prerequisites (verified below, with install hints):
#   * rustup + the target's std (auto-added via `rustup target add`)
#   * zig                 (brew install zig      | https://ziglang.org/download)
#   * cargo-zigbuild      (cargo install cargo-zigbuild)
#
# Usage:
#   scripts/cross-build.sh <target-triple> [--glibc <ver>] [--no-console] [--out <path>]
#
#   <target-triple>  e.g. x86_64-unknown-linux-gnu, armv7-unknown-linux-gnueabihf,
#                    aarch64-unknown-linux-gnu, arm-unknown-linux-gnueabihf
#   --glibc <ver>    minimum glibc to link against (default: 2.31 = Debian 11
#                    "bullseye" / Ubuntu 20.04 — matches CI). Use "" to disable.
#   --no-console     build the API-only gateway (skip the embedded web console);
#                    faster, and does not require console/dist.
#   --console        build with the embedded web console (the default; provided
#                    as the explicit inverse of --no-console).
#   --out <path>     output path for the staged binary (default:
#                    dist/nanobpm-gateway-rest-server-<os>-<arch>). A relative
#                    path is resolved against the current directory.
#
# Final line is machine-parseable: "BUILD_OK <target> <path> (<secs>s)".
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_NAME="nanobpm-gateway-rest-server"

die() { echo "cross-build: $*" >&2; exit 1; }
# Guard optional-value flags so a missing value fails with a clear message rather
# than a cryptic `set -u` "$2: unbound variable". Call as `need_val "$@"`.
need_val() { [ $# -ge 2 ] || die "flag $1 needs a value (e.g. $1 <value>)"; }

# Wrap the entire body in main() so bash parses the whole script before running
# any of it. Bash reads a script by byte offset as it executes; without this,
# editing the file on disk while a long `cargo` build is in flight would make
# bash resume at a stale offset afterwards and fail with a spurious
# "syntax error near unexpected token". A fully-parsed main() is edit-safe.
main() {
TARGET=""; GLIBC="2.31"; CONSOLE=1; OUT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --glibc)      need_val "$@"; GLIBC="$2"; shift 2 ;;
    --no-console) CONSOLE=0; shift ;;
    --console)    CONSOLE=1; shift ;;
    --out)        need_val "$@"; OUT="$2"; shift 2 ;;
    -h|--help)    awk 'NR==1{next} /^#/{print; next} {exit}' "$0"; exit 0 ;;
    -*)           die "unknown flag: $1" ;;
    *)            if [ -z "$TARGET" ]; then TARGET="$1"; else die "unexpected arg: $1"; fi; shift ;;
  esac
done
[ -n "$TARGET" ] || die "missing <target-triple> (e.g. x86_64-unknown-linux-gnu). See --help."

# Map the Rust triple to the CI asset name so local artifacts match the released
# ones (nanobpm-gateway-rest-server-linux-{x64,arm64,armv7,armv6}).
asset_for() {
  case "$1" in
    x86_64-unknown-linux-gnu)       echo "$BIN_NAME-linux-x64" ;;
    aarch64-unknown-linux-gnu)      echo "$BIN_NAME-linux-arm64" ;;
    armv7-unknown-linux-gnueabihf)  echo "$BIN_NAME-linux-armv7" ;;
    arm-unknown-linux-gnueabihf)    echo "$BIN_NAME-linux-armv6" ;;
    *)                              echo "$BIN_NAME-$1" ;;
  esac
}
[ -n "$OUT" ] || OUT="$PROJECT_ROOT/dist/$(asset_for "$TARGET")"

# --- toolchain checks -------------------------------------------------------
command -v rustup  >/dev/null 2>&1 || die "rustup not found — install Rust from https://rustup.rs"
command -v zig     >/dev/null 2>&1 || die "zig not found — 'brew install zig' or https://ziglang.org/download"
command -v cargo-zigbuild >/dev/null 2>&1 || die "cargo-zigbuild not found — 'cargo install cargo-zigbuild'"

if ! rustup target list --installed 2>/dev/null | grep -qx "$TARGET"; then
  echo "cross-build: adding rust std for $TARGET (rustup target add $TARGET)"
  rustup target add "$TARGET"
fi

# --- prerequisite sources (git-ignored; produced by codegen + console build) ---
[ -f "$PROJECT_ROOT/generated/Cargo.toml" ]     || die "generated/ missing — run 'make generate' (or use the 'make cross-*' targets)."
[ -f "$PROJECT_ROOT/server/src/stub_impls.rs" ] || die "server/src/stub_impls.rs missing — run 'make generate'."
if [ "$CONSOLE" = 1 ]; then
  [ -d "$PROJECT_ROOT/console/dist" ]                    || die "console/dist missing — run 'make console-frontend' (or use 'make cross-*'; or pass --no-console)."
  [ -f "$PROJECT_ROOT/generated-console/Cargo.toml" ]   || die "generated-console/ missing — run 'make generate'."
fi

# The `.<glibc>` suffix pins the minimum glibc; zigbuild still writes to the
# unsuffixed target dir (server/target/<triple>/release/).
zig_target="$TARGET"
[ -n "$GLIBC" ] && zig_target="$TARGET.$GLIBC"

features=(--bin "$BIN_NAME")
if [ "$CONSOLE" = 1 ]; then
  features=(--features console "${features[@]}")
  # Force the RustEmbed derive to re-run so the current console/dist is baked in,
  # even if the gateway sources are otherwise unchanged (mirrors `make release`).
  touch "$PROJECT_ROOT/server/src/console/mod.rs"
fi

echo "cross-build: $BIN_NAME -> $TARGET (glibc floor: ${GLIBC:-none}, console: $CONSOLE)"
start=$(date +%s)
(
  cd "$PROJECT_ROOT/server"
  cargo zigbuild --release --target "$zig_target" "${features[@]}"
)
secs=$(( $(date +%s) - start ))

built="$PROJECT_ROOT/server/target/$TARGET/release/$BIN_NAME"
[ -f "$built" ] || die "expected binary not found at $built"
mkdir -p "$(dirname "$OUT")"
cp "$built" "$OUT"

echo "cross-build: staged $(command -v file >/dev/null 2>&1 && file -b "$OUT" || echo "$OUT")"
echo "cross-build: -> $OUT"
echo "BUILD_OK $TARGET $OUT (${secs}s)"
}

main "$@"
