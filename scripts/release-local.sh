#!/usr/bin/env bash
# release-local.sh — cut a nanobpmn release by cross-compiling ALL distribution
# binaries locally, then handing them to the (secret-bearing) distribute workflow
# via a GitHub Release.
#
# Why
# ---
# The tag-triggered CI matrix cross-compiled 12 binaries across macOS (10× minute
# cost), Windows (2×) and Linux runners on every release — heavy and expensive.
# The toolchain already exists locally (zig + cargo-zigbuild for Linux, the Apple
# SDK for macOS, cargo-xwin for Windows MSVC), so we build here for near-zero
# marginal cost and let a tiny ubuntu-only workflow do the parts that genuinely
# need secrets (pushing to the public plugin repo + the S3 mirror).
#
# The flow
# --------
#   1. Cross-compile every target locally into dist/ (CI-identical asset names,
#      same cargo-zigbuild glibc floor as CI, version stamped from the tag).
#   2. Create a GitHub Release on THIS repo for the tag and upload all binaries.
#      This is the ProcessOS binaries' real home AND the handoff vehicle for the
#      gateway binaries.
#   3. Publishing that release fires `.github/workflows/distribute-binaries.yml`
#      (release: published), which downloads these assets and — with the repo
#      secrets — uploads the gateway to the public plugin repo (+ marker bump) and
#      mirrors ProcessOS to S3. By default this script leaves the release as a
#      DRAFT so a human clicks "Publish" (the deliberate, irreversible gesture);
#      pass --publish to publish immediately.
#
# The two heavy matrix workflows (publish-c8ctl-binaries.yml /
# publish-processos-binaries.yml) remain as manual clean-room fallbacks
# (workflow_dispatch) for when this host is unavailable.
#
# Targets (CI-identical)
# ----------------------
#   Gateway (nanobpm-gateway-rest-server, --features console):
#     darwin-arm64  darwin-x64  linux-x64  linux-arm64  linux-armv7  linux-armv6  win32-x64
#   ProcessOS (processos, DuckDB bundled):
#     darwin-arm64  darwin-x64  linux-x64  linux-arm64  win32-x64(CI-only*)
#
#   * ProcessOS-Windows is NOT built locally: DuckDB's amalgamation decorates
#     deleted functions with __declspec(dllexport), which native MSVC accepts but
#     clang-cl (cargo-xwin) rejects. That one target stays on the CI fallback
#     (publish-processos-binaries.yml, native windows-2022). The gateway has no
#     DuckDB, so gateway-win32 DOES build locally via cargo-xwin.
#
# Prerequisites
# -------------
#   * rustup (+ targets auto-added), zig, cargo-zigbuild   (Linux legs)
#   * Apple Command Line Tools / Xcode SDK                  (macOS legs)
#   * cargo-xwin  (`cargo install cargo-xwin`)              (Windows legs)
#   * Java(JDK) + uv + Node + wasm-pack                     (gateway codegen+console)
#   * gh authenticated with contents:write on this repo     (the release step)
#
# Usage
# -----
#   scripts/release-local.sh [--version vX.Y.Z] [options]
#     --version vX.Y.Z    version to stamp/release (default: the v* tag on HEAD).
#     --gateway-only      build only the gateway binaries.
#     --processos-only    build only the ProcessOS binaries.
#     --targets "a b"     build only these asset suffixes (e.g. "linux-x64 win32-x64").
#     --no-console        build the API-only gateway (skip the embedded console).
#     --build-only        just build into dist/; do not touch GitHub Releases.
#     --release           build + create/refresh a DRAFT release with the assets.
#     --publish           build + create the release AND publish it (fires
#                         distribution immediately — no human gate).
#     --out DIR           output dir for staged binaries (default: dist/).
#   Default action is --release (draft).
#
# Prefer `make release-local` (it builds the codegen+console prerequisites first).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GW_BIN="nanobpm-gateway-rest-server"
PO_BIN="processos"
GLIBC="2.31"
WIN_TRIPLE="x86_64-pc-windows-msvc"

die() { echo "release-local: $*" >&2; exit 1; }
log() { echo "release-local: $*" >&2; }

VERSION=""; SCOPE="all"; ONLY_TARGETS=""; CONSOLE=1; ACTION="release"; OUT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --version)       VERSION="${2:?}"; shift 2 ;;
    --gateway-only)  SCOPE="gateway"; shift ;;
    --processos-only) SCOPE="processos"; shift ;;
    --targets)       ONLY_TARGETS="${2:?}"; shift 2 ;;
    --no-console)    CONSOLE=0; shift ;;
    --build-only)    ACTION="build"; shift ;;
    --release)       ACTION="release"; shift ;;
    --publish)       ACTION="publish"; shift ;;
    --out)           OUT="${2:?}"; shift 2 ;;
    -h|--help)       awk 'NR==1{next} /^#/{print; next} {exit}' "$0"; exit 0 ;;
    -*)              die "unknown flag: $1" ;;
    *)               die "unexpected arg: $1" ;;
  esac
done

# --- resolve version --------------------------------------------------------
if [ -z "$VERSION" ]; then
  VERSION="$(git -C "$PROJECT_ROOT" describe --tags --exact-match 2>/dev/null || true)"
  [ -n "$VERSION" ] || die "no v* tag on HEAD — pass --version vX.Y.Z (tag must exist & be pushed to release)."
fi
case "$VERSION" in v*) ;; *) VERSION="v$VERSION" ;; esac
VER_NUM="${VERSION#v}"   # what build.rs stamps (no leading v)
export NANOBPM_VERSION="$VERSION"
export PROCESSOS_VERSION="$VERSION"

[ -n "$OUT" ] || OUT="$PROJECT_ROOT/dist"
mkdir -p "$OUT"

# --- target matrix (single source; asset-suffix -> triple) ------------------
# Kept in lockstep with the CI matrices in publish-{c8ctl,processos}-binaries.yml.
# method: native | clang | zig | xwin
gw_targets=(
  "darwin-arm64|aarch64-apple-darwin|native"
  "darwin-x64|x86_64-apple-darwin|clang"
  "linux-x64|x86_64-unknown-linux-gnu|zig"
  "linux-arm64|aarch64-unknown-linux-gnu|zig"
  "linux-armv7|armv7-unknown-linux-gnueabihf|zig"
  "linux-armv6|arm-unknown-linux-gnueabihf|zig"
  "win32-x64.exe|${WIN_TRIPLE}|xwin"
)
po_targets=(
  "darwin-arm64|aarch64-apple-darwin|native"
  "darwin-x64|x86_64-apple-darwin|clang"
  "linux-x64|x86_64-unknown-linux-gnu|zig"
  "linux-arm64|aarch64-unknown-linux-gnu|zig"
  # ProcessOS bundles DuckDB's C++ amalgamation. DuckDB applies __declspec(dllexport)
  # to deleted functions, which real MSVC cl.exe accepts but clang-cl (what cargo-xwin
  # drives) rejects ("attribute 'dllexport' cannot be applied to a deleted function").
  # There is no local clang-cl path for it, so this single target stays on the CI
  # fallback (publish-processos-binaries.yml builds it on a native windows-2022 runner).
  "win32-x64.exe|${WIN_TRIPLE}|ci"
)

want_target() { # $1 = asset suffix; honour --targets filter
  [ -z "$ONLY_TARGETS" ] && return 0
  local t base="${1%.exe}"
  for t in $ONLY_TARGETS; do [ "${t%.exe}" = "$base" ] && return 0; done
  return 1
}

ensure_std() {
  rustup target list --installed 2>/dev/null | grep -qx "$1" || {
    log "adding rust std for $1"; rustup target add "$1"
  }
}

# cargo-xwin needs LLVM's clang-cl / llvm-lib (Homebrew `llvm` keg) plus lld-link
# (Homebrew `lld` keg). Both are keg-only, so prepend their bins to PATH for the
# Windows legs only (leaving native/darwin builds on the Apple toolchain). A no-op
# when the kegs are absent — cargo-xwin then relies on whatever LLVM is already on
# PATH (e.g. a Linux distro's llvm/lld packages) and errors clearly if none found.
xwin_path() {
  local prefix=/opt/homebrew/opt
  [ -d "$prefix" ] || prefix=/usr/local/opt   # Intel Homebrew prefix
  local p="$PATH"
  [ -d "$prefix/lld/bin" ] && p="$prefix/lld/bin:$p"
  [ -d "$prefix/llvm/bin" ] && p="$prefix/llvm/bin:$p"
  echo "$p"
}

BUILT=()   # staged asset paths, for the release step
CI_FALLBACK=()  # asset suffixes that only the CI fallback workflow can produce

# ---- gateway build methods -------------------------------------------------
gw_build() { # $1 suffix  $2 triple  $3 method
  local suffix="$1" triple="$2" method="$3"
  local asset="$GW_BIN-$suffix" out="$OUT/$GW_BIN-$suffix"
  local feat=(); [ "$CONSOLE" = 1 ] && feat=(--features console)
  log "gateway -> $asset ($triple, $method, console=$CONSOLE)"
  case "$method" in
    zig)
      # Reuse the shared cross recipe (glibc floor + console embed + staging).
      local args=(--glibc "$GLIBC" --out "$out")
      [ "$CONSOLE" = 0 ] && args+=(--no-console)
      "$SCRIPT_DIR/cross-build.sh" "$triple" "${args[@]}" >&2
      ;;
    native|clang)
      ensure_std "$triple"
      [ "$CONSOLE" = 1 ] && touch "$PROJECT_ROOT/server/src/console/mod.rs"
      ( cd "$PROJECT_ROOT/server" && cargo build --release --target "$triple" ${feat[@]+"${feat[@]}"} --bin "$GW_BIN" ) >&2
      cp "$PROJECT_ROOT/server/target/$triple/release/$GW_BIN" "$out"
      ;;
    xwin)
      ensure_std "$triple"
      [ "$CONSOLE" = 1 ] && touch "$PROJECT_ROOT/server/src/console/mod.rs"
      ( cd "$PROJECT_ROOT/server" && PATH="$(xwin_path)" cargo xwin build --release --target "$triple" ${feat[@]+"${feat[@]}"} --bin "$GW_BIN" ) >&2
      cp "$PROJECT_ROOT/server/target/$triple/release/$GW_BIN.exe" "$out"
      ;;
    *) die "unknown build method: $method" ;;
  esac
  BUILT+=("$out")
  log "staged $out"
}

# ---- processos build methods (DuckDB bundled C++) --------------------------
po_build() { # $1 suffix  $2 triple  $3 method
  local suffix="$1" triple="$2" method="$3"
  local asset="$PO_BIN-$suffix" out="$OUT/$PO_BIN-$suffix"
  if [ "$method" = ci ]; then
    log "SKIP processos $suffix — no local clang-cl path (DuckDB dllexport-on-deleted); use the CI fallback."
    CI_FALLBACK+=("$asset")
    return 0
  fi
  log "processos -> $asset ($triple, $method)"
  case "$method" in
    zig)
      ensure_std "$triple"
      # zig supplies the cross C/C++ toolchain (libstdc++/libc++) DuckDB's bundled
      # amalgamation needs; the .<glibc> suffix pins the floor (CI links via apt
      # g++, but the resulting -gnu binary's glibc floor is what matters).
      ( cd "$PROJECT_ROOT/processos" && cargo zigbuild --release --target "$triple.$GLIBC" --bin "$PO_BIN" ) >&2
      cp "$PROJECT_ROOT/processos/target/$triple/release/$PO_BIN" "$out"
      ;;
    native|clang)
      ensure_std "$triple"
      ( cd "$PROJECT_ROOT/processos" && cargo build --release --target "$triple" --bin "$PO_BIN" ) >&2
      cp "$PROJECT_ROOT/processos/target/$triple/release/$PO_BIN" "$out"
      ;;
    xwin)
      ensure_std "$triple"
      ( cd "$PROJECT_ROOT/processos" && PATH="$(xwin_path)" cargo xwin build --release --target "$triple" --bin "$PO_BIN" ) >&2
      cp "$PROJECT_ROOT/processos/target/$triple/release/$PO_BIN.exe" "$out"
      ;;
    *) die "unknown build method: $method" ;;
  esac
  BUILT+=("$out")
  log "staged $out"
}

# --- prerequisite check for the gateway codegen/console ---------------------
if [ "$SCOPE" != "processos" ]; then
  [ -f "$PROJECT_ROOT/generated/Cargo.toml" ] || die "generated/ missing — run 'make generate' (or use 'make release-local')."
  [ -f "$PROJECT_ROOT/server/src/stub_impls.rs" ] || die "server/src/stub_impls.rs missing — run 'make generate'."
  if [ "$CONSOLE" = 1 ]; then
    [ -d "$PROJECT_ROOT/console/dist" ] || die "console/dist missing — run 'make console-frontend' (or 'make release-local')."
  fi
fi

# --- build ------------------------------------------------------------------
log "version $VERSION (stamp $VER_NUM), scope=$SCOPE, out=$OUT"
if [ "$SCOPE" != "processos" ]; then
  for spec in "${gw_targets[@]}"; do
    IFS='|' read -r suffix triple method <<<"$spec"
    want_target "$suffix" || continue
    gw_build "$suffix" "$triple" "$method"
  done
fi
if [ "$SCOPE" != "gateway" ]; then
  for spec in "${po_targets[@]}"; do
    IFS='|' read -r suffix triple method <<<"$spec"
    want_target "$suffix" || continue
    po_build "$suffix" "$triple" "$method"
  done
fi

log "built ${#BUILT[@]} binaries:"
for b in ${BUILT[@]+"${BUILT[@]}"}; do
  printf '  %s  %s\n' "$b" "$(command -v file >/dev/null 2>&1 && file -b "$b" || echo)" >&2
done

if [ "${#CI_FALLBACK[@]}" -gt 0 ]; then
  log "-----------------------------------------------------------------------"
  log "NOTE: ${#CI_FALLBACK[@]} target(s) cannot be built locally and must come from"
  log "the CI fallback (native MSVC on a windows runner):"
  for a in "${CI_FALLBACK[@]}"; do log "    - $a"; done
  log "Produce it with the CI fallback (builds the full ProcessOS matrix on"
  log "native runners and attaches to this tag's release):"
  log "    gh workflow run publish-processos-binaries.yml --ref $VERSION"
  log "Its win32 asset then lands on the '$VERSION' release automatically."
  log "-----------------------------------------------------------------------"
fi

if [ "$ACTION" = "build" ]; then
  echo "RELEASE_LOCAL_OK $VERSION build-only ${#BUILT[@]} binaries in $OUT"
  exit 0
fi

# --- GitHub Release on THIS repo (handoff to distribute-binaries.yml) --------
command -v gh >/dev/null 2>&1 || die "gh CLI not found — needed for the release step (or pass --build-only)."
git -C "$PROJECT_ROOT" rev-parse -q --verify "refs/tags/$VERSION" >/dev/null 2>&1 \
  || die "tag $VERSION not found locally — create & push it first (git tag $VERSION && git push origin $VERSION)."

draft_flag=(--draft)
[ "$ACTION" = "publish" ] && draft_flag=()

if gh release view "$VERSION" -R "$PROJECT_ROOT" >/dev/null 2>&1; then
  log "release $VERSION exists — uploading assets (--clobber)"
else
  log "creating release $VERSION ($([ "$ACTION" = publish ] && echo published || echo draft))"
  gh release create "$VERSION" -R "$PROJECT_ROOT" \
    --title "$VERSION" \
    --notes "nanobpmn $VERSION — gateway + ProcessOS binaries (built locally; distributed by distribute-binaries.yml)." \
    --verify-tag ${draft_flag[@]+"${draft_flag[@]}"}
fi
if [ "${#BUILT[@]}" -eq 0 ]; then
  die "no binaries were built locally for this scope — nothing to upload (use the CI fallback)."
fi
gh release upload "$VERSION" "${BUILT[@]}" -R "$PROJECT_ROOT" --clobber

if [ "$ACTION" = "publish" ]; then
  # If the release already existed as a draft, flip it live now to fire distribution.
  gh release edit "$VERSION" -R "$PROJECT_ROOT" --draft=false >/dev/null
  log "published release $VERSION — distribute-binaries.yml will run."
  echo "RELEASE_LOCAL_OK $VERSION published ${#BUILT[@]} binaries"
else
  log "draft release $VERSION ready with ${#BUILT[@]} assets."
  log "Publish it (GitHub UI 'Publish release', or: gh release edit $VERSION --draft=false) to fire distribution."
  echo "RELEASE_LOCAL_OK $VERSION draft ${#BUILT[@]} binaries"
fi
