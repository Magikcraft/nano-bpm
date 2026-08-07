#!/usr/bin/env bash
# publish-c8ctl-assets.sh — distribute already-built gateway binaries to the
# PUBLIC c8ctl plugin repo (jwulf/c8ctl-plugin-nano).
#
# This is the single source of truth for the c8ctl distribution side-effects,
# shared by:
#   * .github/workflows/distribute-binaries.yml  (the cheap, secret-bearing
#     publish job that consumes LOCALLY cross-compiled binaries), and
#   * .github/workflows/publish-c8ctl-binaries.yml (the manual clean-room CI
#     fallback that builds the same binaries in-matrix).
# Both just stage the gateway binaries into a dist/ dir and call this script, so
# the upload + marker-bump recipe never drifts between them.
#
# What it does (mirrors the old inline `publish` job):
#   1. Ensures the rolling `binaries` release exists on the plugin repo and
#      uploads the gateway assets to it (--clobber, idempotent).
#   2. Bumps the bundled-binary marker file (nanobpmn-binary.json) on the plugin
#      repo's main branch. That commit is what makes the plugin's semantic-release
#      cut a new npm version shipping the freshly-uploaded binaries. The
#      conventional-commit TYPE is derived from the server SemVer delta (issue #1):
#        * server minor-or-higher bump -> feat(binary): -> plugin MINOR
#        * server patch bump (or commit-only) -> fix(binary): -> plugin PATCH
#      No `!`/`BREAKING CHANGE:` is ever emitted. No-ops (skips push) if unchanged.
#
# Inputs (env):
#   GH_TOKEN     fine-grained PAT with Contents:rw on PLUGIN_REPO ONLY (required)
#   PLUGIN_REPO  owner/name of the plugin repo (default: jwulf/c8ctl-plugin-nano)
#   VERSION      the nanobpmn version being shipped, WITHOUT a leading 'v'
#                (required; e.g. 0.0.12)
#   COMMIT       source commit sha (short or long) to record in the marker
#                (required)
#
# Args:
#   $1  DIST_DIR — directory containing the staged gateway binaries
#       (nanobpm-gateway-rest-server-*). Required.
#
# Final line is machine-parseable: "C8CTL_PUBLISH_OK <version> <marker: bumped|unchanged>".
set -euo pipefail

die() { echo "publish-c8ctl-assets: $*" >&2; exit 1; }

DIST_DIR="${1:-}"
[ -n "$DIST_DIR" ] || die "usage: publish-c8ctl-assets.sh <dist-dir>"
[ -d "$DIST_DIR" ] || die "dist dir not found: $DIST_DIR"

: "${GH_TOKEN:?GH_TOKEN (plugin-repo PAT) is required}"
: "${VERSION:?VERSION (no leading v) is required}"
: "${COMMIT:?COMMIT (source sha) is required}"
PLUGIN_REPO="${PLUGIN_REPO:-jwulf/c8ctl-plugin-nano}"

ver="${VERSION#v}"
sha="${COMMIT:0:7}"

# Collect only the gateway assets from the dist dir (defensive: the dir may hold
# processos binaries too when a caller stages everything into one place).
shopt -s nullglob
assets=("$DIST_DIR"/nanobpm-gateway-rest-server-*)
shopt -u nullglob
[ "${#assets[@]}" -gt 0 ] || die "no gateway binaries (nanobpm-gateway-rest-server-*) in $DIST_DIR"

echo "publish-c8ctl-assets: uploading ${#assets[@]} asset(s) to $PLUGIN_REPO 'binaries' release:"
printf '  %s\n' "${assets[@]}"

export GH_TOKEN
gh release view binaries --repo "$PLUGIN_REPO" >/dev/null 2>&1 || \
  gh release create binaries \
    --repo "$PLUGIN_REPO" \
    --title "nanobpmn binaries (rolling)" \
    --notes "Prebuilt gateway binaries consumed by the release workflow. Auto-updated." \
    --latest=false \
    --prerelease
gh release upload binaries "${assets[@]}" --repo "$PLUGIN_REPO" --clobber

# --- bump the bundled-binary marker (triggers the plugin's npm release) -------
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
git clone --depth 1 \
  "https://x-access-token:${GH_TOKEN}@github.com/${PLUGIN_REPO}.git" "$tmp"

# Previous bundled server version, read BEFORE we overwrite the marker.
prev="$(jq -r '.version // empty' "$tmp/nanobpmn-binary.json" 2>/dev/null || true)"

# Pick the conventional-commit type from the server SemVer delta. "feat" when the
# significant field (pre-1.0: minor; else major/minor) changed, otherwise "fix".
# Unparseable/absent prev -> fix (safe).
commit_type() {
  local p="${1%%-*}" n="${2%%-*}"
  p="${p%%+*}"; n="${n%%+*}"
  [ -z "$p" ] && { echo fix; return; }
  local pa pb na nb
  # Only the significant field (pre-1.0 minor, else major/minor) drives the bump
  # type; the patch field is discarded into a throwaway.
  IFS=. read -r pa pb _ <<<"$p"
  IFS=. read -r na nb _ <<<"$n"
  pa=${pa:-0}; pb=${pb:-0}; na=${na:-0}; nb=${nb:-0}
  if [ "$pa" = 0 ] && [ "$na" = 0 ]; then
    # pre-1.0: the minor field carries "new capability" significance.
    [ "$nb" != "$pb" ] && echo feat || echo fix
  else
    { [ "$na" != "$pa" ] || [ "$nb" != "$pb" ]; } && echo feat || echo fix
  fi
}
type="$(commit_type "$prev" "$ver")"

cat > "$tmp/nanobpmn-binary.json" <<JSON
{
  "version": "${ver}",
  "commit": "${sha}",
  "updated": "${now}"
}
JSON
(
  cd "$tmp"
  if git diff --quiet -- nanobpmn-binary.json; then
    echo "publish-c8ctl-assets: marker unchanged; nothing to release."
    echo "C8CTL_PUBLISH_OK ${ver} unchanged"
    exit 0
  fi
  git config user.name "nanobpmn-ci"
  git config user.email "nanobpmn-ci@users.noreply.github.com"
  git add nanobpmn-binary.json
  git commit \
    -m "${type}(binary): bundle nanobpmn ${ver} (${sha})" \
    -m "Bundled nano server ${prev:-unknown} -> ${ver}."
  git push origin HEAD:main
  echo "C8CTL_PUBLISH_OK ${ver} bumped"
)
