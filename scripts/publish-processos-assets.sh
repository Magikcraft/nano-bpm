#!/usr/bin/env bash
# publish-processos-assets.sh — distribute already-built ProcessOS binaries: attach
# them to the GitHub Release for the tag IN THIS (private) repo and mirror them to
# the public S3 bucket. ProcessOS binaries are distributed only to people with
# access to this repo and are NEVER exposed on the public plugin repo.
#
# Single source of truth for the ProcessOS distribution side-effects, shared by:
#   * .github/workflows/distribute-binaries.yml   (cheap secret-bearing publish of
#     LOCALLY cross-compiled binaries), and
#   * .github/workflows/publish-processos-binaries.yml (manual clean-room CI
#     fallback that builds the binaries in-matrix).
# Both stage the processos binaries into a dist/ dir and call this script, so the
# release-attach + S3 recipe never drifts between them. Every step is idempotent
# (--clobber / cp overwrite), so it is safe to run again over an already-published
# release (e.g. the local build already attached the assets to this repo's
# release before the distribute workflow re-runs the S3 mirror).
#
# What it does (mirrors the old inline `publish` job):
#   1. Attach the processos-* binaries to this repo's Release for the tag (created
#      if absent). A rolling `processos-binaries` prerelease is used when there is
#      no tag (manual run).
#   2. Mirror the binaries to S3: a tag push writes an immutable per-version copy
#      under processos/<tag>/ AND refreshes the rolling processos/latest/ pointer;
#      a no-tag run refreshes latest/ only. Skipped (not failed) when AWS creds
#      are absent.
#   3. For a tag, publish a small version.json next to the binaries (per-tag +
#      latest/) so token-less clients can learn the latest version.
#
# Inputs (env):
#   GH_TOKEN           token with Contents:write on THIS repo (required for the
#                      release attach). In CI: ${{ github.token }}. Locally: your
#                      gh auth token.
#   REPO               owner/name of THIS repo for the release attach (optional;
#                      defaults to the gh-resolved repo of $PWD).
#   TAG                the git tag being released (e.g. v0.0.12). Empty => rolling
#                      prerelease + latest-only S3.
#   COMMIT             source commit sha recorded in version.json (required).
#   S3_BUCKET          target bucket (default: sitapati-storage).
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  when set, enable the S3 mirror.
#   AWS_DEFAULT_REGION default us-east-1.
#
# Args:
#   $1  DIST_DIR — directory containing the staged processos binaries
#       (processos-*). Required.
#
# Final line is machine-parseable: "PROCESSOS_PUBLISH_OK <tag-or-rolling> <s3: mirrored|skipped>".
set -euo pipefail

die() { echo "publish-processos-assets: $*" >&2; exit 1; }

DIST_DIR="${1:-}"
[ -n "$DIST_DIR" ] || die "usage: publish-processos-assets.sh <dist-dir>"
[ -d "$DIST_DIR" ] || die "dist dir not found: $DIST_DIR"

: "${GH_TOKEN:?GH_TOKEN (contents:write on this repo) is required}"
: "${COMMIT:?COMMIT (source sha) is required}"
TAG="${TAG:-}"
S3_BUCKET="${S3_BUCKET:-sitapati-storage}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_DEFAULT_REGION

# Collect only the processos assets (defensive: dir may hold gateway bins too).
shopt -s nullglob
assets=("$DIST_DIR"/processos-*)
shopt -u nullglob
[ "${#assets[@]}" -gt 0 ] || die "no ProcessOS binaries (processos-*) in $DIST_DIR"

echo "publish-processos-assets: ${#assets[@]} asset(s):"
printf '  %s\n' "${assets[@]}"

# --- 1. attach to this repo's release ---------------------------------------
repo_args=()
[ -n "${REPO:-}" ] && repo_args=(--repo "$REPO")
export GH_TOKEN
if [ -n "$TAG" ]; then
  rel="$TAG"
  gh release view "$rel" ${repo_args[@]+"${repo_args[@]}"} >/dev/null 2>&1 || \
    gh release create "$rel" ${repo_args[@]+"${repo_args[@]}"} \
      --title "$rel" \
      --notes "ProcessOS binaries for $rel (private — Nano BPM access only)." \
      --verify-tag
else
  rel="processos-binaries"
  gh release view "$rel" ${repo_args[@]+"${repo_args[@]}"} >/dev/null 2>&1 || \
    gh release create "$rel" ${repo_args[@]+"${repo_args[@]}"} \
      --title "ProcessOS binaries (rolling)" \
      --notes "Prebuilt ProcessOS binaries from manual runs. Auto-updated." \
      --latest=false \
      --prerelease
fi
gh release upload "$rel" "${assets[@]}" ${repo_args[@]+"${repo_args[@]}"} --clobber

# --- 2. S3 mirror (skipped when creds absent) --------------------------------
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
  echo "publish-processos-assets: AWS credentials absent — skipping S3 mirror."
  echo "PROCESSOS_PUBLISH_OK ${TAG:-rolling} skipped"
  exit 0
fi
command -v aws >/dev/null 2>&1 || die "aws CLI not found but AWS creds are set — install awscli."

upload_to() {
  # $1 = s3 key prefix (e.g. processos/v0.0.2 or processos/latest)
  local f name
  for f in "${assets[@]}"; do
    name="$(basename "$f")"
    echo "↑ s3://${S3_BUCKET}/$1/${name}"
    aws s3 cp "$f" "s3://${S3_BUCKET}/$1/${name}" \
      --no-progress \
      --content-type application/octet-stream \
      --cache-control "public, max-age=300"
  done
}
if [ -n "$TAG" ]; then
  upload_to "processos/${TAG}"
fi
upload_to "processos/latest"

# --- 3. version.json (tag only) ----------------------------------------------
if [ -n "$TAG" ]; then
  version="${TAG#v}"
  names="$(for f in "${assets[@]}"; do basename "$f"; done | sed 's/.*/"&"/' | paste -sd, -)"
  vjson="$(mktemp)"
  cat > "$vjson" <<JSON
{
  "version": "${version}",
  "tag": "${TAG}",
  "commit": "${COMMIT}",
  "updated": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "assets": [${names}]
}
JSON
  echo "version.json:"; cat "$vjson"
  for prefix in "processos/${TAG}" "processos/latest"; do
    echo "↑ s3://${S3_BUCKET}/${prefix}/version.json"
    aws s3 cp "$vjson" "s3://${S3_BUCKET}/${prefix}/version.json" \
      --no-progress \
      --content-type application/json \
      --cache-control "public, max-age=300"
  done
  rm -f "$vjson"
fi

echo "PROCESSOS_PUBLISH_OK ${TAG:-rolling} mirrored"
