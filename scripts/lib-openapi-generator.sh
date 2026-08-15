#!/usr/bin/env bash
#
# Shared helper for locating and fetching the pinned openapi-generator-cli JAR.
#
# Sourced by scripts/generate.sh and scripts/generate-console.sh so the JAR
# location and (rate-limit tolerant) download logic live in exactly one place —
# no drift between the two generators.
#
# Contract: the caller must have PROJECT_ROOT and OPENAPI_GENERATOR_VERSION set.
# After sourcing, call `ensure_openapi_generator_jar` to populate the `JAR`
# variable with a ready-to-use path, downloading it into the build cache
# (build/tools, git-ignored) on first use.

# shellcheck shell=bash

ensure_openapi_generator_jar() {
  local tools_dir="${PROJECT_ROOT}/build/tools"
  local jar_url="https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/${OPENAPI_GENERATOR_VERSION}/openapi-generator-cli-${OPENAPI_GENERATOR_VERSION}.jar"
  JAR="${tools_dir}/openapi-generator-cli-${OPENAPI_GENERATOR_VERSION}.jar"

  if [[ -f "${JAR}" ]]; then
    return 0
  fi

  echo "Downloading openapi-generator-cli ${OPENAPI_GENERATOR_VERSION} into build/tools"
  mkdir -p "${tools_dir}"

  # Maven Central occasionally rate-limits (HTTP 429) or hiccups on a single
  # request, which must not fail the whole build: retry with exponential backoff
  # so a transient network error is not surfaced as a defect.
  local fetch_jar
  if command -v curl >/dev/null 2>&1; then
    fetch_jar() {
      curl -fsSL --retry 5 --retry-delay 2 --retry-connrefused --retry-all-errors \
        -o "${JAR}.tmp" "${jar_url}"
    }
  elif command -v wget >/dev/null 2>&1; then
    fetch_jar() {
      wget -q --tries=5 --waitretry=2 -O "${JAR}.tmp" "${jar_url}"
    }
  else
    echo "error: neither 'curl' nor 'wget' is available to download the generator JAR." >&2
    exit 1
  fi

  local attempts=5
  local attempt=1
  local backoff
  until fetch_jar; do
    if [[ "${attempt}" -ge "${attempts}" ]]; then
      echo "error: failed to download the generator JAR after ${attempts} attempts." >&2
      rm -f "${JAR}.tmp"
      exit 1
    fi
    backoff=$(( attempt * 5 ))
    echo "warning: download attempt ${attempt} failed; retrying in ${backoff}s..." >&2
    rm -f "${JAR}.tmp"
    sleep "${backoff}"
    attempt=$(( attempt + 1 ))
  done
  mv "${JAR}.tmp" "${JAR}"
}
