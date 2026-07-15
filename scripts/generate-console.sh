#!/usr/bin/env bash
#
# Generate the Rust REST layer (models + axum router + Api traits) for the
# **console API** from spec-console/console-api.yaml.
#
# This mirrors scripts/generate.sh (which generates the Camunda REST surface
# under spec/), but for the hand-authored console API. The console spec is a
# single self-contained file, so there is no multi-file preprocess/patch step.
#
# Output (generated-console/) is a git-ignored build artifact: regenerated on
# demand from the spec. The server crate depends on it under the `console`
# feature and implements the generated per-tag Api traits.
set -euo pipefail

OPENAPI_GENERATOR_VERSION="${OPENAPI_GENERATOR_VERSION:-7.21.0}"
OPENAPI_GENERATOR_VERSION="${OPENAPI_GENERATOR_VERSION#v}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SPEC_REL="spec-console/console-api.yaml"
OUTPUT_REL="generated-console"
CONFIG_REL="openapi-generator-config-console.yaml"

TOOLS_DIR="${PROJECT_ROOT}/build/tools"
JAR="${TOOLS_DIR}/openapi-generator-cli-${OPENAPI_GENERATOR_VERSION}.jar"
JAR_URL="https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/${OPENAPI_GENERATOR_VERSION}/openapi-generator-cli-${OPENAPI_GENERATOR_VERSION}.jar"

if ! command -v java >/dev/null 2>&1; then
  echo "error: 'java' not found on PATH. Install a JRE/JDK (Java 11+) to run the" >&2
  echo "       openapi-generator CLI, or set JAVA_HOME and add java to PATH." >&2
  exit 1
fi

if command -v uv >/dev/null 2>&1; then
  PY=(uv run --project "${PROJECT_ROOT}" python)
else
  echo "warning: 'uv' not found; falling back to system python3 (needs PyYAML installed)." >&2
  PY=(python3)
fi

# Fetch the generator JAR once, into the build cache (shared with generate.sh).
if [[ ! -f "${JAR}" ]]; then
  echo "Downloading openapi-generator-cli ${OPENAPI_GENERATOR_VERSION} into build/tools"
  mkdir -p "${TOOLS_DIR}"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o "${JAR}.tmp" "${JAR_URL}"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "${JAR}.tmp" "${JAR_URL}"
  else
    echo "error: neither 'curl' nor 'wget' is available to download the generator JAR." >&2
    exit 1
  fi
  mv "${JAR}.tmp" "${JAR}"
fi

echo "Generating console API Rust layer with openapi-generator-cli ${OPENAPI_GENERATOR_VERSION} (local Java)"
echo "  spec:   ${SPEC_REL}"
echo "  output: ${OUTPUT_REL}"

java -jar "${JAR}" generate \
  -g rust-axum \
  -i "${PROJECT_ROOT}/${SPEC_REL}" \
  -o "${PROJECT_ROOT}/${OUTPUT_REL}" \
  -c "${PROJECT_ROOT}/${CONFIG_REL}" \
  --skip-validate-spec

echo "Post-processing generated code to fix known rust-axum generator bugs"
"${PY[@]}" "${SCRIPT_DIR}/postprocess-generated.py" "${PROJECT_ROOT}/${OUTPUT_REL}"

if command -v cargo >/dev/null 2>&1; then
  echo "Formatting generated console crate with cargo fmt"
  (cd "${PROJECT_ROOT}/${OUTPUT_REL}" && cargo fmt) || true
fi

echo "Done. Generated console crate is in ${OUTPUT_REL}"
