#!/usr/bin/env bash
#
# Generate the Rust REST layer (models + axum router + Api traits) and the server
# stub implementations from the bundled OpenAPI specification.
#
# This project is self-contained: the spec lives under spec/ and the generator
# runs from the official openapi-generator-cli JAR via the local Java runtime —
# no Docker and no global install required. The JAR is downloaded once (pinned by
# version) into a git-ignored cache under build/.
#
# Output (generated/ and server/src/stub_impls.rs) and the temporary sanitized
# spec (build/spec/) are build artifacts and are git-ignored: regenerated on
# demand from the spec.
set -euo pipefail

# Version of the openapi-generator CLI. A leading "v" is tolerated (the former
# Docker image tag used it); the Maven coordinate has no "v".
OPENAPI_GENERATOR_VERSION="${OPENAPI_GENERATOR_VERSION:-7.21.0}"
OPENAPI_GENERATOR_VERSION="${OPENAPI_GENERATOR_VERSION#v}"

# Resolve paths relative to the project root so the script works from anywhere.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SPEC_DIR_REL="spec"
SPEC_ENTRYPOINT="rest-api.yaml"
# The source spec under spec/ is never edited. It is first sanitized (and local
# overlays from spec-patches/ are applied) into a temporary build directory (see
# preprocess-spec.py) that the generator consumes.
SANITIZED_SPEC_DIR_REL="build/spec"
SPEC_REL="${SANITIZED_SPEC_DIR_REL}/${SPEC_ENTRYPOINT}"
OUTPUT_REL="generated"
CONFIG_REL="openapi-generator-config.yaml"
PATCHES_REL="spec-patches/patches.yaml"

# Where the pinned generator JAR is cached (git-ignored: build/ is ignored).
TOOLS_DIR="${PROJECT_ROOT}/build/tools"
JAR="${TOOLS_DIR}/openapi-generator-cli-${OPENAPI_GENERATOR_VERSION}.jar"
JAR_URL="https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/${OPENAPI_GENERATOR_VERSION}/openapi-generator-cli-${OPENAPI_GENERATOR_VERSION}.jar"

if ! command -v java >/dev/null 2>&1; then
  echo "error: 'java' not found on PATH. Install a JRE/JDK (Java 11+) to run the" >&2
  echo "       openapi-generator CLI, or set JAVA_HOME and add java to PATH." >&2
  exit 1
fi

# Fetch the generator JAR once, into the build cache.
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

echo "Sanitizing spec into ${SANITIZED_SPEC_DIR_REL}"
python3 "${SCRIPT_DIR}/preprocess-spec.py" \
  "${PROJECT_ROOT}/${SPEC_DIR_REL}" \
  "${PROJECT_ROOT}/${SANITIZED_SPEC_DIR_REL}" \
  "${PROJECT_ROOT}/${PATCHES_REL}"

echo "Generating Rust REST layer with openapi-generator-cli ${OPENAPI_GENERATOR_VERSION} (local Java)"
echo "  spec:   ${SPEC_REL}"
echo "  output: ${OUTPUT_REL}"

# The spec entrypoint $refs sibling YAML files; pointing the generator at the
# sanitized copy under the project root keeps those relative references
# resolvable.
java -jar "${JAR}" generate \
  -g rust-axum \
  -i "${PROJECT_ROOT}/${SPEC_REL}" \
  -o "${PROJECT_ROOT}/${OUTPUT_REL}" \
  -c "${PROJECT_ROOT}/${CONFIG_REL}" \
  --skip-validate-spec

echo "Post-processing generated code to fix known rust-axum generator bugs"
python3 "${SCRIPT_DIR}/postprocess-generated.py" "${PROJECT_ROOT}/${OUTPUT_REL}"

echo "Generating stub trait implementations for the server"
python3 "${SCRIPT_DIR}/gen-stub-server.py" \
  "${PROJECT_ROOT}/${OUTPUT_REL}/src/apis" \
  "${PROJECT_ROOT}/server/src/stub_impls.rs"

# Format the generated artifacts with the host toolchain. Only generated files
# are formatted: the whole generated/ crate, and the single generated
# server/src/stub_impls.rs. The server's hand-written sources are left untouched
# so regeneration never churns tracked, manually formatted code.
if command -v cargo >/dev/null 2>&1; then
  echo "Formatting generated crate with cargo fmt"
  (cd "${PROJECT_ROOT}/${OUTPUT_REL}" && cargo fmt) || true
fi
if command -v rustfmt >/dev/null 2>&1; then
  echo "Formatting generated server stub impls with rustfmt"
  rustfmt --edition 2024 "${PROJECT_ROOT}/server/src/stub_impls.rs" || true
fi

echo "Done. Generated crate is in ${OUTPUT_REL}"
