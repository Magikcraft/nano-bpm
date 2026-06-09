#!/usr/bin/env bash
#
# Generate the Rust REST layer (models + axum router + Api traits) and the server
# stub implementations from the bundled OpenAPI specification.
#
# This project is self-contained: the spec lives under spec/ and the generator
# runs inside the official openapi-generator-cli Docker image, so no local
# install is required.
#
# Output (generated/ and server/src/stub_impls.rs) and the temporary sanitized
# spec (build/spec/) are build artifacts and are git-ignored: regenerated on
# demand from the spec.
set -euo pipefail

# Version of the openapi-generator CLI image.
OPENAPI_GENERATOR_VERSION="${OPENAPI_GENERATOR_VERSION:-v7.21.0}"
IMAGE="openapitools/openapi-generator-cli:${OPENAPI_GENERATOR_VERSION}"

# Resolve paths relative to the project root so the script works from anywhere.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SPEC_DIR_REL="spec"
SPEC_ENTRYPOINT="rest-api.yaml"
# The source spec under spec/ is never edited. It is first sanitized into a
# temporary build directory (see preprocess-spec.py) that the generator consumes.
SANITIZED_SPEC_DIR_REL="build/spec"
SPEC_REL="${SANITIZED_SPEC_DIR_REL}/${SPEC_ENTRYPOINT}"
OUTPUT_REL="generated"
CONFIG_REL="openapi-generator-config.yaml"

echo "Sanitizing spec into ${SANITIZED_SPEC_DIR_REL}"
python3 "${SCRIPT_DIR}/preprocess-spec.py" \
  "${PROJECT_ROOT}/${SPEC_DIR_REL}" \
  "${PROJECT_ROOT}/${SANITIZED_SPEC_DIR_REL}"

echo "Generating Rust REST layer with ${IMAGE}"
echo "  spec:   ${SPEC_REL}"
echo "  output: ${OUTPUT_REL}"

# The spec entrypoint $refs sibling YAML files, so the project root is mounted to
# keep relative references resolvable.
docker run --rm \
  -u "$(id -u):$(id -g)" \
  -v "${PROJECT_ROOT}:/local" \
  "${IMAGE}" generate \
  -g rust-axum \
  -i "/local/${SPEC_REL}" \
  -o "/local/${OUTPUT_REL}" \
  -c "/local/${CONFIG_REL}" \
  --skip-validate-spec

echo "Post-processing generated code to fix known rust-axum generator bugs"
python3 "${SCRIPT_DIR}/postprocess-generated.py" "${PROJECT_ROOT}/${OUTPUT_REL}"

echo "Generating stub trait implementations for the server"
python3 "${SCRIPT_DIR}/gen-stub-server.py" \
  "${PROJECT_ROOT}/${OUTPUT_REL}/src/apis" \
  "${PROJECT_ROOT}/server/src/stub_impls.rs"

# Format with the host toolchain (the generator image has no rustfmt).
if command -v cargo >/dev/null 2>&1; then
  echo "Formatting generated crate with cargo fmt"
  (cd "${PROJECT_ROOT}/${OUTPUT_REL}" && cargo fmt) || true
  (cd "${PROJECT_ROOT}/server" && cargo fmt) || true
fi

echo "Done. Generated crate is in ${OUTPUT_REL}"
