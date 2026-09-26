#!/usr/bin/env bash
# FEEL differential fuzz: generate a corpus from the Lean reference semantics
# (formal/lean/Feel) and check the Rust FEEL evaluator (engine-core/src/feel)
# agrees on every case. Any divergence is a hard failure — no tolerated
# mismatch, no retries.
#
# Usage: formal/lean/feel-diff.sh [num-cases]
#   num-cases defaults to $FEEL_FUZZ_CASES, else 3000.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # formal/lean
repo_root="$(cd "$here/../.." && pwd)"
cases="${1:-${FEEL_FUZZ_CASES:-3000}}"

corpus="$(mktemp -t feel-corpus.XXXXXX.tsv)"
trap 'rm -f "$corpus"' EXIT

echo "==> lake build (all Lean targets)"
( cd "$here" && lake build )

echo "==> generating $cases FEEL cases from the Lean reference"
( cd "$here" && lake exe feelfuzz "$cases" ) > "$corpus"

echo "==> checking Rust engine-core/src/feel against the Lean reference"
( cd "$repo_root/engine-core" && cargo run --quiet --example feel_diff -- "$corpus" )
