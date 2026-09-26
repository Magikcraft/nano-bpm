#!/usr/bin/env bash
# Generate (or check) the committed trace-validation fixtures (#1226,
# Deliverable B).
#
#   formal/tla/gen-traces.sh            # regenerate formal/tla/traces/<Spec>/<Model>.json
#   formal/tla/gen-traces.sh --check    # regenerate to a temp dir and fail on drift
#
# For every model listed in a spec descriptor's SPEC_TRACE_MODELS, this runs TLC
# in tool mode over a generated witness module whose invariant `~completed`
# forces TLC to emit the shortest completing behaviour as a counterexample. A
# node script (trace/parse.mjs) turns that behaviour + the TLC-evaluated process
# graph into a machine-readable fixture, which the Rust trace-validation harness
# (engine-core/tests/trace_validation) replays against the real engine.
#
# The fixtures are a checked-in derived artifact: `--check` (run in CI, after
# node is available in the `formal` job) regenerates them and `git diff`s, so a
# spec change that alters a behaviour fails until the fixtures are refreshed —
# the same drift-guard discipline as formal/parity.
#
# Needs Java (for TLC) and node. TLC is fetched/pinned by check.sh; this script
# reuses that cache via TLA2TOOLS_JAR when set, else resolves it the same way.
set -euo pipefail

mode="write"
[[ "${1:-}" == "--check" ]] && mode="check"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

TLA_VERSION="1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
sha256() {
  if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}
if [[ -n "${TLA2TOOLS_JAR:-}" && -f "${TLA2TOOLS_JAR}" ]]; then
  jar="$TLA2TOOLS_JAR"
else
  jar="${XDG_CACHE_HOME:-$HOME/.cache}/nanobpm-formal/tla2tools-$TLA_VERSION.jar"
fi
[[ -f "$jar" && "$(sha256 "$jar")" == "$TLA_SHA256" ]] || {
  echo "error: tla2tools jar missing or unpinned at $jar; run formal/tla/check.sh first to fetch it" >&2
  exit 1
}

outroot="$here/traces"
tmproot="$(mktemp -d)"
trap 'rm -rf "$tmproot"' EXIT

# The CONSTANTS block for a spec, taken from its descriptor.
constants_block() { printf '    %s\n' "${SPEC_CONSTANTS[@]}"; }

gen_one() { # spec_name models_dir model out_json
  local sname="$1" mdir="$2" model="$3" out="$4"
  local work; work="$tmproot/$model"; mkdir -p "$work"
  # Copy the model, its spec base module and anything they need (flat corpus).
  cp "$here/$mdir"/*.tla "$work/" 2>/dev/null || true
  local wit="${model}_trace"
  cat >"$work/$wit.tla" <<EOF
---- MODULE $wit ----
EXTENDS $model, TLC
Witness_NotDone == ~completed
ASSUME PrintT(<<"GRAPHJSON",
    [nodes |-> MCNodes, kind |-> [n \\in MCNodes |-> MCKind[n]],
     edges |-> MCEdges, start |-> MCStart]>>)
====
EOF
  {
    echo "SPECIFICATION Spec"
    echo "CONSTANTS"
    constants_block
    echo "INVARIANT Witness_NotDone"
  } >"$work/$wit.cfg"
  # -workers 1 keeps BFS deterministic, so the shortest counterexample (and thus
  # the fixture) is stable. -deadlock disables deadlock checking so only the
  # witness invariant preempts. tool mode frames each state for the parser.
  ( cd "$work" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers 1 -tool -deadlock \
      -cleanup -config "$wit.cfg" "$wit.tla" ) >"$work/tlc.out" 2>&1 || true
  node "$here/trace/parse.mjs" --spec "$sname" --model "$model" <"$work/tlc.out" >"$out"
}

run_spec() { # spec_file
  set -euo pipefail
  SPEC_NAME="" SPEC_MODELS_DIR="." SPEC_CONSTANTS=() SPEC_TRACE_MODELS=()
  # shellcheck disable=SC1090
  source "$1"
  [[ ${#SPEC_TRACE_MODELS[@]} -gt 0 ]] || return 0
  local dest="$outroot/$SPEC_NAME"
  local m out
  for m in "${SPEC_TRACE_MODELS[@]}"; do
    if [[ "$mode" == "write" ]]; then
      mkdir -p "$dest"; out="$dest/$m.json"
      gen_one "$SPEC_NAME" "$SPEC_MODELS_DIR" "$m" "$out"
      echo "wrote $out"
    else
      out="$tmproot/$SPEC_NAME-$m.json"
      gen_one "$SPEC_NAME" "$SPEC_MODELS_DIR" "$m" "$out"
      if ! diff -u "$dest/$m.json" "$out" >/dev/null 2>&1; then
        echo "FAIL trace fixture drift: $SPEC_NAME/$m (run formal/tla/gen-traces.sh and commit)" >&2
        diff -u "$dest/$m.json" "$out" >&2 || true
        return 1
      fi
      echo "ok    $SPEC_NAME/$m trace fixture current"
    fi
  done
}

status=0
for s in "$here"/specs/*.spec; do
  ( run_spec "$s" ) || status=1
done
exit $status
