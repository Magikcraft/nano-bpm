#!/usr/bin/env bash
# Model-check every TLA+ model in formal/tla with TLC, and compare each result
# with its expected outcome.
#
#   formal/tla/check.sh            # check every model
#   formal/tla/check.sh MCFoo ...  # check the named models only
#
# The EXPECTED table below is the single record of what each model should do.
# `pass` means TLC finds no error: every invariant and property holds, and no
# state deadlocks. `violates:<Invariant>` records a known engine defect that
# the model reproduces: TLC must report that invariant as violated (possibly
# among others). The fix PR updates the spec to model the fixed engine. TLC
# then stops reporting the violation, and this script fails until the entry is
# flipped to `pass`. That is the ratchet: a known bug cannot be forgotten, and
# the fixed behaviour stays guarded. (The spec cannot see the Rust code;
# trace validation, #1226, closes that gap.)
#
# Set FORMAL_LOG_DIR to keep each model's generated .cfg and full TLC log
# (including counterexample traces).
#
# TLC is pinned by version and SHA-256. Set TLA2TOOLS_JAR to use a pre-fetched
# jar; it must match the pinned hash.
set -euo pipefail

TLA_VERSION="1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"

# model                       expected outcome
EXPECTED=(
  "MCParallelDiamond           pass"
  "MCInclusiveDiamond          pass"
  "MCChainedInclusive          pass"
  "MCInclusiveInParallel       pass"
  "MCExclusiveLoop             pass"
  "MCParallelDuplicateFlows    pass"
  "MCParallelJoinMultiArrival  violates:ParallelJoinWaitsForEveryFlow  #1233"
)

# Every model is checked against the same, full property set. The generated
# TLC config is identical for all models, so none can silently drop a property.
# JoinFiresAtMostOnce only holds without cycles, so the spec guards it with
# `Acyclic`, which is derived from the graph rather than declared.
INVARIANTS=(TypeOK JoinBookkeepingCoherent ParallelJoinWaitsForEveryFlow NoStuckInstance JoinFiresAtMostOnce)
PROPERTIES=(Termination)

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

sha256() {
  if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

fetch_tla2tools() {
  mkdir -p "$(dirname "$1")"
  echo "fetching tla2tools $TLA_VERSION -> $1"
  curl -fsSL -o "$1.tmp" \
    "https://github.com/tlaplus/tlaplus/releases/download/v$TLA_VERSION/tla2tools.jar"
  # Verify before the jar enters the cache, so a bad download is never kept.
  if [[ "$(sha256 "$1.tmp")" != "$TLA_SHA256" ]]; then
    rm -f "$1.tmp"
    echo "error: downloaded tla2tools does not match the pinned SHA-256 for $TLA_VERSION" >&2
    exit 1
  fi
  mv "$1.tmp" "$1"
}

if [[ -n "${TLA2TOOLS_JAR:-}" ]]; then
  jar="$TLA2TOOLS_JAR"
  if [[ ! -f "$jar" || "$(sha256 "$jar")" != "$TLA_SHA256" ]]; then
    echo "error: TLA2TOOLS_JAR=$jar is missing or does not match the pinned SHA-256 for $TLA_VERSION" >&2
    exit 1
  fi
else
  jar="${XDG_CACHE_HOME:-$HOME/.cache}/nanobpm-formal/tla2tools-$TLA_VERSION.jar"
  # A cached jar that no longer matches (for example, corrupted) is replaced
  # instead of failing every run.
  if [[ -f "$jar" && "$(sha256 "$jar")" != "$TLA_SHA256" ]]; then
    echo "cached $jar does not match the pinned SHA-256; refetching"
    rm -f "$jar"
  fi
  [[ -f "$jar" ]] || fetch_tla2tools "$jar"
fi

# bash 3.2 (macOS) has no associative arrays, so look outcomes up by scan.
expected_outcome() {
  local row model outcome
  for row in "${EXPECTED[@]}"; do
    read -r model outcome _ <<<"$row"
    if [[ "$model" == "$1" ]]; then echo "$outcome"; return 0; fi
  done
  return 0
}

write_cfg() { # out
  {
    echo "SPECIFICATION Spec"
    echo "CONSTANTS"
    echo "    Nodes <- MCNodes"
    echo "    Kind  <- MCKind"
    echo "    Flows <- MCFlows"
    echo "    Src   <- MCSrc"
    echo "    Tgt   <- MCTgt"
    echo "    Start <- MCStart"
    echo "INVARIANTS"
    printf '    %s\n' "${INVARIANTS[@]}"
    echo "PROPERTIES"
    printf '    %s\n' "${PROPERTIES[@]}"
  } >"$1"
}

# Guard against drift between the table and the model files, in both directions.
status=0
for tla in MC*.tla; do
  m="${tla%.tla}"
  [[ -n "$(expected_outcome "$m")" ]] || { echo "error: $tla has no entry in EXPECTED" >&2; status=1; }
done
seen=" "
for row in "${EXPECTED[@]}"; do
  read -r m _ <<<"$row"
  if [[ "$seen" == *" $m "* ]]; then
    echo "error: EXPECTED lists $m more than once" >&2; status=1
  fi
  seen="$seen$m "
  [[ -f "$m.tla" ]] || { echo "error: EXPECTED lists $m but $m.tla does not exist" >&2; status=1; }
done
[[ $status -eq 0 ]] || exit $status

models=()
if [[ $# -gt 0 ]]; then
  models=("$@")
else
  for row in "${EXPECTED[@]}"; do read -r m _ <<<"$row"; models+=("$m"); done
fi

metadir="$(mktemp -d)"
trap 'rm -rf "$metadir"' EXIT

for m in "${models[@]}"; do
  want="$(expected_outcome "$m")"
  [[ -n "$want" ]] || { echo "error: unknown model $m" >&2; exit 1; }
  log="$metadir/$m.log"
  cfg="$metadir/$m.cfg"
  write_cfg "$cfg"
  # A known-defect model runs with -continue so TLC reports every violated
  # invariant. Which violation BFS happens to reach first is an accident of
  # state order and not a property of the defect, so the ratchet checks that
  # the expected invariant is among them.
  continue_flag=()
  if [[ "$want" == violates:* ]]; then continue_flag=(-continue); fi
  set +e
  java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -cleanup ${continue_flag[@]+"${continue_flag[@]}"} \
    -metadir "$metadir/$m" -config "$cfg" "$m.tla" >"$log" 2>&1
  code=$?
  set -e

  violated="$(grep -oE 'Invariant [A-Za-z0-9_]+ is violated' "$log" | awk '{print $2}' | sort -u | tr '\n' ' ' || true)"
  if [[ $code -eq 0 ]] && grep -q "Model checking completed. No error has been found." "$log"; then
    got="pass"
  elif [[ -n "$violated" ]]; then
    got="violates:${violated% }"
  elif grep -qE 'Temporal properties were violated|Deadlock reached' "$log"; then
    got="violates:$(grep -oE 'Temporal properties were violated|Deadlock reached' "$log" | head -1 | tr ' ' '_')"
  else
    got="error(exit $code)"
  fi

  matches=false
  if [[ "$want" == pass ]]; then
    if [[ "$got" == pass ]]; then matches=true; fi
  elif [[ " $violated " == *" ${want#violates:} "* ]]; then
    matches=true
  fi

  if [[ -n "${FORMAL_LOG_DIR:-}" ]]; then
    mkdir -p "$FORMAL_LOG_DIR"
    cp "$cfg" "$log" "$FORMAL_LOG_DIR/"
  fi

  states="$(grep -oE '[0-9,]+ distinct states found' "$log" | tail -1 || true)"
  if grep -q '^Warning' "$log"; then
    echo "FAIL  $m  TLC emitted a warning" >&2
    grep -A1 '^Warning' "$log" >&2
    status=1
  elif $matches; then
    echo "ok    $m  $got  ($states)"
  else
    echo "FAIL  $m  expected $want, got $got" >&2
    cat "$log" >&2
    status=1
  fi
done
exit $status
