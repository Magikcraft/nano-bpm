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
# among others). Once the defect is fixed, TLC stops reporting that
# violation and this script fails until the entry is flipped to `pass`. That is
# the ratchet: a fixed bug cannot quietly lose its guard, and a known bug cannot
# be forgotten.
#
# TLC is pinned by version and SHA-256. Set TLA2TOOLS_JAR to use a pre-fetched
# jar; it must match the pinned hash.
set -euo pipefail

TLA_VERSION="1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"

EXPECTED=(
  "MCParallelDiamond           pass"
  "MCInclusiveDiamond          pass"
  "MCChainedInclusive          pass"
  "MCInclusiveInParallel       pass"
  "MCExclusiveLoop             pass"
  "MCParallelDuplicateFlows    pass"
  "MCParallelJoinMultiArrival  violates:ParallelJoinWaitsForEveryFlow  #1233"
)

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

sha256() {
  if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

jar="${TLA2TOOLS_JAR:-${XDG_CACHE_HOME:-$HOME/.cache}/nanobpm-formal/tla2tools-$TLA_VERSION.jar}"
if [[ ! -f "$jar" ]]; then
  mkdir -p "$(dirname "$jar")"
  echo "fetching tla2tools $TLA_VERSION -> $jar"
  curl -fsSL -o "$jar.tmp" \
    "https://github.com/tlaplus/tlaplus/releases/download/v$TLA_VERSION/tla2tools.jar"
  mv "$jar.tmp" "$jar"
fi
if [[ "$(sha256 "$jar")" != "$TLA_SHA256" ]]; then
  echo "error: $jar does not match the pinned SHA-256 for tla2tools $TLA_VERSION" >&2
  exit 1
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

# Guard against drift between the table and the model files, in both directions.
status=0
for tla in MC*.tla; do
  m="${tla%.tla}"
  [[ -n "$(expected_outcome "$m")" ]] || { echo "error: $tla has no entry in EXPECTED" >&2; status=1; }
  [[ -f "$m.cfg" ]] || { echo "error: $tla has no $m.cfg" >&2; status=1; }
done
for row in "${EXPECTED[@]}"; do
  read -r m _ <<<"$row"
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
  # A known-defect model runs with -continue so TLC reports every violated
  # invariant. Which violation BFS happens to reach first is an accident of
  # state order and not a property of the defect, so the ratchet checks that
  # the expected invariant is among them.
  continue_flag=()
  if [[ "$want" == violates:* ]]; then continue_flag=(-continue); fi
  set +e
  java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -cleanup ${continue_flag[@]+"${continue_flag[@]}"} \
    -metadir "$metadir/$m" -config "$m.cfg" "$m.tla" >"$log" 2>&1
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
