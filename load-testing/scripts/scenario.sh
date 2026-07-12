#!/bin/bash
# scenario.sh — run one cell of the standard test matrix end to end, from the loadbox.
#
#   (optional) build feature branch + console on the build host
#        -> stage binary to all nodes
#        -> deploy (wipe + restart OOTB defaults, with disk preflight)
#        -> soak (payload x duration) with a background rail monitor
#        -> print RESULT lines + a monitor tail
#
# The matrix: payload {neg,50kb} x duration {5m,30m} on OOTB defaults.
#
# Usage:
#   scenario.sh <neg|50kb> <5m|30m> [--build [--branch <branch>]] [--label <label>]
#
#   --build            rebuild on the build host (node0) with the console embedded
#                      (build-server.sh) and stage before deploying. Omit to reuse
#                      the already-staged ~/nano-gw-new.
#   --branch <branch>  git branch to build (default: current checkout on build host)
#   --label <label>    result/monitor label (default: scenario-<payload>-<duration>)
set -eu
DIR="$(cd "$(dirname "$0")" && pwd)"
SSHK="-i $HOME/.ssh/google_compute_engine -o StrictHostKeyChecking=no -o ConnectTimeout=10"
BUILD_HOST="10.128.0.19"
BUILD_REPO="\$HOME/build-console"   # repo checkout on the build host

PAYLOAD="${1:?usage: scenario.sh <neg|50kb> <5m|30m> [--build [--branch B]] [--label L]}"
DURATION="${2:?usage: scenario.sh <neg|50kb> <5m|30m> [--build [--branch B]] [--label L]}"
shift 2
BUILD=0; BRANCH=""; LABEL=""
while [ $# -gt 0 ]; do
  case "$1" in
    --build) BUILD=1; shift ;;
    --branch) BRANCH="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
LABEL="${LABEL:-scenario-${PAYLOAD}-${DURATION}}"
case "$DURATION" in 5m) SECS=300 ;; 30m) SECS=1800 ;; *) SECS="$DURATION" ;; esac

if [ "$BUILD" -eq 1 ]; then
  echo "=== [1/4] build feature branch + console on build host ($BUILD_HOST) ==="
  CO=""; [ -n "$BRANCH" ] && CO="git fetch --all --quiet && git checkout '$BRANCH' && git pull --quiet;"
  ssh $SSHK "$BUILD_HOST" "cd $BUILD_REPO && $CO CARGO=\$HOME/.cargo/bin/cargo load-testing/scripts/build-server.sh --stage \$HOME/nano-gw-new"
  echo "=== [2/4] stage binary to all nodes ==="
  "$DIR/stage-binary.sh" --from node0
else
  echo "=== [1-2/4] skip build (reuse already-staged ~/nano-gw-new) ==="
fi

echo "=== [3/4] deploy OOTB defaults (wipe + restart + disk preflight) ==="
"$DIR/deploy.sh" default

echo "=== [4/4] soak $PAYLOAD $DURATION (label=$LABEL) ==="
ITERS=$(( SECS/60 + 2 ))
"$DIR/monitor.sh" "$LABEL" "$ITERS" 60 > "$HOME/$LABEL-monitor.log" 2>&1 &
MON_PID=$!
"$DIR/soak.sh" "$PAYLOAD" "$DURATION" "$LABEL"
kill "$MON_PID" 2>/dev/null || true

echo "=== monitor tail ($LABEL) ==="
tail -8 "$HOME/$LABEL-monitor.log" 2>/dev/null
echo "=== SCENARIO DONE: $LABEL ==="
