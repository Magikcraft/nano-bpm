#!/bin/bash
# soak.sh — unified soak runner for the standard test matrix.
#
#   OOTB defaults, 3 loadgens (producers + workers), stream transport.
#   Payload  x  Duration  =  the 2x2 matrix we validate:
#     payload:  neg  -> VAR_BYTES=0            (negligible variables)
#               50kb -> VAR_BYTES=51200 + VAR_MODE=json (honest ~6.7x-compressible)
#     duration: 5m | 30m | <seconds>
#
# A background disk watchdog (disk-guard.sh watch) aborts the run if any node runs
# low on space, and a mid-run disk-write/completes/backlog sample is emitted.
#
# Usage: soak.sh <neg|50kb> <5m|30m|SECONDS> [label]
set -u
SSHK="-i $HOME/.ssh/google_compute_engine -o StrictHostKeyChecking=no -o ConnectTimeout=8"
IPS="10.128.0.19 10.128.0.20 10.128.0.18"
LG="$HOME/rw-build/target/release/loadgen"
GUARD="$(dirname "$0")/disk-guard.sh"; [ -x "$GUARD" ] || GUARD="$HOME/disk-guard.sh"

# Steady operating point (the validated knee): see RUNBOOK.md.
W=200; PC=128; MP=112; RATE=14000; MI=50000

PAYLOAD="${1:?usage: soak.sh <neg|50kb> <5m|30m|SECONDS> [label]}"
DURSPEC="${2:?usage: soak.sh <neg|50kb> <5m|30m|SECONDS> [label]}"
case "$PAYLOAD" in
  neg)  VB=0;     VBENV=() ;;
  50kb) VB=51200; VBENV=(VAR_MODE=json) ;;
  *) echo "payload must be 'neg' or '50kb'"; exit 2 ;;
esac
case "$DURSPEC" in
  5m)  DUR=300 ;;
  30m) DUR=1800 ;;
  *[!0-9]*) echo "duration must be 5m, 30m, or an integer of seconds"; exit 2 ;;
  *) DUR="$DURSPEC" ;;
esac
LABEL="${3:-soak-${PAYLOAD}-${DURSPEC}}"

rm -f "$HOME/$LABEL"-*.out
PDK=$(curl -s --max-time 10 -F resources=@"$HOME/test-job-process.bpmn" \
  http://10.128.0.19:8080/v2/deployments \
  | grep -o "\"processDefinitionKey\":\"[0-9]*\"" | grep -o "[0-9]*" | head -1)
[ -z "$PDK" ] && { echo "[$(date +%H:%M:%S)] $LABEL DEPLOY FAILED"; exit 1; }
echo "[$(date +%H:%M:%S)] $LABEL START payload=$PAYLOAD VB=$VB dur=${DUR}s PDK=$PDK rate=${RATE}/prod MI=$MI"

# Background disk watchdog: kills loadgens (ends the soak) if a node drops below floor.
WATCH_PID=""
if [ -x "$GUARD" ]; then
  "$GUARD" watch "${DISK_FLOOR_GB:-15}" 30 & WATCH_PID=$!
fi

sleep 3
i=0
for ip in $IPS; do
  env BASE_URL=http://$ip:8080 PDK=$PDK WORKERS=$W PROD_CONNS=$PC MAXPAR=$MP RATE=$RATE \
    WARMUP_S=12 DURATION_S=$DUR DRAIN_S=15 MAX_INFLIGHT=$MI TRANSPORT=stream \
    VAR_BYTES=$VB "${VBENV[@]}" \
    "$LG" > "$HOME/$LABEL-$i.out" 2>&1 &
  i=$((i+1))
done

# Mid-run steady-state sample: per-node sda write MB/s + completes/s + backlog.
SNAP_AT=$(( DUR/2 )); [ $SNAP_AT -lt 25 ] && SNAP_AT=25
( sleep $SNAP_AT
  echo "--- [$(date +%H:%M:%S)] $LABEL DISK/THROUGHPUT SAMPLE (10s window) ---"
  for ip in $IPS; do
    read w1 c1 < <(ssh $SSHK $ip "awk '{print \$7}' /sys/block/sda/stat; curl -s localhost:8080/metrics|awk '/^nanobpm_job_completions_total.protocol=.stream/{print \$2}'" 2>/dev/null | paste -sd" ")
    sleep 10
    read w2 c2 < <(ssh $SSHK $ip "awk '{print \$7}' /sys/block/sda/stat; curl -s localhost:8080/metrics|awk '/^nanobpm_job_completions_total.protocol=.stream/{print \$2}'" 2>/dev/null | paste -sd" ")
    mbps=$(awk "BEGIN{printf \"%.0f\", ($w2-$w1)*512/10/1048576}")
    cps=$(awk "BEGIN{printf \"%.0f\", ($c2-$c1)/10}")
    bl=$(ssh $SSHK $ip "curl -s localhost:8080/metrics|awk '/^nanobpm_active_backlog /{print \$2}'" 2>/dev/null)
    echo "  ${ip##*.}: disk_write=${mbps}MB/s completes=${cps}/s backlog=${bl%.*}"
  done
) &
wait
[ -n "$WATCH_PID" ] && kill "$WATCH_PID" 2>/dev/null
echo "[$(date +%H:%M:%S)] $LABEL DONE:"
grep -h "^RESULT" "$HOME/$LABEL"-*.out
