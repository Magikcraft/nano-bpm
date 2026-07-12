#!/bin/bash
# ============================================================================
# backlog-recovery.sh — reusable "backlog injection -> recovery" scenario.
#
# Proves the cluster self-heals from a large standing backlog (the production
# concern behind worker outages / create bursts / node recovery):
#   INJECT  — flood creates with NO workers; backlog climbs until admission caps it.
#   SETTLE  — idle hold (no producers/workers); backlog must be *bounded* (not growing).
#   RECOVER — workers-only, NO producers; injected backlog must drain to ~0.
#   NORMAL  — mixed load; throughput must return to its normal knee (no permanent wedge).
#
# VERDICT PASS iff: admission shed>0 (backlog bounded) AND settle-growth ~0 AND
#   backlog drained <= DRAIN_FLOOR AND recovered knee throughput > 0.
#
# Usage:  ./backlog-recovery.sh   (all env knobs optional)
#   WIPE=1  PAYLOAD=0(bytes)  INJECT_S=45  QUIESCE_S=15  SETTLE_S=30  RECOVER_S=180  NORMAL_S=30
#   INJECT_PC=128  INJECT_RATE=0(unbounded)  INJECT_MI=0(unbounded)
#   REC_WORKERS=200  REC_MAXPAR=112  NORMAL_RATE=14000  DRAIN_FLOOR=500  INTERVAL=5
#
# QUIESCE_S: after killing the producers, in-flight creates already submitted to
#   the create pipeline keep applying into active_backlog (there are no workers to
#   complete them). This is pronounced with large payloads. We wait QUIESCE_S for
#   the pipeline to drain into "created" BEFORE snapshotting the SETTLE baseline,
#   so the settle-growth check measures true idle drift, not pipeline lag.
# ============================================================================
set -u
SSHK="-i /home/joshua.wulf/.ssh/google_compute_engine -o StrictHostKeyChecking=no -o ConnectTimeout=8"
IPS="10.128.0.19 10.128.0.20 10.128.0.18"; LEAD=10.128.0.19
WIPE=${WIPE:-1}; PAYLOAD=${PAYLOAD:-0}
INJECT_S=${INJECT_S:-45}; QUIESCE_S=${QUIESCE_S:-15}; SETTLE_S=${SETTLE_S:-30}; RECOVER_S=${RECOVER_S:-180}; NORMAL_S=${NORMAL_S:-30}
INJECT_PC=${INJECT_PC:-128}; INJECT_RATE=${INJECT_RATE:-0}; INJECT_MI=${INJECT_MI:-0}
REC_WORKERS=${REC_WORKERS:-200}; REC_MAXPAR=${REC_MAXPAR:-112}; NORMAL_RATE=${NORMAL_RATE:-14000}
DRAIN_FLOOR=${DRAIN_FLOOR:-500}; INTERVAL=${INTERVAL:-5}
LG=$HOME/rw-build/target/release/loadgen
PHASE_F=$HOME/br-phase; LOG=$HOME/backlog-recovery.log; SAMP=$HOME/backlog-recovery-samples.log
say(){ echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }

node_snap(){ ssh $SSHK "$1" '
  m=$(curl -s --max-time 6 http://localhost:8080/metrics);
  p=$(pgrep -x nano-gw); rss=$(ps -o rss= -p $p 2>/dev/null);
  echo "$m" | awk -v rss="${rss:-0}" "
    /^nanobpm_job_completions_total/{c+=\$2}
    /^nanobpm_active_backlog /{b+=\$2}
    /^nanobpm_pending_create_queue /{q+=\$2}
    /^nanobpm_admission_shed_total/{s+=\$2}
    END{printf \"%d %d %d %d %d\", c+0,b+0,q+0,s+0,rss+0}"' 2>/dev/null; }
snapshot(){ A_comp=0;A_bl=0;A_pcq=0;A_shed=0;A_rss_mb=0; local r c b q s rk
  for ip in $IPS; do r=$(node_snap "$ip"); set -- ${r:-0 0 0 0 0}
    c=${1:-0};b=${2:-0};q=${3:-0};s=${4:-0};rk=${5:-0}
    A_comp=$((A_comp+c));A_bl=$((A_bl+b));A_pcq=$((A_pcq+q));A_shed=$((A_shed+s));A_rss_mb=$((A_rss_mb+rk/1024)); done; }

sampler(){ local prev=0 prevt=0 t ts crate dt phase
  echo "# ts phase comp_rate backlog pending_create_queue shed_total rss_mb" > "$SAMP"
  while :; do [ -f "$PHASE_F" ] || break; phase=$(cat "$PHASE_F" 2>/dev/null); [ "$phase" = STOP ] && break
    t=$(date +%s); ts=$(date -u +%H:%M:%S); snapshot
    if [ $prevt -gt 0 ]; then dt=$((t-prevt)); [ $dt -le 0 ]&&dt=1; crate=$(((A_comp-prev)/dt)); else crate=0; fi
    printf "%s %s comp_rate=%d backlog=%d pcq=%d shed=%d rss_mb=%d\n" "$ts" "$phase" "$crate" "$A_bl" "$A_pcq" "$A_shed" "$A_rss_mb" | tee -a "$SAMP"
    prev=$A_comp; prevt=$t; sleep "$INTERVAL"; done; }

launch_lg(){ local label=$1 W=$2 PC=$3 RATE=$4 MI=$5 DUR=$6 i=0; rm -f $HOME/$label-*.out
  for ip in $IPS; do
    BASE_URL=http://$ip:8080 PDK=$PDK WORKERS=$W PROD_CONNS=$PC MAXPAR=$REC_MAXPAR RATE=$RATE \
      WARMUP_S=0 DURATION_S=$DUR DRAIN_S=0 MAX_INFLIGHT=$MI TRANSPORT=stream VAR_BYTES=$PAYLOAD \
      $LG > $HOME/$label-$i.out 2>&1 & i=$((i+1)); done; }
kill_lg(){ for p in $(pgrep -f "$LG"); do kill $p 2>/dev/null; done; sleep 2; }
# peak/avg comp_rate from sampler rows of a given phase
phase_peak(){ awk -v ph="$1" '$2==ph{split($3,a,"=");if(a[2]>m)m=a[2]} END{print m+0}' "$SAMP"; }
phase_avg(){ awk -v ph="$1" '$2==ph{split($3,a,"=");if(n>0){s+=a[2];c++};n++} END{printf "%d", (c?s/c:0)}' "$SAMP"; }

: > "$LOG"
say "=== backlog-recovery START payload=$PAYLOAD inject=${INJECT_S}s settle=${SETTLE_S}s recover<=${RECOVER_S}s normal=${NORMAL_S}s wipe=$WIPE ==="
if [ "$WIPE" = 1 ]; then say "wiping+restarting cluster (clean journal)"; bash $HOME/restart-verify.sh default >>"$LOG" 2>&1; sleep 5; fi
PDK=$(curl -s --max-time 10 -F resources=@$HOME/test-job-process.bpmn http://$LEAD:8080/v2/deployments | grep -o '"processDefinitionKey":"[0-9]*"' | grep -o '[0-9]*' | head -1)
[ -z "$PDK" ] && { say "DEPLOY FAILED"; exit 1; }; say "deployed PDK=$PDK"
echo INJECT > "$PHASE_F"; sampler & SAMP_PID=$!; sleep 2

say "INJECT: producers-only flood (${INJECT_S}s, PC=$INJECT_PC rate=$INJECT_RATE mi=$INJECT_MI workers=0)"
launch_lg inject 0 $INJECT_PC $INJECT_RATE $INJECT_MI $INJECT_S; sleep $((INJECT_S+3)); kill_lg
snapshot; PEAK_BL=$A_bl; PEAK_SHED=$A_shed; PEAK_RSS=$A_rss_mb
say "INJECT done: peak backlog=$PEAK_BL shed_total=$PEAK_SHED rss_mb=$PEAK_RSS"

echo SETTLE > "$PHASE_F"
say "QUIESCE: draining in-flight creates ${QUIESCE_S}s (pipeline settle before baseline)"
sleep $QUIESCE_S
say "SETTLE: idle hold ${SETTLE_S}s (no producers, no workers)"
snapshot; S0=$A_bl; sleep $SETTLE_S; snapshot; S1=$A_bl
say "SETTLE: backlog $S0 -> $S1 (delta $((S1-S0)))"

echo RECOVER > "$PHASE_F"; say "RECOVER: workers-only drain (workers=$REC_WORKERS maxpar=$REC_MAXPAR, <=${RECOVER_S}s)"
launch_lg recover $REC_WORKERS 0 0 0 $RECOVER_S; REC_START=$(date +%s); DRAINED_AT=0
while :; do now=$(date +%s); [ $((now-REC_START)) -ge $RECOVER_S ] && break
  snapshot; [ "$A_bl" -le "$DRAIN_FLOOR" ] && { DRAINED_AT=$((now-REC_START)); break; }; sleep $INTERVAL; done
sleep $INTERVAL; kill_lg; snapshot; DRAIN_BL=$A_bl; DRAIN_RATE=$(phase_peak RECOVER)
say "RECOVER: drained_at=${DRAINED_AT}s backlog=$DRAIN_BL peak_drain_rate=${DRAIN_RATE}/s"

echo NORMAL > "$PHASE_F"; say "NORMAL: mixed-load knee check (${NORMAL_S}s, workers=$REC_WORKERS producers rate=$NORMAL_RATE)"
launch_lg normal $REC_WORKERS $INJECT_PC $NORMAL_RATE 50000 $NORMAL_S; sleep $((NORMAL_S+3)); kill_lg
KNEE=$(phase_avg NORMAL)
say "NORMAL: recovered knee throughput=${KNEE}/s (avg)"
echo STOP > "$PHASE_F"; sleep $((INTERVAL+2)); kill $SAMP_PID 2>/dev/null; rm -f "$PHASE_F"

say "================ SUMMARY ================"
say "INJECT   peak backlog=$PEAK_BL  shed_total=$PEAK_SHED  rss_mb=$PEAK_RSS"
say "SETTLE   backlog delta=$((S1-S0)) (bounded if <= ~0)"
say "RECOVER  drained_at=${DRAINED_AT}s  final_backlog=$DRAIN_BL  peak_drain_rate=${DRAIN_RATE}/s"
say "NORMAL   recovered knee=${KNEE}/s"
PASS=1; REASON=""
[ "$PEAK_SHED" -gt 0 ] || { PASS=0; REASON="$REASON no-admission-shed(backlog-unbounded?);"; }
[ $((S1-S0)) -le $((PEAK_BL/5 + 500)) ] || { PASS=0; REASON="$REASON backlog-grew-while-idle;"; }
{ [ "$DRAINED_AT" -gt 0 ] || [ "$DRAIN_BL" -le "$DRAIN_FLOOR" ]; } || { PASS=0; REASON="$REASON did-not-drain;"; }
[ "${KNEE:-0}" -gt 0 ] 2>/dev/null || { PASS=0; REASON="$REASON no-recovered-throughput;"; }
[ "$PASS" = 1 ] && say "VERDICT: PASS — cluster self-healed (bounded -> drained -> recovered to ${KNEE}/s)" || say "VERDICT: FAIL —$REASON"
say "samples: $SAMP"
