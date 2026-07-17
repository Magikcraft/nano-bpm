#!/bin/bash
# tier2-diff-soak.sh — ADR-0020 Tier-2 differential isolation soak.
#
# Deploys the two differential definitions (orders-fast, orders-slow) that SHARE
# the `common-job` job type but differ in topology (orders-slow adds a starved
# `slow-job` task), then drives BOTH lanes at the same create rate with diffgen.
# Because only orders-slow's `slow-job` is under-provisioned, ONLY its
# per-definition in-flight backlog L_P climbs, so the Tier-2 compressor must raise
# nanobpm_tier2_pressure{proc=orders-slow} and shed its creates while
# orders-fast stays at zero pressure and full admission.
#
# It samples nanobpm_tier2_pressure{proc} on the gateway over the run, then prints
# the per-lane diffgen RESULT lines and asserts the isolation invariant:
#   PASS  = slow pressure>0 AND slow shedRate>0  AND  fast pressure==0 AND fast sheds ~0.
#   FAIL  = fast throttled, or slow never throttled.
#
# Env (with defaults):
#   GW        gateway base URL for deploy + metrics   (http://10.128.0.19:8080)
#   BASES     space-separated gateway URLs for producers (default: GW)
#   DG        path to the diffgen binary              ($HOME/rw-build/target/release/diffgen)
#   SCEN      dir holding orders-fast.bpmn/orders-slow.bpmn (script dir ../scenarios/tier2-diff)
#   RATE      per-lane offered create rate            (2000)
#   CONNS     producer conns per lane                 (32)
#   COMMON_WORKERS / COMMON_MAXPAR                     (128 / 8)
#   SLOW_WORKERS / SLOW_MAXPAR / SLOW_DELAY_MS         (4 / 1 / 250)
#   WARMUP_S DURATION_S DRAIN_S                        (10 / 120 / 5)
#   LABEL                                              (tier2-diff)
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
GW=${GW:-http://10.128.0.19:8080}
BASES=${BASES:-$GW}
DG=${DG:-$HOME/rw-build/target/release/diffgen}
SCEN=${SCEN:-$HERE/../scenarios/tier2-diff}
RATE=${RATE:-2000}
CONNS=${CONNS:-32}
COMMON_WORKERS=${COMMON_WORKERS:-128}
COMMON_MAXPAR=${COMMON_MAXPAR:-8}
SLOW_WORKERS=${SLOW_WORKERS:-4}
SLOW_MAXPAR=${SLOW_MAXPAR:-1}
SLOW_DELAY_MS=${SLOW_DELAY_MS:-250}
WARMUP_S=${WARMUP_S:-10}
DURATION_S=${DURATION_S:-120}
DRAIN_S=${DRAIN_S:-5}
LABEL=${LABEL:-tier2-diff}

[ -x "$DG" ] || { echo "diffgen not found/executable at $DG (set DG=)"; exit 2; }
for f in orders-fast.bpmn orders-slow.bpmn; do
  [ -f "$SCEN/$f" ] || { echo "missing $SCEN/$f (set SCEN=)"; exit 2; }
done

deploy() { # $1 = bpmn path -> prints processDefinitionKey
  curl -s --max-time 10 -F resources=@"$1" "$GW/v2/deployments" \
    | grep -o "\"processDefinitionKey\":\"[0-9]*\"" | grep -o "[0-9]*" | head -1
}

FAST_PDK=$(deploy "$SCEN/orders-fast.bpmn")
SLOW_PDK=$(deploy "$SCEN/orders-slow.bpmn")
[ -z "$FAST_PDK" ] || [ -z "$SLOW_PDK" ] && { echo "[$(date +%H:%M:%S)] $LABEL DEPLOY FAILED (fast=$FAST_PDK slow=$SLOW_PDK)"; exit 1; }
echo "[$(date +%H:%M:%S)] $LABEL START fastPDK=$FAST_PDK slowPDK=$SLOW_PDK rate=${RATE}/lane conns=$CONNS commonW=$COMMON_WORKERS slowW=$SLOW_WORKERS slowDelay=${SLOW_DELAY_MS}ms dur=${DURATION_S}s"

rm -f "$HOME/$LABEL"-*.out

# ── Launch diffgen(s), one per producer base ─────────────────
i=0
PIDS=()
for base in $BASES; do
  env BASE_URL="$base" FAST_PDK=$FAST_PDK SLOW_PDK=$SLOW_PDK \
      FAST_RATE=$RATE SLOW_RATE=$RATE FAST_CONNS=$CONNS SLOW_CONNS=$CONNS \
      COMMON_WORKERS=$COMMON_WORKERS COMMON_MAXPAR=$COMMON_MAXPAR \
      SLOW_WORKERS=$SLOW_WORKERS SLOW_MAXPAR=$SLOW_MAXPAR SLOW_DELAY_MS=$SLOW_DELAY_MS \
      WARMUP_S=$WARMUP_S DURATION_S=$DURATION_S DRAIN_S=$DRAIN_S TRANSPORT=stream PROGRESS=1 \
      "$DG" > "$HOME/$LABEL-$i.out" 2>&1 &
  PIDS+=($!)
  i=$((i+1))
done

# ── Sample Tier-2 pressure over the run ──────────────────────
pressure() { # $1 = proc id -> per-mille (0 if absent)
  curl -s --max-time 5 "$GW/metrics" \
    | awk -v p="$1" '$0 ~ "^nanobpm_tier2_pressure{proc=\""p"\"}" {print $2; found=1} END{if(!found)print 0}' \
    | head -1
}

echo "--- Tier-2 pressure timeline (per-mille) ---"
SLOW_MAX=0; FAST_MAX=0
END=$(( $(date +%s) + WARMUP_S + DURATION_S ))
while [ "$(date +%s)" -lt "$END" ]; do
  sf=$(pressure orders-slow); ff=$(pressure orders-fast)
  sf=${sf%.*}; ff=${ff%.*}
  [ "${sf:-0}" -gt "$SLOW_MAX" ] && SLOW_MAX=$sf
  [ "${ff:-0}" -gt "$FAST_MAX" ] && FAST_MAX=$ff
  printf '  [%s] slow_pressure=%s fast_pressure=%s\n' "$(date +%H:%M:%S)" "${sf:-0}" "${ff:-0}"
  sleep 5
done

for pid in "${PIDS[@]}"; do wait "$pid"; done

echo "[$(date +%H:%M:%S)] $LABEL RESULTS:"
grep -h "^RESULT" "$HOME/$LABEL"-*.out | sort

# ── Isolation assertion ──────────────────────────────────────
FAST_SHED=$(grep -h "^RESULT lane=fast" "$HOME/$LABEL"-*.out | grep -o "shedRate=[0-9]*" | grep -o "[0-9]*" | awk '{s+=$1} END{print s+0}')
SLOW_SHED=$(grep -h "^RESULT lane=slow" "$HOME/$LABEL"-*.out | grep -o "shedRate=[0-9]*" | grep -o "[0-9]*" | awk '{s+=$1} END{print s+0}')
echo "--- isolation summary ---"
echo "  peak pressure per-mille:  slow=$SLOW_MAX  fast=$FAST_MAX"
echo "  aggregate shedRate:       slow=${SLOW_SHED}/s  fast=${FAST_SHED}/s"

if [ "$SLOW_MAX" -gt 0 ] && [ "${SLOW_SHED:-0}" -gt 0 ] && [ "$FAST_MAX" -eq 0 ] && [ "${FAST_SHED:-0}" -le "$((RATE/100))" ]; then
  echo "[$(date +%H:%M:%S)] $LABEL PASS — Tier-2 isolated the sick definition (orders-slow throttled, orders-fast untouched)."
  exit 0
else
  echo "[$(date +%H:%M:%S)] $LABEL FAIL — isolation not demonstrated (want slow pressure>0 & shed>0, fast pressure=0 & shed~0)."
  exit 1
fi
