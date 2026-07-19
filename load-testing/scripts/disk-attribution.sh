#!/bin/bash
# disk-attribution.sh — split a node's disk write load into its components so you
# can prove whether a soak is RAFT-LOG bound or READ-MODEL bound (and hence
# whether moving the read-model exporter off-node — see the "Remote exporter"
# section of RUNBOOK.md — should lift the ceiling).
#
# Per node it reports, over one INTERVAL window:
#   dev   = gross DEVICE write throughput  (MB/s)  <- the real PD ceiling signal
#   wiops = gross DEVICE write IOPS        (ops/s) <- the fsync/small-write signal
#   app   = nano-gw process write_bytes    (MB/s)  <- gross logical writes by the app
#   syscw = nano-gw write() syscalls       (ops/s)
#   raft  = net growth of  nano-data/raft/            (MB/s)
#   rm    = net growth of  nano-data/read-model.*     (MB/s)  <- exporter offload target
#   var   = net growth of  nano-data/var-store.*      (MB/s)
#   spill = net growth of  nano-data/var-spill.*      (MB/s)
#   jrnl  = net growth of  nano-data/journal.jsonl*   (MB/s)
#
# How to read it:
#   * `dev` is what hits the ~276 MB/s per-VM Persistent-Disk write ceiling. If
#     `dev` is near that cap the node is DISK-bound; if it is well below it and
#     throughput still won't rise, the node is CPU/RAFT-commit bound.
#   * The per-component columns are NET on-disk growth (du delta). Because SQLite
#     WAL checkpoints and Raft segment compaction rewrite/delete bytes, the SUM of
#     the net columns is LESS than `app`/`dev` — the gap is write amplification +
#     compaction churn (mostly raft-log + SQLite WAL). Use the columns for
#     ATTRIBUTION (which store is growing fastest), and `dev`/`app` for the
#     absolute load. If `rm`+`var`+`spill` dominate the growth AND `dev` is near
#     the cap => read-model/payload disk is the wall => remote-exporter mode
#     should free it. If `raft` dominates or `dev` is low while throughput is
#     capped => you are raft/CPU bound and moving the exporter won't help.
#
# Subcommands:
#   sample [INTERVAL_S=10]              One window; print one line per node.
#   watch  [INTERVAL_S=10] [ITERS=30]   Repeat `sample` ITERS times.
#
# Env overrides: NODES, SSHK, DATA_DIR, GW_PROC, BLOCK_DEV (empty = auto-detect).
set -u
SSHK="${SSHK:--i $HOME/.ssh/google_compute_engine -o StrictHostKeyChecking=no -o ConnectTimeout=8}"
NODES="${NODES:-10.128.0.19 10.128.0.20 10.128.0.18}"
DATA_DIR="${DATA_DIR:-\$HOME/nano-data}"   # remote-side expansion
GW_PROC="${GW_PROC:-nano-gw}"
BLOCK_DEV="${BLOCK_DEV:-}"                  # e.g. sda; empty => auto-detect on node

# Remote probe: snapshot counters, sleep INTERVAL, snapshot again, print rates.
# Emits one line: "MBps wiops appMBps syscw raft rm var spill jrnl" (all numbers).
remote_probe() {
  local interval="$1"
  cat <<REMOTE
set -u
DATA="$DATA_DIR"
DEV="$BLOCK_DEV"
if [ -z "\$DEV" ]; then
  SRC=\$(df --output=source "\$DATA" 2>/dev/null | tail -1)
  DEV=\$(lsblk -ndo PKNAME "\$SRC" 2>/dev/null)
  [ -z "\$DEV" ] && DEV=sda
fi
PID=\$(pgrep -x "$GW_PROC" | head -1)
dub() { du -scb \$1 2>/dev/null | tail -1 | cut -f1; }   # sum bytes of a glob, 0 if none
stat_line() { awk '{print \$5, \$7}' /sys/block/\$DEV/stat; }             # writes_completed sectors_written
io_line()   { awk -F':[ ]*' '/^write_bytes/{wb=\$2}/^syscw/{sc=\$2} END{print wb+0, sc+0}' /proc/\$PID/io 2>/dev/null; }

read w0 s0 < <(stat_line)
read wb0 sc0 < <(io_line)
raft0=\$(dub "\$DATA/raft"); rm0=\$(dub "\$DATA/read-model.*"); var0=\$(dub "\$DATA/var-store.*")
spill0=\$(dub "\$DATA/var-spill.*"); jrnl0=\$(dub "\$DATA/journal.jsonl*")

sleep $interval

read w1 s1 < <(stat_line)
read wb1 sc1 < <(io_line)
raft1=\$(dub "\$DATA/raft"); rm1=\$(dub "\$DATA/read-model.*"); var1=\$(dub "\$DATA/var-store.*")
spill1=\$(dub "\$DATA/var-spill.*"); jrnl1=\$(dub "\$DATA/journal.jsonl*")

awk -v it=$interval \
    -v w0=\${w0:-0} -v s0=\${s0:-0} -v w1=\${w1:-0} -v s1=\${s1:-0} \
    -v wb0=\${wb0:-0} -v sc0=\${sc0:-0} -v wb1=\${wb1:-0} -v sc1=\${sc1:-0} \
    -v r0=\${raft0:-0} -v r1=\${raft1:-0} -v m0=\${rm0:-0} -v m1=\${rm1:-0} \
    -v v0=\${var0:-0} -v v1=\${var1:-0} -v p0=\${spill0:-0} -v p1=\${spill1:-0} \
    -v j0=\${jrnl0:-0} -v j1=\${jrnl1:-0} 'BEGIN{
      mb=1048576;
      printf "%.1f %d %.1f %d %.1f %.1f %.1f %.1f %.1f",
        (s1-s0)*512/mb/it, (w1-w0)/it, (wb1-wb0)/mb/it, (sc1-sc0)/it,
        (r1-r0)/mb/it, (m1-m0)/mb/it, (v1-v0)/mb/it, (p1-p0)/mb/it, (j1-j0)/mb/it }'
REMOTE
}

one_sample() {
  local interval="$1"
  local script; script="$(remote_probe "$interval")"
  local tmp; tmp="$(mktemp -d)"
  for ip in $NODES; do
    ( out="$(ssh $SSHK "$ip" "bash -s" <<<"$script" 2>/dev/null)"; echo "$out" > "$tmp/${ip##*.}" ) &
  done
  wait
  local line="[$(date +%H:%M:%S)]"
  for ip in $NODES; do
    read dev wi app sc raft rm var spill jrnl < "$tmp/${ip##*.}" 2>/dev/null
    if [ -z "${dev:-}" ]; then
      line="$line  ${ip##*.}:UNREACHABLE"
    else
      line="$line
  node-${ip##*.}  dev=${dev}MB/s wiops=${wi} app=${app}MB/s syscw=${sc}/s | raft=${raft} rm=${rm} var=${var} spill=${spill} jrnl=${jrnl} (net MB/s)"
    fi
  done
  echo "$line"
  rm -rf "$tmp"
}

case "${1:-sample}" in
  sample) shift; one_sample "${1:-10}" ;;
  watch)
    shift; interval="${1:-10}"; iters="${2:-30}"
    echo "disk-attribution watch: interval=${interval}s iters=${iters} nodes='$NODES'"
    for k in $(seq 1 "$iters"); do one_sample "$interval"; done
    echo "DISK-ATTRIBUTION DONE"
    ;;
  *) echo "usage: disk-attribution.sh {sample [INTERVAL_S] | watch [INTERVAL_S] [ITERS]}" >&2; exit 64 ;;
esac
