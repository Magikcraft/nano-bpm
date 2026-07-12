#!/bin/bash
# disk-guard.sh — ensure the soak cluster has disk headroom before/during a run.
#
# A soak writes tens of GB per node (Raft segments + read-model SQLite + var-store):
# a 20-min 50 KB soak consumed ~33 GB/node. On a too-small disk this fills to 100%
# and the server aborts with ENOSPC ("No space left on device") — a REAL crash, not
# a codec bug (observed 2026-07-12 on node0, which also carries the build cache).
# This guard refuses to launch a soak without headroom and can watchdog a live run.
#
# Subcommands:
#   report                          Print per-node df(/) + nano-data size.
#   preflight [MIN_FREE_GB=40]      Exit non-zero if ANY node has < MIN_FREE_GB free.
#   watch     [FLOOR_GB=15] [INT=30] Poll; if any node drops below FLOOR_GB, kill
#                                   loadgens on THIS host and exit 2 (abort the soak).
#
# Env overrides: NODES, SSHK, DISK_MIN_FREE_GB, DISK_FLOOR_GB.
#
# Resize headroom (you approved growing disks): the node disks were grown to 200 GB
# with, per node:
#   gcloud compute disks resize nano-node-N --size=200 --zone=us-central1-a --quiet
#   ssh node 'sudo growpart /dev/sda 1 && sudo resize2fs /dev/sda1'
set -u
SSHK="${SSHK:--i $HOME/.ssh/google_compute_engine -o StrictHostKeyChecking=no -o ConnectTimeout=8}"
NODES="${NODES:-10.128.0.19 10.128.0.20 10.128.0.18}"

# Integer GB available on / for a node ip (empty if unreachable).
free_gb() { ssh $SSHK "$1" "df -PBG / | awk 'NR==2{gsub(/G/,\"\",\$4); print \$4+0}'" 2>/dev/null; }

cmd_report() {
  for ip in $NODES; do
    printf 'node-%-3s ' "${ip##*.}"
    ssh $SSHK "$ip" "df -Ph / | awk 'NR==2{printf \"%s free / %s (%s used)\", \$4,\$2,\$5}'; \
      printf '  nano-data='; du -sh ~/nano-data 2>/dev/null | cut -f1 || echo -" 2>/dev/null \
      || echo 'UNREACHABLE'
    echo
  done
}

cmd_preflight() {
  local min="${1:-${DISK_MIN_FREE_GB:-40}}" bad=0
  echo "=== disk preflight: require >= ${min}G free on every node ==="
  for ip in $NODES; do
    local f; f="$(free_gb "$ip")"
    if [ -z "$f" ]; then echo "  node-${ip##*.}: UNREACHABLE  <-- FAIL"; bad=1; continue; fi
    if [ "$f" -lt "$min" ]; then
      echo "  node-${ip##*.}: ${f}G free < ${min}G required  <-- FAIL"; bad=1
    else
      echo "  node-${ip##*.}: ${f}G free (>= ${min}G) OK"
    fi
  done
  if [ "$bad" -ne 0 ]; then
    echo "DISK PREFLIGHT FAILED. Free space (wipe ~/nano-data, cargo clean the build host)"
    echo "or grow the disk:  gcloud compute disks resize nano-node-N --size=<GB> --zone=us-central1-a"
    echo "then on the node:  sudo growpart /dev/sda 1 && sudo resize2fs /dev/sda1"
    return 1
  fi
  echo "DISK PREFLIGHT OK (>= ${min}G on all nodes)"
}

cmd_watch() {
  local floor="${1:-${DISK_FLOOR_GB:-15}}" int="${2:-30}"
  echo "[$(date +%H:%M:%S)] disk watchdog: floor=${floor}G interval=${int}s nodes='$NODES'"
  while true; do
    for ip in $NODES; do
      local f; f="$(free_gb "$ip")"
      [ -z "$f" ] && continue
      if [ "$f" -lt "$floor" ]; then
        echo "[$(date +%H:%M:%S)] DISK FLOOR BREACH node-${ip##*.} ${f}G < ${floor}G — aborting soak (killing loadgens)"
        for p in $(pgrep -x loadgen); do kill "$p" 2>/dev/null; done
        return 2
      fi
    done
    sleep "$int"
  done
}

case "${1:-report}" in
  report)    cmd_report ;;
  preflight) shift; cmd_preflight "$@" ;;
  watch)     shift; cmd_watch "$@" ;;
  *) echo "usage: disk-guard.sh {report|preflight [MIN_FREE_GB]|watch [FLOOR_GB] [INT]}" >&2; exit 64 ;;
esac
