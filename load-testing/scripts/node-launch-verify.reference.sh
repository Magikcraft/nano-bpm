# DECODED REFERENCE ONLY — the live copy is embedded base64 in restart-verify.sh.
# This is the per-node systemd launcher deployed to each cluster node.

#!/bin/bash
# Verification launcher: NEW binary (nano-gw-new -> nano-gw) under leader-durable
# with the NEW DEFAULTS. Does NOT set NANOBPMN_REPLICATE_ACTIVATION (so the new
# mode-dependent default applies: leader-local under leader-durable). Admission
# backlog is parameterized: "default" omits the env (adaptive backstop), "off"
# disables, a number sets an explicit per-node cap.
set -e
MAXBKLOG="${1:-default}"
CAP="${2:-100000}"
LIVENESS="${3:-600000}"
NID="${HOSTNAME##*-}"
NODES="http://10.128.0.19:8080,http://10.128.0.20:8080,http://10.128.0.18:8080"
cp -f "$HOME/nano-gw-new" "$HOME/nano-gw"
BKLOG_ARG=()
case "$MAXBKLOG" in
  default) : ;;                                             # omit -> adaptive default
  *)       BKLOG_ARG=(--setenv=NANOBPMN_ADMISSION_MAX_BACKLOG="$MAXBKLOG") ;;
esac
sudo systemd-run --unit=nano --collect \
  --uid=$(id -u) --gid=$(id -g) \
  --setenv=HOME=$HOME --setenv=PORT=8080 \
  --setenv=NANOBPMN_NODES="$NODES" --setenv=NANOBPMN_NODE_ID="$NID" \
  --setenv=NANOBPMN_RF=3 --setenv=NANOBPMN_PARTITIONS=12 \
  --setenv=NANOBPMN_JOURNAL=segmented --setenv=NANOBPMN_LEAN_SNAPSHOT=1 \
  --setenv=NANOBPMN_DATA_DIR=$HOME/nano-data \
  --setenv=NANOBPMN_VAR_SPILL=adaptive --setenv=NANOBPMN_VAR_SPILL_MB=700 \
  --setenv=NANOBPMN_COLD_SPILL=adaptive --setenv=NANOBPMN_COLD_SPILL_MB=700 \
  --setenv=NANOBPMN_HISTORY_RETENTION=adaptive --setenv=NANOBPMN_HISTORY_RETENTION_MB=6000 \
  --setenv=NANOBPMN_EXPORTER_QUEUE=adaptive --setenv=NANOBPMN_MEM_WATERMARK=adaptive \
  --setenv=NANOBPMN_RAFT=1 --setenv=NANOBPMN_REPLICATION=leader-durable \
  --setenv=NANOBPMN_DURABILITY=sync --setenv=NANOBPMN_JOURNAL_LINGER_US=0 \
  --setenv=NANOBPMN_SLA_MODE=latency \
  "${BKLOG_ARG[@]}" \
  --setenv=NANOBPMN_ADMISSION_MAX_CREATE_QUEUE="$CAP" \
  --setenv=NANOBPMN_STREAM_LIVENESS_MS="$LIVENESS" \
  $HOME/nano-gw
sleep 3
systemctl is-active nano.service && echo "LAUNCHED-VERIFY node-$NID replicate_activation=DEFAULT maxBacklog=$MAXBKLOG liveness=$LIVENESS sha $(sha256sum ~/nano-gw|cut -c1-16)"
