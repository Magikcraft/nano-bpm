# DECODED REFERENCE ONLY — the live copy is embedded base64 (LB64) in deploy.sh.
# Keep this file in sync whenever you change the embedded launcher (decode → edit →
# re-encode: base64 -i node-launch-verify.reference.sh | tr -d '\n'). See RUNBOOK.md
# "The node launcher" section.
#
#!/bin/bash
# Verification launcher: NEW binary (nano-gw-new -> nano-gw) under leader-durable
# with the NEW DEFAULTS. Does NOT set NANOBPMN_REPLICATE_ACTIVATION (so the new
# mode-dependent default applies: leader-local under leader-durable). Admission
# backlog is parameterized: "default" omits the env (adaptive backstop), "off"
# disables, a number sets an explicit per-node cap.
#
# Read-model exporter mode is parameterized via positional args 4 & 5 (forwarded
# by deploy.sh from NANO_EXP_MODE / NANO_EXP_ENDPOINT), so the remote-exporter A/B
# needs no re-encoding of this launcher:
#   $4 EXP_MODE     sqlite (default) | tee | remote   -> NANOBPMN_READ_EXPORTER
#   $5 EXP_ENDPOINT central nano-exporter batch URL    -> NANOBPMN_EXPORTER_ENDPOINT
set -e
MAXBKLOG="${1:-default}"
CAP="${2:-100000}"
LIVENESS="${3:-600000}"
EXP_MODE="${4:-}"
EXP_ENDPOINT="${5:-}"
# Falcon cluster-channel secret (ADR 0039). Peers dial /cluster; when this is set the
# handshake requires x-nano-cluster-secret. Forwarded by deploy.sh as $6 from
# NANO_CLUSTER_SECRET. Omitted (default) => /cluster is open (single-VPC load test;
# a multi-node deployment then logs a startup warning). All nodes must share the value.
CLUSTER_SECRET="${6:-}"
NID="${HOSTNAME##*-}"
NODES="http://10.128.0.19:8080,http://10.128.0.20:8080,http://10.128.0.18:8080"
cp -f "$HOME/nano-gw-new" "$HOME/nano-gw"
BKLOG_ARG=()
case "$MAXBKLOG" in
  default) : ;;                                             # omit -> adaptive default
  *)       BKLOG_ARG=(--setenv=NANOBPMN_ADMISSION_MAX_BACKLOG="$MAXBKLOG") ;;
esac
# Read-model exporter selection (pluggable-exporter epic #133). Omitted => sqlite.
EXP_ARG=()
if [ -n "$EXP_MODE" ] && [ "$EXP_MODE" != "sqlite" ]; then
  EXP_ARG+=(--setenv=NANOBPMN_READ_EXPORTER="$EXP_MODE")
  [ -n "$EXP_ENDPOINT" ] && EXP_ARG+=(--setenv=NANOBPMN_EXPORTER_ENDPOINT="$EXP_ENDPOINT")
fi
# Falcon cluster-channel secret (ADR 0039). Omitted => launcher does not set the env
# (byte-identical to the pre-0039 launcher: /cluster stays open on the load-test VPC).
SEC_ARG=()
[ -n "$CLUSTER_SECRET" ] && SEC_ARG+=(--setenv=NANOBPMN_CLUSTER_SECRET="$CLUSTER_SECRET")
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
  "${EXP_ARG[@]}" \
  "${SEC_ARG[@]}" \
  --setenv=NANOBPMN_ADMISSION_MAX_CREATE_QUEUE="$CAP" \
  --setenv=NANOBPMN_STREAM_LIVENESS_MS="$LIVENESS" \
  $HOME/nano-gw
sleep 3
systemctl is-active nano.service && echo "LAUNCHED-VERIFY node-$NID replicate_activation=DEFAULT maxBacklog=$MAXBKLOG liveness=$LIVENESS exporter=${EXP_MODE:-sqlite} cluster_secret=$([ -n "$CLUSTER_SECRET" ] && echo set || echo unset) sha $(sha256sum ~/nano-gw|cut -c1-16)"
