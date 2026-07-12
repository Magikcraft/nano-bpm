#!/bin/bash
# monitor.sh — per-node soak monitor: memory + storage rails, one line per interval.
#
# Columns per node: raftlog GB (full on-disk tail) / RAM GB (resident hot window) /
# RSS GB / backlog / partition-shutdowns / DISK-FREE GB. The disk-free column is the
# early-warning for the ENOSPC crash class (a node trending toward 0 free will abort).
#
# Usage: monitor.sh [label] [iterations] [interval_s]
SSHK="-i $HOME/.ssh/google_compute_engine -o StrictHostKeyChecking=no -o ConnectTimeout=8"
IPS="10.128.0.19 10.128.0.20 10.128.0.18"
LABEL="${1:-mon}"; ITERS="${2:-22}"; INT="${3:-60}"
echo "monitor '$LABEL': per node = logGB/ramGB/rssGB/backlog/shut/freeGB"
for k in $(seq 1 "$ITERS"); do
  line="[$(date +%H:%M:%S)]"
  for ip in $IPS; do
    read lb rb bl sh rss fg < <(ssh $SSHK "$ip" "
      curl -s localhost:8080/metrics | awk '
        /^nanobpm_raft_log_bytes /{lb=\$2}
        /^nanobpm_raft_log_ram_bytes /{rb=\$2}
        /^nanobpm_active_backlog /{bl=\$2}
        /^nanobpm_raft_partition_shutdown/{sh+=\$2}
        END{printf \"%s %s %d %d\", lb, rb, bl, sh}'
      printf ' %s' \$(awk '/VmRSS/{print \$2}' /proc/\$(pgrep -x nano-gw)/status 2>/dev/null)
      printf ' %s\n' \$(df -PBG / | awk 'NR==2{gsub(/G/,\"\",\$4); print \$4+0}')
    " 2>/dev/null)
    lgb=$(awk "BEGIN{printf \"%.1f\", ${lb:-0}/1073741824}")
    rgb=$(awk "BEGIN{printf \"%.1f\", ${rb:-0}/1073741824}")
    sgb=$(awk "BEGIN{printf \"%.1f\", ${rss:-0}/1048576}")
    line="$line  ${ip##*.}:$(printf '%s/%s/%s/%s/%s/%sG' "$lgb" "$rgb" "$sgb" "${bl%.*}" "${sh:-0}" "${fg:-?}")"
  done
  echo "$line"
  sleep "$INT"
done
echo "MON DONE ($LABEL)"
