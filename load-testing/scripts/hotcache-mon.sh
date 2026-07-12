#!/bin/bash
# 50KB/neg soak monitor incl. raft_log_ram_bytes (resident) vs raft_log_bytes (full tail).
SSHK="-i /home/joshua.wulf/.ssh/google_compute_engine -o StrictHostKeyChecking=no -o ConnectTimeout=8"
IPS="10.128.0.19 10.128.0.20 10.128.0.18"
LABEL=${1:-hot}; ITERS=${2:-24}
prev=0; prevt=0
for n in $(seq 1 $ITERS); do
  t=$(date +%s); ts=$(date +%H:%M:%S)
  comp=0; bl=0; lb=0; ram=0; le=0
  detail=""
  for ip in $IPS; do
    m=$(curl -s --max-time 20 http://$ip:8080/metrics 2>/dev/null)
    c=$(echo "$m" | awk '/^nanobpm_job_completions_total/{s+=$2} END{printf "%d", s+0}')
    b=$(echo "$m" | awk '/^nanobpm_active_backlog /{s+=$2} END{printf "%d", s+0}')
    rlb=$(echo "$m" | awk '/^nanobpm_raft_log_bytes/{s+=$2} END{printf "%d", s+0}')
    rram=$(echo "$m" | awk '/^nanobpm_raft_log_ram_bytes/{s+=$2} END{printf "%d", s+0}')
    rle=$(echo "$m" | awk '/^nanobpm_raft_log_entries/{s+=$2} END{printf "%d", s+0}')
    comp=$((comp+${c%.*})); bl=$((bl+${b%.*}))
    lb=$((lb+${rlb%.*})); ram=$((ram+${rram%.*})); le=$((le+${rle%.*}))
    detail="$detail ${ip##*.}:lb$(( ${rlb%.*}/1048576 ))/ram$(( ${rram%.*}/1048576 ))mb"
  done
  if [ $prevt -gt 0 ]; then dt=$((t-prevt)); crate=$(((comp-prev)/dt)); else crate=0; fi
  echo "[$ts] comp_rate=${crate}/s backlog=$bl entries=$le raftlog_gb=$(awk "BEGIN{printf \"%.2f\",$lb/1073741824}") ram_gb=$(awk "BEGIN{printf \"%.2f\",$ram/1073741824}") |$detail"
  prev=$comp; prevt=$t
  sleep 30
done
echo "MON DONE ($LABEL)"
