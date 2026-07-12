#!/bin/bash
# 30m max-throughput soak, NEGLIGIBLE payload (VB=0), latency (self-optimizing) SLA mode.
# Sustainable max operating point: RATE=14000/prod => ~42k/s aggregate (knee w/ p99<50ms).
IPS="10.128.0.19 10.128.0.20 10.128.0.18"
W=200; PC=128; MP=112; RATE=14000; MI=50000; DUR=300; VB=0
LABEL=cleanneg
rm -f $HOME/$LABEL-*.out
PDK=$(curl -s --max-time 10 -F resources=@$HOME/test-job-process.bpmn \
  http://10.128.0.19:8080/v2/deployments \
  | grep -o "\"processDefinitionKey\":\"[0-9]*\"" | grep -o "[0-9]*" | head -1)
if [ -z "$PDK" ]; then echo "[$(date +%H:%M:%S)] DEPLOY FAILED"; exit 1; fi
echo "[$(date +%H:%M:%S)] $LABEL START dur=${DUR}s PDK=$PDK rate=${RATE}/prod (~$((RATE*3/1000))k/s agg) MI=$MI VB=$VB"
sleep 3
i=0
for ip in $IPS; do
  BASE_URL=http://$ip:8080 PDK=$PDK WORKERS=$W PROD_CONNS=$PC MAXPAR=$MP RATE=$RATE \
    WARMUP_S=10 DURATION_S=$DUR DRAIN_S=15 MAX_INFLIGHT=$MI TRANSPORT=stream VAR_BYTES=$VB \
    $HOME/rw-build/target/release/loadgen > $HOME/$LABEL-$i.out 2>&1 &
  i=$((i+1))
done
wait
echo "[$(date +%H:%M:%S)] $LABEL DONE:"
grep -h "^RESULT" $HOME/$LABEL-*.out
