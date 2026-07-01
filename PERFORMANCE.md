# nanobpmn Performance & Stress Tests

A running log of throughput / durability stress tests against the nanobpmn
engine. Newest runs first. Each entry records the environment, cluster
topology, load configuration, and observed results so runs stay comparable.

## Test process

Unless noted, the workload is `test-job-process.bpmn`: a single service task
with job type `test-job`. One job == one process instance, so **completed jobs/s
== completed process instances/s**. The load generator (`loadgen`, from
`ts-performance-matrix/rust-worker`) runs coupled producers + workers in one
process: producers create instances while holding server-granted submission
credits; workers stream-activate and complete `test-job`. `tput` is measured
completed jobs/s during the steady-state window; `producedRate ≈ tput` means the
cluster keeps up with the create rate (no growing backlog).

---

## 2026-07-01 — 3-node / 3-partition / RF=3 cluster, ~34k PI/s (30 min)

Build: `0.0.3-stress` (commit `21d954a`), release profile.

### Environment

- **3× GCP `c2-standard-16`** (16 vCPU, 64 GB), Debian 12, 200 GB `pd-ssd`, all
  in `us-central1-a` (same zone → low inter-node latency).
- Cluster: 3 nodes, 3 partitions, **RF=3** (every partition replicated to all
  three nodes). Each node leads one partition and follows the other two.
- Config per node: `NANOBPMN_NODES=<3 internal IPs>:8080`, `NANOBPMN_NODE_ID`,
  `NANOBPMN_RF=3`, `NANOBPMN_PARTITIONS=3`, `NANOBPMN_DATA_DIR` on the SSD,
  `PORT=8080`. Segmented multi-partition journal, snapshot/compaction every 60s.
- **Load placement:** `loadgen` **co-located on each node**, hitting
  `localhost:8080`. This is the design-intended topology — every node is a
  gateway, so clients spread across gateways and each node activates jobs on its
  own partitions locally (zero-hop hot path). Note: create *placement* still
  round-robins cluster-wide by design, so ~2/3 of creates are forwarded to peer
  partition owners over the Raft quorum-commit path (intentionally exercised).
- Load config per node: `WORKERS=256`, `PROD_CONNS=64`, `MAXPAR=4`,
  `MAX_INFLIGHT=6000`, `TRANSPORT=stream`, `DURATION_S=1800`.

### Results (30-minute steady-state window)

| Metric | node-0 | node-1 | node-2 | **Cluster** |
|---|--:|--:|--:|--:|
| Throughput (PI/s) | 11,236 | 11,572 | 11,623 | **34,431** |
| Instances processed | 20.23 M | 20.83 M | 20.92 M | **61.98 M** |
| p50 latency | 865 ms | 901 ms | 855 ms | ~875 ms |
| p90 latency | 2,286 ms | 2,325 ms | 2,263 ms | ~2.29 s |
| p99 latency | 2,810 ms | 2,857 ms | 2,794 ms | ~2.82 s |

Sustained throughput was flat across the run (sampled 34,142 → 34,233 →
34,431 PI/s), i.e. no degradation over 30 min / many compaction cycles.

### Resource usage (mid-run, under full load)

| Node | nano-gw RSS | loadgen RSS | System used (of 64 GB) | CPU idle (16 cores) |
|---|--:|--:|--:|--:|
| node-0 | 300 MB | 144 MB | 1.6 GB | ~43% |
| node-1 | 319 MB | 150 MB | 1.6 GB | ~43% |
| node-2 | 309 MB | 148 MB | 1.6 GB | ~43% |

- **Memory:** the engine holds a flat **~300–320 MB RSS** per node while
  leading one partition and replicating two peers' partitions at ~11.4k PI/s.
  Memory is not a constraint (jemalloc + idle-purge story holds under load).
- **CPU:** load average ~10/16, i.e. **~43% CPU idle**. nano-gw ≈ 6.9 cores,
  loadgen ≈ 1.3 cores → only ~8.2 of 16 cores busy.

### Disk / durability (post-run)

| File | Size / node | Behaviour |
|---|--:|---|
| `journal.jsonl` (live log) | **0 B** | Compacted to zero every 60s |
| `msnapshot.bin` (snapshot) | ~6.9 MB | Bounded, tiny |
| `read-model.sqlite` (projection) | 6.6–7.7 GB | Grows with history (~368 B/instance) |

- The **segmented journal + snapshot compaction bounds the durable log**: the
  live `journal.jsonl` truncates to ~0 and rolls into a few-MB snapshot each
  cycle. Mid-run the live segment was seen at 43–129 MB (accumulating between
  60s ticks) — bounded by the compaction interval, as designed.
- The **read-model projection is the disk-growth vector**, ~368 B per completed
  instance (≈ 7.7 GB for 20.9 M instances). This — not the journal — is the
  target for any history retention / rotation / exporter work. 179 GB was still
  free at the end (~6% used).

### Has the cluster hit its ceiling? **No.**

~34k PI/s was **not** the hardware ceiling — ~43% of CPU was idle and memory was
untouched. The run was bounded by the client-side concurrency knobs
(`MAX_INFLIGHT=6000/node`, worker/producer counts) and the resulting queuing
(p99 ~2.8 s), not by the machines. Two levers to push higher next time:

1. **Raise in-flight / producer concurrency** until CPU saturates.
2. **Move `loadgen` off the nodes** — co-location steals ~1.3 cores/node from
   the engine. A dedicated load generator (or a 4th box driving all three
   gateways) would free those cores.

Linear extrapolation from ~8.2 busy cores → 34k PI/s suggests **~50k+ PI/s** is
reachable on this same hardware before becoming CPU-bound.

### Reproduce

```bash
# Provision (us-central1-a, Debian 12, 200 GB pd-ssd)
gcloud compute instances create nano-node-0 nano-node-1 nano-node-2 \
  --zone=us-central1-a --machine-type=c2-standard-16 \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=200GB --boot-disk-type=pd-ssd --tags=nano-cluster

# On each node-N (N=0,1,2), launch the gateway (durable via systemd-run):
NANOBPMN_NODES='http://<ip0>:8080,http://<ip1>:8080,http://<ip2>:8080' \
NANOBPMN_NODE_ID=N NANOBPMN_RF=3 NANOBPMN_PARTITIONS=3 \
NANOBPMN_DATA_DIR=$HOME/run/data PORT=8080 ./nano-gw

# Deploy once, note processDefinitionKey:
curl -s -F resources=@test-job-process.bpmn http://localhost:8080/v2/deployments

# On each node, co-located load (localhost):
BASE_URL=http://localhost:8080 PDK=<key> WORKERS=256 PROD_CONNS=64 MAXPAR=4 \
MAX_INFLIGHT=6000 TRANSPORT=stream DURATION_S=1800 ./loadgen
```
