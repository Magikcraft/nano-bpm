# ProcessOS

The runtime **process-optimization plane** for Nano BPM — a separate component, on
the principle that **Nano handles production, ProcessOS handles optimization.**

It runs as its own process with its own webserver and talks to a Nano cluster *only*
over Nano's public HTTP contracts. The dependency is one-way: ProcessOS reads Nano;
**Nano never depends on ProcessOS** and runs unaffected when this binary is absent.

See [`../docs/processos-design.md`](../docs/processos-design.md) for the full design
and [`../docs/process-optimization-design.md`](../docs/process-optimization-design.md)
for the optimization loop it implements.

## Status — Stage T1 (Insights)

This is the first, read-only stage: ingest Nano's exported traces + metrics and fold
them into a per-process / per-element **Insights** report (bottleneck element,
queue-vs-service split, incident clusters, live gauges). Every later capability
(cost model, simulation, canary, LLM reasoning) builds on this foundation.

It uses only Nano's **read contract**:

- `GET /console/api/traces` — recent instance trace summaries
- `GET /console/api/traces/{key}` — one instance's canonical trace
- `GET /console/api/metrics` — live node gauges

No writes, no engine, no LLM yet.

## The optimization harness (MVP — design §7)

The MVP optimization harness embeds the **real `engine-core`** (a one-way `path`
dependency) and drives it with a virtual clock to evaluate process variants. A
**scenario** supplies a test model, a pool of seeded **mock workers** (each with a
cost / latency / failure model), a set of **latent worker-swap options** (the MVP
transform space), and **inputs carrying expected outputs**. The `SimRunner` runs
every candidate over every input and ranks them across cost / latency /
incident-rate / correctness, reporting whether exploration recovered the optional
**golden** variant.

This is the *same loop as production* with the data source swapped: in production a
ClusterRunner reads live Nano traces instead of simulating; the generation +
ranking stay identical. No LLM yet (that is M2) — this validates the measurement
and ranking rig.

```sh
# Run the bundled example scenario and see the ranked candidates
curl http://localhost:8090/api/harness/example/run | jq
# …or open the harness dashboard
open http://localhost:8090/harness
```

## Run

```sh
# Build + test
make processos-build
make processos-test

# Run against a Nano gateway (defaults shown)
PROCESSOS_PORT=8090 NANO_BASE_URL=http://localhost:8080 cargo run
```

Then open the dashboard at `http://localhost:8090/` or fetch the raw report:

```sh
curl http://localhost:8090/api/insights | jq
```

| Env | Default | Meaning |
|-----|---------|---------|
| `PROCESSOS_PORT` | `8090` | Port ProcessOS listens on |
| `NANO_BASE_URL` | `http://localhost:8080` | Base URL of the Nano gateway to read |

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/` | Single-file dashboard (fetches `/api/insights`) |
| `GET` | `/health` | Liveness (`ok`) |
| `GET` | `/api/insights?limit=&sample=` | Folded performance report (`limit` summaries scanned, `sample` detailed) |
| `GET` | `/harness` | Optimization-harness dashboard (runs the example scenario) |
| `GET` | `/api/harness/example` | The bundled example scenario JSON (a template to copy) |
| `GET` | `/api/harness/example/run` | Run the example scenario, return the ranked report |
| `POST` | `/api/harness/run` | Run a caller-supplied scenario, return the ranked report |

## Layout

```
src/
  main.rs        webserver bootstrap, config, routes, the dashboards
  contracts.rs   typed mirror of Nano's read-contract DTOs + the HTTP client
  report.rs      pure aggregation: traces -> Insights (with unit tests)
  harness/
    mod.rs       scenario / worker / variant types + seeded PRNG
    sim.rs       SimRunner: drives engine-core on a virtual clock (M0)
    rank.rs      candidate enumeration + evaluation + ranking (M1)
    example.rs   the bundled Classify->Summarize worker-swap demo
```

Future stages (T2+: verification, canary, reasoning) add modules here. The MVP
harness already embeds `engine-core` for exact native replay — without ever adding
a back-edge from Nano to ProcessOS.
