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

## Layout

```
src/
  main.rs        webserver bootstrap, config, routes, the dashboard
  contracts.rs   typed mirror of Nano's read-contract DTOs + the HTTP client
  report.rs      pure aggregation: traces -> Insights (with unit tests)
```

Future stages (T2+: simulation, verification, canary, reasoning) add modules here
and will embed `engine-core` for exact native/WASM replay — without ever adding a
back-edge from Nano to ProcessOS.
