# ADR 0035 — Full-fidelity Prometheus, a standalone console, and runtime observability config

Status: **Accepted.** §A (full-fidelity gauges) and the `metrics` half of §C shipped in PR 1 (#216);
§B (standalone off-cluster console) is **implemented**. §C's `console` off/observe/studio runtime setting
remains the follow-up (Option 3).
Date: 2026-07-23.
Relates to:
ADR 0034 (`0034-console-build-profiles.md`, the build-time studio/observe split — this ADR adds the
*runtime* axis: turn the console and the metrics endpoint on/off, and run the console off-cluster),
Issue #126 (Phase 1 — console reads console-less peers via their Prometheus endpoint; **already
shipped** in PR #128),
Issue #127 (Phase 2 — full-fidelity gauges + standalone console; this ADR),
`server/src/main.rs` (`metrics_handler`, `handle_cli_flags`, the router assembly),
`server/src/console/mod.rs` (`build_local_metrics`, `build_recovery`, `metrics_dto_from_prometheus`,
the peer-probe fallback).

## Context

Three threads converge:

1. **Metrics fidelity across headless nodes.** The cluster dashboard already aggregates every node's
   metrics, falling back to a console-less peer's always-on `GET /metrics` Prometheus exposition when
   its `GET /console/api/metrics` 404s (#126, shipped). But two dashboard fields aren't Prometheus
   series, so they're *approximated* for scraped peers:
   - `active_instances` — an on-demand read-model `COUNT` (deliberately not an always-on gauge, to keep
     steady-state cost at zero). Approximated by `nanobpm_active_backlog` (created − completed).
   - `recovery` — per-partition leadership / catching-up state. Left at its steady-state default.

2. **A console that can run anywhere.** Today the console is embedded in an engine node. Operators want
   to run **one** console for the whole fleet, off-cluster, pointed at a list of `/metrics` endpoints —
   and to build engine nodes entirely without the `console` feature. The same series then also power
   Grafana / alerting.

3. **Runtime control of the observability surface.** ADR 0034 chose *what to embed* at build time
   (headless / observe / studio). But a deployer also needs *runtime* control on a given binary:
   - turn the console **off** on a hardened node without a separate build, and
   - turn the **`/metrics`** endpoint off entirely (attack-surface / privacy), via a **CLI flag or a
     config file**, not only an environment variable.

   The gateway is configured entirely by environment variables today, with only `-h`/`-V` as flags and
   no config file (`handle_cli_flags`, `gateway_usage`).

## Decision

### A. Promote the two console-only fields to always-on, scrape-computed gauges

Export both missing fields on every node's `/metrics`, computed **inside the handler on scrape** (never
in the engine tick loop), so a scrape is full-fidelity yet imposes **zero steady-state cost** and never
perturbs a running perf demo:

- `nanobpm_active_instances` — from `store.active_instance_count()` (already exists, non-gated).
- Recovery, as labelled/aggregate gauges derived from the existing `build_recovery` logic — which reads
  only engine + raft state (`topology`, `raft_registry`, per-partition raft metrics), **not** any
  console-only state, so it moves into the base build:
  `nanobpm_partition_owned`, `nanobpm_partition_reclaimed`, `nanobpm_partition_catching_up`,
  `nanobpm_partition_handing_off`, `nanobpm_handoff_lag_entries`.

`metrics_handler` gains access to server state via a captured clone (the pattern `/debug/raft` and
`/debug/instances` already use). `console/src/…::metrics_dto_from_prometheus` then reads the new series
directly; when they're absent (an older peer, pre-this-ADR) it keeps today's approximation as a
fallback, so mixed-version clusters degrade gracefully. The `recovery.detail` string is recomputed from
the counts by the parser.

### B. Standalone off-cluster console (a run mode, not a new binary)

Add a **console-standalone run mode** to the existing gateway binary: it starts the console router and a
remote-scrape aggregator **without** starting the engine / raft. It is configured with a peer list
(`--console-peer <url>` repeatable, `--console-standalone <csv>`, env `NANOBPMN_CONSOLE_STANDALONE`, or
YAML `observability.consolePeers` / `consoleStandalone`). **Every configured peer doubles as a seed:** the
aggregator queries peers' always-on `/v2/topology` until one answers, expands that into the full cluster
membership (substituting the reached URL for the peer's self-advertised `0.0.0.0` entry), then scrapes
each member's `/metrics` on the dashboard's refresh cadence. It serves the existing observability
dashboard APIs (cluster topology + metrics + per-node health); engine-only endpoints answer `503`.

_Implemented_ in `server/src/console/standalone.rs`: a `RemoteCluster` aggregator + an axum router that
reuses the console SPA handlers and the existing peer-probe/DTO machinery, branched into early in `main()`
(before any engine/journal/raft setup) when a peer list is configured. Because a pure Prometheus consumer
can only serve what Prometheus carries, standalone mode serves the **observe** subset (topology / metrics
/ node health); engine-only detail views (instance/trace/worker drill-down, project authoring) are not
offered off-cluster. This makes "standalone console" the runtime sibling of the build-time **observe**
profile, sourced from remote scrapes instead of a local engine.

### C. Runtime observability config (flag / file / env)

Introduce a small typed runtime-config layer resolved once at startup, covering the observability
surface (and extensible later). It defines two settings; the `metrics` toggle ships first (PR 1), and
the `console` toggle lands with the Option 3 runtime-profile work (PR 3), reusing the same layer:

- **`metrics`** — `on` (default) | `off`. When `off`, the `/metrics` route is **not registered** (so it
  404s, no handler compiled out — it's a runtime gate).
- **`console`** — `studio` | `observe` | `off`. `off` doesn't mount the console router at all; `observe`
  serves the operator subset and refuses the authoring routes; `studio` is today's full behaviour. (The
  runtime `observe`↔`studio` distinction over a studio *build* is a thin gate; the byte-savings version
  remains the ADR 0034 build profile. `off` is the headless runtime.)

Resolution precedence, highest wins:

```
CLI flag  >  config file  >  environment variable  >  built-in default
```

- **CLI flags:** `--metrics <on|off>`, `--console <off|observe|studio>`, `--config <path>`, plus the
  convenience `--no-metrics`. Parsed in an expanded `handle_cli_flags` (still hand-rolled — no new arg
  crate — to keep the dependency surface and binary size flat).
- **Config file:** an optional **YAML** file at `--config <path>` (or `NANOBPMN_CONFIG`). YAML (not
  TOML) is chosen to match the Kubernetes ecosystem operators deploy into — one config language across
  their manifests and the gateway — mirroring Zeebe's own TOML→YAML migration. E.g.

  ```yaml
  observability:
    metrics: "off"
    console: "observe"
    # standalone console (each peer doubles as a seed):
    # consolePeers:
    #   - "http://10.0.0.11:8080"
    #   - "http://10.0.0.12:8080"
    # or as a CSV scalar:
    # consoleStandalone: "http://10.0.0.11:8080,http://10.0.0.12:8080"
  ```

- **Env vars:** `NANOBPMN_METRICS=off`, `NANOBPMN_CONSOLE=observe|off`, mirroring the file keys — the
  existing configuration idiom, kept as the lowest-precedence layer.

Unknown flags/keys warn and are ignored (forward-compatible), matching today's lenient arg handling.

## Consequences

- **Full-fidelity fleet monitoring from one console**, including nodes built with **no** `console`
  feature — the "two headless + one observe" (or one off-cluster console) topology works with exact
  numbers, not approximations.
- **Interaction to document:** turning `metrics` **off** on a node makes it invisible to a remote/observe
  console (its scrape 404s) — the explicit attack-surface ↔ observability tradeoff the operator opted
  into. The console renders such a node as unreachable, as it does today for a genuinely down peer.
- **Zero steady-state cost preserved:** the new gauges are computed only when `/metrics` is scraped;
  the engine tick loop is untouched.
- **No new heavy dependencies:** flags stay hand-parsed; the config file uses `serde_yaml` (small,
  serde-based) to parse a single typed struct.
- **Grafana / alerting** get `active_instances` and per-partition leadership for free.

## Delivery

1. **PR 1 — full-fidelity metrics + the metrics toggle (this ADR + §A + the `metrics` half of §C).**
   Scrape-computed `active_instances` + recovery gauges; console parser reads them with fallback;
   `metrics` on/off via flag/file/env, with the config-file + flag plumbing that §C's `console` setting
   will later reuse. Independently valuable and low-risk.
2. **PR 2 — standalone off-cluster console (§B).** ✅ The `RemoteCluster` remote aggregator and the
   console-standalone run mode (`server/src/console/standalone.rs`), configured via the same layered
   config from PR 1.
3. **Then** the ADR 0034 runtime-profile polish (Option 3) lands §C's `console` off/observe/studio
   setting on the same config layer.

## Follow-ups

- Feature-gate the authoring API off the base/observe build (carried from ADR 0034) so `console=observe`
  and a console-less build share one lean, mutation-free surface.
- Peer discovery for standalone mode beyond `/v2/topology` (e.g. DNS/service-discovery) if fleets grow.
