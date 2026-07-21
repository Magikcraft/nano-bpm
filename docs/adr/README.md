# Architecture Decision Records

This directory holds Nano's Architecture Decision Records (ADRs). Each ADR captures one decision — its
context, the choice made, its consequences, and the questions left open — in a durable, numbered
document. ADRs are append-only: rather than rewrite a superseded decision, a later ADR revises or
supersedes it and says so in its `Status` / `Relates to` header.

New ADRs take the next free number and follow the house shape: a `# ADR NNNN — <title>` heading, then
`Status:` / `Date:` / `Relates to:` headers, then `## Context`, `## Decision`, `## Consequences`, and
`## Open questions`. This table is the index; the ADR files themselves are the source of truth.

## Index

| # | Title | Status | Date |
|---|---|---|---|
| [0001](0001-cluster-job-activation-fairness.md) | Cluster job-activation fairness | Accepted (shipped) | 2026-06-21 |
| [0002](0002-leader-local-activation-and-lease-digest.md) | Leader-local job activation & best-effort lease digest | Accepted (shipped) | 2026-06-23 |
| [0003](0003-write-path-durability-tiers.md) | Write-path durability tiers (leader-durable replication) | Accepted (implemented) | 2026-06-23 |
| [0004](0004-investigator-outcome-eval-harness.md) | Investigator outcome-eval harness (replay-as-gating-scorer) | Proposed | 2026-06-26 |
| [0005](0005-embedded-u-nano.md) | Bernd (the embedded Nano engine) | Revised | 2026-07-05 |
| [0006](0006-subagent-delegation-mode.md) | Subagent delegation mode (context-protecting research handoff) | Proposed | 2026-07-04 |
| [0007](0007-rad-extension-system.md) | RAD extension system (npm-installable IDE packs) | Accepted (implemented) | 2026-06-29 |
| [0008](0008-polyglot-language-packs.md) | Polyglot RAD: language packs (Rust first) | Accepted (implemented) | 2026-06-29 |
| [0009](0009-gui-application-projects.md) | GUI application projects (served-UI binaries) | Accepted (implemented) | 2026-06-29 |
| [0010](0010-extensible-tool-surface.md) | Extensible tool surface (user-defined investigation tools) | Proposed | 2026-06-30 |
| [0011](0011-editable-model-workbench.md) | Editable model workbench (human-authored variants in investigations) | Proposed | 2026-06-30 |
| [0012](0012-decoupling-terminal-state-from-exporter-lag.md) | Decoupling terminal-state memory reclamation from exporter lag | Accepted (implemented) | 2026-07-03 |
| [0013](0013-sla-modes-and-varstore-wal-bounding.md) | SLA modes at the saturation ceiling, and bounding the var-store WAL | Accepted (implemented) | 2026-07-03 |
| [0014](0014-create-placement-protection-and-load-awareness.md) | Create-placement protection and cluster load-awareness | Accepted (implemented) | 2026-07-03 |
| [0015](0015-artists-sign-their-work.md) | Artists sign their work | Accepted | 2026-07-05 |
| [0016](0016-falcon-protocol.md) | The Falcon protocol (unified bidirectional command stream) | Accepted (implemented) | 2026-07-05 |
| [0017](0017-worker-concurrency-governor.md) | Worker-concurrency governance (server-side right-sizing of the push dispatcher) | Accepted (implemented) | 2026-07-09 |
| [0018](0018-write-domain-granularity-vs-beam-rewrite.md) | Write-domain granularity vs. a BEAM/Erlang rewrite (research) | Proposed (research) | 2026-07-09 |
| [0019](0019-leadership-handoff-reclaim.md) | Leadership hand-off reclaim for a rejoining static owner | Proposed | 2026-07-15 |
| [0020](0020-two-tier-admission-compression.md) | Two-tier admission compression (saturation guard + per-definition compressors) | Proposed | 2026-07-17 |
| [0021](0021-process-sla-as-a-first-class-abstraction.md) | Process SLA as a first-class abstraction (unifying job priority and admission) | Proposed | 2026-07-17 |
| [0022](0022-nano-rad-application.md) | Nano RAD Application (the App bundle — triggers, process, forms, decisions, data) | Proposed | 2026-07-21 |
| [0023](0023-adhoc-subprocess-execution-parity.md) | Ad-hoc sub-process execution parity (agentic Tier-1) | Proposed | 2026-07-21 |
| [0024](0024-urban-data-layer-datasource-abstraction.md) | Urban data layer & datasource abstraction (the BDE alias) | Proposed | 2026-07-21 |
| [0025](0025-urban-trigger-runtime.md) | Urban trigger runtime (the Zapier primitive: sources → inbox → engine) | Proposed | 2026-07-21 |
| [0026](0026-urban-human-surfaces-and-run-model.md) | Urban human surfaces & the App run model (rendering, action API, dev loop) | Proposed | 2026-07-21 |
| [0027](0027-urban-app-manifest-spec.md) | Urban App manifest (`nano.app.json`) — the binding, spec-first | Proposed | 2026-07-21 |
| [0028](0028-urban-app-user-auth-identity-authorization.md) | Urban App-user authentication, identity & authorization | Proposed | 2026-07-21 |
| [0029](0029-urban-bindings-domain-model.md) | Urban bindings & the domain model (typed references over untyped runtime) | Proposed | 2026-07-21 |
| [0030](0030-domain-process-duality.md) | The domain–process duality: directed evolution of typed state | Proposed | 2026-07-21 |
| [0031](0031-process-relational-mapper.md) | The Process-Relational Mapper (an ORM whose third bank is the engine) | Proposed | 2026-07-21 |
| [0032](0032-domain-resource-api.md) | The domain-resource API (the Kogito seam: process instances as first-class REST resources) | Proposed | 2026-07-22 |

## Reading paths

The ADRs fall into a few coherent threads:

- **Engine & cluster internals** (throughput, durability, fairness, admission): 0001–0003, 0012–0014,
  0016, 0017, 0019, 0020. The performance and correctness substrate of the Raft-clustered engine.
- **SLA & time** as first-class: 0013, 0021 — culminating in the "state with a future tense" view.
- **Investigator / agent tooling**: 0004, 0006, 0010, 0011 — the eval harness and human-in-the-loop
  surfaces.
- **RAD / IDE platform**: 0005 (**Bernd**, the embedded engine), 0007–0009 (the pack system, language
  packs, served-UI binaries) — the foundation the Urban App bundle grows from.
- **Urban — the Nano RAD Application**: 0022 (the product keystone) and its expansions — 0023 (agentic
  parity), 0024 (data layer), 0025 (triggers), 0026 (human surfaces), 0027 (manifest spec), 0028
  (auth/identity), 0029 (bindings & domain model) — resting on the conceptual keystone **0030** (the
  domain–process duality: why Urban is a distinct kind of computing) and its mapping mechanism **0031**
  (the Process-Relational Mapper: how one domain type stays coherent across form, engine, and database),
  and its API surface **0032** (the domain-resource API: the domain object as a first-class, typed,
  self-describing REST resource whose CRUD verbs are its lifecycle — the Kogito seam).
- **Philosophy / signature**: 0015 (artists sign their work) and 0030 (the computational primitive) —
  the pieces that state *why*, not just *what*.
