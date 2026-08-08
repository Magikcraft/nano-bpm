# ADR 0056 — The Nano agentic protocol: an app-tier channel for agent networks, visibility, and coordination

Status: **Proposed.**
Date: 2026-08-09.

Relates to:
ADR 0002 (`0002-leader-local-activation-and-lease-digest.md`, the durable job lease — the
**coordination** plane this ADR is deliberately *not*),
ADR 0046 (`0046-agent-as-worker-vs-agent-in-the-node.md`, **agent-as-worker** — the topology whose
workers this ADR organizes, makes visible, and steers),
ADR 0051 (`0051-nano-workforce.md`, the crew orchestrator — this ADR supplies its missing transport,
registry, and visibility, and resolves its open "crewmate transport" and "lease home" questions),
ADR 0028 (`0028-urban-app-user-auth-identity-authorization.md`, the identity the app — and therefore
this channel — already authenticates under),
ADR 0050 (Urban I/O surface / **App View** — the console-embedded app-view this ADR's cockpit rides
in),
`@nanobpm/urban` (the generic Urban runtime — the home of this capability, not any one app),
`nano-workforce/app/blackboard.ts` (the per-plan advisory blackboard — the seed this ADR generalizes
into a first-class channel family),
and the MIT-licensed prior art the relay machinery cribs its hard parts from:
[`stablyai/orca`](https://github.com/stablyai/orca) — a desktop ADE for parallel coding agents whose
relay (`src/relay/`, `src/shared/relay-frame-decoder.ts`, `src/relay/pty-source-credit-record.ts`)
already solved multiplexed PTY streaming with credit-based backpressure, resumable delivery, and a
mobile/VPS transport story.

## Context

Nano's engine is a **frozen, Camunda-8-compatible orchestration substrate**. Its wire contract is the
C8 job protocol — `activateJobs` / complete / fail, keyed on an **exact job-type string, matched
1:1** — and its entire value is that stock C8 clients work against it unmodified. We therefore do not,
and cannot, extend that protocol with Nano-proprietary concerns.

Building an agentic developer workstation on top of it — agent-workers (ADR 0046) coordinated as a
crew (ADR 0051) — needs three things the engine deliberately does not provide:

- **A vocabulary for what work exists.** Hand-wiring a job-type per model, per task, combinatorially
  explodes and couples every model to every machine: adding a novel model means editing worker
  capabilities fleet-wide, and those workers live on different hardware. The coupling is wrong.
- **Visibility.** Which work is *demanded*, who is *serving* it, on which machine, and what each agent
  is doing right now — so an operator can see a missing agent type at a glance.
- **Coordination + observation.** A shared side-channel for agents to coordinate, plus a live terminal
  to watch and steer them, durable enough to replay after an ephemeral agent exits and resumable after
  a cockpit reconnects.

These are **application concerns, not engine concerns.** Hosting them on the engine — or on the
engine's command transport — would drag agent semantics into the C8-compatible core and forfeit the
property that makes Nano a drop-in C8 backend: the engine is a dumb, frozen router that has never
heard of an agent.

## Decision

### 1. Two tiers: one frozen, one ours

- **Engine (C8-compatible), coordination only.** Job protocol, leases, messages, timers. Frozen. It
  routes plain job-type strings 1:1 and never learns what a token *means*.
- **The Nano agentic layer (this ADR).** Everything agentic — a vocabulary, a registry, visibility, a
  coordination side-channel, and live terminals. Ours, extensible, and **app-tier**.

### 2. One app-tier channel

All agentic traffic rides a **single channel served by the app on its own port** — the port the Nano
Workforce app already serves, where the blackboard capability lives — **not** on the engine or its
transport. One channel carries every non-C8 concern:

- **register / heartbeat** — supply + presence;
- **demand × supply** — monitoring;
- **blackboard** — shared coordination knowledge;
- **relay** — live terminal bytes + steer-in.

One connection per worker for everything agentic. The worker's C8 job protocol to the *engine* is a
separate, unchanged conversation.

### 3. Three QoS lanes on the one channel

Facts and bytes have opposite characteristics, so the channel multiplexes **prioritized lanes**:

| Lane | Traffic | Rate | Loss tolerance | Priority |
|---|---|---|---|---|
| **control / facts** | register, heartbeat, blackboard, steer-in | low | must not drop | highest |
| **interactive** | prompt echo, an agent asking a question | low | low | high |
| **bulk** | terminal stdout firehose | high, bursty | coalescible | lowest |

Priority classes plus per-lane credit so a bulk-output storm can never head-of-line-block a heartbeat
(worker looks dead) or a blackboard write (crew miscoordinates).

### 4. A generic `@nanobpm/urban` capability; the app is the hub

The machinery is a **generic Urban runtime capability**, not nano-workforce-specific — any agentic
Urban app gets agent networks, visibility, blackboard, and terminals for free, the way apps get pages
and workers today. Roles:

- **Worker** (c8ctl, Node): allocates the PTY, frames output, tags every frame with its `jobKey`,
  produces; declares its capability on connect.
- **Hub** (the Urban app): the registry, demand×supply, blackboard, the relay ring, credit accounting,
  fan-out, and transcript store — over the app's DataLayer / SQLite.
- **Cockpit** (console-embedded App View, standalone app, phone): attach, replay, steer.

The engine hosts none of it. The thing that must be present to observe agents is the *app*, not a bare
C8 engine — a bare engine has no agents to watch.

### 5. Hub-down: worker-side buffer + flush-on-reconnect

The hub is app-tier and may be down while an ephemeral worker is mid-run. Each worker holds a
**bounded local ring** and **flushes on reconnect**. A worker that starts before the app simply
buffers and drains — no loss, and "worker before hub" is not a special case.

### 6. The networks vocabulary

Work is organized as **networks of agents** (departments), optionally nested (networks of networks).
The routing token — the C8 job type, matched 1:1 — is a **dotted path**:

```
network[.subnetwork…].role[#seat]
```

- `network(.sub…)` — the department hierarchy (`planning`, `qa`, `implementation`, `ci`, …).
- `role` — the semantic job (`spar`, `review`, `rust`, …).
- `#seat` — an optional diversity / anti-affinity partition (`#red` / `#blue`).

The model's service task emits this string; the worker's `activateJobs` polls this string; the engine
routes it verbatim. Tab A = slot A at the leaf. The hierarchy means something only to the *app*
(enrolment roll-up, registry grouping); the engine sees one flat string per leaf.

### 7. Capability is an enrolment gate, never in the token

**Cognition** (local → standard → frontier), **execution weight** (knowledge → workstation → heavy),
**model family**, and **host** are worker *attributes* declared at enrolment — never part of the
routing string. A role's requirement ("`planning.spar` needs frontier"; "seats must be distinct
families") is declared in the vocab artifact and **enforced by the registry, not the engine**.

This is what stops the explosion: token count scales with the **design vocabulary** (departments ×
roles), not with models × machines. A novel model enrols with its capability and serves the existing
tokens it qualifies for — **zero new tokens, zero model edits, no fleet-wide sync.**

### 8. Core vocabulary, extensible by authors

The vocabulary is a **versioned artifact in two layers of identical shape**:

- **Core** — shipped with the capability. Opinionated networks (`planning.*`, `qa.*`,
  `implementation.*`, `ci.*`, `decide`, seats), capability→token resolution rules, and defaults. Nano
  Workforce **works out of the box** with it.
- **Extension** — author-declared entries in the *same* schema, shipped inside their app. Networks we
  never imagined (`legal.contract-review`, `homeauto.trigger#…`). Because it is the same schema, the
  registry treats extensions uniformly (autocomplete, demand, SLO); they **imply fleet configuration**
  only insofar as some machine must qualify and enrol to satisfy the new demand — the registry shows a
  red "missing agent type" until one does.

```jsonc
// vocab artifact — core is shipped; authors append extensions in the same shape
{
  "networks": {
    "planning": { "roles": {
      "spar":     { "requires": "frontier", "seats": ["red","blue"], "seatsDistinctFamily": true },
      "finalize": { "requires": "frontier" } } },
    "qa": { "roles": {
      "review": { "requires": "standard" },
      "lint":   { "requires": "standard", "weight": "workstation" } } }
    // author extension, identical shape:
    // "legal": { "roles": { "contract-review": { "requires": "frontier" } } }
  }
}
```

### 9. The capability-declaration handshake (no map in the worker)

The capability→token map lives in the app's **versioned vocab artifact** and is applied **over the
channel** — never baked into c8ctl:

```
worker → app:    REGISTER { capability: { family, cognition, weight }, host }
app    → worker: SERVE    [ resolved leaf tokens from the vocab ]
worker → engine: activateJobs(<each token>)      # dumb 1:1 pollers, one process, many types
```

One `nano work` process multiplexes **many pollers**, so one machine serves a whole department it
qualifies for. c8ctl stays a dumb bridge: it declares capability and opens whatever tokens the app
returns. The operator may scope/trim the resolved set (cost control); the supervisor may reconcile it
live as the vocab or fleet changes.

### 10. Diversity SLO

Seats exist because two frontier agents in a spar must be **genuinely different model families** — two
instances of the same family collude. Distinctness must live in the **routing key** (a header cannot
partition worker pools), backed by **enrolment discipline**: the seats of a role map to different
families. The registry asserts `family(#red) ≠ family(#blue)` and surfaces *"sparring degraded — both
seats are ‹family›."* Mismatch is **warn / amber by default, strict (refuse to register / hold) opt-in
per role.** Seats generalize to anti-affinity (different machines) and to n-way ensembles.

### 11. Visibility is an Urban page, embedded in the console

The registry groups **demand × supply by network**:

- **demand** = deployed models' task-definition leaves, bucketed by prefix ("the `planning` department
  needs `spar#red`, `spar#blue`, `finalize`");
- **supply** = registered workers per leaf, with family and host;
- **missing agent type** = a demanded leaf with no supplier (red);
- **diversity SLO** = seat families distinct (green / amber);
- **what they're doing** = drill into a worker → its live relay bytes.

This is a **page shipped by the capability and embedded in the console via App View** — not a bespoke
console feature. Standalone app and phone are the same page over the same channel.

### 12. Reuse the proven relay machinery (Orca, MIT), in TypeScript

The relay's hard parts are cribbed from Orca and reimplemented in the TypeScript Urban runtime:

- **bounded replay ring + resume-from-offset** — late-join / reconnect catches up without unbounded
  buffering; a cockpit resumes from its last accepted offset (no loss, no duplication);
- **credit-based backpressure** — a slow consumer (phone, lossy link) throttles the producer instead
  of OOMing the hub;
- **generation / incarnation fencing** — a stale reconnect cannot double-attach or steal write
  ownership; steering is gated on the current owner generation;
- **three-lane QoS** (§3) — interactive frames are never buried behind bulk output;
- **retention by lifecycle** — *ephemeral* → flush the ring to a durable transcript artifact on job
  completion ("see what the agent did"); *long-lived* → live attach + resumable reattach.

## Worked example — the planning spar

`nano-workforce/resources/processes/plan-fanout.bpmn` opens with a sparring pair. Under this ADR:

- **Model:** two parallel tasks `planning.spar#red` (role header `propose`) ∥ `planning.spar#blue`
  (role header `steelman`), inside the existing visible convergence loop; an optional `decide.*` judge
  gauges convergence and calls the round.
- **Vocab (core):** `planning.spar` requires `frontier`; seats must be distinct families.
- **Fleet:** an **Opus 4.8** box enrols → serves `planning.spar#red`; a **Kimi-K2 / Qwen-3.8-max** box
  → `planning.spar#blue`.
- **Registry:** demand `{spar#red, spar#blue}` × supply `{opus@red, kimi@blue}` → SLO green.
- **Console:** embeds the app's visibility page → `red = opus`, `blue = kimi`, both connected, each
  watchable live.

Swap Kimi for Qwen tomorrow: re-enrol the blue box. **The BPMN does not change; no other box changes.**

## Consequences

- Nano ships a **frozen C8 engine** plus a **generic agentic capability**; any Urban app gets agent
  networks, visibility, blackboard, and live terminals for free.
- The engine stays a dumb 1:1 router with no agent knowledge; all agent semantics live in the app-tier
  vocab + registry.
- Authors build apps we never imagined by **extending the core vocabulary**; an extension implies only
  that the fleet gains a qualifying, enrolled worker — visible as a registry gap until it does.
- New surface to own in the Urban runtime (TypeScript): the multiplexed channel with three-lane QoS,
  the registry + vocab resolver, the relay ring + transcript store, and worker-side buffering.
  De-risked by Orca's working implementation.
- ADR 0051's open **"crewmate transport"** and **"lease home"** resolve here: transport = this channel;
  liveness = heartbeat → app registry row; the engine job lease stays a pure C8 coordination detail the
  agentic layer never reaches into.

## Open questions

- **PTY vs pipe per role** — full interactivity (ANSI, prompts, steer) vs plain stdout capture; likely
  per-role opt-in in the vocab.
- **Transcript store** — reuse the app's DataLayer / SQLite with a dedicated schema vs a separate
  store; retention / eviction policy for long transcripts.
- **Mirror vs matchmaker** — does the registry stay a read-only mirror, or grow app-tier placement
  policy (auto-fill an unserved role, hold a seat's job until a distinct family is free)? Legitimate at
  the app tier; deferred to a follow-up.
- **Vocab distribution** — does a worker fetch the vocab over the channel at enrol, or is it bundled
  with the capability version it runs — and how do core/extension versions reconcile across app and
  fleet?
- **Correlation shape** — beyond `jobKey`, how process-instance / plan correlation is surfaced so a
  cockpit lines up "this terminal" with "that process."

## Companion — relay epic rework

The 16-issue relay epic (#547) is reworked to this tier: the Rust frame-codec, Falcon-transport, and
`relay → engine` dependency-guard issues are **dropped or re-targeted to the TypeScript Urban runtime**;
the console Workforce view becomes an **embedded app page**; remote-cockpit auth folds into the app's
**ADR 0028 identity** plus a capability credential (the pattern the blackboard already uses).
