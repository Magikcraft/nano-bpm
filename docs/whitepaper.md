# Nano: A Self‑Optimizing Process Engine

### The reification of emergent distributed‑systems patterns as first‑class design, behind a fixed Camunda 8 contract

**Status: Advanced Research Prototype.** This paper documents the design of a
research engine and the reasoning behind its choices. It is not a product
announcement. Measured results are from the prototype on the configurations
stated; where a figure is still to be measured it is marked `[MEASURE]`.

**Working draft.** Part I (the engine) is being written first; Part II
(ProcessOS — empirical process optimization on historical workloads) is
scaffolded as later sections. Author‑facing notes are marked `> DRAFTING NOTE`.

---

## Abstract

> DRAFTING NOTE — write last, once the sections settle. Target ~200 words.
> Must land four claims: (1) a distributed process engine that self‑optimizes
> with **zero technical configuration**, exposing exactly **one** irreducible
> business decision; (2) achieved by promoting patterns that *emerged* over a
> decade of distributed BPMN (backpressure, client adaptation, feedback‑driven
> scaling) from add‑ons to **first principles**; (3) delivered as an
> **API‑compatible drop‑in** for Camunda 8 — a paradigm shift behind a fixed,
> commensurable interface, so it is adoptable without abandoning existing
> tooling; (4) small enough (Rust, no JVM/RocksDB, WASM‑capable) to open a new
> frontier: coexisting with local LLMs and replaying real historical workloads
> against counterfactual model variants. Performance and an honestly‑named
> throughput ceiling are presented as *evidence for the architecture*, not the
> headline.

---

## 1. Introduction: a paradigm shift, not a faster horse

Process automation engines have accreted, over a decade, a large surface of
operational tuning: backpressure thresholds, replication factors, exporter
batching, snapshot cadences, resource limits. Each knob is individually
reasonable and collectively they demand that an operator become an expert in the
engine's internals — tuning the machine by watching a metric and applying a rule
they had to learn.

This paper argues that most of that surface is **derivable**. Anything the engine
can decide correctly from a signal a human would only ever have watched can be
decided *by the engine, continuously, from that same signal*. What remains is the
one thing the engine genuinely cannot know: a business value judgement about how
the service should behave at the edge of its capacity envelope.

We frame this as a Kuhnian paradigm shift. A decade of "normal science" around a
distributed BPMN engine (Zeebe / Camunda 8) surfaced a set of recurring patterns
and a set of *anomalies* — places where the founding assumptions strained. The
acceleration of local LLMs supplied an exogenous shock (§2.3). Nano is the
attempt to start again *knowing what we now know*: to take the patterns that
emerged at the edges and make them the **first principles** of the design.

Crucially, the shift is engineered to be **commensurable at the interface**
(§4). Kuhn's paradigms typically force a field to abandon its tools; Nano's does
the opposite — it preserves the Camunda 8 contract exactly, so the revolution
happens entirely behind an API the ecosystem already speaks. It is a paradigm
shift you can deploy as a drop‑in.

> DRAFTING NOTE — the "faster horse" line (attributed to Ford) is almost
> certainly apocryphal; use it as illustration, not as cited history. The point
> stands regardless: no customer running on the JVM would have *requested* a
> ground‑up Rust reimplementation. Revolutions come from the edge of practice,
> not its center — which is itself a Kuhnian observation.

---

## 2. The old paradigm and its anomalies

The prior paradigm was not wrong. Each choice below was correct under its
constraints (the JVM ecosystem, an Elasticsearch‑first export model, a large
installed base whose needs rightly drove the roadmap). The anomalies are what
those correct‑under‑constraint choices could not, in aggregate, resolve.

### 2.1 The server/client dualism and the open feedback loops

Server and clients were built as distinct concerns, but at runtime they are a
**single system**: producers create work, the engine drains it, workers pull it,
and the rate of each depends on the others. The control loops that couple them —
backpressure, client‑side retry/adaptation, AIMD limiting — *emerged
progressively* rather than being designed as one loop, and several loops that a
cohesive design would close were left **open**. Scaling the system as a whole,
rather than tuning its parts independently, was correspondingly hard.

> DRAFTING NOTE — this is the deepest anomaly; it unifies §§7–9 (the create →
> adapt → drain control system) under one principle: *close the loops the old
> system left open.*

### 2.2 Roadmap capture (why incremental evolution could not arrive here)

Adoption is a gravity well: engineering effort flows to what existing customers
need next, and existing customers deploy on the JVM. The improvements that a
ground‑up reconsideration enables (a different substrate, a smaller footprint,
an in‑browser engine) are precisely the ones no current customer would think to
ask for. This is not a criticism of the prior roadmap; it is a structural reason
the new paradigm had to come from outside the well.

### 2.3 The exogenous shock: local LLMs make footprint a first‑class constraint

The rise of capable **local** LLMs changes the resource contract on a developer
workstation: the model wants the RAM. An engine that expects to coexist with a
local model must have the smallest possible memory footprint — not as an
efficiency nicety, but as a hard precondition for the new class of workflow
(LLM‑assisted development and analysis) to exist at all. Footprint moves from a
metric to a *constraint*, and then (§8) to a *capability*.

### 2.4 Emergence without cohesion

The overall shape of a system around the engine — modeler, getting‑started
repository, developer IDE, metrics interface — emerged, but sequentially and
disconnected. The patterns were real; their integration was incidental. Nano's
response is **cohesion by design**: one self‑contained binary in which engine,
durable journal, read model, web console, and an in‑browser build of the engine
are facets of a single artifact (§9, §11).

---

## 3. New first principles

### 3.1 Emergence → first‑class

The organizing move of the entire design: every pattern that *emerged* under the
old paradigm is promoted to a designed, first‑class concern. Backpressure is not
a valve bolted onto a gateway; it is one arc of a cluster‑wide control loop.
Client adaptation is not the client's private problem; it is part of the same
loop. Memory reclamation is not a background chore gated on an unrelated
subsystem; it is a designed invariant.

### 3.2 Self‑optimization as a discipline

> **If we can tell you *how* to do it, and *when* to do it — why don't we just
> do it?**

Anything the engine can decide correctly on its own, it decides on its own.
Tuning a human would only ever set by watching a metric and applying a rule is
instead performed by the engine, continuously, from that same metric. The
subsystems that realize this are on by default and are no‑ops on a single node
and under balanced load, so the defaults are byte‑identical to prior behaviour
exactly where that behaviour was already correct. (See README, "Self‑optimizing
by design".)

> DRAFTING NOTE — precision guard: the claim is **one business decision you ever
> *need* to set**, and "everything else is *derived*." Override switches do
> exist (all default‑on); we state that plainly rather than claim "literally one
> setting exists," which a reader would falsify by finding the env vars.

### 3.3 The one irreducible decision

The single thing the engine cannot derive is a value judgement: *what do you want
to do when the queue to your business is full?* Do you admit all comers and let
the meal‑serving time stretch (value new business), or ask newcomers to come back
later so patrons already seated are served as fast as possible (value speed of
service to those already inside)? This is formalized in §5.

---

## 4. The invariant: a drop‑in replacement (revolution behind a fixed contract)

A hard design constraint: Nano is an **API‑compatible drop‑in replacement for
Camunda 8**. This is not maintained by hand — the REST layer (models, `axum`
router, and service traits under `generated/`) is **code‑generated from the
Camunda 8 v2 REST OpenAPI specification** (`spec/rest-api.yaml`) via a pinned
`openapi-generator-cli`; the engine is a stub implementation behind Camunda's own
contract (DEVELOPMENT.md §"code‑generation pipeline"). Regenerate from an updated
C8 spec and the surface tracks.

Three consequences make this invariant load‑bearing for the whole paper:

1. **A controlled experiment.** With the interface generated from Camunda's own
   spec and held fixed, every difference in footprint and behaviour is
   attributable to the *architecture behind* the contract, not to interface
   changes. The paper can therefore make causal claims most architecture papers
   cannot.
2. **Commensurability by design.** The paradigm shift preserves interface
   commensurability: existing C8 clients, SDKs, and tooling work unchanged. The
   revolution is deployable as a drop‑in, not a migration.
3. **The on‑ramp to the frontier.** Because Nano speaks the same contract and can
   already ingest Camunda 8 record exports (`processos/src/camunda_import.rs` →
   Nano `traces.json`), real historical production workloads can be replayed on
   it — the bridge to empirical, counterfactual optimization (§8.3, Part II).

**The honest tension.** A fixed contract costs design freedom: Nano must
reproduce C8 client‑facing semantics (job activation, message correlation, FEEL,
key/domain types) even where a cleaner model exists. The resolution is additive,
not substitutive — the standard API remains the compatible default, and **Falcon**
(§9) is an *optional* native protocol for higher performance. Compatible by
default; faster if you opt in.

---

## 5. The decision, formalized: the compressor/limiter model

The behaviour at the capacity ceiling is a dynamics‑processing problem. Treat
offered load as an input signal and the capacity ceiling as a threshold; "what do
we do with signal above the threshold?" has exactly two canonical answers, and
they are the two SLA modes:

- **`admission` mode = a compressor.** Nothing above the threshold is rejected;
  per‑request "gain" (speed) is reduced so the whole signal passes with its
  dynamic range squashed. Latency rises smoothly; no one is turned away. *No
  information is lost — only dynamic range.*
- **`latency` mode = a brick‑wall limiter.** End‑to‑end latency is held flat by
  clamping the input: creates above the ceiling are clipped (`503`). What passes
  stays fast and clean; peaks above the ceiling are removed. *Clipping loses data
  but keeps what remains pristine.*

This is the drop‑vs‑delay SLA tradeoff, and the metaphor is *isomorphic to the
mechanism, not decorative*: the terminology (limiting/clipping vs. compression;
and pointedly **not** gating, which cuts signal *below* a threshold) exposes the
real invariant. `admission` mode is honestly a soft‑knee compressor followed by
an always‑in‑circuit **safety limiter** (the memory‑safety rails, §8), so the
signal can never clip into destruction (a crash). The AIMD limiter's
additive‑increase / multiplicative‑decrease *is* the compressor's attack/release
time‑constant. The control is a **switch, not a knob**, because it offers two
discrete behaviours — an affordance drawn directly from a rack compressor/limiter
with a mode switch. (Full treatment: ADR 0013.)

> DRAFTING NOTE — the model *predicted* the design (switch not knob; safety
> limiter always in circuit; attack/release = AIMD). Present it that way, not as
> a metaphor applied after the fact.

---

## 6. Closing the loops: the create → adapt → drain control system

Six years ago, in a three-hour podcast conversation, Falko — whose name this
protocol carries — walked through a spreadsheet he kept of how L2 cache behaviour
moved engine throughput, tuning what felt like every available knob by hand. Two
things were true at once: the craft was extraordinary, and it was *manual*. Every
knob was a place where a human stood in for a controller the system did not yet
contain. Listening, one question kept surfacing: why can't the clients auto-scale
*inside* the system? The engine knew when it was saturated; the client learned it
only by being refused. That gap is an **open feedback loop** — and an open loop is
an invitation to close it.

Closing it was a years-long approach, not a single act. First, backpressure
backoff became the default in the client SDKs, so a refused client waited instead
of hammering. Then the backoff became **AIMD** — additive-increase,
multiplicative-decrease — so a client probed for headroom and yielded on the first
sign of loss, the way a well-behaved network flow does. Then the controller was
tuned to the *integrated* system specifically: a modified TCP-congestion algorithm
whose constants reflect the characteristics of a BPMN engine under load rather
than a generic link. Each step moved the intelligence a little further from the
human and a little deeper into the machine. Nano and Falcon are where the loop
finally closes — and not only between one client and one server, but *between the
nodes of the cluster and within the engine itself*: throughput, available
resources, and backlog sensed everywhere and shared, so the whole system behaves as
one organism that sizes itself. This is the §2.1 anomaly resolved, and the central
thesis made concrete: a pattern that *emerged* progressively across the SDKs is
promoted to a **first-class**, always-on property of the system.

### 6.1 What the old paradigm could close, and what it could not

The old paradigm did close a loop — a good one — but a *local* one. Zeebe limits
admission at a **per-partition valve**: Netflix concurrency-limits
(`StabilizingAIMDLimit`, plus Vegas/Gradient/Fixed variants) installed at
`LogStreamPartitionTransitionStep.buildLogStream` via `withRequestLimit`. When a
partition is saturated it rejects, and the gateway's role is essentially to *map*
the broker's `RESOURCE_EXHAUSTED` back to the caller. It is a real controller, and
per-partition AIMD is a genuine feedback loop — but the loop's horizon is one
partition, and the client sits *outside* it, learning the system's state only
through a refusal. Cluster-level questions — which node should take this create,
is that node's problem local or shared, how should work be shared fairly across a
heterogeneous fleet — are outside the valve's field of view.

### 6.2 Nano: one cluster-wide loop, three coupled controllers

Nano treats server and clients as **one system** and closes the loop across the
whole of it. Three controllers act together:

1. **Admission** — a self-sizing AIMD watermark with create-shedding
   (`backpressure.rs`), latency-driven rather than count-driven: the watermark
   grows while end-to-end latency stays healthy and multiplicatively backs off the
   moment it does not. This is the direct descendant of the SDK-side story above,
   now resident in the server and shared by every client.
2. **Placement** — load-aware smooth weighted round-robin over **gossiped
   per-node load** (ADR 0014). A create is steered toward the node most able to
   absorb it, and a saturated owner that receives a forwarded create **sheds it
   back to ingress for rerouting** rather than queueing it — node self-protection
   as a first-class move, not an emergent accident.
3. **Fairness** — backlog-weighted job-activation across the cluster (ADR 0001), so
   that under contention work is *shared* in proportion to demand rather than
   captured by whoever polls most aggressively.

The distinction from the old paradigm is not "we added AIMD" — Zeebe has AIMD too.
It is *where the loop lives and how far it sees*. Zeebe's is a per-partition valve;
Nano's is a cluster-wide control system with the client inside it. A client sees
backpressure only under genuine cluster-wide saturation, and when it does, it
adapts — because it is a participant in the loop, not a supplicant outside it.

### 6.3 Why this degrades gracefully

Because sensing is distributed and the controllers are coupled, a heterogeneous
fleet degrades *gracefully* rather than as a monolith. Nodes hit their throughput
and memory ceilings at different times; when one does, placement steers new work
elsewhere, self-protection sheds what would have piled up, fairness keeps the
drain equitable, and admission sheds at ingress only when the *cluster* — not one
unlucky node — is genuinely full. The human who once read a spreadsheet and turned
a knob is replaced not by a bigger knob but by a loop that turns itself. That the
one decision left for a human to make is a *business* decision, not a tuning
decision, is the subject of §5 — and the whole point.

---

## 7. The engine core

> DRAFTING NOTE. Points: deterministic, event‑sourced `engine-core`; single‑writer
> per‑partition actor (`DeepthiHandle`) — a *convergence* with Zeebe's
> `StreamProcessor` actor, stated honestly (not everything is a divergence, which
> makes the divergences credible). The distinctive move: the same Rust
> `engine-core` compiles to WASM as **µ‑nano** (~0.5 MB, ~0.2 MB gzipped) and runs
> in the browser (ADR 0005) — a correctness argument (the browser engine *is* the
> production engine) and impossible under a RocksDB‑backed design.

---

## 8. Footprint as capability

### 8.1 Memory as a designed invariant, not a background chore

In the old paradigm, memory is a background chore: you provision generously, watch
a gauge, and reach for backpressure when it climbs. Nano treats the resident
footprint as a *designed invariant* — a quantity the engine is built to hold near
its true working set and to return to the OS when the work is gone. The clearest
illustration is a coupling we inherited by default, examined it, and severed.

Consider what pins memory in a log-structured engine. State and log can only be
reclaimed once every consumer that must see a record has seen it. In Zeebe the
snapshot/compaction bound is exactly this lower envelope —
`Math.min(exportedPosition, backupPosition, lastProcessedPosition)`
(`StateControllerImpl.java:213`) — and a stale or slow exporter therefore "pins
the snapshot/compaction position and prevents log compaction"
(`ExporterDirector.java:496‑497`; `ExportersState.getLowestPosition`). This is not
an oversight. It is *forced* by a uniform, disk-backed RocksDB design: state and
event log share one compaction gate, and correctness demands the gate wait for the
slowest acknowledged reader. Under that architecture the coupling is the right
call, and RocksDB's mmap'd pages soften the cost — memory pressure pages state out
to disk rather than pinning the heap.

Nano hits the *same class* of problem and, freed from that architecture, resolves
it the opposite way. Our hot state is a Rust `HashMap` on the heap: it cannot page
out, so a downstream reader's lag translating into resident RAM is strictly
*worse* for us — but it is also removable, because two properties Zeebe's uniform
design does not isolate hold for us (ADR 0012). First, **the exporter reads the
event log, not hot state**: the read-model projection (`readstore.rs
upsert_variables`) is fed by event payloads, never by a read of a live instance.
Second, **a terminal instance's variables are write-only to the engine**: once an
instance is `Completed`/`Terminated`, nobody — not workers, not the exporter, not
journal recovery — ever reads its variables from hot state again. They are pure
liability in the heap the moment the instance ends.

So Nano **reclaims terminal-instance variable memory on completion, independent of
exporter position** (ADR 0012). On `ProcessInstanceCompleted`/`Terminated` the
~50 KB payload is dropped immediately; a lightweight control-only shell (key +
terminal state, no variables) lingers only until the exporter has projected the
instance, so point-in-time status still resolves during the projection gap.
Exporter lag now costs disk retention and read-model staleness — never hot-state
RAM, and never admission. On the 3-node GCP soak (sha `15f5239f4d6cff96`), peak
single-node resident variable memory fell from **~4.35 GB** (residue that lingered
for *minutes* after load, gated on exporter progress) to **131 MB**, reaching
**0 immediately** post-load regardless of exporter position (`PERFORMANCE.md`,
ADR 0012 soak section). Same anomaly, inverted outcome — and the honest reason it
inverts is that we had the freedom to choose a non-uniform state model, not that
the earlier choice was wrong for its world.

### 8.2 The mechanisms

Decoupling terminal state is the load-bearing idea; a handful of supporting
mechanisms keep the footprint honest across the whole lifecycle. They fall into
three layers.

**The live working set is bounded, not just watched.** When creates outrun
completions, the Active backlog is capped by *byte-aware adaptive variable spill*:
the working set of live instances is written to disk under memory pressure, driven
by measured jemalloc `resident` rather than instance counts, with a byte-based
guard that avoids futile spill sweeps when no candidate can actually shed bytes
(ADR 0012, Fix 2; `journal.rs maybe_var_spill_pressure`). Snapshots stay lean:
the periodic snapshot carries **control state only** while variables live in an
authoritative durable `var-store.sqlite` that recovery reads directly, so a
snapshot never has to clone the payload heap (ADR 0012, Fix 1a/1b;
`journal.rs snapshot_and_rotate_lean`, `varstore.rs`). The var-store's WAL is in
turn bounded by periodic `wal_checkpoint(TRUNCATE)` so the durability tier cannot
itself become an unbounded balloon (ADR 0013).

**Freed memory is actually returned to the OS.** A general-purpose allocator keeps
freed pages on its own free lists, so an *idle* server pins its burst peak long
after the work is gone. Nano vendors and statically links **jemalloc**, configured
to return dirty/muzzy pages on a ~5 s decay (`dirty_decay_ms:5000,
muzzy_decay_ms:5000`), with the background purge thread enabled where the platform
supports it (Linux). Where it does not (macOS has no jemalloc background thread),
an **idle-purge tick** compacts the hot-state maps after a configurable quiescence
window and forces `arena.<all>.purge` via `mallctl`, returning the pages
immediately (`server/src/memory.rs`; `NANOBPMN_IDLE_PURGE_MS`, default 5000, 0 to
disable). This is why the post-load curve reaches 0 rather than merely ceasing to
grow.

**The footprint is measured truthfully.** The engine exports its own allocator
decomposition — `nanobpm_jemalloc_bytes{kind="allocated"|"active"|"resident"|
"mapped"|"retained"}` (`main.rs`) — so operators reason about `resident` (the
figure that matters), not a proxy. This also encodes a hard-won measurement
caveat: on macOS `ps -o rss` under-reports dramatically; the meaningful number is
jemalloc `resident` / `phys_footprint` (what `footprint` and Activity Monitor
report). Reporting the right quantity is part of treating footprint as a
first-class invariant rather than a background chore.

### 8.3 From constraint to capability

Held small on purpose, the footprint stops being a virtue to defend and becomes an
*enabling constraint* — it lets the engine live in places the old paradigm could
not host, and answer questions it could not pose.

The first is **coexistence with a local LLM**. A developer running a quantized
model on a workstation needs every gigabyte of RAM for weights and KV-cache; the
process engine is now a lodger that must leave the room mostly empty. A JVM +
RocksDB engine provisioned for a comfortable heap is simply the wrong houseguest.
A Rust engine that idles near **13 MB** resident (jemalloc `resident`; ≈10 MB
`phys_footprint` for a freshly started, empty console-enabled release build on
macOS) and returns memory to the OS the moment it goes quiet is one you can leave
running next to the model.
This is not a micro-optimization; it is what makes an *agentic*, engine-in-the-loop
workflow feasible on a single machine at all.

The second is **counterfactual replay**. If a single engine instance is cheap
enough in memory to run many at once, you can take a historical production workload
and re-run it — not once, but across a fan of model-variant hypotheses — to ask
"what would have happened if the process had been shaped differently?" Small
footprint is the precondition: it is what turns the engine from a thing you deploy
into a thing you *instantiate by the dozen* for empirical exploration. This is the
bridge to Part II, and the sharpest expression of the constraint-to-capability
move: the discipline the LLM shock forced on us is exactly the property that opens
a new class of question.

---

## 9. Distributed runtime and heterogeneity

> DRAFTING NOTE. Points: **Falcon** bidirectional WebSocket protocol (additive
> native protocol; contrast Zeebe's gRPC long‑poll `LongPollingActivateJobsHandler`
> + broker fanout `RoundRobinActivateJobsHandler`, and the newer push
> `StreamJobsHandler`/`ClientStreamer`). Leader‑local activation + lease digest
> (ADR 0002). **Durability as a spectrum** (ADR 0003): Zeebe uses Atomix Raft with
> fixed majority quorum (`SimpleVoteQuorum` = n/2+1) and *no configurable
> durability tiers*; Nano adds a leader‑durable tier + app‑driven auto‑recovery,
> separating local‑journal durability (`sync`/`async`) from replication durability.
> Heterogeneous clusters: nodes hit throughput/memory ceilings independently;
> placement + fairness + self‑protection make the cluster degrade gracefully
> rather than as a monolith.

---

## 10. Hindsight on Zeebe: choices revisited (code‑cited, both sides)

Framing for every row: *Zeebe made the right call under its constraints; freed
from those constraints, we chose differently.* Convergences are stated as
convergences.

| # | Concern | Zeebe (cited) | Nano (cited) | Anomaly resolved |
|---|---|---|---|---|
| 1 | Exporter ↔ compaction | Snapshot/compaction bound = `min(exported, backup, processed)`; a stale exporter "prevents log compaction" (`StateControllerImpl.java:213`, `ExporterDirector.java:496‑497`, `ExportersState.getLowestPosition`) | Terminal‑state reclamation **decoupled** from exporter position — 4.35 GB→131 MB, →0 post‑load (ADR 0012) | Open loops; footprint |
| 2 | Authoritative state | RocksDB via `zb-db`; `ZbColumnFamilies`; per‑entry `deleteIfExists` | In‑mem hot state + append‑only journal + SQLite read model + tiered spill; WASM‑capable (µ‑nano) | Footprint → capability |
| 3 | Backpressure | Broker‑side per‑partition Netflix concurrency‑limits (`StabilizingAIMDLimit`/Vegas/Gradient/Fixed) at `LogStreamPartitionTransitionStep`; gateway maps `RESOURCE_EXHAUSTED` | Self‑sizing AIMD + create‑shed **+ cluster placement + activation fairness** (`backpressure.rs`, ADRs 0001/0014) | Server+client one system |
| 4 | Ceiling behaviour | One outcome: reject | Two: compressor vs. limiter — the one business knob (ADR 0013) | The irreducible decision |
| 5 | Job delivery | gRPC long‑poll (`LongPollingActivateJobsHandler` + `RoundRobinActivateJobsHandler`) + push (`StreamJobsHandler`/`ClientStreamer`) | Falcon bidirectional WS + leader‑local activation + lease digest (ADR 0002) | Cohesion |
| 6 | Replication | Atomix Raft, fixed majority quorum (`SimpleVoteQuorum` = n/2+1); no durability tiers | Same quorum path **+ leader‑durable tier** + app‑driven recovery (ADR 0003) | Hindsight |
| 7 | Cohesion | Broker + gateway + external exporters + Operate/Tasklist + separate modeler | One self‑contained binary: engine + read model + console + in‑browser µ‑nano | Emergence without cohesion |
| — | Single writer (convergence) | `StreamProcessor` actor per partition on `ActorSchedulingService` | Per‑partition single‑writer actor (`DeepthiHandle`) | (agreement) |

> DRAFTING NOTE — decide how pointed to be per row; §10 could be the most
> compelling or the most delicate. Current tone: hindsight, not critique.

---

## 11. Observability as the meter

> DRAFTING NOTE — the compressor/limiter's gain‑reduction meter, realized: ceiling
> "LED" metrics (`nanobpm_ceiling_active{ceiling=throughput|memory}` +
> `..._hits_total`) and worker‑provisioning/starvation hints
> (`nanobpm_job_type_{activatable,workers,starved}`), published ~1 Hz off the hot
> path. The operator's feedback that the processor is working. (ADR 0013.)

---

## 12. Evaluation

> DRAFTING NOTE — measured, honest, reproducible. Include:
> - Throughput: 3× `c2‑standard‑16`, RF=3, P=12 — ~13k PI/s/node, ~39k agg
>   (PERFORMANCE.md 2026‑07‑01).
> - **The honest ceiling, named as future work:** the single per‑node read‑model
>   exporter is CPU‑bound on SQLite inserts (~250k events/s, one core ~100%, 15/16
>   vCPU idle); throughput is flat across fsync modes. Understanding our own wall
>   *strengthens* the paper and sets up the next chapter.
> - Memory: ADR 0012 soak (4.35 GB→131 MB→0). Cold-idle ≈13 MB resident
>   (measured, empty console release build, macOS).
> - Latency floor: un‑pipelined quorum ≈ ~100 jobs/s single‑worker vs 33k @64
>   workers (ADR 0003) — a property of synchronous quorum, not of Raft.
> - Compatibility: the surface is *generated* from the C8 spec; validate against
>   the C8 SDK conformance suites (`camunda-sdk-conformance-test`, the
>   orchestration‑cluster‑api SDKs).

---

## 13. Related work and the commensurable‑interface refinement of Kuhn

> DRAFTING NOTE — Zeebe/Camunda lineage (respectful: this stands on a decade of
> their engineering — the "signed subsystems" ethos). Audio‑dynamics borrowing
> (compressor/limiter) as a *correct model transplanted*, not a skin. The Kuhn
> refinement: a paradigm shift engineered to preserve interface commensurability,
> so adoption does not require abandoning tooling — arguably a contribution in its
> own right. Prior art on backpressure (Netflix concurrency‑limits), event
> sourcing, LSM vs. journal+read‑model.

---

## 14. Conclusion: the frontier

> DRAFTING NOTE — restate: emergence → first‑class, one decision, drop‑in, tiny.
> Then the trajectory this opens (Part II): replaying real history against
> counterfactual model variants to optimize processes *empirically*. It is an
> Advanced Research Prototype — but the direction is legible.

---

## Part II — ProcessOS (later)

> DRAFTING NOTE — companion sections once Part I is drafted. Empirical process
> optimization: counterfactual replay of historical C8 workloads
> (`camunda_import` → traces → replay harness), model‑variant experiments, and the
> LLM‑assisted investigation layer (personas, replay ranking, the RAD/extension
> and language‑pack tooling — ADRs 0004, 0006–0011). The tiny footprint (§8) is
> what makes running many alternative‑reality instances feasible.

---

## Appendix A — Source map (for authors)

- **Nano:** `engine-core/` (deterministic engine, µ‑nano WASM), `server/` +
  `generated/` (C8 v2 REST, generated from `spec/rest-api.yaml`),
  `server/src/{backpressure,journal,readstore,varstore,falcon,raft,placement,
  metrics}.rs`, `docs/adr/0001‑0014`, `PERFORMANCE.md`, `README.md`.
- **Zeebe** (`~/workspace/camunda/zeebe`, HEAD ~2026‑06): `broker/` (exporter
  director, state controller, backpressure transition), `engine/` +
  `zb-db/` (RocksDB, `ZbColumnFamilies`), `stream-platform/` (`StreamProcessor`),
  `gateway/` + `gateway-grpc/` (activate‑jobs handlers), `atomix/` (Raft quorum),
  `protocol/` (`ZbColumnFamilies`).
