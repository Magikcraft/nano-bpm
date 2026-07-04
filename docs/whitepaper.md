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

And it is usable **today**, not only as a thesis. The most immediate use is the
most modest one: a local development replacement for a full Camunda 8 deployment.
A developer who wants Camunda 8 on their workstation today reaches for `c8run`,
Camunda's own single‑distribution way to run the stack locally; they do it to get
the *API* to build and test against. Nano offers that same drop‑in API from one
small binary that idles at a few megabytes and returns its memory to the OS when
quiet (§8), for as long as the subset of Camunda 8 it currently implements covers
what a given project exercises. The claim is deliberately bounded and honest — not
*every* C8 feature, but the contract you actually use in local development — and
that supported subset is expanding continuously. The paradigm argument that
follows is the long game; running your dev cluster on it is available now.

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

Concretely, Falcon is that loop made into a wire protocol: a single, persistent,
bidirectional, **credit-metered** WebSocket per client (`server/src/falcon.rs`; see
§9.1 for the wire in full). Its shape mirrors the loop directly — over one socket
run **two credit lanes**, the loop's two directions. A **demand-pull** lane
delivers work: a worker advertises how many jobs it can take as a credit count, and
the server pushes jobs only up to that number, so a worker is never flooded beyond
what it asked for. A **submission** lane admits work: process-instance creation
draws on credits fed directly from the engine's own processing headroom through the
backpressure controller, so under saturation the server simply *withholds credits*
and the client stalls its intake — no `503`, no retry, no thundering herd. That
credit window is the closed loop reified on the wire: rather than refuse a client
and leave it to infer the system's state, Falcon hands each client exactly as much
demand, and accepts exactly as much new work, as the cluster can currently absorb —
and no more. Job completion, by contrast, flows **unmetered**: draining backlog must
never be throttled. The transport mechanics are §9's subject; here the point is only
that the control loop of §6 has a physical embodiment, one persistent socket wide,
and that it is named for the craftsman whose L2-cache spreadsheet first made the
loop visible: Falko Menge. Artists sign their work.

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

Underneath the control loop and the wire sits a small, stubborn thing: a
deterministic, event-sourced state machine that turns commands into events and
nothing else. `engine-core` is `std`-only and zero-dependency; it never reads a
wall clock (every command is applied at an injected instant, `apply_command_at(cmd,
now)`), so the same commands always produce the same events — the definition of
determinism — and events carry enough information to rebuild state by replay. Most
tellingly, **persistence is a caller's concern, not the engine's**: the core
appends its event log wherever the embedder wants (in-memory, a WAL, `redb`,
SQLite) and replays to recover. The engine does not contain a storage engine. That
single decision is what the rest of this section — and much of the paper — rests
on.

**A convergence, stated plainly.** Around that core, each partition is driven by a
**single-writer command actor** — Nano calls it *Deepthi* (`server/src/deepthi.rs`,
`DeepthiHandle`) — that owns the durable journal and serializes every mutation
through one thread. This is a genuine **convergence** with Zeebe, whose
`StreamProcessor` is likewise a single-writer, event-sourced actor per partition.
It is worth saying so directly: not every choice here is a divergence, and a paper
that pretended otherwise would not be trustworthy. The single-writer,
event-sourced, replayable design is one of Zeebe's best ideas, and Nano keeps it.
The divergence is narrow and already stated — engine-core carries no embedded
key-value store; storage is injected from outside.

**Why the empty core matters: one engine, many hosts.** Because the core is
`std`-only, zero-dependency, and storage-agnostic, the *same* Rust `engine-core`
compiles to three very different targets without change: natively for the server,
via a C-ABI/FFI surface (`engine-core/src/ffi.rs`) for embedding in iOS/Android
apps, and to `wasm32` as **µ-nano**, which runs in a browser tab. The shipped
browser artifact is **577 KB** (229 KB gzipped, 175 KB brotli) — an entire BPMN
engine small enough to download as part of a web page. This yields two arguments
the old paradigm cannot make.

The first is a **correctness** argument. The engine that simulates a model live in
the browser modeler is not a reimplementation that can drift from the server — it
is *literally the same code*, fed the same commands, producing the same events by
the same deterministic rules. A discrepancy between "how it ran when I designed it"
and "how it runs in production" cannot arise from two engines, because there is one
engine. The second is an argument from **impossibility**: none of this is available
to a RocksDB-backed design. RocksDB is a native C++ library that does not compile
to `wasm32` and will not live in a browser tab or a 229 KB download; an engine that
embeds its storage engine cannot be lifted whole into those hosts. By making
persistence a caller concern, Nano can put the entire engine wherever it is needed
— the same property that lets a single-node build embed itself inside an
application ("Embed Nano", ADR 0005) and lets many lightweight instances spin up for
the counterfactual replay of Part II.

A candid scope note, in keeping with the prototype's status: today the browser
build drives the modeler's *simulator* (virtual clock, no dispatch loop), and the
fully embedded single-node engine is a proposed direction (ADR 0005), not a shipped
product. But the load-bearing fact is already true and already in production use in
the console: one deterministic `engine-core`, compiled to `wasm32`, is the engine
running in the page. The portability is not aspirational; only some of its
destinations are.

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

If §6 is the control system, §9 is the machinery it runs on: how work is delivered
over the wire, where a job's lease lives, how far durability is allowed to bend,
and how a cluster of *unequal* nodes behaves when parts of it fill up. In each case
the old paradigm made a sound, conservative choice; Nano, working under the
drop-in constraint of §4, keeps the client-facing contract and revisits the choice
underneath it.

### 9.1 The wire: two generations of closing the loop

Zeebe's own history is instructive, because it is already a story of the loop
being closed incrementally. The first generation of job delivery is a gRPC
**long-poll**: a worker calls `ActivateJobs` and the gateway holds the request
open (`LongPollingActivateJobsHandler`) while fanning demand across partitions
round-robin (`RoundRobinActivateJobsHandler`), returning when work appears. The
second generation is **push**: the worker opens a job stream and the broker
pushes jobs to it as they arrive (`StreamJobsHandler`,
`transport/.../ClientStreamer`), removing the poll latency entirely. That
progression — poll, then push — is the same instinct that animates §6: move the
initiative toward the system, let the server hand out work as it becomes
available.

Falcon (defined in §6) takes that trajectory to its conclusion and adds the piece
neither Zeebe generation carries: it unifies delivery *and* the write path on the
one credit-coordinated socket of §6, so a single backpressure account governs both
job push and instance admission rather than each fending for itself. It is an
**additive native protocol**, not a replacement: a Nano cluster still speaks the
full Camunda 8 gRPC/REST surface (§4), so an unmodified C8 client — long-poll or
job-stream — works unchanged. Falcon is the "faster if you opt in" path, and its
advantage is not merely a different framing but that admission and delivery share
one credit account. Compatible by default; unified if you choose it.

**The transport, in full.** One socket carries a small set of JSON frames, each a
tagged union keyed by a camelCase `type`. On connect the server sends a `welcome`
(the initial submission window and heartbeat cadence) and an opening
`submissionCredits` grant; idle sockets exchange `heartbeat`s. Client→server frames
are `subscribe`, `jobCredits`, `createInstance`, `completeJob`, `failJob`,
`throwError`, `awaitInstance`, and `heartbeat`; server→client frames are `welcome`,
`job`, `commandResult`, `instanceCompleted`, `submissionCredits`, `pressure`, and
`heartbeat` (`server/src/falcon.rs`; the full AsyncAPI 3.1 schema of every frame is
`docs/falcon.asyncapi.yaml`). Two credit lanes share the one engine thread. On the
**job-push lane**, a worker `subscribe`s to a job type with a credit count; a single
server-side dispatcher leases jobs *round-robin across all subscribers* and pushes
`job` frames while credits remain, topped back up by `jobCredits` — and the lease
itself is the at-least-once guarantee (a job pushed to a worker that never completes
it is reclaimed by the lock-expiry tick, so a dropped socket needs no special
handling). On the **submission lane**, `createInstance` draws on the credit window
of §6 while completions (`completeJob`/`failJob`/`throwError`) flow unmetered.

**Await-completion without a held request.** A `createInstance` may ask to await its
outcome, but rather than pin a request open for the unbounded life of an instance,
the server answers the create immediately with a `commandResult` carrying the
`processInstanceKey`, then emits an asynchronous `instanceCompleted` frame later,
correlated by the create's `corr`. If the socket drops in between, the client
reconnects and sends `awaitInstance` with the persisted key; because the read model
is durable history, an already-terminal instance resolves *immediately*, so
`awaitInstance` doubles as a completion poll. A long-lived await costs a map entry,
not a held connection.

**Ordering and throughput are deliberately orthogonal.** Frames on one socket are
processed in arrival order with each engine command awaited inline, so successive
`createInstance`s on a *single* socket serialize at journal-fsync latency;
throughput instead comes from *concurrency across sockets*, where the group-commit
journal writer batches many connections' appends into one fsync. The job-lifecycle
commands go further with **ack-before-fsync pipelining**: the server applies the
command (fixing its order in the log), replies immediately, and lets fsync complete
asynchronously (~5 ms later), batching many connections' completions into one group
commit — measured at 4× the throughput of awaiting fsync inline (2280 vs 572
writes/s). The trade-off is explicit and, again, at-least-once: a crash in that
~5 ms window re-activates the job on restart when its lock expires (the equivalent
REST completion endpoint still awaits fsync; only Falcon pipelines). The practical
consequence is a worker topology — one stream per job-type worker, plus a small pool
of submission sockets sized to the create rate — which the companion SDK adopts by
default.

The choice of **WebSocket** rather than gRPC for that native path is deliberate
and field-driven. gRPC is technically capable, but a decade of customer
deployments surfaced recurring *operational* friction that had nothing to do with
the protocol's semantics: HTTP/2-aware load balancers and proxies, TLS/ALPN
negotiation, corporate network middleboxes, and a code-generation toolchain that
each deployment had to keep working. A WebSocket rides ordinary HTTP(S) that every
proxy, gateway, and firewall already understands, and needs no per-language stub
generation to speak. Falcon trades gRPC's schema-tooling for a transport that
tends to *just connect* through the infrastructure customers actually run — a
choice made not for elegance but because the friction was real and repeatedly
paid.

### 9.2 Where the lease lives

A job moves create → activate → complete. The subtle observation (ADR 0002) is
that only two of those three record durable *progress*; **activation records a
lease**, not progress — a `worker` and a `deadline`, whose sole cross-replica
effect is the latch the completion guard reads. Yet under RF>1 the naïve design
replicates all three, so every job costs **three quorum commits**, and, worse,
per-worker activation fragments into many small commits that compete with the
create/complete traffic that actually matters (measured: ~1425 jobs/s at 8 workers
*collapsing* to ~196 at 16 as activation floods the commit budget).

Because the system is **at-least-once by construction** — an overrun lease is
reclaimed and the job redelivered, so workers are idempotent regardless —
replicating the lease buys no correctness, only a narrower window of duplicate work
on the rare event of leader failover. So Nano makes it optional
(`NANOBPMN_REPLICATE_ACTIVATION`): in **leader-local** mode the lease lives in the
leader's single-writer actor only, each job costs **two quorum commits instead of
three**, and activation stops competing for the commit budget.

Crucially, this changes nothing about the **authority model**, which is identical
to Zeebe's: completion is **by key alone** — a valid job key *is* the capability,
keys are only ever minted by activation, and neither engine binds completion to the
activating worker's identity (`engine/mod.rs:616`, "Completion is by key alone").
This is not an incidental property but a *deliberate capability*: because
possession of the key — not the identity of the holder — authorizes completion, a
worker can **forward** a job. It activates the job and fire-and-forgets the key to
a decoupled downstream system, which completes the job later using that key
directly, without ever holding the original stream. Binding completion to holder
identity would break this pattern, so Nano preserves it exactly. The replicated
`activated` latch was never a holder check; it only asserts that a job was
activated at least once. Leader-local mode merely lets a follower apply a
replicated `CompleteJob` to a job it only ever observed as `Created` — the drop-in
contract (any client holding a valid key may complete the job) is untouched.

What makes a leader-local lease *safe* — and why the other replicas need no
knowledge of it — is that **activation is leader-exclusive per partition**. Every
job key embeds its partition id (`main.rs:587`), so a job belongs to exactly one
partition; a node activates jobs only for the partitions it *leads* and skips the
rest, "their leader activates" (`main.rs:7362`); and Raft guarantees a single
leader per partition per term, fenced against stale leaders (`main.rs:277`). There
is thus never a second node that *could* activate the same job — not because
followers are told about the lease, but because followers do not activate at all.
The one node that activates a partition's jobs is precisely the one holding that
partition's lease in its own memory; coordination is unnecessary because there is
only one actor. What the rest of the cluster *does* know is the **job key itself**:
it is minted on the replicated job-creation path (deterministic key allocation on
every replica, ADR 0002), so every follower holds the job — in `Created` state,
without the lease. Key: replicated, known everywhere. Lease: leader-local. That
asymmetry is the whole design.

Is this distinct from Zeebe? The *exclusivity* is not — it is a **convergence**,
stated plainly: Zeebe also runs one leader per partition and activates only there
(its `StreamProcessor`). The real distinction is narrower and more honest. Zeebe
writes activation as a **replicated follow-up event** —
`JobBatchActivateProcessor.java:193`,
`stateWriter.appendFollowUpEvent(jobBatchKey, JobBatchIntent.ACTIVATED, …)` — so
the lease is durable state that *survives failover*: a new leader knows the
in-flight deadline and waits it out before redelivering. **Nano's default mode does
exactly the same thing.** The divergence is the *optional* leader-local mode,
which — having recognized that activation is a lease, not durable progress, in an
already at-least-once system — takes the lease out of the replicated stream
entirely. The cost is stated honestly: a leader-local lease does not survive
failover, so a new leader re-dispatches in-flight jobs immediately rather than
honoring a deadline it never received — a bounded, at-least-once-tolerable
duplication, not a correctness loss. A middle setting, the best-effort **lease
digest** (`NANOBPMN_REPLICATE_ACTIVATION=digest`), has the leader broadcast its
held `{job_key → deadline}` leases to followers without durability; on promotion
the new leader soft-recovers them and honors the deadlines, shrinking the duplicate
window at no per-job quorum cost. Nano thus turns a single fixed point in Zeebe —
activation is always replicated — into a **three-position choice** (replicated /
digest / leader-local), available only because it separates *lease* from
*progress*.

### 9.3 Durability as a spectrum, not a constant

Zeebe replicates every partition through Atomix Raft with a **fixed majority
quorum** — `SimpleVoteQuorum.java:31` computes `members.size() / 2 + 1` — and
exposes no operator knob to trade that quorum against latency. It is the correct
conservative default: committed means durable on a majority, full stop. But it
means every `create`/`complete` under RF=3 pays a synchronous cross-node quorum
round-trip on the client's critical path — fine under concurrency (group commit
amortizes to ~33k jobs/s at 64 workers) but a hard latency *floor* on
low-concurrency work (~10 ms/job ≈ one quorum round-trip).

Nano treats durability as **two orthogonal dials**, not one constant (ADR 0003):

- **Local journal durability** — `NANOBPMN_DURABILITY=sync|async`: ack after
  `fsync` (survives power loss, ~4 ms media barrier on the path) versus ack after
  the page-cache write with amortized fsync (survives process crash via replay;
  loses a bounded unfsynced tail on OS crash). This axis explicitly "mirrors
  Zeebe's async-exporter model."
- **Replication durability** — `NANOBPMN_REPLICATION=quorum|leader-durable`: the
  Kafka `acks=all` vs `acks=1` model applied to the workflow command log. `quorum`
  is today's behaviour; `leader-durable` acks on the leader's local durable append
  and ships the log to followers in the background, tracking a committed-vs-
  replicated watermark, taking the network round-trip off the critical path at the
  cost of an un-replicated tail on simultaneous leader loss — the same *shape* of
  window as the local `async` knob, but across the replication axis.

Both bounded-loss modes are consistent with the at-least-once contract (a lost
completion tail redelivers; a lost create tail is retried by an at-least-once
producer) and neither is allowed to violate ordering or exactly-once *within* what
it acks. The point is not that `leader-durable` is better — it is opt-in and
default-off, and quorum remains the safe default — but that durability becomes a
**position on a spectrum the operator chooses per workload**, rather than a single
architectural constant.

### 9.4 Heterogeneity: a cluster of unequal nodes

The old paradigm's mental model is a fleet of **interchangeable** brokers — and
this is visible directly in the code, not merely in spirit. Zeebe places partitions
with a `RoundRobinPartitionDistributor` that "distributes the partitions in a round
robin fashion over the set of members," taking as input exactly the member set, the
sorted partition ids, and the replication factor — and nothing else
(`dynamic-config/.../RoundRobinPartitionDistributor.java`). Leader-election
priorities are then assigned to spread leadership *evenly* across those members, the
code's own comment stating the goal: "so that if node 0 dies, the leadership is
evenly distributed on the rest of the followers" (`getPriorities`). No CPU, memory,
or load signal enters the partitioning subsystem anywhere; the one refinement,
`ZoneAwarePartitionDistributor`, spreads replicas across *failure domains* for fault
tolerance while still treating the nodes within them as equals. Job fanout has the
same shape — `RoundRobinActivateJobsHandler` cycles partitions in turn. This is a
*coherent and correct* design for a homogeneous fleet: if every broker is the same,
even distribution is optimal and a capacity signal would be noise. Zeebe does not
assume nodes are heterogeneous — it assumes, reasonably for its era and target
deployment, that they are interchangeable.

Nano assumes the opposite, because its target environments are unequal by
construction: a developer laptop beside a cloud VM, a node sharing a workstation
with a memory-hungry local LLM, or simply nodes that filled up at different times
under an uneven workload. This is where §6's controllers pay off as a *distributed*
system rather than a single valve. Each node senses its own throughput and memory
ceilings independently (§8, §11); per-node load is gossiped (ADR 0014) so placement
steers new creates toward the node most able to absorb them — smooth *weighted*
round-robin, where Zeebe's is plain round-robin; a saturated owner sheds a forwarded
create back to ingress for rerouting rather than queueing it; and backlog-weighted
fairness (ADR 0001) keeps the drain equitable across the uneven fleet. The result is
a cluster that degrades **gracefully and locally** — the busy node slows or sheds
while the rest keep serving — rather than as a monolith paced by its weakest member.
The contrast is not that Zeebe is careless and Nano careful; it is that the two
engines make *different assumptions about the fleet*, and Nano's assumption — nodes
are unequal — is the one the new environments (local LLMs, laptops,
counterfactual-replay fleets) actually present. Heterogeneity stops being a failure
mode to engineer away and becomes an operating assumption the control loop is built
to exploit.

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

A rack compressor/limiter has a light on its face: the gain-reduction meter, which
glows when the unit is actually clamping the signal. It is not decoration — it is
how the engineer *sees* the processor working, so they know the sound is being
shaped rather than merely passing through. §5 argued that Nano's behaviour at the
ceiling *is* a compressor/limiter; §11 completes the instrument by giving it that
meter. Observability here is not a generic dashboard bolted on afterward; it is the
face-plate of the specific machine the paper describes.

**The ceiling LED.** When the engine reaches the edge of its envelope — the point
where the §5 switch actually engages — it lights up, legibly, in Prometheus.
`nanobpm_ceiling_active{ceiling="throughput"|"memory"}` reads 1 while the system is
pinned against that ceiling (the "gain-reduction is happening now" light), and
`nanobpm_ceiling_hits_total{ceiling=…}` counts the *rising edges* — how often the
envelope was reached at all. Together they answer the two questions an operator of
a self-protecting system actually has: *is it clamping right now, and how often does
it?* Crucially, this makes the engine's self-protection **visible rather than
silent**. Without it, an operator sees only symptoms — latency climbing
(`admission` mode) or a trickle of `503`s (`latency` mode) — and must infer the
cause. With it, the engine states plainly, "I am at my throughput ceiling and the
switch you set is doing what you asked." A system that makes its own decisions owes
the operator a light that shows it.

**The worker-provisioning meter.** The other thing an operator needs to know is not
about the engine at all, but about *them*: are there enough workers to drain the
work the engine is admitting? Three per-job-type gauges surface this directly —
`nanobpm_job_type_activatable` (jobs ready to be worked), `nanobpm_job_type_workers`
(workers currently subscribed to that type), and `nanobpm_job_type_starved`, which
lights when a backlog exists but the workers to clear it do not. This is
deliberately a *hint*, because it is the one thing the self-optimizing engine
**cannot** do for you: it can place, shed, compress, and reclaim on its own, but it
cannot conjure a worker process for a job type you have under-provisioned. So,
faithful to the design philosophy of §3 — *if we can tell you how and when, why not
just do it?* — where the engine cannot act, it does the next best thing and *tells
you precisely where to act*, per job type, rather than leaving you to correlate
queue depth against a fleet of pollers by hand.

Both meters, together with the allocator decomposition of §8
(`nanobpm_jemalloc_bytes{kind=…}`, the honest footprint), are published on the
~1 Hz monitor tick, **off the hot path** — the act of measuring must never perturb
the thing measured, least of all on an engine whose whole argument is about what it
does under load (ADR 0013). The meter is thus also the visible boundary of the
system's self-optimization: everything the engine can derive and act on, it does
silently; the small residue it cannot — the one business decision of §5, and the
worker provisioning only you control — it renders legible on the face-plate instead
of hiding.

---

## 12. Evaluation

A paradigm claim has to survive measurement. This section gives the headline
numbers, the honest ceiling behind them, and the trade-offs the design exposes.
All figures are from real runs logged in `PERFORMANCE.md` (with topology and
build), reproducible from the load generator and switches in the repository; no
`[MEASURE]` placeholders remain here.

### 12.1 Throughput

On a cluster of 3× `c2‑standard‑16`, RF=3, 12 partitions, the engine sustains
**~95 000 process instances/s aggregate — ~30 000 per node** (one job per
instance, so this is also jobs/s), and it does so on the *strongest* durability
setting, not a relaxed one: the default quorum-durable replication path (a
majority replicates **and** applies before the client is acked) over local
fsync-before-ack journalling. The lighter tiers of §9.3 (leader-durable, async)
are opt-in and were left off — and, counter-intuitively, turning them on would
*not* raise this number. The sweep behind it already tested the stronger form of
both relaxations: dropping to RF=1 (no replication at all) and moving the journal
to a RAM disk (no fsync) each left aggregate throughput unchanged, at ~0% I/O
wait. Neither replication nor durable I/O is on the binding path; the lighter
tiers buy *latency* (§12.2's floor), not throughput. At this plateau, end-to-end
completion latency measured **p99 ≈ 2.1 s** (fresh state), with the client's
in-flight window set deliberately deep (~16k/node) to hold the cluster at
saturation — so that figure reflects the queue depth chosen to *find* the ceiling,
not a latency floor; `MAX_INFLIGHT` is a latency/throughput dial, and backing it
off trades a little throughput for lower latency. The ceiling is
*coordination-bound*, not resource-bound: at that rate the nodes still run with
roughly a third of their CPU idle and negligible I/O wait, the limit being
cross-thread contention around the single-writer engine actor and the replication
round-trips. That matters for the paper's thesis: the next gains come from
*reducing coordination* — batching the replication path, spreading the writer
across more partitions — not from adding hardware. (Two false ceilings were
cleared to get here: a single read-model
exporter thread, fixed by sharding it per partition for a 2.4× lift, and a
mis-read profiler artifact — the honest arc is in `PERFORMANCE.md`.)

### 12.2 The latency floor is a property of quorum, not of Raft

Throughput and per-request latency answer different questions, and §9.3's
durability spectrum lets the operator choose between them. On the
synchronous-quorum path a single low-concurrency worker sees a **~10 ms/job floor
(~100 jobs/s)** — one unpipelined quorum commit — while the *same* path delivers
**~33 000 jobs/s at 64 concurrent workers**, because group commit amortizes the
quorum across many in-flight jobs (ADR 0003). The floor is thus the physics of
synchronous quorum, not a property of Raft or the engine, and the operator who is
constrained by it rather than by aggregate can choose a lower tier
(leader-durable, async). We report both numbers because quoting only the second
would hide the trade. Concretely, "group commit amortizes the quorum" means one
quorum round-trip durably commits a whole *batch* of pending completions at once,
so its fixed ~10 ms cost is split across the hundreds of jobs riding in that batch
rather than paid once per job — which is why 64 concurrent workers reach ~33k/s,
not 64 × 100/s.

### 12.3 The two durability tiers, and what a node crash actually costs

The choice of §9.3 is not abstract; it changes what the word "completed" promises
when hardware fails. The two tiers we benchmark are the default **strict**
(replication = `quorum`, journal = `sync`) and **relaxed** (replication =
`leader-durable`, journal = `async`).

**Strict — what an ack means.** The client is told a job completed only after a
*majority* of nodes have both replicated and applied the entry and the leader has
fsynced it to disk. Concretely, if any single node dies — leader or follower — every
completion the client ever saw acknowledged is already durable on a majority, so
the promoted leader has it. Nothing the client observed is lost; the price is the
quorum round-trip on the critical path (the §12.2 floor).

**Relaxed — what an ack means.** The client is told a job completed as soon as the
*leader alone* has applied it and written it to the OS page cache (async: the fsync
is deferred, bounded to a ~10 ms / 8 MiB window); followers receive it in the
background. Concretely: if the leader process merely crashes and restarts, it
recovers from its own disk. But if the leader is lost **permanently and
simultaneously** — disk failure, or the VM destroyed — *before* its followers have
caught up, the un-replicated tail (completions the client was already told
succeeded) is gone, and a follower is promoted from the most complete log it holds,
which may be slightly behind.

**Why relaxed is a latency trade, not a correctness hole.** The lost tail does not
corrupt anything, because the whole system is at-least-once. A dropped completion
simply means that job's lease expires and it is redelivered — and because
completion is by key alone (§9.2), an idempotent worker already tolerates
redelivery. A dropped *create* means the instance was never durably admitted, and
the producer (also at-least-once) retries. What relaxed durability must never do —
and does not — is reorder, or lose anything it has already replicated: the leader's
local log stays the single ordered source of truth per partition. So the trade is
exact: relaxed durability converts a rare simultaneous-permanent-leader-loss from
*no data loss* into *a bounded tail of millisecond-scale redeliveries*, in return
for taking the quorum round-trip off every completion.

Empirically, at the ~95k-PI/s operating point of §12.1, the two tiers measure:

> DRAFTING NOTE — table below is being filled from a live fresh-state A/B on the
> 3× `c2‑standard‑16` / 12‑partition / RF=3 cluster (strict vs relaxed, ceiling
> config). Replace the `[MEASURE]` cells with the measured numbers on return.

| Tier | replication | journal | aggregate throughput | p50 | p99 |
| --- | --- | --- | --- | --- | --- |
| **strict** (default) | quorum | sync | `[MEASURE]` | `[MEASURE]` | `[MEASURE]` |
| **relaxed** | leader-durable | async | `[MEASURE]` | `[MEASURE]` | `[MEASURE]` |

The expectation set by §12.1 is that relaxed will *not* materially raise aggregate
throughput — the ceiling is coordination-bound, and the earlier sweep showed
removing replication entirely (RF=1) and fsync entirely (tmpfs) each left aggregate
flat — but that it should visibly lower latency, because it removes the quorum
round-trip from the critical path. The measured table is the honest test of that
prediction.

### 12.4 Footprint under load and at rest

§8 argued footprint is a designed invariant; the soak evidence bears it out. Under
a deliberately hostile flood — worker-starved, 50 KB variable payloads — a node's
resident variable memory holds at **131 MB** and returns to **~0 the moment load
stops**, where before the terminal-state decoupling of ADR 0012 the same residue
sat at **~4.35 GB for minutes** after the cluster went idle. Whole-process memory
stays flat at a few hundred MB per node even under the ~95k-PI/s load above, and a
fresh empty binary idles at **~13 MB**. The engine is small when empty and *stays
bounded* when hammered — the property that makes the co-located local-LLM future of
§2.3 and Part II physically possible.

### 12.5 Compatibility by construction

Nano's central invariant (§4) is that it is a drop-in Camunda 8 replacement, and
the evidence is architectural, not anecdotal: the API surface is **generated from
the Camunda 8 specification**, not hand-written to resemble it, so conformance is
something the build is held to against the C8 SDK suites and generated SDKs that
drive a real cluster. Honest scope for a prototype: the surface is generated and
the semantics this paper leans on are exercised by those clients, but an
exhaustive endpoint-by-endpoint conformance run is evaluation still owed. The
claim is drop-in-compatible *by construction*.

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
