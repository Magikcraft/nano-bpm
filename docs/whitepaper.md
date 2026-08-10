# Nano — An Advanced Research Prototype Engine

**Status: Advanced Research Prototype.** Nano is a distributed BPMN engine that
speaks the Camunda 8 API. It is a research engine, not a product; it implements a
bounded, honestly-stated subset of Camunda 8 that is expanding continuously.

Nano is built for a **single use-case**: the **orchestration of software-delivery
epics across networks of hundreds of agents.** It is used internally at Camunda by
Camunda engineers. **It is not an official product of Camunda.** The engine also
runs as a drop-in local-development replacement for `c8run` — one small binary that
gives you the Camunda 8 API to build and test against — but that compatibility is a
means, not the thesis. This paper is about the thesis.

## The emerging problem

There is a new problem in software engineering, and it is not "how do I make one
agent write better code." It is: **how do you go from fast-pair programming with an
LLM to orchestrating hundreds of agents?**

Boris Cherny's [Steps of AI Adoption][adoption] maps the progression. It moves from
a single supervised pair — one engineer, one agent, every change reviewed (**Step
1: Assisted**) — to an engineer juggling ~10 agents across worktrees (**Step 2:
Parallel**), to a manager-of-managers running ~100 (**Step 3: Supervised
autonomy**), to a VP steering ~1,000+ by intent and monitoring by exception
(**Step 4: AI-native**). The unlock at the top is that a quarter-long migration
becomes a workflow you kick off and check on.

Step 1 is a **tooling** problem — a better editor, a faster model, a tighter loop.
Everything past it is an **orchestration** problem. As the agent count climbs, the
bottleneck stops being *how fast can one agent code* and becomes *how do a hundred
of them run reliably* — durable loops that survive crashes and restarts,
self-verification (tests, build, lint, review) before a human sees a diff, review
convergence against an automated reviewer, fan-out with back-pressure so a thousand
agents don't stampede, and monitoring by exception instead of by babysitting. That
is a **process-orchestration** problem. It is the problem Nano is built for, and it
is the reason a *durable process engine* — not another chat surface — is the right
substrate for Steps 2 through 4.

[adoption]: https://claude.ai/code/artifact/bfdfaef9-bc62-4dfe-ba9e-c58a26c9accf

## Loops vs. graphs vs. agent networks

The current discourse names two units of agentic engineering. **Loop engineering** is
getting one agent's iterate–verify–repeat cycle right: the prompt, the tools, the
self-check, the stop condition. **Graph engineering** is wiring those loops into a
workflow — steps, branches, and waits composed into a single process. Both are real,
and Nano does both well; but both hold *one agent* or *one process* in view. Steps 2–4
are past that. When one engineer is steering ~10, then ~100, then ~1,000+ agents, the
unit that has to be engineered is the **network**: many graphs and many agents,
running at once, fanning out, coordinating over shared work, converging, and escalating
by exception.

| Unit | What you engineer | In view | Where it runs out |
|---|---|---|---|
| **Loop** | one agent's iterate–verify–repeat cycle | a single agent | it can't coordinate a *second* agent |
| **Graph** | loops wired into one durable workflow | a single process instance | many concurrent graphs sharing work and contending for agents |
| **Agent network** | many graphs + many agents as one system | the whole fleet | — (this is the frontier) |

Nano's subject is therefore **agent network engineering** and **agent network
orchestration**: authoring, running, observing, and steering the entire network as one
durable, inspectable system — with the capability-addressed workforce and the app-tier
coordination plane described below as its machinery — rather than hand-tending a loop or
a lone graph. Loop and graph are what you engineer *inside* a node; the network is the
thing you now orchestrate.

## New abstractions need new primitives

Each rise in the level of abstraction in application development has been marked by
a *new first-class verb* — the one operation the environment makes primitive. Nano
tells the lineage as a sequence of families, each told apart by that primitive — and
Nano is the next entry in it:

| Lineage | Primitive | Noun | Verb | Answers |
|---|---|---|---|---|
| **Delphi / CRUD** | state **at rest** | the record | *mutate* | "what **is**" |
| **IDE / editor** | state **as artifact** | the file | *transform, then run* | "what did I **make**" |
| **Process engine** | state **in motion** | the instance | **directed evolution over time** | "what is **happening**, and what **will** happen next" |
| **Nano / agent graph** | motion **under agency** | the agent graph | **orchestration toward intent** — direction reasoned at runtime, at network scale | "what **outcome** am I steering toward, and how does a network of agents **get there**" |

The third row was already genuinely new. A CRUD app *photographs* the world and
throws away how a record reached its state; a process engine's substrate **is** the
trajectory — the event log is not audit bolted on, it is the ontology. "Directed
evolution of state" decomposes into three irreducible primitives:

- **Matter** — *what* is moved: the domain object, the state. The **noun**.
- **Motion** — *the moving*: the process, the lifecycle, the directed evolution.
  The **verb**.
- **Director** — *what chooses the direction* at each fork: decisions, conditions,
  rules — and, at the limit, an **agent**. The **governing force**.

The fourth row is what happens when the **director** is promoted from a drawing to a
*mind*. A normal process is directed evolution whose graph is *drawn in advance*; an
agentic process is directed evolution whose direction is *reasoned at runtime*. Same
verb — "state evolves under control toward an end" — but the director moves from
design-time to run-time, and the authoring act changes with it: you no longer draw
every path, you declare the **intent** and a network of agent-directors discovers the
path, steering by intent and monitoring by exception (the Step-4 unlock). Orchestrating
hundreds of agents is therefore not a gadget bolted onto an editor — it is the
process-engine primitive taken one level up: a **network of reasoning directors** over
one durable, observable, crash-surviving substrate. That is Nano's primitive, and
Steps 2–4 need it the way OLTP needed transactions.

This is not a projection — it is what **Nano Workforce** already does today. Hand it a
GitHub issue and a planning agent (`senior:plan`) decomposes it into levelized tasks; a
parallel multi-instance fan-out drives one implementation agent (`senior:feature`) per
task, one PR each; every opened PR is enrolled into a durable `convergence-loop` that
runs multi-round review against an automated reviewer (`senior:pr-review`), **parking on
a durable message-catch between rounds** so agent slots and job timeouts are never held
hostage to review latency; a converged PR flows into a `merge-loop` that clears
dependencies, auto-fixes failing CI (`senior:fix-ci`), and merges queue-aware —
escalating to a human only when an agent is stuck or a round cap is hit. It is a Nano
Urban app: four explicit BPMN processes (`plan-fanout`, `convergence-loop`,
`merge-loop`, `retro`), executed by the engine, so the whole lifecycle survives
restarts, latency, and failure. The agents are **decoupled external workers you
"hire"** (any coding-agent CLI harness, e.g. the GitHub Copilot CLI): the app owns the
orchestration, the agents do the work, and neither names the other. This is where the
primitive stands **now** — agent *graphs* orchestrated over durable processes; the fully
closed Step-4 loop (where most agents are kicked off by other agents, at thousands of
concurrent workers) is the trajectory this opens, not a claim of arrival.

## Loops vs. graphs vs. agent networks

The current discourse names two units of agentic engineering. **Loop engineering** is
getting one agent's iterate–verify–repeat cycle right: the prompt, the tools, the
self-check, the stop condition. **Graph engineering** is wiring those loops into a
workflow — steps, branches, and waits composed into a single process. Both are real,
and Nano does both well; but both hold *one agent* or *one process* in view. Steps 2–4
are past that. When one engineer is steering ~10, then ~100, then ~1,000+ agents, the
unit that has to be engineered is the **network**: many graphs and many directors,
running at once, fanning out, coordinating over shared work, converging, and escalating
by exception.

| Unit | What you engineer | In view | Where it runs out |
|---|---|---|---|
| **Loop** | one agent's iterate–verify–repeat cycle | a single agent | it can't coordinate a *second* agent |
| **Graph** | loops wired into one durable workflow | a single process instance | many concurrent graphs sharing work and contending for agents |
| **Agent network** | many graphs + many directors as one system | the whole fleet | — (this is the frontier) |

Nano's subject is therefore **agent network engineering** and **agent network
orchestration**: authoring, running, observing, and steering the entire network as one
durable, inspectable system — with the capability-addressed workforce and the app-tier
coordination plane described below as its machinery — rather than hand-tending a loop or
a lone graph. Loop and graph are what you engineer *inside* a node; the network is the
thing you now orchestrate.

## Decomposition vs. integration

The deeper reason a chat surface cannot hold Steps 2–4 of Boris Cherny's Steps of
AI Adoption is structural, and it is an
old pendulum: application platforms swing between **decomposition** (split the
concerns into independent services, each owning one thing) and **integration**
(hold the concerns together so the seams between them vanish). The two mature
expressions of the process-engine primitive each landed on opposite ends, and each
holds only *half* of the picture:

- **Delphi had matter without motion.** Its domain model *was* the database schema;
  its record had a face (the form) and a memory (the row) — but **no life**.
  Nothing in it expressed "this order is *becoming* paid," "this instance is
  *waiting* on the world."
- **Camunda 8 has motion without matter.** The engine moves an untyped JSON payload
  — deliberately, for flexibility and speed — and *disclaims the domain*. The
  business object exists nowhere and everywhere at once: implicit in the form
  fields, the DMN types, the payload, the customer's own DB schema, and the worker
  code that shovels between them. The customer's real job becomes hand-reconciling
  all five into agreement.

Camunda owns only motion, so — as a **structural consequence of its decomposition**,
not a missing feature — it cannot own the domain: it disclaims the resting
substrate, and half a bridge spans nothing. The same fault line reappears one level
up in the agent story: a frozen, Camunda-8-compatible job protocol (exact job-type
string, matched 1:1) is the right *decomposed* contract for stock clients, but it
deliberately provides none of what a *network* of agent-workers needs — a vocabulary
for what work exists, visibility into what the crew is doing, or a plane to
coordinate on.

The naïve fix — hand-wire an atomic job type per model, per task — is exactly the
decomposition trap: it explodes combinatorially and couples every model to every
machine, because a novel model then means editing worker capabilities fleet-wide
across hardware that lives apart. So Nano does not address agents as atomic job-type
endpoints; it addresses them as a **network, by capability**. An agent is *hired*
with a profile — a **rank** and a set of **capabilities** — and a derived
**rank × capability matrix** yields the job-type tokens it will service (rank alone,
rank + one capability, rank + all), so a process can target a worker at any
granularity without anyone hand-wiring a token per task (the concrete mechanism is
`c8ctl nano hire`/`assign`/`work`; `assign` unions in new capabilities and a running
worker reconciles its pollers live). Above the engine's plain 1:1 routing, a single
**app-tier agentic channel** then carries everything a network needs and the frozen
engine must never learn: registration and heartbeat (supply and presence),
demand × supply (what work exists vs. who serves it, and on which machine), a shared
**blackboard** for coordination, and live **relay** terminals to watch and steer each
agent — multiplexed over prioritized QoS lanes so a stdout firehose can never
head-of-line-block a heartbeat. Agent networks, in other words, are a first-class
*plane*, not glue hand-wired one atomic job type at a time.

Nano's bet is **integration where it counts**. Because it embeds the engine *and*
owns the application's datasource, it is the first system for
which a first-class domain — the bridge between state-in-motion and state-at-rest —
is even *possible*, while the engine underneath stays untyped and Zeebe-faithful so
switch-over parity is preserved. The method itself becomes a process: OOP unified
data and behaviour, but the behaviour was an *ephemeral* method — a stack frame that
runs and returns. Here the behaviour is a **durable, directed, temporal process**.
`Order.fulfill()` is not a function; it is a process instance that *lives* — it
waits, is observable mid-flight, survives a crash, carries its own history. **A
domain object's methods are lifecycles.** For an agent network, that same integrated
substrate is what turns "a hundred agents each in their own terminal" into "a
hundred directors over one durable, observable process," which is the only version
of Step 4 that a human can actually steer by intent.

That bet is concrete: Nano is one **vertically-integrated stack**, owned end to end
rather than assembled from independently-decomposed products, so the seams between
the layers disappear:

| Layer | What it is | What it owns |
|---|---|---|
| **Nano Workforce** — *application* | Agent-powered SDLC orchestration. Hire agents and coding harnesses, then run plan → implement → review → test → merge → QA → retro as durable graphs. | The end-to-end epic across a network of agents. |
| **Urban** — *application framework* | Author Nano apps as code *or* model. Go **code-first** and Urban **derives** the executable model, job types, message correlation, and a generic worker from your code; go **model-first** and the authored BPMN model *is* the source of truth. Either way, one source of truth — no hand-wired orchestration. | The domain and its reconciliation — no drift between code, model, and workers. |
| **Nano Studio** — *IDE* | The RAAD (Rapid Agent Application Development) environment: a polyglot IDE to scaffold, run, and inspect agent applications, with live traces and the web console in one place. | The authoring and observation loop on the workstation. |
| **Nano** — *engine · foundation* | The load-bearing runtime, in Rust, Camunda 8-compatible. Durable by default: on restart a graph resumes at the exact step it left off; journal-committed steps are never replayed. Small enough to start on a Raspberry Pi. | Motion — the durable, event-sourced execution substrate. |

Each layer would be a *decomposed* product on its own — an engine here, an IDE
there, a framework and an app bolted on top. Nano integrates them so the domain, the
motion, and the director are reconciled by construction, not hand-wired by the maker.
That is the difference between an *engine you integrate against* and a *substrate you
build on* — and it is what makes steering hundreds of agents by intent tractable.

The sections that follow describe the engine that makes this substrate real: its
footprint and durability, the transport that carries agent-workers at scale, and the
honestly-bounded scope.

## Features

- **Small memory footprint** — a sub-50 MB binary that idles at ~10 MB resident
  and holds ~400 MB per node at 100k process-instances/s across three nodes.
- **Sub-second cold start** — a native binary with no JVM warm-up.
- **Cross-platform** — a single portable binary that runs on anything from a
  Raspberry Pi to a cloud server, spanning architectures and operating systems with
  no runtime dependencies.
- **Self-optimizing operation** — most operational tuning is *derived* rather than
  set: the engine decides from live signals what a human would otherwise tune by
  hand, leaving one deployment-time business decision (how to behave at the
  capacity ceiling: shed admission, or absorb it and let latency rise).
- **Closed-loop adaptive scaling** — distributed clients and the cluster form one
  self-sizing control loop (AIMD backpressure + load-aware placement), so client
  fleets adapt themselves instead of being hand-tuned (when using the additive Falcon protocol).
- **Fault-tolerant clustering** — Raft-replicated partitions (openraft) for
  high-availability operation.
- **Tunable durability** — a declared deployment choice trades data-safety
  guarantees for throughput (strict quorum + sync vs. relaxed leader-durable +
  async).
- **In-browser engine** — the same engine core compiles to WebAssembly and runs in
  the browser (the console's in-page test-run).
- **Embeddable engine** — the same engine runs embedded in a worker, microservice,
  or application, or is invoked directly as a tool call.
- **Camunda 8 API-compatible** — a drop-in replacement behind the standard Camunda
  8 REST contract.
- **Polyglot SDKs & ecosystem tooling** — because it speaks the Camunda 8 REST API,
  Nano inherits Camunda's polyglot client surface (drive workflows and workers from
  Java, Python, C#, TypeScript, Rust, or Go with the existing first-class Camunda 8
  SDKs) *and* the broader ecosystem of tooling built on that API, which works against
  Nano unchanged.
- **Zeebe engine lineage** — the proven Zeebe execution model (single-writer actor
  per partition, event-sourced state), refined for a smaller footprint and higher
  throughput.

## The Falcon transport

Falcon is an **optional native transport**, offered *alongside* the standard API,
never in place of it. A Nano cluster speaks the full Camunda 8 REST surface,
so an unmodified Camunda 8 client works unchanged. Falcon is the "faster if you
opt in" path.

It is a single persistent, bidirectional, credit-metered WebSocket per client that
carries both directions of work on one connection: a **demand-pull** lane where a
worker advertises how many jobs it can take and the server pushes only that many,
and a **submission** lane where instance creation draws on credits fed from the
engine's own processing headroom — so under load the server simply withholds
credits and the client stalls its intake, with no `503`/retry storm. Job delivery
and instance admission thus share **one backpressure account** rather than each
fending for itself. WebSocket (rather than gRPC) was chosen because it rides
ordinary HTTP(S) that existing proxies, gateways, and firewalls already handle,
and needs no per-language stub generation.

## Scope and status

Nano is an Advanced Research Prototype. It deliberately keeps Camunda 8's
client-facing semantics and Zeebe's battle-tested execution model, and revisits
only a few choices underneath a fixed, compatible contract. It is usable today for
local development against the subset it implements; it is not a general replacement
for a production Camunda 8 deployment.
