# ADR 0030 — The domain–process duality: directed evolution of typed state (Urban's computational primitive)

Status: **Proposed.**
Date: 2026-07-21.
Relates to: ADR 0022 (`0022-nano-rad-application.md`, **Urban** — the *product* keystone; this ADR
is the *conceptual* keystone beneath it: the ontology 0022's App bundle assumes but never states),
ADR 0005 (`0005-embedded-u-nano.md`, **Bernd** — embedded engine + "choose deployment by config";
integration is what lets a first-class domain exist, see §4),
ADR 0007 (`0007-rad-extension-system.md`, declared data — the manifest binds these primitives),
ADR 0021 (`0021-process-sla-as-a-first-class-abstraction.md`, time/SLA — the clearest existing case
of "the process is state with a *future tense*"),
ADR 0024 (`0024-urban-data-layer-datasource-abstraction.md`, state **at rest** — one bank of the
motion↔rest bridge this ADR names),
ADR 0028 (`0028-urban-app-user-auth-identity-authorization.md`, whose "engine has no tenancy" gap
this ADR reframes as *carry-a-key-and-filter*, §5),
ADR 0029 (`0029-urban-bindings-domain-model.md`, the *how* — the symbol index + typed-reference +
domain-type layer that **implements** the duality this ADR charters),
ADR 0023 (`0023-adhoc-subprocess-execution-parity.md`, the agentic seam — the **director** as a
runtime intelligence, §2's limit case),
`console/src/components/{FormEditor,BpmnModeler,DmnModeler}.tsx` (the three editors that already ship
— this ADR observes they *are* the three primitives), `engine-core/src/` (process **variables are
untyped JSON/MessagePack** by design — Zeebe-faithful; this ADR keeps that pure), and — for the
lineage — Borland **Delphi** (matter without motion) versus Camunda 8 / `~/workspace/camunda/zeebe`
(motion without matter).

## Context

Three families of Rapid Application Development can be told apart by their **primitive verb** — the
one operation the environment makes first-class:

| Lineage | Primitive | Noun | Verb | Answers |
|---|---|---|---|---|
| **Delphi / CRUD** | state **at rest** | the record | *mutate* | "what **is**" |
| **IDE / editor** | state **as artifact** | the file | *transform, then run* | "what did I **make**" |
| **Process engine** | state **in motion** | the instance | **directed evolution over time** | "what is **happening**, and what **will** happen next" |

The third is genuinely new, and it is Nano's. A CRUD app *photographs* the world and throws away how
a record reached its state; a process engine's substrate **is** the trajectory — the event log is not
audit bolted on, it is the ontology. This is why the RAD persona keeps resolving to *automation*:
automation is a procedure that runs itself, and a procedure — a lab sample through testing, an RMA, a
permit approval, a patient pathway, a heating cycle, a CI pipeline — is irreducibly *temporal and
causal*. CRUD can only store its aftermath; a process engine can **be** it while it runs.

But the two mature expressions of this each hold only **half** of the picture, and the missing half is
the same in mirror image:

- **Delphi had matter without motion.** Its domain model *was* the database schema; its record had a
  face (the form) and a memory (the row) — but **no life**. Nothing in Delphi expressed "this record
  is *becoming* paid," "this order is *overdue*," "this instance is *waiting* on the world."
- **Camunda 8 has motion without matter.** The engine moves an **untyped, unknown JSON payload** —
  deliberately (flexibility + MessagePack performance) — and *disclaims the domain*. The business
  object exists nowhere and everywhere: implicit in the form fields, implicit in the DMN `typeRef`s,
  implicit in the payload, implicit in the customer's own DB schema, implicit in the worker code that
  shovels between them. The customer's real job is **hand-reconciling all five into agreement.** The
  variables are untyped *because the domain is absent*.

Nano is the first system positioned to hold **both** — because (ADR 0005) it embeds the engine *and*
(ADR 0024) it owns the App's datasource. That is not an incidental convenience; §4 argues it is the
*structural precondition* for a first-class domain, and the reason Camunda cannot have one.

## Decision (proposed)

Adopt, as Urban's foundational position, that Urban is a distinct kind of computing — **the directed
evolution of typed state** — and that its charter is to make **both the motion and the matter
first-class and mutually reconciled**, over an engine that stays untyped and Zeebe-faithful. The five
statements below are the charter the rest of the Urban cluster (0022/0024/0028/0029) inherits and
implements; this ADR owns the *why*, those ADRs own the *how*.

### 1. The trinity: matter, motion, director — and the three editors already are them

"Directed evolution of state" decomposes into three irreducible primitives:

- **Matter** — *what* is moved: the domain object / state. The **noun**.
- **Motion** — *the moving*: the process, the lifecycle, the directed evolution. The **verb**.
- **Director** — *what chooses the direction* at each fork: decisions, conditions, rules — and, at the
  limit, an agent. The **governing force**.

The load-bearing observation: **the three model editors Nano already ships are exactly these three
primitives.** `FormEditor` is matter's *face*; `BpmnModeler` is *motion*; `DmnModeler` is the
*director*. They were never three unrelated tools — they are the three parts of one idea, each with
its own surface. What is missing is not an editor; it is the **reconciling spine** that makes them one
thing instead of three a maker hand-wires into agreement. That spine is the domain model (ADR 0029).
The editors are the trinity; 0029 is the ring that binds them.

### 2. Both first-class means mutually defining — and the director is where the agent lives

When matter and motion are *both* first-class they stop being two things bolted together and become
**mutually defining projections of one truth**:

- **The motion is the type's dynamics.** A process is not "a flowchart with a JSON blob riding along";
  it is the *lawful behaviour of a domain type* — every gateway condition reads a known field, every
  task's I/O is typed, the reachable states are the graph.
- **The matter is the motion's phase space.** The domain type declares the state space; the process
  declares which trajectories through it are legal. Change the type and the process re-points; change
  the process and the type accretes.

The **director** is first-class alongside them, and it is where the agentic seam lands: a normal
process is directed evolution whose graph is *drawn in advance*; an ad-hoc / agent process (ADR 0023)
is directed evolution whose direction is *discovered at runtime*. Same verb — "state evolves under
control toward an end" — with the control moved from design-time to run-time. The agent work is
therefore not a bolt-on; it is what this primitive looks like when the **director** becomes a runtime
intelligence.

### 3. The falsifiable test — authoring symmetry

The claim "both first-class" is not decorative; it has a **test**. Both are first-class *iff* a maker
can author from **either end** and have the other live-reconcile:

- **Domain-first** — declare `Order` (its shape, its states), and the lifecycle, forms, table, and
  read model scaffold *out of it*.
- **Process-first** — draw the flow, and the domain type *accretes* from what the tasks touch (ADR
  0029 §5's inference on-ramp).

If both directions work and stay reconciled, both primitives are real. If only one drives and the
other is a dead derivation, the system secretly still has one primitive and one afterthought. **Delphi
passes in one direction only** (matter-first; there is no motion). **Camunda passes in one direction
only** (motion-first; matter is absent). Urban's whole claim is that it passes in **both** — and this
is the criterion the authoring surface (the console App panels) must be held to.

### 4. Integration is the structural precondition (why Camunda cannot own the domain)

A domain model is the **bridge between state-in-motion and state-at-rest**, and a bridge needs *both
banks*. The process instance is the object *in flight* (engine variables); the datasource row is the
same object *at rest* (ADR 0024). Camunda owns only motion, so it **cannot** own the domain — not as a
missing feature but as a structural consequence of its decomposition: it disclaims the resting
substrate, and half a bridge spans nothing. Delphi owned only rest, so its domain *was* the schema,
with no life.

Nano owns both — embedded engine (0005) and App datasource (0024) — so it is the first system for
which a first-class domain is even *possible*. This is the same **eliminate-vs-encapsulate**
principle that governs the rest of Urban, applied to its deepest layer: the wiring's concerns are
irreducible, so Urban does not *eliminate* the domain wiring — it **encapsulates** it. Concretely
(implemented by 0029, over 0024):

- **The engine stays untyped and Zeebe-pure** — JSON on the wire, types erased at runtime,
  switch-over parity intact (lift the App onto Camunda 8 and the engine contract is byte-identical).
- **The domain is a first-class *boundary*** in the integrated App tier — payloads are validated
  against the domain type at the seams (`createInstance`, `complete`, worker result, persist). This
  closes 0029's honest gap ("a remote worker can write off-type JSON") not by typing the engine but by
  making the *integrated boundary* the enforcement point the engine refuses to be.
- **The domain owns the motion↔rest *projection*** — declare the type once and the mapping between
  "object-in-flight (process variables)" and "object-at-rest (datasource row)" is **generated, not
  hand-written.** The two workers every Camunda customer writes — persist-to-DB and rehydrate-from-DB
  — vanish, because the domain model *is* the projection.

### 5. The method became a process — and the read model becomes domain-shaped

Two consequences follow that neither predecessor could reach:

- **Behaviour-as-process.** OOP unified data + behaviour, but the behaviour was an *ephemeral* method
  — a stack frame that runs and returns. Urban unifies data + behaviour where the behaviour is a
  **durable, directed, temporal process**. `Order.fulfill()` is not a function; it is a process
  instance that *lives* — it waits, is observable mid-flight, survives a crash, has an SLA (ADR 0021),
  carries its own history. **A domain object's methods are lifecycles.** The one-line form of the
  whole position: **Urban's domain object = Delphi's record + a lifecycle** (face + memory + *life*).
- **Domain-shaped observability.** Because the domain is first-class, the read model can be shaped by
  the *business noun*, not the *engine noun*. Operate shows "instances parked at `Task_3`"; a
  domain-first read model shows "**Orders awaiting payment**." Observability in the maker's language is
  only possible once the domain exists as more than an implicit blob — and it may be the most visible
  payoff of the whole duality.

## Consequences

- Urban gains an explicit **charter** the product ADRs can cite: a distinct kind of computing
  (directed evolution of typed state), not "Camunda with a nicer UI." 0022 remains the product
  keystone; this is the conceptual keystone beneath it.
- The three existing editors gain a *stated reason to be one system*: they are the matter/motion/
  director trinity, and 0029's domain model is their reconciling spine.
- The engine is untouched and stays fast, untyped, and Zeebe-faithful — all the new power lives in the
  integrated App tier (boundary + projection), so switch-over parity is preserved.
- The design acquires a **falsifiable target** (authoring symmetry, §3) to hold the console App panels
  to, rather than a vibe.
- Tenancy (ADR 0028) is reframed downstream: because "a store has no identity — identity is a layer
  above it," the engine needs only to *carry an opaque scoping key and honour it at the read/command/
  dispatch seams*, with identity resolution staying App-tier — a far smaller engine change than "grow
  tenancy."

## Open questions

- **Authoring inversion** — is the primary surface domain-first (declare `Order`, scaffold the
  lifecycle) or process-first (draw the flow, infer the type), and does §3's symmetry demand *both* be
  first-class entry points on day one, or is one the default with the other a follow-on?
- **Twin vs. native** — is the distinctive App a *digital twin* of a real-world procedure (fidelity,
  observation) or a *native* procedure that exists only because the maker invented it (ergonomics of
  invention)? The two pull the design differently; Urban is likely strongest where it is both, but
  which it privileges shapes the killer app.
- **How much the domain boundary enforces** — §4's App-tier validation is design/compile/boot-time
  plus edge checks; do we want runtime edge-validation *on by default* (safety) or opt-in (raw-JSON
  ergonomics for the single-user maker who does not want a schema yet)?
- **Nominal vs. structural domain identity** (shared with ADR 0029) — does `Order` match by name or by
  shape across processes, forms, and the datasource, and how does that interact with DMN `typeRef`s?
- **Director as runtime intelligence** — when the director is an agent (ADR 0023), the "reachable
  states are the graph" invariant (§2) softens (the graph is discovered). What does authoring symmetry
  (§3) even mean for an ad-hoc container whose motion is emergent?
- **Read-model shape** — is the domain-shaped read model a projection *over* the process read model
  (two layers) or the primary read model with the process view derived from it (which noun is
  canonical)?
