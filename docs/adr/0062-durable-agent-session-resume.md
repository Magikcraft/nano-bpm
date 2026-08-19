# ADR 0062 — Durable agent-session resume: an authoritative per-activation session log, a harness replay contract, and the mind/world checkpoint

Status: **Proposed.**
Date: 2026-08-19.

Relates to:
ADR 0056 (`0056-agent-relay-command-stream-plane.md`, the Nano agentic protocol — this ADR **promotes a
per-activation subset of its transcript plane from advisory to authoritative**, and reuses its replay
ring + incarnation fence as the substrate),
ADR 0046 (`0046-agent-as-worker-vs-agent-in-the-node.md`, **agent-as-worker** — the topology whose
external, leased coding-agents this ADR makes resumable),
ADR 0002 (`0002-leader-local-activation-and-lease-digest.md`, the **durable job lease** — the engine
boundary this ADR layers on and never reaches into),
ADR 0051 (`0051-nano-workforce.md`, the crew orchestrator whose `convergence-loop` `senior:pr-review`
round is the reference consumer),
ADR 0060 (`0060-institutional-memory-context-grounding.md`, model-grounded context for agent sessions —
the *inbound* context this ADR's *outbound* session capture is the mirror of),
`@nanobpm/agentic` (the app-tier plane that owns the session contract; not any one app),
and the prior art it cribs from: `@deepseek-ai/dsh` (**DeepSeek Harness**) — whose event-sourced
session (`packages/core/session/`, append-only `SessionEvent` + `seed`/`restore`, where replaying the
log *rehydrates the agent itself*) is the reference implementation of the harness half of this contract.

## Context

Nano runs coding agents as **external workers** (ADR 0046). A `convergence-loop` round dispatches a
`senior:pr-review` job (`nano-workforce/resources/processes/convergence-loop.bpmn`); a **hired agent
harness** — Copilot CLI, Claude Code, DeepSeek Harness, … — leases it via the frozen C8 job protocol
(`activateJobs` → work → complete), and between rounds the process parks on a **durable message-catch**.

Two durability layers already exist, and one is missing:

- **Orchestration — durable (engine, ADR 0002).** The process instance, the round loop, and the
  lease→redrive of `senior:pr-review` survive a crash. If a harness dies mid-round without completing,
  the lease expires and the job is redriven.
- **Visibility — durable but *advisory* (ADR 0056).** The agentic relay/transcript is append-only,
  offset-addressable, and incarnation-fenced, with reattach/replay. But ADR 0056 §12 scopes it to
  *"see what the agent did"*: it flushes to a transcript artifact on completion, and the thing that
  *resumes from an offset is a **cockpit/consumer**, not the producing agent*. It renders **terminal
  bytes**, not the structured conversation the model consumed.
- **Agent session — ephemeral (the gap).** The harness's own working state within one activation — its
  LLM message history and its mutated working tree — is owned entirely by the external harness and is
  **not durable in Nano**. So a mid-round crash means the engine redrives the **whole round from
  scratch**: wasted tokens, a non-deterministic restart, and re-execution of irreversible side-effects
  (a second `git push`, a duplicate PR comment).

DeepSeek Harness closes this at the harness tier: its session **is** the source of truth, and replaying
its `SessionEvent` log seeds/restores the agent so it *continues* rather than *restarts*. We want that
property for the Nano fleet — but the fleet is **heterogeneous and external**, and durable resume of a
real coding agent is not one problem. It is two, with different owners.

### The resume problem splits into *mind* and *world*

A crashed round is resumed by a **replacement** activation on a **fresh worktree**. To continue rather
than restart, two independent kinds of state must be reconstructed to the *same* point:

- **World — the mutated local assets** (the git working tree, staged/committed/pushed changes). **Nano
  owns this.** Our `c8ctl` worker performs the effects and can reconstruct them. Crucially, the
  authoritative durable store for committed work is *already* the **git remote**: the agent *pushed* a
  SHA. So world-restore is an **inversion of the forward operation** — the round's `git push`
  (outbound) becomes a `git fetch && checkout <sha>` (inbound) on resume. We therefore **derive** the
  tree from `remote SHA + effect-tail`, never snapshot the tree into a log (no duplicate source of
  truth). The lossy frontier is work done *after* the last push, which drives the resume boundary to
  **push-checkpoints** and pairs with effect-fencing for the non-idempotent tail.
- **Mind — the LLM context.** To restart the model *from* that checkpoint, we need **exactly what the
  hired harness was sending to the LLM** — the system prompt, tool schemas, and the message history
  (interleaved model turns and tool-call/tool-result pairs). This is **not** the relay's rendered
  terminal bytes, and **Nano does not own it**: it lives inside the hired harness. Without a tap into
  the harness's model-facing context, we can restore the world to a checkpoint but cannot restart the
  mind from it — the agent would begin a *new* conversation on top of a half-mutated tree, which is
  worse than a clean redrive.

The consequence is the load-bearing constraint of this ADR: **durable resume is impossible for a
harness that does not expose its model context.** Nano supplies the world half; the harness must supply
the mind half. Only DeepSeek Harness does today.

## Decision

### 1. Promote a per-activation session subset from advisory to authoritative

Add, in the `@nanobpm/agentic` plane, an **authoritative, event-sourced session log** keyed by the
activation — `(processInstanceKey, elementId, incarnation)` — distinct from the advisory transcript
(ADR 0056 §12) even though it **reuses the same substrate**: the bounded replay ring, resume-from-offset,
and generation/incarnation fencing (ADR 0056 §12). "Authoritative" means: on re-lease of the same job,
replaying this log **restores the producing agent**, not merely a watching cockpit. Everything else in
ADR 0056 stays advisory; this is a narrow, named promotion, not a re-basing of the plane.

This crosses ADR 0056's *"visibility never gates"* boundary, so it is a **first-class decision here**,
not an implicit widening of 0056. It still never touches the **engine**: the C8 job protocol is
unchanged, and resume is an **optimization *inside* an activation**, never a new sequence-flow gate.

### 2. Two owners, one checkpoint: the mind/world contract

A **checkpoint** is the atomic join, at a single turn boundary, of:

- **World marker (Nano / `c8ctl`):** the pushed `commitSha` plus an **effect ledger** of irreversible
  actions since the last checkpoint (each with an idempotency key — `callId`, commit SHA, comment id).
  Restore = `fetch`+`checkout <commitSha>`, then replay any post-checkpoint effects **through the
  effect fence** so an already-applied action is skipped, not repeated.
- **Mind marker (hired harness):** the model-facing context up to the same turn — system prompt + tool
  schemas + message history — emitted into the session log as structured `SessionEvent`s (à la
  DeepSeek's `seed`). Restore = re-seed the model with that context and continue.

Both markers are written under the **same offset / turn boundary** so mind and world cannot diverge
(the classic failure: the harness believes it has not pushed, but the push landed — or the reverse).
The checkpoint is committed **at push-checkpoints** — the only points where the world half is already
durable in the remote — bounding both the replay cost and the non-idempotent frontier.

### 3. Harness replay is a capability-gated enrolment, not an assumption

Because the mind half lives in the hired harness, durable resume is a **declared capability**, resolved
exactly like every other worker attribute (ADR 0056 §7: *capability is an enrolment gate, never in the
token*). A harness advertises `durable-resume` at enrolment iff it implements the session
write/restore contract (§4). The app-tier registry gates on it:

- **Harness advertises `durable-resume`** → its sessions are captured authoritatively; a re-lease
  restores mind+world and resumes at the last checkpoint.
- **Harness does not** → **graceful degradation to today's behaviour**: the round is redriven from
  scratch on re-lease. Nothing regresses; resume is purely additive.

The routing token (`network.role#seat`) is unchanged; `durable-resume` is an attribute, so no BPMN and
no job type changes. **DeepSeek Harness is the reference implementation** of the harness side; other
harnesses adopt the contract (or a thin adapter wraps their session stream into it) to opt in.

### 4. The `@nanobpm/agentic/session` contract

A harness participating in durable resume implements a small contract over the existing agentic channel:

- **`emit(sessionEvent)`** — append a model-facing turn event (prompt, model output, tool-call,
  tool-result) to the authoritative session log at the current offset. This is the **mind tap**; it is
  what today's relay does *not* provide (relay = rendered bytes; this = structured context).
- **`checkpoint(commitSha, effectLedger)`** — mark a mind/world join at a push boundary (§2).
- **`restore(fromCheckpoint) -> seed`** — on re-lease, the harness is handed the last checkpoint's mind
  seed to re-enter the conversation; Nano's `c8ctl` worker has already reconstructed the world (pull the
  SHA, fence the effect tail) before the harness is resumed.

The contract lives in `@nanobpm/agentic` (the generic Urban capability), not in any single app, so any
agentic Urban app inherits durable resume the way it inherits the relay and blackboard today.

## Worked example — a `senior:pr-review` round that survives a crash

1. `convergence-loop` dispatches round *N* as `implementation.review` (`senior:pr-review`); a
   DeepSeek-Harness worker (advertising `durable-resume`) leases it.
2. The harness works: reads the diff, edits files, and at a natural boundary **commits and pushes**
   `sha=abc123`. Nano's `c8ctl` worker records a **checkpoint**: world marker `{commitSha: abc123,
   effects: [push#abc123]}`, mind marker = the harness's `SessionEvent` seed up to that turn.
3. The harness continues into the next turn and the **box dies** (redeploy, OOM). The C8 lease expires;
   the engine **redrives** the job (ADR 0002).
4. A replacement worker leases the redriven job on a **fresh worktree**. `c8ctl` restores **world**:
   `git fetch && git checkout abc123` (the *inversion* — the push becomes a pull), then replays the
   post-checkpoint effect tail through the fence (there were none committed past `abc123`).
5. The replacement harness restores **mind**: it is seeded with the checkpoint's model context and
   **continues the conversation** from that turn — not a fresh review of the whole PR.
6. Net effect: the round loses only the *uncommitted* work after `abc123`, not the whole round; no
   duplicate push; tokens and wall-clock for rounds 1..N-1 and the committed part of round N are saved.

A non-`durable-resume` harness at step 4 simply re-reviews the PR from scratch — correct, just not
cheap.

## Where this lands — repo split (this ADR vs its implementation)

This decision follows the established precedent of ADR 0056: **the agentic-plane *decision* is
nano-bpm canon even though the plane's *code* lives in `nanobpm/nano-ide` (`packages/agentic`).** So
this ADR lives in **nano-bpm** — it is a protocol-level decision that crosses ADR 0056's
advisory→authoritative boundary — while its implementation fans out across three repos plus the
external harness. Each row below is a separate follow-up issue; **none is in scope for this ADR.**

| Piece | Repo | Notes |
|---|---|---|
| **Decision — ADR 0062** | `Magikcraft/nano-bpm` | *this document*; extends nano-bpm ADR 0056, referenced by number from nwf/nano-ide the way 0056 already is |
| Generic session substrate + `@nanobpm/agentic/session` contract (`emit` / `checkpoint` / `restore`), reusing the relay ring + incarnation fence | `nanobpm/nano-ide` — `packages/agentic` | same home as the relay it is promoted from (ADR 0056 §12); ships as the `@nanobpm/agentic` capability |
| **World** restore: `c8ctl` reconstructs the working tree (invert push → `fetch`+`checkout <sha>`, replay the effect tail through the fence), convergence-loop resume semantics, the `durable-resume` **enrolment gate** on the app registry | `nanobpm/nano-workforce` | the app that leases `senior:pr-review`; consumer of the nano-ide contract |
| **Mind** tap: emit the model-facing context (`SessionEvent` seed) and restore from it | external **harness** (reference impl: **DeepSeek Harness**); adapters per harness | not a Nano repo — the reason resume is capability-gated, not assumed |

The dependency order is nano-ide (contract) → nano-workforce (consumer wiring) → harness adoption; a
harness that already event-sources (DeepSeek) needs only an adapter to the nano-ide contract to
advertise `durable-resume`.

## Consequences

- Nano gains **durable agent-session resume** for participating harnesses, layered on the engine's
  existing durable lease (ADR 0002) and the agentic replay substrate (ADR 0056) — a narrow promotion,
  not new transport.
- The engine stays a **frozen, agent-oblivious C8 router**; resume is app-tier and inside an
  activation. ADR 0056's advisory boundary is crossed **only** for the explicitly-named per-activation
  session subset.
- **World state is derived, never duplicated:** the git remote is the source of truth for committed
  work; resume inverts the push into a pull; only the small non-idempotent effect tail needs fencing.
- **The hired harness is now a first-class dependency for the *resume* capability** (not for running a
  round). This is contained by capability-gated enrolment and graceful degradation — a fleet mixing
  resumable and non-resumable harnesses is well-defined.
- New app-tier surface to own in `@nanobpm/agentic`: the authoritative session log (schema + retention),
  the effect ledger + fence, the `checkpoint`/`restore` join, and the `session` contract. De-risked by
  DeepSeek Harness on the mind side and by the existing relay ring on the substrate side.
- Payoff scales with round length: long implement/review rounds (many tool calls, minutes of work)
  benefit greatly from push-boundary resume; short rounds do not justify the capture cost, so retention
  is bounded by lifecycle like the transcript already is.

## Open questions

- **Checkpoint granularity vs push cadence.** Push-checkpoints are the natural mind/world join, but a
  long turn between pushes still redoes uncommitted work. Do we encourage intra-turn "wip" commits, or
  accept the last-push frontier as the resume unit?
- **Effect-fence scope.** `git push` dedupes cleanly by SHA; PR comments and `gh merge` need their own
  idempotency keys. Is the ledger a fixed vocabulary of fenced effect types, or an open per-tool key
  the harness declares?
- **Mind-context fidelity & size.** Capturing full model context per turn can be large and may include
  provider-proprietary framing. Do we store the exact request payload, or a harness-normalized
  `SessionEvent` form (DeepSeek's shape) — and how does that interact with provider/tool-schema drift
  between the original and replacement harness versions?
- **Adapter vs native.** For harnesses that event-source internally but not to our contract (only
  DeepSeek does even that today), is a wrapping adapter sufficient, or must the contract be native to
  claim `durable-resume`?
- **Relationship to ADR 0060.** 0060 grounds the *inbound* context (institutional memory) a session
  starts from; this ADR captures the *outbound/evolving* context a session must be restored to. Is
  there one session-state spine that both should share, rather than two stores?
- **Security/PII of stored context.** The mind log contains full prompts (possibly secrets, customer
  code). Retention, encryption, and access must match or exceed the transcript's — likely stricter,
  since this is model input, not rendered output.
