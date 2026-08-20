# ADR 0063 — Nano-controlled embedded coding-agent harness: an adopted wasm agent whose capability-nil boundary *is* the mind/world split

Status: **Proposed.**
Date: 2026-08-21.

Relates to:
ADR 0062 (`0062-durable-agent-session-resume.md`, **durable agent-session resume** — this ADR is its
*Nano-owns-the-mind* corollary: 0062 negotiates a resume tap from a **hired** harness across a seam we
do not control; this ADR describes the harness where Nano owns **all four** seams, so the tap is not
negotiated but structural),
ADR 0046 (`0046-agent-as-worker-vs-agent-in-the-node.md`, **agent-as-worker vs agent-in-the-node** — the
topology axis this ADR's two shapes sit on),
ADR 0056 (`0056-agent-relay-command-stream-plane.md`, the agentic plane — its §7 capability boxes /
provider policy are what a Nano-owned inference transport routes through),
`@nanobpm/agentic` (the app-tier plane that owns the session contract 0062 defines and this harness is
the first full consumer of),
and the prior art it draws on: `fx` (`vercel-labs/fx`, Apache-2.0, Zig, "experimental") — an embeddable
coding agent that **inverts all four seams to be host-injectable** and whose **wasm core has zero
ambient authority**, making it the reference implementation for the harness described here.

## Context

ADR 0062 makes a **hired** coding-agent harness (Copilot CLI, Claude Code, opencode, …) resumable by
tapping the *mind* it owns — its model-facing context — over a per-harness seam (ACP where spoken, a
`stream-json`/native normalizer otherwise). That works, but it inherits three structural costs from the
fact that **the harness owns the mind and Nano does not**:

1. **A capability negotiation.** `durable-resume` is a *declared* attribute a harness may or may not
   advertise (ADR 0062 §3); the fleet is a mix of resumable and non-resumable workers.
2. **A normalizer per dialect.** Nano never sees the mind directly — it reconstructs it from whatever
   transcript the harness chooses to emit, at whatever fidelity that transcript preserves (e.g. Copilot's
   `reasoningOpaque`). Coverage is partial and fidelity is the harness's call, not Nano's.
3. **A convention-based effect fence.** The *world* half (git working tree, PR comments) is restored by
   Nano, but the harness performs those effects out of Nano's sight; fencing non-idempotent actions
   relies on the harness surfacing idempotency keys Nano can dedupe on.

All three costs come from the same root: **an ownership boundary between Nano and the agent's runtime.**
The ADR 0062 harness survey surfaced a harness that *removes that boundary* — `fx`. Two properties make
it categorically different from a hired CLI:

- **It inverts every seam to be host-injectable.** `createFxAgent({ fetch, sessionStore, onEvent,
  onPermission, workspace, env, configStore })` is a single host program that is *simultaneously* the
  ACP client (`session.prompt()` yields the ACP `session/update` stream; `openSession`/`listSessions` =
  `session/load`) **and** the supplier of session store, inference transport, permission gate, and
  workspace. The host owns persistence, inference, protocol, and effects at once.
- **Its wasm core has zero ambient authority.** Compiled to `fx-core.wasm`, the agent's WASI import
  table stubs out *every* filesystem and process primitive (`path_open`, `path_unlink_file`,
  `path_rename`, … all `unavailable`; `proc_exit` marks-and-throws). The agent cannot touch the
  environment on its own. **Every** environmental effect is instead an imported host function in the `fx`
  namespace that the wasm calls *out* across the boundary — `fx_workspace_exec` (shell), `fx_http_request`
  / `fx_http_stream_*` (network + inference), `fx_session_commit`/`_load` (persistence), `fx_config_*`,
  `fx_oauth_*` — each wrapped `WebAssembly.Suspending` (JSPI), so the agent's tool loop *suspends* while
  the host performs the real effect and *resumes* with a bounded result.

The consequence is the crux of this ADR: **the wasm boundary is a structural, capability-enforced
implementation of ADR 0062's mind/world split.**

- **wasm core = the mind** — it decides *which* tool to call and *with what arguments* (reasoning +
  tool-call decisions), and performs **no** effects itself.
- **host = the world** — every filesystem write, shell command, `git push`, and inference call is a host
  function the host admits, executes, bounds, and can **record**.

Where ADR 0062 asks a hired harness to *expose* its mind by convention, this harness *cannot hide* it:
the mind and the world both flow through host functions Nano writes. That is worth its own decision
record, because it changes durable resume from a negotiated capability into a property of the runtime.

## Decision

### 1. Adopt, don't build — Nano supplies the injectors around an existing agent core

Nano does **not** write a coding agent. The *mind* — context assembly, the tool loop, prompt
engineering, per-provider streaming, compaction, reasoning capture — is differentiation-free complexity
that drifts with every model release and is already shipped, well, by the eight harnesses surveyed in
ADR 0062 §5. Rebuilding it is a treadmill with no moat.

Instead Nano **adopts `fx` as the agent core** and supplies the four injectors it already has:

| Seam | fx host hook | Nano supplies |
|---|---|---|
| Persistence | `sessionStore` (`load`/`commit(id, bytes, expectedRevision)`/`list`/`remove`) | the ADR 0062 authoritative session log |
| Inference transport | `fetch` / host stream provider | the fleet provider policy (ADR 0056 §7), behind an AI-SDK-v4 shim (§4) |
| Protocol | native ACP (`session/*`) | the ACP client (ADR 0062's preferred ingestion backend) |
| Effects | `workspace.exec` (`fx_workspace_exec`) | the `c8ctl` world layer + effect fence (ADR 0062 §2) |

The starting posture is **vendor, not fork**: consume upstream `fx` and own only the injectors. Fork
**reactively**, only if upstream will not expose a seam Nano needs. This keeps Nano off the treadmill and
off a Zig-maintenance burden until there is a concrete reason to take either on.

### 2. This is a *tier*, not a replacement

The Nano-controlled harness is the fleet's **always-available, fully-owned floor** — the known-good
worker that still runs when a vendor changes its CLI flags, revokes a key, or throttles a quota. It sits
**alongside** the enrolled best-of-breed hired harnesses of ADR 0062, not instead of them. A round is
still dispatched to whichever enrolled worker fits (ADR 0056 §7 routing is unchanged); this harness is
one more enrollee, distinguished only by Nano owning its internals.

Because Nano owns its internals, this harness advertises `durable-resume` (ADR 0062 §3) **unconditionally
and at full fidelity** — it is the reference against which the negotiated-tap harnesses are measured, and
the safety net beneath them.

### 3. Two shapes on the ADR 0046 axis — A now, B as the endgame

The harness can take two forms, differing only in where the host runs:

- **A) Agent-as-worker, Nano-owned.** `fx` runs as a process Nano fully wires, leased via the frozen C8
  job protocol exactly like any other harness (ADR 0046's *agent-as-worker* topology, unchanged). Nano is
  the SDK host in that process: ACP client + store + transport + permission gate. Incremental; extends
  ADR 0062; **the recommended on-ramp.**
- **B) Agent-in-the-node.** `fx-core.wasm` runs **in-process with Nano's own WASM engine** — the agent
  *is* a node in the running process (ADR 0046's *agent-in-the-node*). Higher payoff (true co-location,
  no lease/redrive dance), and a bigger claim about the engine's runtime.

These are not independent designs. **Choosing A *on the wasm backend* already collapses toward B**
(§4): it puts `fx-core.wasm` inside a host Nano controls end-to-end, so B becomes "move that host into the
engine process" — an increment, not a re-architecture. This ADR **decides A-on-wasm as the on-ramp and
names B as the intended endgame**, deferring B's engine-integration specifics to a follow-up once A has
validated the host-mediation model in production.

### 4. Run the wasm backend, not native — host mediation is the whole point

`fx` offers a **native** backend (a Node addon, real OS access — native processes, MCP, WASI filesystem)
and a **wasm** backend (`fx-core.wasm`, the capability-nil boundary of §Context). They are not
interchangeable for Nano's purposes:

- The **native** backend gives the agent **ambient authority Nano cannot see** — it can spawn processes
  and touch the filesystem without crossing a host function. That defeats the entire premise.
- The **wasm** backend routes **every** effect through a host import Nano implements. That interposition
  *is* what "Nano-controlled" means.

So the harness **deliberately runs the wasm backend**, trading the agent's raw ambient capability for
total host interposition. Two payoffs follow directly, and both are ADR 0062 problems that this topology
dissolves rather than solves:

- **Durable resume is nearly free.** Every world-effect (`fx_workspace_exec`) and every mind-update
  (`fx_session_commit`, which is **CAS-versioned** — `expectedRevision` in, revision out, i.e. exactly
  0062's incarnation-fence semantics) funnels through host functions Nano owns. **Nano *is* the tap** —
  no capability to negotiate, no normalizer, no fidelity loss. The mind/world checkpoint (ADR 0062 §2)
  reduces to "flush both host-side ledgers at the same turn boundary."
- **The effect fence is host-side and enforceable.** `fx_workspace_exec` is the *only* path from the
  agent to the world, so Nano dedupes/fences non-idempotent effects (push, PR comment, merge) **at that
  import** — the fence lives in Nano's code, not in a convention the harness must honour (ADR 0062's third
  cost, removed).

### 5. The one net-new dependency: an AI-SDK-v4 inference shim

`fx`'s inference is **Vercel-AI-Gateway-native**: it emits the Vercel AI-SDK v4 data-stream and resolves
`provider/model` catalog ids through `https://ai-gateway.vercel.sh`. Its remote base-URL override is
gated to loopback HTTP, and `OPENAI_API_KEY`/`ANTHROPIC_API_KEY` appear only as log-redaction patterns —
there is **no** generic OpenAI-compatible client. To drive Nano's fleet providers (ADR 0056 §7, incl.
local/frontier boxes) through the injected `fetch`/stream seam, the host must present a **Vercel AI-SDK-v4
data-stream shim** that bridges Nano's provider access to what `fx` expects on the wire.

This shim is the **single piece of genuinely net-new engineering** the harness requires, and it is
**reusable** (anything embedding `fx` needs it). It is scoped as its own implementation slice. (The
Vercel AI Gateway is itself provider-agnostic + BYOK, so a hosted deployment could alternatively route
through it — but the shim is what keeps Nano un-locked from Vercel.)

## Where this lands — repo split (this ADR vs its implementation)

This ADR is the decision; the code is follow-up issues, **none in scope here**:

| Concern | Repo | Note |
|---|---|---|
| The `@nanobpm/agentic/session` store contract this harness binds to | **nano-ide** (`@nanobpm/agentic`) | ADR 0062 slice (nano-ide#365); crib fx's `sessionStore` CAS shape |
| ACP ingestion backend the harness speaks | **nano-ide** (`@nanobpm/agentic`) | ADR 0062 slice (nano-ide#366); fx is the native-ACP reference |
| AI-SDK-v4 inference shim over the fleet provider seam (§5) | **nano-ide** (`@nanobpm/agentic`) or a small shared lib | net-new; reusable by any fx embedder |
| The Nano-controlled harness itself: fx-on-wasm host, `workspace.exec` → c8ctl world + effect fence, session-log binding, enrolment (shape A) | **nano-workforce** | first full consumer of the 0062 contract; validates it end-to-end with a live agent |
| Agent-in-the-node engine integration (shape B) | **nano-ide** engine / a later ADR | deferred; the endgame A collapses toward |

The recommended sequencing makes shape A the **first real consumer** of the ADR 0062 contract slices:
land the store contract and ACP backend, then stand up fx-on-wasm as a nano-workforce harness that
exercises them with a live agent — pressure-testing the contract *before* five third-party harnesses are
asked to conform to it.

## Worked example — a `senior:pr-review` round on the Nano-controlled harness

1. A `convergence-loop` round dispatches `senior:pr-review`; the Nano-controlled harness leases it (shape
   A). Nano boots `fx-core.wasm` on the wasm backend, injecting: `sessionStore` → the authoritative
   session log, `fetch` → the fleet provider (via the §5 shim), `workspace.exec` → c8ctl's world layer.
2. The agent reasons and calls tools. Each tool call is a **host import**: a read is `fx_workspace_exec`
   returning bounded output; an edit + `git push` is `fx_workspace_exec` that c8ctl runs and records in
   the effect ledger; each model turn is `fx_session_commit` (CAS) into the session log.
3. At a push boundary Nano commits a **checkpoint** (ADR 0062 §2): world marker = the pushed SHA + effect
   ledger, mind marker = the session bytes already in the store — **both already host-side**, so the
   checkpoint is a flush, not a capture.
4. The **box dies** mid-next-turn. The C8 lease expires; the round is redriven.
5. Nano re-boots `fx-core.wasm`, restores the **world** (fetch + checkout the checkpoint SHA, fence the
   post-checkpoint effect tail) and the **mind** (`sessionStore.load` at the checkpoint revision; ACP
   `session/load` replays it). The agent resumes at the last push boundary — no re-review from scratch,
   no negotiated tap, no normalizer.

## Consequences

- **ADR 0062's three ownership costs are dissolved for this harness**, not merely paid: no capability
  negotiation (advertises `durable-resume` unconditionally), no per-dialect normalizer (Nano *is* the
  tap), no convention-based fence (host-side at `fx_workspace_exec`). It becomes the fidelity reference
  and the safety net beneath the hired-harness tier.
- **New surface Nano owns:** the fx-on-wasm host (shape A), the `workspace.exec` → c8ctl-world + effect
  fence binding, the session-log `sessionStore` binding, and the AI-SDK-v4 inference shim (§5, the only
  net-new dependency). Everything else — tool loop, prompt engineering, ACP — is upstream fx's.
- **A vendor dependency on `fx`**, an "experimental" Apache-2.0 Zig project. Contained by the
  vendor-not-fork posture and by this being a *tier* (the fleet still runs without it); escalates to a
  fork only reactively.
- **Provider independence is preserved but not free:** the fleet can drive fx with its own providers, at
  the cost of maintaining the AI-SDK-v4 shim — a bounded, reusable adapter, not an ongoing treadmill.
- **A becomes B by increment.** Committing to the wasm backend now means the eventual agent-in-the-node
  step (B) is host-relocation, not redesign — this ADR keeps that path open deliberately.
- **Payoff concentrates where ADR 0062's does:** long implement/review rounds resumed at push boundaries;
  plus the standalone value of a self-hosted, fully-owned floor beneath a fleet of external vendors.

## Open questions

- **Vendor vs fork, and the Zig burden.** The posture is vendor-first, fork-reactively. What concrete
  seam gaps (if any) force a fork, and what is the maintenance cost of carrying a Zig fork if we cross
  that line?
- **Where the AI-SDK-v4 shim lives.** In `@nanobpm/agentic`, a standalone shared lib, or the harness
  package? Who else embeds fx and reuses it, and does it belong next to the ADR 0056 §7 provider policy?
- **A→B trigger.** What signal promotes the harness from agent-as-worker (A) to agent-in-the-node (B) —
  a latency/throughput target, a co-location need, or engine-runtime readiness? B is a separate ADR;
  what does it need to decide that this one defers?
- **Does embedded fx need ACP on the wire at all?** When Nano is the *in-process* SDK host, the ACP
  framing may be redundant (Nano already holds the session objects directly); ACP's value is chiefly for
  the *out-of-process, hired* harnesses. Is ACP optional for the embedded case and mandatory only for the
  hired seam?
- **Effect-fence vocabulary at `workspace.exec`.** The agent's world path is a single `exec(command)`
  import — Nano must parse/classify commands (push, comment, merge) to fence them. Is that a fixed
  recognizer, or does the harness host wrap known effectful tools behind typed sub-imports rather than a
  raw shell string?
- **Trust boundary of the workspace host.** wasm removes ambient authority, but `workspace.exec` still
  runs real commands on the host. What admits a command (allowlist, policy, human gate), and how does
  that relate to ADR 0056's permission model and `onPermission`?
