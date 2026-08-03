# ADR 0056 — The agent relay: a durable command-stream plane for observing and steering agent-workers

Status: **Proposed.**
Date: 2026-08-04.

Relates to:
ADR 0016 (`0016-falcon-protocol.md`, the **Falcon** unified bidirectional command stream — the
persistent `/falcon` WebSocket + connection `Registry` this ADR carries the relay over, rather than
inventing a second transport),
ADR 0046 (`0046-agent-as-worker-vs-agent-in-the-node.md`, **agent-as-worker** — the topology whose
worker this ADR makes *observable and steerable*),
ADR 0051 (`0051-nano-workforce.md`, the crew orchestrator — this ADR **resolves its open "Crewmate
transport" question on the visibility axis** and supplies the "each crewmate in its own visible
terminal" substrate that firstmate got from a local tmux pane),
ADR 0055 (`0055-nano-sdk-transport-spine.md`, one engine client over Falcon — the relay is the
*second* thing a worker connection carries, alongside the job protocol),
ADR 0002 (`0002-leader-local-activation-and-lease-digest.md`, the durable job lease — the
**coordination** plane this ADR is explicitly *not*),
ADR 0028 (`0028-urban-app-user-auth-identity-authorization.md`, the identity a remote cockpit
authenticates as),
`server/src/console/pty.rs` (the **local IDE integrated terminal**, #496 — the prior art this ADR
shares plumbing with but is *not* an extraction of),
`server/src/falcon.rs` + `server/src/consumers.rs` (the Falcon `Registry`, `last_seen_ms`, liveness
timeout the relay reuses),
and the MIT-licensed prior art it cribs its hard parts from:
[`stablyai/orca`](https://github.com/stablyai/orca) — a desktop ADE for parallel coding agents whose
relay (`src/relay/`, `src/shared/relay-frame-decoder.ts`, `src/relay/pty-source-credit-record.ts`)
already solved multiplexed PTY streaming with credit-based backpressure, resumable delivery, and a
mobile/VPS transport story.

## Context

Nano is growing an agentic SDLC surface: agent-workers (ADR 0046) activated by the engine, and a crew
orchestrator to coordinate them (ADR 0051, Nano Workforce). Two motivating shapes exist:

- **Ephemeral workers** — e.g. a PR-convergence agent: fire-and-forget, but we want to *see what it
  did* after it's gone (the transcript).
- **Long-lived feature-development agents** — stateful, interactive, need to be *watched and steered*
  mid-run, and must survive a cockpit disconnecting and reattaching.

The engine already gives us the **coordination** primitives: durable job leases (ADR 0002), message
correlation, signals, timers — everything for *agents coordinating with each other* ("I need your npm
publish before I proceed", "how do we split this", "the plan changed and it affects you"). That is a
solved plane and this ADR does not touch it.

What is missing is the orthogonal **observability + interactivity** plane: a live byte-stream of an
agent's I/O, delivered to a remote cockpit, steerable by writing input back, durable enough to replay
after the agent exits, and resumable after a cockpit reconnects. ADR 0051 hand-waves this as "each
crewmate in its own visible terminal" (firstmate's local tmux pane) and lists **"Crewmate transport"**
as an explicit open question. This is that answer.

`server/src/console/pty.rs` is *not* it. That is the IDE's integrated terminal (#496): one WebSocket
glued to one PTY running the **operator's own shell**, **loopback-only** (a shell is RCE), off by
default, and dying with the socket. It is 1:1, ephemeral, local, and operator-driven — the opposite
of what agent visibility needs (N cockpits over M worker-registered sources, durable, remote,
resumable). We reuse its low-level plumbing, not its module.

We looked hard at [`stablyai/orca`](https://github.com/stablyai/orca) (MIT). Its load-bearing idea is
to decouple a **framed, multiplexed session protocol** from the **transport**: the same PTY protocol
runs over a Unix socket / named pipe (local), an SSH channel (remote/VPS), and a WebSocket (mobile).
It has already paid down the hard problems we would otherwise hit: credit-based backpressure
(`pty-source-credit-record.ts`), bounded replay + resume-from-offset (`acceptedSourceEndSu`),
generation/incarnation fencing for multi-attach and reconnect, interactive-vs-bulk frame
prioritisation, and a pairing-code auth model for remote clients. Orca is desktop-hosted hub-and-spoke
(the always-on desktop app is the hub); our workers are headless, ephemeral, and gateway-brokered, so
we invert that: **the hub lives in the server, cockpits are interchangeable clients.**

## Decision

### 1. Two planes, kept separate

- **Coordination plane** = the engine (ADR 0002/0046/0051). Agent↔agent dependencies, work-splitting,
  plan changes ride messages/signals/variables/leases. We do **not** build a bespoke agent bus.
- **Observability + interactivity plane** = the **agent relay** (this ADR). Stream stdout, inject
  stdin, resize, replay, resume. Volatile session state lives here; **durable orchestration state
  (plan, assignments, progress) stays in the engine/app data**, not in the relay buffer.

### 2. The relay is a new server primitive, beside the engine — not the console

The relay is a first-class server-side service at the **engine tier** (always-on when the cluster is
up), because agent visibility is shared infrastructure: a headless cluster must serve streams to the
standalone Workforce app and to a phone even with no console compiled in. The console *gains a
Workforce view* that is merely **one cockpit** over the relay; it is not the relay's home.

### 3. Two protocol primitives; an agent speaks both

| Primitive | Contract | Speakers |
|---|---|---|
| **Engine** (exists) | **job protocol** | worker ⟷ engine |
| **Relay** (new) | **stream protocol** | agent ⟶ relay (produce / steer-in), cockpit ⟷ relay (attach / replay / steer) |

**An agent = a worker wired to both.** An ordinary (non-agent) worker never touches the relay.

### 4. Transport is Falcon, not a new WebSocket

Falcon (ADR 0016) is already a persistent bidirectional WebSocket command stream at `/falcon` with a
connection `Registry` + liveness. The relay is a **new message family on Falcon plus a durable stream
store**, not a new endpoint. Remote cockpits (phone, VPS) are therefore reachable through the
transport spine we already ship (ADR 0055), and Orca's "tunnel the framed protocol over SSH" pattern
maps onto "connect Falcon from the remote host".

### 5. Feature-gated; the engine builds without it

`relay` is an independent Cargo feature. Dependency direction is **one-way: `relay → engine`, never
`engine → relay`.** The relay may *optionally* tag a source with a job / process-instance correlation
id; the engine neither knows nor reaches for the relay. Compose freely:

| Build | engine | relay | console | Use |
|---|---|---|---|---|
| `cargo build` (default, no features) | ✓ | — | — | pure orchestration; ordinary job workers, **no agent streaming** |
| `--features relay` | ✓ | ✓ | — | **headless agentic** — agents stream to standalone app / phone, no UI |
| `--features console` (⇒ `console = ["relay", …]`) | ✓ | ✓ | ✓ | full cockpit; Workforce view over the relay |

The default build — engine + gateway REST + Falcon, no relay — is exactly today's `cargo build` (the
crate has no `default` features). "Engine without the relay" is the baseline, not a special case.

### 6. Net-new module, sharing only primitives with `console/pty.rs`

The relay is **not** an extraction of `console/pty.rs`. It reuses `portable-pty`, the axum `ws`
upgrade, and — for consistency — that terminal's proven wire convention (**binary frames = raw bytes
both directions; text frame = `{"type":"resize",cols,rows}` JSON control**). Everything else differs:
multiplexed (N⟷M) not 1:1; worker-**registered** sources not console-spawned shells; sources
**outlive** any cockpit; and — decisively — an **inverted security posture**: `pty.rs` is
loopback-locked *because* a shell is RCE, whereas the relay's whole purpose is remote cockpits, so it
needs real auth (pairing-code / endpoint-credential, Orca-style, threaded through ADR 0028 identity),
not loopback gating. `console/pty.rs` stays exactly as it is.

### 7. Crib these hard-won pieces from Orca (MIT)

- **Bounded replay ring + resume-from-offset** (`pty-source-credit-record.ts`, `acceptedSourceEndSu`):
  a per-source ring so late-join / reconnect catches up without unbounded buffering; the cockpit
  resumes from its last accepted offset — no loss, no duplication.
- **Credit-based backpressure**: producer slows when a slow consumer (phone / lossy link) lags,
  instead of OOMing the server.
- **Generation / incarnation fencing**: a stale reconnect can't double-attach or steal write
  ownership; steering is gated on the current owner-generation.
- **Interactive-vs-bulk QoS**: interactive frames are not buried behind bulk output.
- **Retention policy by lifecycle**: *ephemeral* → flush the ring to a **durable transcript artifact**
  on job completion (this is "see what the ephemeral agent did"); *long-lived* → live attach +
  resumable reattach + serialize/revive.

## Consequences

- Nano ships **two composable server primitives** (engine, relay) and interchangeable cockpits
  (console Workforce view, standalone Urban Workforce app, phone). Users building their own SDLC
  workflows/apps get agent visibility "for free" the way they get job workers today.
- ADR 0051's crewmates and ADR 0046's agent-workers get a real transport for their live terminal and
  status; the coordination plane stays on the engine.
- The default and console-less builds are unaffected; the relay is pure opt-in and *unlocks* a
  headless-agentic build that is impossible today (PTY currently lives only under `console`).
- New surface to own: a durable stream store, credit accounting, and reconnect fencing — non-trivial,
  but the design is de-risked by Orca's working implementation.

## Open questions

- **PTY vs pipe** for non-TTY workers: allocate a PTY (full interactivity, ANSI, prompts — needed for
  "steer the agent") or just capture stdout/stderr? Likely per-source opt-in.
- **Durable transcript store**: reuse the server's `rusqlite` substrate with a separate schema, or a
  dedicated store? Retention/eviction policy for long transcripts.
- **Relay frames on Falcon**: a distinct Falcon message family, or a sub-protocol tunnelled verbatim
  over a Falcon channel? Affects how much of Orca's `relay-frame-decoder` we reimplement.
- **Remote-cockpit auth**: pairing-code + endpoint-credential (Orca) reconciled with existing Falcon
  auth and ADR 0028 identity/tenancy.
- **Correlation id shape**: how a source is tagged to a job / process-instance so a cockpit lines up
  "this terminal" with "that process", kept optional and one-directional.
