# ADR 0057 — Console App View: mounting bespoke Urban app UIs (iframe-sandboxed)

Status: **Proposed.**
Date: 2026-08-09.

Relates to:
ADR 0056 (`0056-agent-relay-command-stream-plane.md`, the **Nano agentic protocol** — the app-tier
channel this ADR's first consumer, the Workforce cockpit, is a client of),
ADR 0051 (`0051-nano-workforce.md`, the Nano Workforce cockpit — the **first consumer** of this
surface: an Urban app whose bundle carries xterm.js + the agentic-channel client),
ADR 0042 (`0042-urban-page-screen-composer.md`, the **declarative** Page Composer — the fixed-palette
page runtime this ADR *complements* with an escape hatch, not replaces),
ADR 0041 (`0041-urban-app-import-registry.md`, import-by-reference — how an app becomes available to
the console to be framed),
ADR 0028 (`0028-urban-app-user-auth-identity-authorization.md`, the captain/tenancy identity handed
to the framed app),
ADR 0034 (`0034-*`, the `console-observe` lean profile — the bundle budget this surface protects),
ADR 0052 (`0052-urban-runtime-decoupled-manifest-interpreter.md`, the Urban runtime that serves the
app's own assets **and** the agentic channel on the app's own port),
and `server/src/console/extensions.rs` (the pack trust model — *console executes no untrusted JS* —
that this surface deliberately preserves).

## Context

The Nano agentic layer (ADR 0056) lives **app-tier**: agent networks, visibility, blackboard, and live
terminals ride a single channel **served by the app on its own port**, and the machinery is a generic
`@nanobpm/urban` capability. Its visibility surface is therefore an **Urban page**, and ADR 0051 wants
that cockpit UI to run standalone *or* in the console. The console must not bloat: agent visibility
can't drag xterm.js, an agentic-channel client, and a whole Workforce UI into the base SPA (and the
`console-observe` profile, ADR 0034, exists precisely to keep it lean).

Two existing surfaces both fall short:

- **The pack/extension mechanism** (`ExtManifest`) deliberately **cannot ship interactive UI** — pack
  content is *"read as data, forwarded verbatim"* and untrusted packs render as **inert text**, so a
  live terminal (the relay bytes ADR 0056 defines) is exactly what it excludes.
- **The Page Composer** (ADR 0042) is a **declarative, fixed-palette** renderer (`page.json` →
  text / actionForm / dataGrid, bound to datasource + actions). A cockpit — xterm.js, a live agentic
  stream, attach / steer / transcript — is **not expressible** in that palette.

What's missing is a way for the console to host a **bespoke, code-shipping app view** without either
bloating its bundle or executing the app's JavaScript inside the console's own trust boundary. This
ADR designs that surface. The cockpit is its first consumer; "run any Urban app in-console" is the
general capability it unlocks.

## Decision

Add the **Console App View**: the console mounts an Urban-app-declared view **in a sandboxed
`<iframe>`**, served by the app's own runtime, and talks to it over a **narrow, typed, versioned
`postMessage` handshake**. The console injects **no** app code into its own SPA; the framed app holds
its **own** client to its **own** backend — including the app-tier agentic channel (ADR 0056) — and
authenticates that connection itself.

### 1. The app declares a view; the console frames its URL

An Urban app manifest declares one or more **view entrypoints** (a built SPA served by the app
runtime, ADR 0052). The console discovers them via import-by-reference (ADR 0041), adds a nav entry,
and renders the view by **framing its URL** — never by importing its bundle. The base console SPA
gains only the thin **App View host** (an iframe container + the postMessage bridge + a nav entry),
never the app's UI, xterm, or agentic-channel client. `console-observe` (ADR 0034) stays lean.

### 2. Trust model: sandboxed frame, console runs no app JS

The frame is `sandbox`ed and the app is served from a path/origin the console pins with
`frame-ancestors`. The console executes **no untrusted JavaScript** — identical in spirit to the pack
model (`extensions.rs`). All capability flows through the mediated `postMessage` channel and through
the app's **own authenticated backend and agentic channel**; the frame gets no privileged reach into
the console. This is why an arbitrary third-party Urban app can be hosted safely, where a pack-injected
view could not.

### 3. The framed app connects to its own backend — the console does not proxy the stream

The cockpit talks to the agentic channel over **its own** connection to **its own** app backend
(ADR 0056), as the same **captain** (ADR 0028). The console performs an **identity handoff** (a
short-lived, narrowly-scoped session token / cookie scope) so the framed app acts as the signed-in
operator — but the console never becomes a stream proxy. Because the agentic channel is the app's own
(not a console- or engine-hosted relay), the cockpit's transport is **identical whether it runs framed
or standalone**: in both cases it is just the app's frontend talking to the app's backend.

#### Amendment (issue #1054): byte-opaque WebSocket tunneling through the app-view proxy

The principle above — *the app's frontend talking to the app's backend* — is preserved, but its
original implementation ("the console never proxies the stream") broke the embedded case. When the
cockpit is viewed **framed**, its live terminal derives its WebSocket URL from `location.host`, which
resolves to the **console** origin, not the app's loopback port. The app-view HTTP reverse proxy
(`/console/app-view/{name}/…`) rejected every `Upgrade` request with `501`, so the framed terminal
could never reach the app's agentic channel and sat forever "waiting for live output". The rejected
alternative — the app self-connecting to its own origin — requires every browser to reach the app's
raw loopback port, which breaks hosted/remote consoles and forces each app to re-derive its
externally-reachable URL.

The app-view proxy therefore **tunnels WebSockets transparently**: a request whose `Connection` header
contains `upgrade` and whose `Upgrade` header is `websocket` is upgraded at the console and bridged
bidirectionally to the app's own UI port, as a **byte-opaque pipe**. This keeps the ADR's core property
true at the byte level (the app's frontend still talks to the app's backend; the console is a dumb pipe
in between), under strict guardrails:

- **Byte-opaque.** The console does not parse, filter, or mutate frames.
- **No credential injection.** The tunnel injects no auth of its own; it forwards the browser's *own*
  end-to-end auth headers (`Cookie`/`Authorization`) upstream, exactly as the HTTP path forwards
  `Authorization`, so the app self-authenticates end-to-end (the app's own token/cookie rides the
  upgrade) and §2's trust model is unchanged — the console gains no privileged reach into the stream.
- **WebSocket-only.** Any *other* `Upgrade` token (e.g. `h2c`) is still refused with `501`; the console
  is not a general stream proxy. An `Upgrade: websocket` request whose handshake is *malformed* (missing
  or invalid `Sec-WebSocket-*` headers) is a client error → `400`, not `501`.
- **Same guards as HTTP.** Upstream resolution is identical to the HTTP path (unsafe name → 400, app
  not running → 503, headless → 404), resolved once at connect time; an unreachable app is a `502`, and
  if the app dies mid-session the browser socket is closed cleanly.

### 4. Typed, versioned handshake

A small message contract, versioned, everything inert/mediated:

| Direction | Message | Payload |
|---|---|---|
| console → app | `context` | `{ nodeId?, source?, correlationId? }` — what to focus |
| console → app | `session` | identity/handoff (ADR 0028), tenancy |
| console → app | `theme` | console theme tokens (so the frame matches) |
| console → app | `route` | deep-link / navigation target |
| console → app | `lifecycle` | `visible` / `hidden` (pause streams when backgrounded) |
| app → console | `ready` | handshake version + capabilities |
| app → console | `resize` | desired height (or `fit`) |
| app → console | `navigate` | request a console route change |
| app → console | `title` / `badge` | tab title / unread indicator |

No message grants host-privileged access; `navigate`/`resize`/`title` are requests the console
honours at its discretion.

### 4a. Boot handshake — the app self-reports its bound port

Framing an app view requires knowing the port the app actually bound — the same port that serves the
app's agentic channel (ADR 0056). The manifest `ui` block can *declare* a port (`ui.port`) or an env
var to read it from (`ui.portEnv`), but neither covers an app that picks its port at runtime — e.g.
behind a custom env var the console does not set, so the port lives only in the app's own code.
Guessing a default opens the wrong port and the left rail reports the app "headless".

The **boot handshake** closes this gap. When the supervising host spawns the app it sets
`NANOBPMN_APP_HANDSHAKE` in the child env; the `@nanobpm/urban` runtime, once its HTTP server has
bound, announces the real port on a machine-readable **stdout control line**:

```
@@NBPM_LISTENING@@{"port":3000}
```

This joins the existing host↔child stdout control family (`@@NBPM_METRIC@@`, `@@NBPM_STATUS@@`): the
supervisor scrapes the line, records the detected port, and swallows the raw token (surfacing a
friendly "app listening on port N" instead). The detected port takes **strict precedence** over any
declared `ui.port`/`ui.portEnv`, so the console frames the webview — and the cockpit reaches the
agentic channel — on the exact port regardless of how the app chose it, with no manifest or env port
declaration required. The emit is gated on `NANOBPMN_APP_HANDSHAKE` so direct terminal runs
(`npm start`) are not cluttered with the machine token, and it is emitted from the host-agnostic
runtime core so both the Node and Deno hosts honour it.

### 5. Standalone parity — framing is additive

The **same** app is served framed or standalone; the handshake **degrades gracefully** when there is
no parent (no `context`/`session` → the app falls back to its own auth + a default view). Nothing in
the cockpit is console-specific; the console is just one host.

### 6. First consumer: the Workforce cockpit

The ADR 0051 Workforce cockpit ships as an Urban app declaring an App View; its own bundle carries
xterm.js + the agentic-channel client (ADR 0056). It mounts in the console via this surface, runs
standalone on Node (the Urban-on-Node rule), and — being a plain responsive web app — is the same
artifact a phone would open (the phone connects to the app's agentic channel directly; framing is
console-only).

### 7. Explicitly deferred

- **First-party inline/trusted mount** (module-federated, no iframe) as a fast-path for *trusted*
  first-party views — a later optimization, not v1.
- **Agentic-aware Page Composer palette components** (an `agentTerminal` node, ADR 0042) — the
  *authorable* embedding path; complementary, out of scope here.

## Consequences

- The console gains a general "host any Urban app UI" capability; the cockpit is the first of many.
  The base bundle grows only by the thin host, protecting ADR 0034.
- The agentic-channel contract (ADR 0056) is unchanged by *where* the cockpit runs; framed and
  standalone are the same client talking to the same app backend.
- The pack trust model is preserved: no untrusted JS in the console; arbitrary app UI is sandbox-safe.
- New seams to own: iframe focus/keyboard (xterm needs key capture), theme propagation, deep-link
  routing sync, and the identity handoff.

## Open questions

- **Origin & CSP**: serve app views same-origin (simpler cookies, weaker isolation) or a distinct
  origin (stronger isolation, explicit handoff)? Exact `sandbox` flags and `frame-ancestors`.
- **Identity handoff mechanics**: short-lived bearer token via `postMessage` vs a scoped cookie; CSRF
  posture for the app's own action calls.
- **Focus & input**: xterm.js needs reliable keyboard focus and paste inside a sandboxed frame —
  confirm no `sandbox` flag starves it.
- **Discovery/registration**: how the console lists available app views (manifest `views[]` +
  import-by-reference, ADR 0041) and where a view appears in console nav.
- **Handshake versioning**: negotiation via `ready.capabilities`; forward-compat rules for unknown
  messages (ignore, per the `pty.rs` resize convention).
