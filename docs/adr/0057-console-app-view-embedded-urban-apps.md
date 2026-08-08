# ADR 0057 — Console App View: mounting bespoke Urban app UIs (iframe-sandboxed)

Status: **Proposed.**
Date: 2026-08-04.

Relates to:
ADR 0056 (`0056-agent-relay-command-stream-plane.md`, the agent relay — the server-side stream this
ADR's first consumer, the Workforce cockpit, observes and steers),
ADR 0051 (`0051-nano-workforce.md`, the Nano Workforce cockpit — the **first consumer** of this
surface: an Urban app whose bundle carries xterm.js + a Falcon/relay client),
ADR 0042 (`0042-urban-page-screen-composer.md`, the **declarative** Page Composer — the fixed-palette
page runtime this ADR *complements* with an escape hatch, not replaces),
ADR 0041 (`0041-urban-app-import-registry.md`, import-by-reference — how an app becomes available to
the console to be framed),
ADR 0028 (`0028-urban-app-user-auth-identity-authorization.md`, the captain/tenancy identity handed
to the framed app),
ADR 0034 (`0034-*`, the `console-observe` lean profile — the bundle budget this surface protects),
ADR 0016 (`0016-falcon-protocol.md`, the `/falcon` connection the framed app opens **itself**),
ADR 0052 (`0052-urban-runtime-decoupled-manifest-interpreter.md`, the Urban runtime that serves the
app's own assets),
and `server/src/console/extensions.rs` (the pack trust model — *console executes no untrusted JS* —
that this surface deliberately preserves).

## Context

ADR 0056 puts agent visibility on a server-side relay; ADR 0051 wants a **cockpit** UI over it that
runs standalone *or* in the console. The console must not bloat: agent visibility can't drag xterm.js,
a Falcon client, and a whole Workforce UI into the base SPA (and the `console-observe` profile, ADR
0034, exists precisely to keep it lean).

Two existing surfaces both fall short:

- **The pack/extension mechanism** (`ExtManifest`) deliberately **cannot ship interactive UI** — pack
  content is *"read as data, forwarded verbatim"* and untrusted packs render as **inert text**, so a
  live terminal is exactly what it excludes (see ADR 0056 §6 and the discussion behind it).
- **The Page Composer** (ADR 0042) is a **declarative, fixed-palette** renderer (`page.json` →
  text / actionForm / dataGrid, bound to datasource + actions). A cockpit — xterm.js, a live Falcon
  stream, attach/steer/transcript — is **not expressible** in that palette.

What's missing is a way for the console to host a **bespoke, code-shipping app view** without either
bloating its bundle or executing the app's JavaScript inside the console's own trust boundary. This
ADR designs that surface. The cockpit is its first consumer; "run any Urban app in-console" is the
general capability it unlocks.

## Decision

Add the **Console App View**: the console mounts an Urban-app-declared view **in a sandboxed
`<iframe>`**, served by the app's own runtime, and talks to it over a **narrow, typed, versioned
`postMessage` handshake**. The console injects **no** app code into its own SPA; the framed app holds
its **own** Falcon/relay client and authenticates its **own** connection.

### 1. The app declares a view; the console frames its URL

An Urban app manifest declares one or more **view entrypoints** (a built SPA served by the app
runtime, ADR 0052). The console discovers them via import-by-reference (ADR 0041), adds a nav entry,
and renders the view by **framing its URL** — never by importing its bundle. The base console SPA
gains only the thin **App View host** (an iframe container + the postMessage bridge + a nav entry),
never the app's UI, xterm, or Falcon client. `console-observe` (ADR 0034) stays lean.

### 2. Trust model: sandboxed frame, console runs no app JS

The frame is `sandbox`ed and the app is served from a path/origin the console pins with
`frame-ancestors`. The console executes **no untrusted JavaScript** — identical in spirit to the pack
model (`extensions.rs`). All capability flows through the mediated `postMessage` channel and through
the app's **own authenticated backend/Falcon connection**; the frame gets no privileged reach into the
console. This is why an arbitrary third-party Urban app can be hosted safely, where a pack-injected
view could not.

### 3. The framed app authenticates itself — the console does not proxy the stream

The cockpit talks to the relay over **its own** `/falcon` connection (ADR 0016/0056), as the same
**captain** (ADR 0028). The console performs an **identity handoff** (a short-lived,
narrowly-scoped session token / cookie scope) so the framed app acts as the signed-in operator — but
the console never becomes a stream proxy. This keeps the relay contract identical whether the cockpit
runs framed or standalone.

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

Framing an app view requires knowing the port the app actually bound. The manifest `ui` block can
*declare* a port (`ui.port`) or an env var to read it from (`ui.portEnv`), but neither covers an app
that picks its port at runtime — e.g. behind a custom env var the console does not set, so the port
lives only in the app's own code. Guessing a default (the legacy "Open app" button hardcoded `8090`)
opens the wrong port and the left rail reports the app "headless".

The **boot handshake** closes this gap. When the supervising host spawns the app it sets
`NANOBPMN_APP_HANDSHAKE` in the child env; the `@nanobpm/urban` runtime, once its HTTP server has
bound, announces the real port on a machine-readable **stdout control line**:

```
@@NBPM_LISTENING@@{"port":3000}
```

This joins the existing host↔child stdout control family (`@@NBPM_METRIC@@`, `@@NBPM_STATUS@@`): the
supervisor scrapes the line, records the detected port, and swallows the raw token (surfacing a
friendly "app listening on port N" instead). The detected port takes **strict precedence** over any
declared `ui.port`/`ui.portEnv`, so the console frames the webview and "Open app" opens the exact
port regardless of how the app chose it — no manifest or env port declaration required. The emit is
gated on `NANOBPMN_APP_HANDSHAKE` so direct terminal runs (`npm start`) are not cluttered with the
machine token, and it is emitted from the host-agnostic runtime core so both the Node and Deno hosts
honour it.

### 5. Standalone parity — framing is additive

The **same** app is served framed or standalone; the handshake **degrades gracefully** when there is
no parent (no `context`/`session` → the app falls back to its own auth + a default view). Nothing in
the cockpit is console-specific; the console is just one host.

### 6. First consumer: the Workforce cockpit

The ADR 0051 Workforce cockpit ships as an Urban app declaring an App View; its own bundle carries
xterm.js + the Falcon/relay client. It mounts in the console via this surface, runs standalone on Node
(the Urban-on-Node rule), and — being a plain responsive web app — is the same artifact a phone would
open (the phone connects to the app/relay directly; framing is console-only).

### 7. Explicitly deferred

- **First-party inline/trusted mount** (module-federated, no iframe) as a fast-path for *trusted*
  first-party views — a later optimization, not v1.
- **Relay-aware Page Composer palette components** (an `agentTerminal` node, ADR 0042) — the
  *authorable* embedding path; complementary, out of scope here.

## Consequences

- The console gains a general "host any Urban app UI" capability; the cockpit is the first of many.
  The base bundle grows only by the thin host, protecting ADR 0034.
- The relay contract (ADR 0056) is unchanged by *where* the cockpit runs; framed and standalone are
  the same client.
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
