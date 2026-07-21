# ADR 0028 — Urban App-user authentication, identity & authorization (the `ApplicationConfiguration` entity)

Status: **Proposed.**
Date: 2026-07-21.
Relates to: ADR 0022 (`0022-nano-rad-application.md`, **Urban** — the RAD App; multi-user secured
apps are the business/deployment end of its single-user→multi-user gradient),
ADR 0005 (`0005-embedded-u-nano.md`, the embedded→remote gradient this rides),
ADR 0007 (`0007-rad-extension-system.md`, declared data — the manifest declares *policy*, not
mutable state or secrets),
ADR 0024 (`0024-...datasource...`, the datasource that stores the `ApplicationConfiguration` entity
and the identity tables — users, roles, sessions — and the row-level-scoping question),
ADR 0026 (`0026-urban-human-surfaces-and-run-model.md`, whose **auth/identity open question** this
ADR resolves — login/admin surfaces + the action-API middleware are the enforcement points),
ADR 0027 (`0027-urban-app-manifest-spec.md`, whose manifest gains the **security policy** block),
ADR 0030 (`0030-domain-process-duality.md`, whose "a store has no identity — identity is a layer
above it" reframes the tenancy gap below into *carry-a-key-and-filter*),
`spec/{users,roles,groups,tenants,authorizations,authentication}.yaml` (the platform already mirrors
**Camunda's identity/authz API shape** — the parity vocabulary), `server/src/stub_impls.rs`
(those endpoints are currently **stubs**, so the gateway does not yet *enforce* identity), and
`engine-core/src/` (**no tenancy** today — the honest gap for multi-tenant apps).

## Context

Makers will want to build **multi-user, secured** apps — a business tool, a shared home dashboard,
a SaaS-lite. That needs authentication, identity, and authorization for **the App's own users**.
Two facts frame the design:

1. **Two distinct auth layers.** *App-user auth* (the end-users of a deployed App — who logs into
   the task inbox / GUI / chat) is different from *App→engine auth* (the App's Deno backend
   presenting a *service* credential to the Nano/Camunda gateway, `CAMUNDA_AUTH_STRATEGY`). This ADR
   is **Layer 1 (App-user auth)** only; it is App-scoped and does not require the engine to grow auth
   first.
2. **The platform has the Camunda identity *vocabulary* but not enforcement.** `spec/*` defines
   users/roles/groups/tenants/authorizations/authentication (Camunda-shaped), but `stub_impls.rs`
   implements them as stubs and `engine-core` has no tenancy. So Urban can align its identity
   concepts with Camunda for switch-over parity *by name*, while implementing App-user auth entirely
   in the App tier.

The non-negotiable constraint: the **default stays zero-auth, single-user** — the Delphi "just runs"
property. Security is **opt-in**, lighting up along the same gradient as SQLite→Postgres and
embedded→remote.

## Decision (proposed)

App-user authentication is an **opt-in, tiered** capability declared in the manifest and enforced in
the App tier, backed by a first-class **`ApplicationConfiguration`** runtime entity.

### 1. The tiers (maker picks one; default is none)

- **Tier 0 — none (default).** No login; single-user unsecured. What exists today; nothing to
  configure.
- **Tier 1 — local username/password + self sign-up.** The App owns a users table in its datasource
  (ADR 0024): sign-up, login, password (hashed with a modern KDF — argon2id/bcrypt), server-side
  sessions (HTTP-only cookies). Zero external dependency — the self-hosted small-business path.
- **Tier 2 — federated OIDC (social login).** Drop-in **GitHub / Google / Auth0** (and any generic
  OIDC provider): the maker ticks a provider and configures its client id/secret (as `${ENV}` refs),
  and login "just works" via Authorization Code + PKCE. This is the **Camunda switch-over path** —
  the identity concepts map onto `spec/{users,roles,groups,authorizations}`.
- **Tier 3 — enterprise directories (deferred).** MS Entra, LDAP. Explicitly *later*; the provider
  seam (§5) is shaped so they are new providers, not a redesign.

Tiers are **composable**: an App may enable Tier 1 *and* Tier 2 (local accounts *and* "Sign in with
Google") — the provider set is a list.

### 2. The `ApplicationConfiguration` entity (first-class, runtime)

Draw a third line beyond the two design-time config files:

| Artifact | Nature | Lives in |
|---|---|---|
| `nanobpm.project.json` | design-time IDE/toolchain | project dir |
| `nano.app.json` | design-time **binding** + *static security policy* (declared) | ships in binary |
| **`ApplicationConfiguration`** | **runtime, per-deployment, admin-mutable** config + resolved security posture + environment profile | the **datasource (0024)** |

The manifest *declares* the policy (auth tier, enabled providers, role definitions, action/surface/
data→role rules) as immutable, shareable, secret-free **declared data** (ADR 0007). The
**`ApplicationConfiguration`** is the *running* entity that resolves that policy against a concrete
environment (dev/staging/prod), holds admin-editable settings, and anchors the identity **tables**
(users, role assignments, sessions, provider registrations) — all relational rows in the datasource.
It cleanly separates *what the app is* (in git) from *how this deployment is configured and secured*
(mutable, environment-specific, sensitive, in the DB). A privileged **admin surface** (an ADR 0026
surface, admin-role-gated) edits it; the **first-admin bootstrap** is a §Open-questions item.

### 3. The manifest security block (ADR 0027)

```jsonc
"security": {
  "mode": "oidc",                       // "none" (default) | "local" | "oidc"; list to combine
  "providers": [
    { "id": "google", "type": "oidc", "preset": "google",
      "clientId": "${GOOGLE_CLIENT_ID}", "clientSecret": "${GOOGLE_CLIENT_SECRET}" },
    { "id": "github", "type": "oidc", "preset": "github",
      "clientId": "${GITHUB_CLIENT_ID}", "clientSecret": "${GITHUB_CLIENT_SECRET}" },
    { "id": "local",  "type": "password", "signup": "open" }   // "open" | "invite" | "closed"
  ],
  "roles": ["admin", "user"],           // maker may add domain roles
  "rules": {                            // authorization: what each role may reach
    "actions":  { "start/*": ["user"], "admin/*": ["admin"] },
    "surfaces": { "/tasks": ["user"], "/admin": ["admin"] },
    "data":     { "app": { "write": ["user"], "admin": ["admin"] } }
  }
}
```

Secrets (client secrets, signing keys) are **`${ENV}` references resolved at boot** (ADR 0027 §5), so
the committed/shared manifest carries none. The block is pure declared data — validated by ADR 0027's
fail-closed gates (a rule naming an undeclared role is an authoring-time error).

### 4. Authorization — role-based, one principal, enforced at the seams

Every authenticated request resolves to a **principal** = `{ userId, roles[], provider }` (and a
`tenant` later — §gaps). Authorization is **role-based**, declared in §3's `rules`, and enforced by a
single middleware at the three seams:

- the **action API** (ADR 0026 §1) — `start`/`message`/`complete` gated by `rules.actions`,
- the **surfaces** — `/tasks`, `/admin`, chat gated by `rules.surfaces`,
- the **datasource** (ADR 0024) — read/write gated by `rules.data` (and row-scoping, §gaps).

Roles map onto `spec/{roles,authorizations}` for Camunda parity; `admin`/`user` are built-in, the
rest are maker-defined.

### 5. The provider seam + "drop-in" ergonomics

A provider implements a small contract — `authenticate(req) -> principal | redirect`,
`callback(req) -> session` — so Tier 3 (Entra/LDAP) and additional OIDC providers arrive as new
providers, not a rewrite (candidate `nano-ide-auth-*` pack axis, consistent with data/trigger axes).
For the maker experience ADR 0022 promises ("tick a box, drop in a component"):

- **Generic surfaces** render a **login page generated from the manifest's enabled providers** — a
  "Sign in with Google/GitHub" button per OIDC provider + a username/password form for `local`. Zero
  frontend written.
- **Bespoke GUIs** get an **auth client + a `<LoginPanel>` component** as an App-side runtime
  dependency (the same shape as the form-js viewer in ADR 0026 §3) — the "drop in a component" path.

### 6. Sessions & transport

Browser surfaces use **HTTP-only, SameSite cookies** over server-side sessions in the
`ApplicationConfiguration`'s datasource; programmatic API callers use a **bearer token**. Cookie
flows carry **CSRF** protection; OIDC flows use **Authorization Code + PKCE** with `state`/`nonce`.
Under `runtime.engine: remote|cluster`, the App may additionally **propagate** the end-user identity
to the engine (assignee, and tenant once the engine supports it) so Operate/Tasklist reflect the real
user — forward-looking, gated on the engine gaps below.

## Honest gaps & dependencies

- **Engine has no tenancy** (`engine-core`): true **multi-tenant** apps (Tier 2 + tenant isolation)
  are deferred to an engine seam. **Single-tenant multi-user** (many users, one shared workspace)
  works now, entirely in the App tier. *Reframed (ADR 0030 §4/Consequences):* this seam is smaller
  than "grow tenancy." A store has no identity — identity is a layer *above* it (Postgres RLS is a
  `WHERE tenant = …` over tables that know nothing of users; app-level filters are the same one tier
  up). So the engine need not learn what a user *is*; it needs only to **(a)** carry an *opaque
  scoping key* on the reachable records (process instances, jobs, user tasks, messages, and their
  read-model rows) and **(b)** honour it at every access seam — instance reads, task/instance queries,
  and, the one place it is more than a `WHERE` clause, the *pushed* surfaces (job-activation **streams**
  and **message correlation**, where the key must ride the subscription and be honoured at dispatch).
  Identity *resolution* (principal → roles → tenant) stays App-tier, exactly as this ADR already
  places it; this mirrors Camunda's own opaque `tenantId` (the broker carries the key, Identity
  resolves membership).
- **Gateway identity endpoints are stubs** (`stub_impls.rs`): App-user auth (Layer 1) does not depend
  on them; only Layer-2 *identity propagation* to the engine does.
- **Row-level data scoping** (per-user/tenant isolation) is an ADR 0024 design question (Postgres RLS
  vs. app-level filters) — required for shared-datasource multi-user privacy.

## Phased plan

1. **auth-core** — the `ApplicationConfiguration` entity (datasource-backed) + the principal/session
   model + the §4 middleware seam on the action API (Tier 0 = pass-through). Ships the enforcement
   skeleton before any provider.
2. **local-identity (Tier 1)** — users table, sign-up/login, KDF-hashed passwords, server sessions,
   the generated login form.
3. **oidc (Tier 2)** — generic OIDC + **GitHub/Google/Auth0 presets**, Authorization Code + PKCE,
   the generated provider buttons, account records.
4. **authorization + admin** — the role model + manifest `rules` enforcement across action
   API/surfaces/datasource, plus the admin surface (manage users/roles) and first-admin bootstrap.

## Open questions

- **Account linking** — one human with Google *and* GitHub *and* a local account: link by verified
  email, or keep separate identities? (MFA later compounds this.)
- **Session store** — server-side sessions in the datasource (revocable, needs the DB) vs. stateless
  signed JWT cookies (no DB, harder to revoke). Default per tier?
- **Sign-up gating** — `open` self-serve vs. `invite`-only vs. `closed` (admin-provisioned); default
  and how invites are issued.
- **First-admin bootstrap** — how the very first admin is created on a fresh multi-user deployment
  (env-seeded credential? a one-time setup route? the maker's OIDC identity?).
- **Password reset / email** — Tier 1 needs an email channel; is that a `nano-ide-trigger-*`/worker
  concern (reuse the trigger runtime for outbound), or a built-in?
- **Row-level scoping** (with ADR 0024) — RLS vs. app-level tenant/user filters, and how a bound
  form control (ADR 0024 §5) is scoped to the current principal.
- **Secret rotation** — OIDC client secrets / session signing keys are `${ENV}`; rotation and
  multi-key validity windows.
- **MFA / passkeys** — Tier 1 TOTP / WebAuthn passkeys — a later increment on the provider seam.
