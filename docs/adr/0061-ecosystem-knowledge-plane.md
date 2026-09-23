# ADR 0061 — Ecosystem knowledge plane: the context/memory split above AGENTS.md

Status: Proposed
Date: 2026-08-17
Relates to: ADR 0060 (institutional memory / model-grounded context — the *derived* half), ADR 0051 (agent authoring brief / `/agent` + `/llms.txt`), ADR 0056 (agent relay / command-stream plane), ADR 0059 (supervisor enrolment — the multi-app / cross-org world)
Repo: nanobpm/nano-bpm (cross-cutting: nanobpmn, nano-ide, nano-workforce, c8ctl-plugin-nano, urban-pr-review, bojtos, nano-sdk-js)

## Context

Agents in the Nano ecosystem work across **many repos** — nanobpmn (engine + console), nano-ide
(Urban toolkit), nano-workforce (the autonomous fleet), c8ctl-plugin-nano (the worker host),
urban-pr-review, bojtos, nano-sdk-js. A fact learned shipping a change in one repo is frequently a
**cross-repo invariant** ("npm OIDC publishing is per-source-repo"; "any PR touching engine-core
must carry a pending engine-wasm bump or CI's release-guard fails"; "feature work goes in a worktree
under `~/workspace/<repo>-worktrees/<name>`, never on `main`"). Today that knowledge has two homes,
each with a specific shortfall:

- **`AGENTS.md`** — static, **per-repo**, human-authored, versioned and PR-reviewed (its virtue). But
  it has **no provenance, no confidence, no decay, and no ecosystem scope**: an invariant that spans
  repos is either duplicated into each `AGENTS.md` or siloed in the one where it was written.
- **Copilot Memory** (repo- and user-scoped) — has **provenance and scope** and an up/down-vote
  signal, but **no ecosystem scope and no maintenance policy** (no TTL, no confidence decay, no
  dedup). It is a memory *store* without a memory *write/maintenance policy*.

Neither is a **meta-layer**: an ecosystem-scoped body of knowledge, above any single repo, that an
agent working *anywhere* in the system can retrieve. This ADR names that layer and the discipline it
needs.

### Two disciplines, one system (the framing we are adopting)

Recent practice separates two disciplines that are usually built together and conflated
("Context vs. Memory Engineering in Agentic AI Systems"):

- **Context engineering** — the design of a *single* inference call: what to include, compress,
  place, and discard. Everything is ephemeral; the window clears when the call ends. Fails by
  **overflow, attention degradation ("lost in the middle"), and noisy assembly**.
- **Memory engineering** — what *survives* a call: the write policy, storage, retrieval, and
  maintenance that let a later call, session, or agent use information. Fails by **retrieval miss,
  staleness, poisoning, and unbounded growth** — precisely when there is *no write policy*.

They meet at the **retrieval boundary**: memory produces candidates; context assembly decides whether
each enters the window, how much, and *where*. The two dominant failure modes at that boundary are
**retrieval without a context budget** (inject everything → the window fills, instructions get
crowded out) and **poor placement** (the right memory is retrieved but buried mid-context, so the
model behaves as if it is missing).

Nano already has both halves, built ad hoc:

| Discipline | Existing Nano surfaces |
|---|---|
| Context engineering | `/agent` brief + `/llms.txt` (ADR 0051); the ADR 0060 system-brief injection; per-session prompt assembly |
| Memory engineering | Copilot Memory (provenance + repo/user scope); nwf retro / feed-forward; session checkpoints |
| Retrieval boundary | ADR 0060 open question #1 (scope the injection to a prompt budget) — *unsolved* |

What is missing is not a store; it is (a) an **ecosystem scope** above per-repo, and (b) the
**memory-engineering discipline** — an explicit write policy and maintenance — without which the
layer rots into the stale-README problem the whole institutional-memory effort (ADR 0060) set out to
kill, now at ecosystem scale and with **poisoning** added.

### Prior art: Walrus Memory (crib the model, not the substrate)

Walrus (`walrus.xyz`, `docs.wal.app`) is decentralized, *verifiable* blob storage on Sui; **Walrus
Memory** wraps it as portable agent memory — encrypted, semantically indexed, governed by ownership +
delegate permissions, with **provable read-back** ("prove what it remembered, trust what it reads
back"). Its two headline properties are **verifiability** and **cross-org portability**.

For a **single-owner** ecosystem (we own every repo), verifiability/decentralization solves a problem
we do not have — we trust our own store. So we adopt **Walrus Memory's interface model** — typed,
provenance-tagged, permissioned (namespace ownership + delegation), semantically indexed, portable
entries — while **rejecting its substrate** (no blockchain; back it with infra we already own).

The exception is real but deferred: once **third-party Urban app authors** publish and share
knowledge across org boundaries (the multi-app world of ADR 0059), verifiable/portable cross-org
memory becomes genuinely useful. We forward-reference that case; we do not build for it now.

## Decision

Introduce an **ecosystem knowledge plane**: an ecosystem-scoped meta-layer above `AGENTS.md` and
per-repo Copilot Memory, defined by a **write policy** and **maintenance loop**, retrieved through a
**budgeted, placement-aware boundary**. Five parts.

### 1. Two tiers, split by how a fact is trusted and maintained

The defining feature is *not* size — a bigger `AGENTS.md` rots faster. Tier by trust/maintenance:

- **Invariants tier** — human-reviewed, versioned, low-volume, ecosystem-scoped and
  cross-repo-addressable. Architecture facts, release rules, conventions. High `trust_level`, no TTL,
  changes land via PR (keeps `AGENTS.md`'s reviewed-and-versioned virtue, adds cross-repo scope).
- **Learned tier** — machine-written by the nwf retro / L2 domain-signal plane and by agents,
  governed by the `MemoryEntry` schema below. This is where "what we learned shipping PR #747" or
  "capability X currently has no worker" lives. Volatile, gated, decayed, pruned.

### 2. A typed entry schema (the memory-engineering unit)

Every learned-tier fact is a typed entry carrying the fields that make write and maintenance logic
reason-able — importance/confidence/trust/provenance/TTL:

```python
class MemoryEntry(BaseModel):
    content: str
    memory_type: str        # working | episodic | semantic | procedural
    scope: str              # ecosystem | repo:<name> | user:<login>
    importance: float       # 0.0–1.0, gates long-term storage
    confidence: float       # decays over time for volatile facts
    trust_level: float      # 1.0 internal system, 0.5 user input, 0.0 external
    created_at: datetime
    expires_at: datetime | None
    provenance: dict        # agent_id, repo, session_id, pr, input_hash
```

`memory_type` chooses backend and retrieval (working → K/V exact key; episodic → semantic search;
semantic → hybrid; procedural → pattern/inject), matching the standard memory-type/storage mapping.

### 3. An explicit write policy (the most-skipped, highest-leverage part)

Retrieval quality is capped by what enters the store. A write is accepted only if it clears the gate:

```python
def should_write_to_long_term(entry: MemoryEntry) -> bool:
    return (entry.importance >= 0.6
            and entry.confidence >= 0.7
            and entry.trust_level >= 0.5)
```

The policy also fixes: which triggers write; which namespaces an agent/tool may write (scoped
permissions — the Walrus ownership/delegate model); how conflicts and corrections are handled; and
retention/TTL per `memory_type`. **Low-`trust_level` (external) content is sanitized before write** —
the anti-poisoning guard.

### 4. A budgeted, placement-aware retrieval boundary (this *is* ADR 0060 OQ#1, generalized)

Context assembly allocates a **token budget before retrieval**, then the memory layer returns only
the highest-value entries that fit — retrieval-aware assembly, not retrieve-then-truncate:

```python
async def retrieve_for_step(step, max_tokens) -> str:
    candidates = await memory.search(
        query=step.retrieval_query, max_results=10,
        filters={"trust_level": {"gte": 0.5}, "expires_at": {"gt": now()}})
    selected, used = [], 0
    for e in sorted(candidates, key=lambda e: e.relevance_score, reverse=True):
        cost = token_count(e.content)
        if used + cost > max_tokens: break
        selected.append(e.content); used += cost
    return "\n\n".join(selected)
```

**Placement:** hard constraints/instructions at the top; retrieved memory near the **end**, adjacent
to the current task/query (the active reasoning region), to beat "lost in the middle." The ADR 0060
system-brief injection becomes one *caller* of this boundary; so does every nwf worker prompt.

### 5. Substrate: infra we already own — not a blockchain, not a new datastore

- **Producer / store** = the nwf retro / **L2 domain-signal plane** writes the learned tier; the
  invariants tier is a reviewed, versioned document set (ecosystem `AGENTS.md`-plus).
- **Retrieval** = the budgeted boundary above, surfaced to agents through the existing `/agent` brief
  discovery path (ADR 0051) and per-app `/app/agent` (ADR 0060).
- **Namespacing / permissions** = the Walrus *model* (scoped namespaces, ownership + delegation),
  implemented over our own store. Copilot Memory's up/down-vote is the seed of the maintenance loop.

### Relationship to ADR 0060 (complementary, not overlapping)

- **ADR 0060 = derived context.** Facts the *models cannot lie about* — call graph, decisions,
  ownership `nano:meta`. Regenerated; drift-checked.
- **ADR 0061 = learned memory.** Facts *nobody wrote down* — why a PR auto-merged wrongly, which
  capability has no worker, a cross-repo release invariant discovered the hard way.

Derived vs learned; both retrieve through the **same budgeted, placement-aware boundary** (part 4).

### Scope / non-goals

- **No blockchain, no decentralized/verifiable substrate now.** Adopt Walrus Memory's *interface*
  (typed, permissioned, semantically indexed, portable), back it with owned infra.
- **Engine and Falcon untouched** (consistent with ADR 0056/0059).
- **`AGENTS.md` stays.** The invariants tier *is* its ecosystem-scoped, cross-repo successor, not a
  replacement of the per-repo file for repo-local guidance.

## Consequences

- **Positive — cross-repo knowledge stops being siloed/duplicated.** An ecosystem invariant is
  written once, scoped `ecosystem`, retrievable from any repo's session.
- **Positive — the layer is net-positive by construction.** The write-policy gate + maintenance
  (confidence decay, dedup, TTL, episodic→summary compression) are what keep signal-to-noise from
  collapsing — the difference between institutional memory and institutional clutter.
- **Positive — one retrieval boundary.** ADR 0060's open question #1 is answered here and reused by
  every agent prompt, with placement rules that actually get the memory *used*.
- **Positive — a clean seam to the future.** When cross-org sharing arrives (ADR 0059), swapping the
  owned store for a Walrus-style verifiable one is a substrate change behind a stable interface.
- **Cost — real memory-engineering machinery.** Schema, write gate, decay/dedup/TTL jobs, scoped
  permissions. This is a subsystem, not a doc; it earns its keep only if the maintenance loop runs.
- **Cost — trust calibration.** `importance`/`confidence`/`trust_level` thresholds need tuning; too
  strict starves retrieval, too loose invites poisoning.

## Open questions

1. **Store choice for the learned tier.** Reuse Copilot Memory (has provenance + votes, lacks
   TTL/decay/ecosystem scope — extend it?), or a purpose-built store on the L2 plane? Leaning:
   extend Copilot Memory's model with `scope=ecosystem` + maintenance rather than a parallel store.
2. **Who runs maintenance.** A scheduled nwf process (decay/dedup/TTL/compaction as a BPMN model —
   dogfooding the engine), or a library job? Leaning: a nwf process, so the maintenance loop is
   itself observable and escalatable.
3. **Poisoning defense depth.** Is a `trust_level` gate + sanitization enough, or do learned-tier
   writes need a review quorum before promotion to the invariants tier?
4. **Placement mechanics.** How does `retrieve_for_step` know the "active reasoning region" for a
   given caller (nwf worker vs. interactive session vs. system-brief injection)? Needs a per-caller
   placement contract.
5. **Cross-org / verifiability trigger (ADR 0059).** What concrete event flips us from owned store to
   a Walrus-style verifiable substrate — the first externally-authored Urban app that publishes
   shared memory? Define the threshold before we need it.
