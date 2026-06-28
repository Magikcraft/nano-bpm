# ADR 0006 — Subagent delegation mode (context-protecting research handoff)

Status: **Proposed.**
Date: 2026-07-04.
Relates to: `processos/src/investigate.rs`, `processos/src/personas.rs`,
`processos/src/main.rs`, `processos/src/cockpit.html`; prior art surveyed in
`~/workspace/safe-agentic-workflow` (SAW). Builds on ADR-aligned Pair AI / Loop
Monitor second-LLM roles.

## Context

ProcessOS drives a primary LLM **investigator** through a tool-using agent loop
(`investigate.rs` + `agent.rs`). Two optional second-LLM roles already exist:
the **Pair AI** reviewer (runs after the primary, persona-kind `Pair`) and the
**Loop Monitor**. Both are *observers* — they cannot do work on the primary's
behalf.

The dominant cost of long investigations is the primary's **context window**.
Exploratory sub-tasks — "scan all 40 job types and tell me which ever error",
"cross-check the recorded boundary timers", "discover the flow shape" — are
high-token, low-yield: they spend thousands of tokens of tool output that, once
summarized, are irrelevant to the primary's remaining reasoning. Today that
output lands in the primary's transcript and is resent (clipped) on every turn.

SAW's pattern is *delegation with isolation*: a primary delegates a
self-contained task to a subagent that runs with its own context and a
read-only tool allowlist, returning only a compact digest. The primary spends
~one tool call's worth of context regardless of how much the subagent explored.

## Decision

Add a **Subagent** role: the primary may call a `delegate` tool that spawns a
second LLM in an **isolated context** with **read-only** tools, returning only a
length-capped digest.

1. **Persona kind.** New `PersonaKind::Subagent` + builtin "Researcher"
   (`subagent-researcher`). It only appears in the subagent picker, never as a
   primary chat persona. `resolve_subagent()` falls back to
   `FALLBACK_SUBAGENT_SYSTEM`.
2. **Isolation.** Each `delegate` call builds the subagent its **own** dataset
   from a cloned `TraceSource` (independent DuckDB connection) and runs
   `run_agent` on a dedicated `std::thread` with its own `current_thread`
   runtime. The subagent has no access to the primary's transcript, no edit
   tools, no Python. `ResearchTools` exposes only `query_traces` /
   `discover_flow` / `read_model`.
3. **Context protection.** The subagent appears in the primary's transcript as a
   single `delegate` tool step; its digest is truncated to `digest_cap`. The
   primary spends one tool call regardless of subagent effort.
4. **Configurable.** Toggle + its own profile (often a small/fast model) +
   persona; advanced: `max_rounds`, `digest_cap`. Each surfaced in cockpit with
   inline guidance.

## Why a thread, not `block_in_place`

`ToolBox::call` is sync; the subagent needs async, and DuckDB connections are
not `Send`. Chat endpoints run on a `current_thread` runtime, so
`block_in_place` is unavailable. Spawning a thread with a fresh runtime + fresh
`Analysis` gives true isolation cleanly.

## Consequences

- Primary context survives long exploratory phases; cost scales with digests.
- Subagent is read-only by construction — cannot corrupt the model or escape.
- One extra profile to run; capped at MAX_SIDECARS=2 if co-located.
- Future: per-tool allowlists, nested delegation, mandatory-isolation review.
