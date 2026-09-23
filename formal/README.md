# Formal verification

Machine-checked models of nanobpm's semantics. This is epic #1224: TLA+ for
concurrency, distribution and state-machine invariants, and Lean 4 for proofs
about pure semantics (both land slice by slice).

```
formal/
└── tla/
    ├── TokenFlow.tla         # single-instance token flow: gateways + join bookkeeping
    ├── MC*.tla               # concrete process graphs to model-check
    └── check.sh              # generates each TLC config, runs TLC, compares with EXPECTED
```

## Running

You need Java 11+ (CI uses Temurin 21). Nothing else to install: on first run,
`check.sh` fetches the pinned `tla2tools.jar` into
`~/.cache/nanobpm-formal/` and verifies its SHA-256.

```bash
formal/tla/check.sh                      # every model (a few seconds)
formal/tla/check.sh MCChainedInclusive   # one model
```

CI runs the `formal (tlc)` job whenever `formal/**` changes.

To read a counterexample trace, keep the logs:

```bash
FORMAL_LOG_DIR=/tmp/tlc formal/tla/check.sh MCParallelJoinMultiArrival
# /tmp/tlc/MCParallelJoinMultiArrival.{cfg,log}
```

A model that does not match its expectation prints its full TLC log anyway.

## `TokenFlow.tla`

This models the engine-core drain loop (`engine-core/src/engine/mod.rs`) for a
single process instance in a single scope:

| Spec | Engine |
|---|---|
| `pending` (a bag of flows being taken) | the `Step::Activate` queue, drained to empty per command |
| `waiting` | tokens parked on wait-state tasks (job completion is `CompleteTask`) |
| `ArriveParallelJoin` | `arrive_at_parallel_join` (`ParallelJoinOpened` / `TokenArrived` / `Fired`) |
| `joinTokens` | `join_flow_arrivals` (per incoming flow, parallel and inclusive) |
| `ArriveInclusiveJoin`, `FireInclusiveJoin` | `arrive_at_inclusive_join`, `fire_ready_inclusive_joins` |
| `Reaching` | `elements_reaching` |
| `CompleteInstance` | `complete_finished_instances` |

The model deliberately covers more behaviours than the engine can produce. It
drains the queue in any order, fires any ready inclusive join, and picks gateway
branches freely instead of evaluating conditions. Each *safety* property (the
invariants and deadlock freedom) checked against this superset therefore also
holds for the engine's deterministic choices. `Termination` does not transfer
that way: it assumes fair routing choices, and real condition data can keep
choosing a loop branch forever. Read it as "the graph allows every instance to
finish", not "the engine always finishes".

The properties checked:

- `JoinBookkeepingCoherent`: a join is open iff it holds tokens, tokens sit
  only on its own incoming flows, and a parallel join never rests with every
  incoming flow fed. This is the class behind the
  missing-`ParallelJoinReset` bugs.
- `ParallelJoinWaitsForEveryFlow`: a parallel join fires only after every
  incoming flow has delivered, matching BPMN and Zeebe.
- `NoStuckInstance`: a settled instance with no runnable task has no open join.
- TLC's deadlock check, which reports any state where nothing can happen and
  the instance has not completed. `violates:` models run without it (see
  below); in this spec such a state is always a `NoStuckInstance` and
  `Termination` violation, so nothing is lost.
- `JoinFiresAtMostOnce`: a join fires at most once per instance. This is
  guarded by `Acyclic`, which the spec derives from the graph, since a rework
  loop legitimately re-fires a join. An acyclic graph that piles several
  tokens onto a join's inputs (not 1-safe, a BPMN lack-of-synchronization)
  also violates it, and that is intended: it is a finding about the model.
- `Termination`: every instance eventually completes, under the fairness
  in `TokenFlow.tla`'s `Fairness`. Every drain and inclusive-join-fire action
  is strongly fair *per routing choice*, so a gateway reached infinitely often
  eventually takes each branch. That is the "fair data" assumption of
  workflow-net soundness. Task completion is strongly fair too, because a task
  can only complete in a settled state, which is intermittent. Instance
  completion is weakly fair. A loop with an exit therefore terminates, and a
  loop without one is reported as a livelock.

The following are out of scope for now: sub-process scopes, boundary and
intermediate events, incidents, listeners and multi-instance. Each one is a
future extension of this spec.

## Expected outcomes and known defects

The `EXPECTED` table in `check.sh` records the expected outcome for each model:
`pass`, or `violates:<P1>,<P2>,...`, the **exact** set of invariants and
properties TLC must report as violated. Every property not listed is thereby
proven to hold for that model. A `violates:` model runs with TLC's `-continue
-deadlock`, so TLC explores the whole state space whatever order it reaches
violations in (a stuck state still shows up, as `NoStuckInstance` and
`Termination`). A `violates:` entry records one of two things:

- **A known engine defect** the model reproduces, linked to its issue. The spec
  models the engine **as it is**, defects included. A PR that fixes the defect
  in Rust must also update the spec to model the fixed behaviour; the model's
  verdict then changes, and `check.sh` fails until the entry is updated. The
  fixed behaviour therefore stays guarded.
- **A deliberately unsound graph**, where the violation is the correct verdict.
  `MCParallelJoinMultiArrival` puts two tokens on one incoming flow of a
  parallel join and one on the other. The join fires once, and, as in Zeebe,
  the surplus token waits forever (`NoStuckInstance,Termination`). It no
  longer fires early: before #1233 the engine counted arrivals rather than
  distinct incoming flows, and the entry also listed
  `ParallelJoinWaitsForEveryFlow`. `MCParallelJoinSurplus` takes every
  incoming flow twice; the join keeps the surplus between firings (Zeebe's
  "Tetris" principle), fires twice (`JoinFiresAtMostOnce`), and the instance
  completes. `MCInclusiveJoinSurplus` is the inclusive-join version: the join
  fires once nothing can reach it, consuming one token per flow, and fires
  again on the surplus (`JoinFiresAtMostOnce`). Before #1237 the first firing
  discarded the surplus, and the model passed.

TLC cannot see the Rust code, so nothing yet forces the spec update when the
engine changes. Until trace validation (#1226) links the two, that step is a
review responsibility.

`check.sh` also fails if a `MC*.tla` has no table entry, if an entry has no
model, or if TLC prints a warning.

## Adding a model

1. Add `MCFoo.tla` (`EXTENDS TokenFlow`) and define `MCNodes`, `MCKind`,
   `MCStart` and `MCEdges` (a record from flow id to `<<source, target>>`),
   plus the derived `MCFlows`, `MCSrc` and `MCTgt` (copy these from an
   existing model). Flows have their own ids, as in the engine, so two
   distinct flows may share endpoints (`MCParallelDuplicateFlows`).
2. Add a row to `EXPECTED` in `check.sh` with its expected outcome. There are
   no hand-written `.cfg` files. `check.sh` generates the same config, with
   every property, for every model, so no model can skip a property.

If the model finds a violation, confirm it against the real engine with a red
Rust test before recording it. The model may simply be wrong.

## Keeping the spec honest

The spec is hand-written, so it can drift from the Rust code. Tying the two
together through trace validation is tracked in #1226: replay TLC-generated
behaviours against `Engine::apply_command`. Until that lands, any change to
the drain loop, the join functions or `elements_reaching` should update
`TokenFlow.tla` in the same PR.
