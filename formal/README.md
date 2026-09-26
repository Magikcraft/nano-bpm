# Formal verification

Machine-checked models of nanobpm's semantics. This is epic #1224: TLA+ for
concurrency, distribution and state-machine invariants, and Lean 4 for proofs
about pure semantics (both land slice by slice).

```
formal/
├── tla/
│   ├── TokenFlow.tla         # single-instance token flow: gateways + join bookkeeping
│   ├── MC*.tla               # concrete process graphs to model-check
│   └── check.sh              # generates each TLC config, runs TLC, compares with EXPECTED
├── lean/                     # Lean 4 reference semantics + differential fuzz (see below)
│   ├── lean-toolchain        # pinned Lean version (elan reads this)
│   ├── lakefile.lean         # Lake project: one lib target per slice + the feelfuzz exe
│   ├── feel-diff.sh          # build Lean, generate a corpus, check Rust against it
│   ├── Feel/                 # FEEL reference semantics + generator (this slice, #1230)
│   ├── Replay/               # replay determinism (#1231) — adds files here only
│   └── Roundtrip/            # processos IR ⇄ BPMN round-trip (#1232) — adds files here only
└── parity/                   # Zeebe parity coverage matrix (see below)
    ├── zeebe-pin.json        # the Zeebe commit the matrix is derived from
    ├── fetch-zeebe.sh        # sparse-fetches the pinned sources
    ├── extract.mjs           # Zeebe sources -> zeebe-surface.json (derived, never hand-edited)
    ├── coverage.json         # maps every cell to Nano evidence, a gap, or out-of-scope
    ├── gaps.json             # the gap ratchet baseline: the only cells a gap rule may claim
    └── check.mjs             # the CI guard + coverage report
```

## Running

You need Java 11+ (CI uses Temurin 21). Nothing else to install: on first run,
`check.sh` fetches the pinned `tla2tools.jar` into
`~/.cache/nanobpm-formal/` and verifies its SHA-256.

```bash
formal/tla/check.sh                      # every model (a few seconds)
formal/tla/check.sh MCChainedInclusive   # one model
```

CI runs the `formal (tlc)` job whenever `formal/**`, `engine-core/src/**`,
`engine-core/tests/**` or `engine-core/examples/**` changes. The job runs the
TLA+ model check, the parity matrix steps described below, and the Lean build +
FEEL differential fuzz described under "Lean 4 layer". The job is skip-tolerant:
it is required only when it runs, so a PR that touches none of those paths skips
it cleanly.

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
| `TakeFlows` | `take_flow`: a flow into a join is counted when it is taken (`ParallelJoinTokenArrived`), as in Zeebe |
| `joinTokens` | `join_flow_arrivals` (per incoming flow, parallel and inclusive) |
| `ArriveJoin` | `activate_join`, the guard for both join kinds (`ParallelJoinOpened` / `Fired`) |
| `HasActivePathTo`, `LiveSources`, `PathReaches` | `has_active_path_to`, `path_reaches_join` (Zeebe's `hasActivePathToTheGateway`) |
| `CompleteInstance` | `complete_finished_instances` |

The model deliberately covers more behaviours than the engine can produce. It
drains the queue in any order and picks gateway branches freely instead of evaluating conditions. Each *safety* property (the
invariants and deadlock freedom) checked against this superset therefore also
holds for the engine's deterministic choices. `Termination` does not transfer
that way: it assumes fair routing choices, and real condition data can keep
choosing a loop branch forever. Read it as "the graph allows every instance to
finish", not "the engine always finishes".

The properties checked:

- `JoinBookkeepingCoherent`: an open join holds tokens, a join holding tokens
  is open unless an activation into it is still queued, tokens sit only on its
  own incoming flows, and a parallel join never has every incoming flow fed
  without an activation on its way to fire it. This is the class behind the
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
  in `TokenFlow.tla`'s `Fairness`. Every drain action is strongly fair *per
  routing choice*, so a gateway reached infinitely often
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
  completes. `MCInclusiveJoinSurplus` is the inclusive-join version. As in
  Zeebe, an inclusive join is only evaluated when a token arrives (#1241): when
  the surplus arrives last it fires the join again (`JoinFiresAtMostOnce`), but
  when it arrives first it waits forever (`NoStuckInstance,Termination`).
  `MCInclusiveDivergentPath` is the same verdict for a sound-looking graph: the
  competing branch leaves through an exclusive gateway, so the join is never
  re-evaluated and waits, as in Zeebe. Before #1241 a quiescence sweep
  re-evaluated waiting joins, and both models completed.

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
the drain loop, the join functions or `path_reaches_join` should update
`TokenFlow.tla` in the same PR.

## Lean 4 layer

`formal/lean/` is a single [Lake](https://github.com/leanprover/lean4) project
holding the Lean 4 reference semantics and proofs. It is the wave-0 seam for the
Lean slices of epic #1224: the shared `lakefile.lean` and `lean-toolchain` are
authored once (#1230) and **pre-declare one library target per slice**, so each
sibling only ADDS files inside its own subdirectory and never edits the shared
lakefile or a shared barrel file.

| Lib target | Directory | Slice |
|---|---|---|
| `Feel` | `formal/lean/Feel/` | FEEL reference semantics + differential fuzz (#1230) |
| `Replay` | `formal/lean/Replay/` | replay determinism (#1231) |
| `Roundtrip` | `formal/lean/Roundtrip/` | processos IR ⇄ BPMN round-trip (#1232) |

`lakefile.lean` also declares the `feelfuzz` executable (`Feel.Fuzz`). Every
target is a `@[default_target]`, so `lake build` builds them all. The package
sets `warningAsError := true`, so any Lean warning fails the build — keep new
code warning-clean.

**Per-slice convention.** Add your `.lean` files under your own directory
(`Feel/`, `Replay/`, `Roundtrip/`); the matching lib target globs its
submodules automatically. Do not edit `lakefile.lean`, `lean-toolchain`, or
another slice's directory.

### Running

`elan` manages the Lean toolchain and installs the pinned version from
`lean-toolchain` on first use:

```bash
curl -fsSL https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh | sh -s -- -y --default-toolchain none
export PATH="$HOME/.elan/bin:$PATH"
cd formal/lean && lake build          # build every Lean target
```

### FEEL differential fuzz (#1230)

The FEEL slice (`formal/lean/Feel/`) is a total reference evaluator for the FEEL
subset that `engine-core/src/feel` implements. Per the epic's anti-drift rule,
the reference is tied to the Rust implementation by a **differential fuzz**:

- `Feel/Semantics.lean` is the reference evaluator, faithful to
  `engine-core/src/feel/eval.rs` (three-valued and/or with short-circuit,
  `=`/`!=` that always yield a boolean — cross-type is `false`, never null —
  ordering (`<`/`<=`/`>`/`>=`) that is a type *error* on incomparable operands
  (including `null`), `if` on a non-bool condition → null, etc.).
- `Feel/Gen.lean` is a type-directed generator that emits FEEL expressions with
  bound contexts, keeping all numeric values exact integers within the
  f64-exact range so number formatting cannot drift.
- The `feelfuzz` exe prints a TSV corpus of `expr⇥ctx⇥reference-outcome`.
- `engine-core/examples/feel_diff.rs` re-evaluates each row with the Rust
  evaluator and asserts the canonical outcome is **byte-identical**. Any
  divergence exits non-zero — there is no tolerated mismatch and no retry.

Run the whole loop (build Lean → generate → check Rust) with the driver:

```bash
formal/lean/feel-diff.sh            # 3000 cases (override: FEEL_FUZZ_CASES or first arg)
formal/lean/feel-diff.sh 20000      # more cases
```

To convince yourself the harness actually bites, perturb one arm of
`Feel/Semantics.lean` (e.g. make `add` compute `a - b`), rerun `feel-diff.sh`,
watch it fail with concrete diverging rows, then revert.

## Zeebe parity coverage matrix

Nano must behave exactly like Camunda 8 wherever Camunda defines behaviour.
The parity suite (#1240) needs a definition of "complete" that nobody writes
by hand. This matrix provides it (#1245). `extract.mjs` reads the Zeebe
sources at the commit pinned in `zeebe-pin.json` and lists every behaviour
Zeebe declares, one **cell** per behaviour, in `zeebe-surface.json`:

| Family | One cell per | Read from |
|---|---|---|
| `element:<Class>` | supported BPMN element | `FlowElementValidator.SUPPORTED_ELEMENT_TYPES` |
| `event:<position>:<definition>` | supported event definition per position | `SUPPORTED_*` lists in the boundary, intermediate-catch and sub-process validators; `*Behavior` classes in the end and intermediate-throw event processors |
| `lifecycle:<BpmnElementType>:<command>` | each lifecycle command (activate, complete, terminate, continue-terminating, complete-execution-listener), plus any `child-*` hook the processor implements | commands from `BpmnStreamProcessor.processEvent`; element types from `BpmnElementProcessors`; hooks from the processor interfaces (and the interfaces they extend), following each processor's `extends` chain |
| `guard:<method>:<message>` | rejection branch of the state-transition guard | `Either.left` in `ProcessInstanceStateTransitionGuard` |
| `incident:<ErrorType>` | incident type | `ErrorType` |
| `intent:<Record>:<INTENT>` | record intent | every `*Intent` enum in `protocol/record/intent` |
| `validation:<Validator>:<message>` | deploy-time rejection message | `addError` calls in the bpmn-model and engine deployment validators |
| `rejection:<Class>:<RejectionType>` | command rejection a processing-layer class produces: a processor, or a validator/helper that builds the rejection a processor writes | `RejectionType.X` uses under `engine/processing` (comparisons excluded), checked against the SBE schema |

Each cell records the source lines it was read from. The extractor fails
loudly when an anchor it relies on moves or changes shape. It also fails when
an anchor appears in a form it does not read (for example
`SUPPORTED_ELEMENT_TYPES.addAll`, or a processor hook missing from
`HOOK_TRANSITIONS`), and when two different messages would collapse into one
cell id. An upstream refactor therefore stops extraction; it never silently
shrinks the matrix.

Known extraction gap: Zeebe declares no list of supported process-level start
event types (`StartEventValidator` only checks their count and form), so those
have no cells yet.

### Mapping cells to evidence

`coverage.json` holds ordered rules. The first rule whose `match` fits a cell
claims it, and `*` in a `match` matches any run of characters. Each rule has
one status:

| Status | Required | Meaning |
|---|---|---|
| `parity` | `evidence`: fixtures in `engine-core/tests/conformance/corpus/` | Nano's verdict is asserted equal to a Zeebe verdict captured in the fixture (`accept` or `reject`; a `diverge` fixture is not parity). Every claimed cell must be listed in one of the fixtures' `<!-- zeebe-cells: … -->` comment |
| `nano-tested` | `evidence`: `path::test_fn` | a Nano `#[test]` (not `#[ignore]`d) exercises the behaviour, but not against a Zeebe oracle. Every claimed cell must be listed in a `// zeebe-cells: …` line among the comments and attributes directly above the test fn |
| `gap` | `issue`, `note` | no evidence yet; the issue closes it. Only cells in the `gaps.json` baseline may be gaps (see below) |
| `out-of-scope` | `issue`, `note` | the cell has no Nano meaning (for example, partition-internal records). Use sparingly |

`check.mjs` fails on any of these:

- an unmapped cell, including new cells from a Zeebe bump
- a rule that claims no cell
- evidence that does not resolve (a renamed test, a missing fixture, a
  fixture or test that does not declare the cell, or one that declares a cell
  not in the surface)
- a malformed rule
- a surface extracted at a different commit than the pin
- a `coverage.json` whose `reviewedAt` is not the pinned commit, so every bump
  is an explicit review of the surface diff
- a gap cell that is not in the `gaps.json` baseline, a baseline entry that is
  no longer a gap (it gained evidence or left the surface), or an unsorted
  baseline

`gaps.json` is a ratchet. It freezes the cells that were gaps when the matrix
landed, so a broad `gap` rule such as `intent:*` cannot absorb a cell that a
Zeebe bump adds: that cell needs evidence or an `out-of-scope` rule. When cells
gain evidence, `node formal/parity/check.mjs --update-gaps` drops them from the
baseline. It only ever removes entries, and CI runs the guard with
`--base-gaps` against the target branch's `gaps.json`, rejecting any id the PR
adds, so the baseline shrinks toward empty.

It prints the per-family counts and, in CI, adds them to the job summary.
Moving cells up the ladder, from `gap` to `nano-tested` to `parity`, is the
work of the parity suite.

```bash
node formal/parity/check.mjs                 # guard + report
node --test formal/parity/*.test.mjs         # extractor and guard tests
```

CI regenerates `zeebe-surface.json` from the pinned sources and fails if the
result differs from the committed file, or if the surface or `gaps.json` is not
tracked.

### Bumping the Zeebe pin

1. Update `ref` and `sha` in `zeebe-pin.json`. Also update `paths` if the
   sources moved.
2. `node formal/parity/extract.mjs "$(formal/parity/fetch-zeebe.sh)"`. The
   extractor refuses to run on a checkout that is not at the pinned commit.
3. If extraction fails, a Zeebe refactor moved an anchor. Update
   `extract.mjs` and its tests.
4. `node formal/parity/check.mjs`. Map every unmapped cell and every new gap
   cell it reports to evidence or `out-of-scope`, and delete any rule it
   reports as dead. Run `--update-gaps` to drop cells the bump removed. Then
   set `reviewedAt` in `coverage.json` to the new commit.
5. Review the `zeebe-surface.json` diff. Removed or renamed cells are Zeebe
   behaviour changes that Nano may need to follow.
