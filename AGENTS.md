## No Such Thing as "Flaky Tests"

Intermittently failing tests must always be root-caused and addressed as a product defect (code) or a production-line defect (test). We do not acknowledge the existence of such a thing as "flaky tests".

## No Test Retries

Tests must pass on the first run. We do not configure test retries anywhere (nextest `retries`, CI re-run-on-fail, etc.) — a retry only masks a real defect (product or test) and lets it reach `main`. If a test only passes on a retry, that is a defect to root-cause, not to paper over.

## Red/Green Discipline

All bug fixes must have a test that reproduces the defect before modifying code. Red/Green—always.

## Fix the Failure Mode, Don't Just Squash the Bug

Whenever we detect an issue, reason broadly about the defect class and write a test guard for the defect class. Prefer securing surfaces — including suggesting an architectural refactor to eliminate the failure mode categorically — over squashing individual bugs.

## Feature Test Coverage

When adding new features, ensure test coverage over the new surface to prevent undetected regressions.

## Derivation Over Duplication: No Drift Surfaces

Identify and eliminate drift surfaces — duplicate sources of truth. Ensure that everything that can be derived is derived from a single source of truth and has a single canonical implementation. Do not introduce duplication.

## Zero Tolerance for Warnings, Errors, and Test Failures

We do not tolerate warnings, errors, or test failures in this project.

There are no pre-existing failures or warnings, and you will not allow any to enter the codebase. Thank you.

## BPMN Models need DI

All BPMN Models need DI for rendering for humans.

## Adding Support for a New BPMN Element

A BPMN element type touches several layers. Because most of these are
compile-time exhaustive matches, the compiler will force some (but **not** all)
of the updates — the non-exhaustive ones (parsers, thin JSON pass-throughs,
regenerated artifacts) are silent and are the usual source of "it parsed but
didn't execute" or drift bugs. Work through **every** surface below. Reference
implementation: the `eventBasedGateway` support (`feat/event-based-gateway`).

**1. `engine-core` — the executable model (always required):**

- `engine-core/src/model.rs`
  - Add a variant to `enum ElementKind` (the engine's real element type), with
    any payload fields, and a doc comment describing its runtime semantics.
  - Add a `ProcessBuilder` constructor method (mirror `exclusive_gateway` /
    `parallel_gateway`) so the element can be built programmatically.
  - Some model transforms match on `ElementKind` (`inline_call_activities`,
    `remap_kind_ids`, …) — only relevant if the element carries embedded ids to
    remap; the compiler flags any exhaustive match.
- `engine-core/src/bpmn.rs` (the XML parser — **not** compiler-checked, easy to
  forget):
  - Add a variant to the parser-local `enum NodeKind`.
  - Add a tag-match arm recognizing the BPMN tag (near `exclusiveGateway` /
    `parallelGateway`). An unrecognized tag is silently dropped, so a flow into
    it fails deploy with a misleading "unknown target element" error at the
    *flow*, not the element.
  - Add the `NodeKind` → `ProcessBuilder` dispatch arm.
  - Update the `## Supported subset` doc comment at the top of the file.
- `engine-core/src/engine/mod.rs` (runtime execution):
  - Pass-through elements (gateways, none events) fall through the `Some(_)` arm
    in `run_activation_body` (activate → immediately `Complete`) and take their
    outgoing flow(s) in `finalize_completion`. Bespoke behaviour goes in
    `activate` / `complete` / `finalize_completion`.
  - `engine-core/src/engine/boundary.rs` only if the element is boundary-like.
- Tests: parser test in `bpmn.rs` (inline `#[cfg(test)]`), execution test in
  `engine/tests.rs`. Update `engine-core/README.md`'s supported-subset list.

**2. `processos` — the reversible IR / structural analysis (compiler-forced +
one parity test):**

- `processos/src/ir_spec.rs`: add a `KindSpec` to `ELEMENT_KIND_SPECS` (the
  canonical supported-element registry), a `sample_instances()` entry, and a
  `variant_witness` arm. The `specs_match_pretty_printer` parity test fails if
  these drift from `ElementKind`.
- `processos/src/model_ir.rs`: `kind_keyword`, `render_kind_attrs`, `build_kind`
  (exhaustive — will not compile until handled).
- `processos/src/bpmn_model.rs`: `kind_label`, `is_gateway`/`is_task` helpers,
  the XML emitter match, and `node_dims` (diagram footprint).
- Regenerate the grammar artifact: `cd processos && cargo run -- emit-gbnf --out
  assets/ir.gbnf` (commit the result — it is a checked-in derived artifact).

**3. `engine-wasm` + `bojtos-kit` — thin pass-throughs, but regenerate the
committed artifacts (silent):**

- `engine-core/src/ffi.rs` and `engine-wasm/src/lib.rs` enumerate no element
  types (JSON snapshots), so no code change — **but** the compiled wasm and its
  generated `.d.ts`/`.js` are committed. Rebuild: `make console-wasm`
  (regenerates `engine-wasm/pkg/`) and `make bojtos` (rebuilds
  `bojtos-kit/dist/` + `bojtos-react/dist/`). `bojtos-kit/src/types.ts`'s
  `Snapshot` is element-type-agnostic and needs no change.

**4. `console` — only if the element needs modeller/palette support:**

- `console/src/components/BpmnModeler.tsx` / `console/src/lib/urbanComponents.ts`
  (element templates). Standard BPMN elements that `bpmn-js` already knows need
  no change; only bespoke `nano:` shapes do.

**Verify end-to-end** by deploying a model using the new element through the
freshly-built wasm (a Node probe: `createBojtosSession({ wasm: bytes })` →
`session.deploy(xml)`), not just the Rust unit tests — that is the surface Play
and the console actually consume.

## Claim Your Task Before You Start

Work here runs in **parallel worktrees across several agents** — an epic routinely
fans a dozen slices out at once. Your worktree is invisible to everyone else, so
the issue (or PR) is the only shared bus, and a claim signal on it is the only
thing preventing two agents from silently building the same slice and colliding
in the same files. Post the signal *before* writing code, not when you open the
PR — by then the duplicate work already happened.

- **Check first.** Before starting any planned task, look for an existing issue
  or PR covering it. If one is **already claimed** — a claim comment, an
  assignee, or an open PR — do **not** start. Stop and flag it to the user with a
  link. Never work a task in parallel with an untracked, unclaimed, or
  already-claimed item.
- **Nothing tracked yet → create it, then claim it.** Open the issue before
  writing any code, so the work is visible at the velocity this repo moves at.
- **Claim it by commenting your worktree name.** The comment is the canonical
  signal, because an agent is not always a repo collaborator and therefore cannot
  always assign or label. Lead with the marker word so claims are greppable
  (`gh issue view <n> --comments | grep Claimed`):

  ```
  Claimed — worktree `guided-journeys-contract`, branch `feat/guided-journeys-contract`.
  ```

  Name the worktree exactly as it appears in `git worktree list` (ours live in
  `~/workspace/nanobpmn-worktrees/<name>`) so a human can find the work in
  progress on disk. Add the PR link to the same thread once you open one, and
  assign yourself **if you have the permissions** — that reinforces the comment,
  it does not replace it. (There is deliberately no `in progress` label to
  maintain: one signal, in one place, cannot drift out of sync with itself.)
- **Release what you drop.** If you abandon or hand off a task, say so in the
  same thread (`Released — worktree <name>, <reason>`). A claim that outlives the
  work is worse than no claim: it deadlocks the slice behind an agent that is
  gone.
- **Reclaiming a stale claim.** A claim is stale when its worktree is absent from
  `git worktree list` **and** its branch has no unmerged commits. Say that in a
  comment, with what you checked, then claim it yourself. Never silently
  double-claim — if the evidence is ambiguous, ask the user rather than risk two
  agents in one file.
- **Cross-repo slices claim in the hub.** Work that lands in `jwulf/nano-ide`,
  `jwulf/c8ctl-plugin-nano` or a demo app is still claimed on its
  `Magikcraft/nano-bpm` issue, and the resulting PRs link back to it — one place
  to look, whatever repo the diff ends up in.

## Merging PRs

This repository does **not** auto-merge pull requests. Opening a PR is *not* the
same as committing to `main` — a PR sits open until a human or agent deliberately
merges it:

- Merge is a manual act. CI runs **once when the PR is opened**; follow-up pushes
  (review-fix commits) deliberately do **not** re-run CI, to keep review cycles
  cheap. So the recommended flow is: **open the PR as a draft** (`gh pr create
  --draft`) — the `opened` run gives early breakage signal — converge Copilot
  review, then **mark it ready at convergence** (`gh pr ready <n>`). Marking ready
  fires a fresh `pull_request` (`ready_for_review`) CI run on the head, and that
  run is what GitHub branch protection counts to allow the merge. Once those head
  checks are green (and review threads are resolved), merge via the UI **Merge**
  button or a **`@mergifyio queue`** comment.
- **Do not use `workflow_dispatch` as the merge finalizer** — its runs do NOT
  satisfy branch-protection required status checks (GitHub only counts the PR's
  own push/pull_request check suite). If a PR wasn't opened as a draft, produce
  the head run instead by **closing and reopening it** (`gh pr close <n> && gh pr
  reopen <n>` → `reopened` event). Symptom of getting this wrong: Mergify enters
  the queue, validates the batch, then dequeues with "N of N required status
  checks are expected". Nothing merges on its own.
- Because a PR stays open until merged, it is **safe to push follow-up commits**
  to an open PR (address review feedback, fix CI, iterate) before you merge it.
- Still keep each PR focused: land unrelated scope in its own PR rather than
  piling it onto an open one.
- Do not merge a branch you know to be in a broken intermediate state; merge only
  when the change is complete and correct.

### Converge Copilot review before merging

Every PR must be driven to **review convergence** before it is merged — use the
`pr-copilot-review-loop` skill (the "review convergence" loop) to do this:

- After opening a PR (and after each round of fixes), **re-request the GitHub
  Copilot review** and wait for its verdict.
- Triage and address each Copilot comment (fix, silently apply nitpicks, or push
  back with evidence on false positives), reply in-thread, then **re-request the
  review again**.
- **Keep looping** until Copilot's review comes back with **no actionable
  comments** — its PR-level summary reports nothing new (Copilot reviews are
  `COMMENTED`, never `APPROVED`, so the summary body is the verdict) — or the
  Copilot review is **exhausted** (it reiterates a point already addressed or
  pushed back on; two rounds of the same substantive point = converged).
- At convergence, **rebase the PR if it is behind `main`, resolve any conflicts
  and review threads**, then produce a fresh head CI run via a `pull_request`
  event — **mark a draft PR ready** (`gh pr ready <n>`) or **close+reopen** it —
  wait for green, and **merge** (UI button or `@mergifyio queue`).
- Stop early and sync with the user only if a comment genuinely **needs their
  input** (a design/product tradeoff you can't decide) — after resolving
  everything else in the round.

