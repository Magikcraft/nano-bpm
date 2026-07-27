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

## Merging PRs

This repository does **not** auto-merge pull requests. Opening a PR is *not* the
same as committing to `main` — a PR sits open until a human or agent deliberately
merges it:

- Merge is a manual act. CI runs **once when the PR is opened**; follow-up pushes
  (review-fix commits) deliberately do **not** re-run CI, to keep review cycles
  cheap. So before merging, run CI **once more on the PR head** to produce the
  required check contexts — either `gh workflow run ci.yml --ref <branch>`
  (`workflow_dispatch`) or by toggling the PR to draft and back to ready
  (`ready_for_review`). Once those head checks are green (and review threads are
  resolved), merge via the UI **Merge** button or a **`@mergifyio queue`** comment.
  Note: Mergify requires the required checks on the PR head as an entry condition,
  so a head with no checks will sit "waiting for queue conditions" — that's why
  the convergence-time head run is required. Nothing merges on its own.
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
  and review threads**, run CI once on the head (`gh workflow run ci.yml --ref
  <branch>`), wait for green, then **merge** (UI button or `@mergifyio queue`).
- Stop early and sync with the user only if a comment genuinely **needs their
  input** (a design/product tradeoff you can't decide) — after resolving
  everything else in the round.

