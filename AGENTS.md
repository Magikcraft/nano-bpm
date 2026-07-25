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

## PRs Auto-Merge

This repository auto-merges pull requests once their checks pass (via the Mergify merge queue). Treat opening a PR as committing to `main`:

- A branch must be **complete and correct before you open the PR** — do not plan to "add a follow-up commit" to an open PR, as it may already be merged and closed by the time you push.
- **The only legitimate reason to push another commit to an open PR is to unblock failing CI.** Any new feature work, follow-up, or scope addition belongs in a *fresh* PR off the latest `main`, never appended to an open PR.
- After pushing to a branch with an open PR, **verify the PR actually picked up your commit** (`gh pr view <n> --json headRefOid`); if the PR has already merged, open a new PR off the latest `main` for the additional change.
- Never leave a branch in a knowingly-broken intermediate state expecting a later fix to land in the same PR.
