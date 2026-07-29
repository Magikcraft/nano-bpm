# Spike: code-first durable execution on nanobpmn (crash-resume proof)

**Status: throwaway spike.** De-risks one claim behind the "code-first durable
orchestration / Camunda Nano" direction before we productize it. Not shipped, not
wired into CI.

## What it proves

The crown-jewel durable-execution property:

> An in-flight workflow survives the **engine being killed mid-execution** and
> resumes from its durable journal **without re-running already-completed steps**.

This is the thing that separates "durable execution" (Temporal-style) from a plain
job queue. If it holds, a developer can write a workflow, have the machine crash
(power cut on the RPi, `kill -9`, OOM), and on restart the workflow continues —
completed side effects (opened PR, sent email, spun up an env) are **not** repeated.

## How

`run.mjs` is a self-contained driver. It uses its **own** dedicated server instance
(temp data dir + a free port), so it never touches any nanobpmn server you already
have running:

1. start the engine, deploy `durable-abc.bpmn` (`A → B → C` service tasks), create one instance
2. start `worker.mjs`; it runs A, then B, then **holds** after B commits
3. `SIGKILL` the engine — a hard crash between B's commit and C's activation
4. restart the engine against the **same data dir** → journal replay
5. the worker reconnects on its own and runs C **exactly once**; the instance ends
6. assert the side-effect ledger has **exactly one** entry per activity
   (a lost/replayed-from-scratch engine would re-run A and B → duplicates → FAIL)

The "side effect" is an append to `.run/ledger.log` — standing in for a real
external effect (open PR, send email). Duplicate lines = a durability violation.

## Run

```bash
# needs a built gateway binary; from the repo root:
make debug          # or point at an existing build:
SERVER_BIN=/path/to/nanobpm-gateway-rest-server \
  node spikes/durable-workflow/run.mjs
```

Expected tail:

```
side-effect ledger counts: {"act-a":1,"act-b":1,"act-c":1}
PASS ✅  Engine crashed after B and resumed from its durable journal ...
```

Artifacts (`.run/`: data dir, ledger, progress, `server-1.log`, `server-2.log`) are
gitignored; the dir is wiped at the start of every run.

## Result (2026-07-29)

**PASS** against the debug gateway (`0.0.10-4-g2a536dc`): engine `SIGKILL`'d after B,
restarted cold, `A`/`B` not replayed, `C` ran once, `{act-a:1, act-b:1, act-c:1}`.
Repeatable across runs.

**Negative control** (`NEGATIVE_CONTROL=1 node run.mjs`) wipes the journal on
restart to simulate a non-durable engine — the workflow then cannot resume (the run
times out waiting for C and does **not** print PASS). This confirms the harness
genuinely detects a durability violation; the PASS above is not a tautology.

## Honest scope / what this does NOT prove

- **Single-node, single-machine** durability only (the v1 target). No cluster/raft
  failover story here.
- The **at-least-once boundary is real**: if the engine dies *between* a side effect
  and its job completion (not the scenario tested), that job is redelivered on
  restart and the side effect repeats. Activities must be **idempotent** — the worker
  logs this case. Documenting/handling it is productization work.
- This uses plain job workers over REST as the stand-in for a future
  `@nanobpm/workflow` SDK. It does **not** yet prove the *authoring ergonomics*
  (write-a-function, `ctx.run`/`ctx.signal`) or the ad-hoc/agent-loop mapping — those
  are the next increment. The durability they'd ride on is what's proven here.

## Increment 2 — the code-first authoring façade (2026-07-29)

The durability above is engine-side; increment 2 proves the *ergonomics* on top of it.
See ADR 0044 (`docs/adr/0044-code-first-durable-orchestration.md`).

`sdk.mjs` is a minimal façade. A developer declares a workflow as code:

```js
const prReview = defineWorkflow("pr-review", (w) => {
  w.run("fetchDiff",  async (job) => ({ diff: await gh.diff(job.variables.prId) }));
  w.run("autoReview", async (job) => ({ findings: await llm.review(job.variables.diff) }));
  w.signal("humanApproval", { correlationKey: "prId" });   // durable wait
  w.run("merge",      async (job) => ({ merged: await gh.merge(job.variables.prId) }));
});
```

and the façade **derives** everything the engine needs — no diagram, no task-type
wiring, no correlation plumbing:

- an executable **BPMN model** (`toBpmn`): `start → steps → end`; a `serviceTask` per
  `run`; an `intermediateCatchEvent` + `<bpmn:message>` + `zeebe:subscription` per `signal`;
- the **job types** (`pr-review:fetchDiff`, …) and the **message name/correlation**;
- a single **generic worker** (`runWorker`) that dispatches activated jobs to the handlers.

The instance is an ordinary nanobpmn instance, so it **inherits the crash-resume
durability proven in increment 1**.

Files: `sdk.mjs` (façade), `pr-review.workflow.mjs` (a code-authored workflow using
`run` + `signal`), `demo.mjs` (end-to-end run+signal), `resume-code-first.mjs`
(crash-resume parity proof for a code-authored workflow).

```bash
node spikes/durable-workflow/demo.mjs             # derive + deploy + run + signal + merge
node spikes/durable-workflow/resume-code-first.mjs  # SIGKILL/restart, exactly-once proof
NEGATIVE_CONTROL=1 node spikes/durable-workflow/resume-code-first.mjs   # must NOT pass
```

**Results (2026-07-29, debug gateway):**

- `resume-code-first.mjs`: **PASS** — a fully code-authored workflow (model + job
  types + worker all derived) survived engine `SIGKILL` after B; `{stepA:1, stepB:1,
  stepC:1}`. Negative control correctly fails.
- `demo.mjs`: ran `fetchDiff → autoReview → [durable wait] → humanApproval → merge` to
  `COMPLETED`, resuming the parked instance via a correlated approval message — no BPMN,
  wiring, or correlation authored by hand.

Still deliberately **not** proven here: imperative `ctx.run`/`await` replay (Strategy B),
the wasm determinism sandbox (Strategy C), versioning, and multi-node — see ADR 0044.
