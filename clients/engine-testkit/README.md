# @nanobpm/engine-testkit

The engine-agnostic **`assertThat*` fluent assertion DSL** for the Nano BPMN
engine read model.

```ts
import { assertThatInstance, assertThatUserTask, byProcessId } from "@nanobpm/engine-testkit";

assertThatInstance(engine, byProcessId("loan-origination-agent"))
  .isActive()
  .hasCompletedElements("AssessApplication")
  .hasVariables({ decision: "approved" })
  .hasNoIncident();

await assertThatUserTask(engine, { instance: byProcessId("loan-origination-agent"), elementId: "SeniorOfficerReview" })
  .isCreated();
```

## Why this package

The DSL was originally introduced in
[`@nanobpm/urban-testkit`](https://github.com/nanobpm/nano-ide/tree/main/packages/urban-testkit)
(issue nanobpm/nano-ide#295), where it was only installable as a devDependency of
**Urban apps**. But its matchers assert over
[`@nanobpm/engine-wasm`](https://github.com/nanobpm/nano-bpm/tree/main/engine-wasm)'s derived **read model** — the single
source of truth *every* Nano consumer already shares (Urban, bojtos-kit,
web-demo-framework). This package lifts that engine-facing DSL out so it is
reusable beyond Urban apps (issue Magikcraft/nano-bpm#894).

It has **zero runtime dependencies**: every matcher is a pure function of the
{@link EngineReadModel} port.

## The port

Matchers never depend on any particular engine, app framework, or transport —
only on a small structural read surface, the `EngineReadModel`:

```ts
interface EngineReadModel {
  snapshot(): Record<string, unknown>;                              // instances, activeElements, elementStats, incidents, variables
  searchUserTasks(query: UserTaskQuery): Promise<readonly UserTaskRow[]>;
  openUserTasks(query: UserTaskQuery): Promise<readonly UserTaskRow[]>; // searchUserTasks pinned to CREATED
}
```

Any adapter that surfaces a process snapshot and answers user-task read queries
in these shapes satisfies it — urban-testkit's `WasmEngineClient`, a bojtos-kit
session, or any thin wrapper over `@nanobpm/engine-wasm`. The port is the
**adapter-output** contract, not the raw engine wire format.

## API

| Export | Purpose |
| --- | --- |
| `assertThatInstance(engine, keyOrSelector?)` | State (`isActive`/`hasCompleted`/`isTerminated`), `hasActiveElement(s)`, `hasCompletedElements`, `hasVariable(s)`/`hasNoVariable`, `hasIncident`/`hasNoIncident`. |
| `assertThatUserTask(engine, selector)` | `isCreated`, `isCompleted`, `hasAssignee`, `hasCandidateGroup` (async — `await` each). |
| `byKey(key)` / `byProcessId(id)` | Instance selectors; omit the selector to default to the single ACTIVE instance. |
| `wasmStateToProcessInstanceState(raw)` | Map the snapshot's mixed-case `state` string onto `ACTIVE`/`COMPLETED`/`TERMINATED`. |
| `formatValue` / `deepEqual` / `deepSubset` / `failAssertion` | The deterministic failure-message helpers the matchers share. |

Every matcher is deterministic — it reads the snapshot / read model synchronously
and never touches a wall-clock (`Date.now`, `setTimeout`) or entropy source
(`Math.random`) — and throws an intent-revealing `node:assert` `AssertionError`
that names the actual state on failure.

## Scripts

```bash
npm run typecheck   # tsc --noEmit
npm run build       # tsc -> dist/
npm test            # node --test (strip-types), src/**/*.test.ts
```
