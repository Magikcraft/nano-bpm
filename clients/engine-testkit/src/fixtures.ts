// A deterministic in-memory {@link EngineReadModel} for unit-testing the
// `assertThat*` matchers without booting any engine. It honours the same query
// filters a real adapter (urban-testkit's `WasmEngineClient`) applies —
// `processInstanceKey` / `state` / `assignee` / `candidateGroup` — so a matcher
// selects a task here exactly as it would against the wasm engine.
//
// Test-only: excluded from the published build (see tsconfig.build.json).

import type { EngineReadModel, UserTaskQuery, UserTaskRow, UserTaskState } from "./port.ts";

/** A fake user task: the identity fields the read model projects onto a row plus
 *  the filterable attributes (`processInstanceKey` / `state` / `assignee` /
 *  `candidateGroups`) the adapter honours through its query, never on the row. */
export interface FakeUserTask {
  readonly userTaskKey: string;
  readonly elementId?: string;
  readonly processInstanceKey?: string;
  readonly state: UserTaskState;
  readonly assignee?: string;
  readonly candidateGroups?: readonly string[];
}

/** Build an {@link EngineReadModel} that serves a fixed snapshot and user-task set. */
export function fakeEngine(opts: {
  readonly snapshot?: Record<string, unknown>;
  readonly userTasks?: readonly FakeUserTask[];
}): EngineReadModel {
  const snapshot = opts.snapshot ?? {};
  const tasks = opts.userTasks ?? [];
  const search = (q: UserTaskQuery): UserTaskRow[] =>
    tasks
      .filter((t) => q.processInstanceKey === undefined || t.processInstanceKey === q.processInstanceKey)
      .filter((t) => q.state === undefined || t.state === q.state)
      .filter((t) => q.assignee === undefined || t.assignee === q.assignee)
      .filter((t) => q.candidateGroup === undefined || (t.candidateGroups ?? []).includes(q.candidateGroup))
      .map((t) => ({ userTaskKey: t.userTaskKey, elementId: t.elementId }));
  return {
    snapshot: () => snapshot,
    searchUserTasks: (q) => Promise.resolve(search(q)),
    openUserTasks: (q) => Promise.resolve(search({ ...q, state: "CREATED" })),
  };
}
