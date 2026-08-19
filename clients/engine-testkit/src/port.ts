// The minimal engine read-model surface the `assertThat*` DSL reads.
//
// The matchers in this package are pure functions of a **read model**, never of
// any particular engine, app framework, or transport. This port is that read
// model, expressed as the small structural interface the matchers actually call:
//
//   • `snapshot()`        — the engine's parsed process snapshot (instances,
//                           activeElements, elementStats, incidents, variables).
//     Read by `assertThatInstance` and the instance selectors.
//   • `searchUserTasks()` — the user-task read channel (Camunda-parity filter).
//   • `openUserTasks()`   — `searchUserTasks` pinned to the CREATED (open) set.
//     Both read by `assertThatUserTask`.
//
// It is deliberately the **adapter-output** contract, not the raw engine wire
// format: an adapter (urban-testkit's `WasmEngineClient`, a bojtos-kit session,
// or any thin wrapper over `@nanobpm/engine-wasm`'s read model) shapes the
// engine's responses into these rows and then satisfies this port structurally.
// Keeping the DSL behind the port is what lets it be reused beyond Urban apps
// (issue Magikcraft/nano-bpm#894) with zero runtime dependencies.

/** A user task's lifecycle state, as the engine's user-task read model reports it.
 *  The full Camunda-parity set; the DSL asserts only over `CREATED`/`COMPLETED`. */
export type UserTaskState = "CREATED" | "COMPLETED" | "CANCELED" | "FAILED";

/** The user-task read-model query the DSL issues. Every field is an optional
 *  narrowing filter; an omitted field does not constrain the result. Mirrors the
 *  engine's user-task search filter.
 *
 *  Adapters **must honour every field defined here** — `assertThatUserTask`'s
 *  `hasAssignee` / `hasCandidateGroup` narrow by re-issuing the query with the
 *  `assignee` / `candidateGroup` filter set and treat a non-empty result as
 *  proof, so an adapter that silently ignores a filter would make those
 *  assertions pass when they should fail (false positives). Only *unknown future*
 *  fields an adapter has not yet learned about may be ignored (a superset of this
 *  shape is harmless); the fields declared below may not. */
export interface UserTaskQuery {
  /** Restrict to tasks owned by this process instance. */
  readonly processInstanceKey?: string;
  /** Restrict to tasks in this lifecycle state. */
  readonly state?: UserTaskState;
  /** Restrict to tasks assigned to this user. */
  readonly assignee?: string;
  /** Restrict to tasks offering this candidate group. */
  readonly candidateGroup?: string;
}

/** The identity subset of a user-task read-model row the DSL reasons over. Rows
 *  carry more (`variables` / `formKey` / …); the matchers read only identity. */
export interface UserTaskRow {
  /** The task's stable key. */
  readonly userTaskKey: string;
  /** The BPMN element id the task instantiates (absent on malformed rows). */
  readonly elementId?: string;
}

/** The engine read model the `assertThat*` DSL asserts over. Any adapter that can
 *  surface a process snapshot and answer user-task read queries in these shapes
 *  satisfies it — no engine, framework, or transport is baked in. */
export interface EngineReadModel {
  /** The engine's parsed process snapshot (`instances`, `activeElements`,
   *  `elementStats`, `incidents`, per-instance `variables`). */
  snapshot(): Record<string, unknown>;
  /** Search the user-task read model, narrowed by `query`. */
  searchUserTasks(query: UserTaskQuery): Promise<readonly UserTaskRow[]>;
  /** The open (CREATED) user tasks matching `query` — `searchUserTasks` pinned to
   *  state CREATED. Exposed separately because an adapter may back the open set
   *  with a dedicated read path. */
  openUserTasks(query: UserTaskQuery): Promise<readonly UserTaskRow[]>;
}
