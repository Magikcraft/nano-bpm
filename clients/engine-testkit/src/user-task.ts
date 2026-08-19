// `assertThatUserTask` — fluent assertions over the engine's user-task read model.
//
// A user task is asserted against the engine's user-task read channel via
// `engine.openUserTasks(query)` / `engine.searchUserTasks({ state, … })`, NOT by
// scraping the primary-state snapshot. The read model honours the lifecycle
// `state` filter itself, and the adapter applies the non-lifecycle selectors
// (`processInstanceKey` / `assignee` / `candidateGroup`) so this DSL can select a
// task exactly the way a production surface does.
//
// SUPPORTED USER-TASK STATE SCOPE (deliberate, bounded design decision):
// {@link UserTaskState} is a 4-member union (`CREATED | COMPLETED | CANCELED |
// FAILED`), but this DSL intentionally exposes state matchers for exactly the TWO
// states an integration test asserts on — CREATED (`isCreated`) and COMPLETED
// (`isCompleted`). CANCELED and FAILED are intentionally OUT OF SCOPE: no
// `isCanceled()` / `isFailed()` matcher is provided.
//
// Every matcher reads asynchronously from the engine read model (a data read,
// never a wall-clock wait): the code path never calls `Date.now`,
// `setTimeout`/`setInterval`, `Math.random`, awaits real time, polls, or sets a
// timeout. Because those reads return Promises, the matcher methods are async;
// chain them by awaiting each in turn.

import type { EngineReadModel, UserTaskRow } from "./port.ts";
import { failAssertion, formatValue } from "./format.ts";
import { type InstanceSelector, resolveInstanceKey } from "./selectors.ts";

/** Selects a user task by its owning instance (a bare process-instance key, `byKey(...)`,
 *  or `byProcessId(...)`) and/or its BPMN element id. Both fields are optional: an omitted
 *  `instance` matches tasks across every instance, and an omitted `elementId` matches every
 *  task of the selected instance(s). A matcher asserts its condition holds for at least one
 *  task the selector resolves to. */
export type UserTaskSelector = {
  readonly instance?: string | InstanceSelector;
  readonly elementId?: string;
};

/** Fluent assertions over a user task's lifecycle state, assignee, and candidate groups.
 *  Each matcher reads the engine's user-task read model and throws an intent-revealing
 *  `AssertionError` (naming the actual matching tasks and their states) on failure. The
 *  methods are async — chain them with `await` — and every one resolves to the same
 *  `UserTaskAssert` so a chain can continue. */
export interface UserTaskAssert {
  /** Assert at least one selected task is in the open/CREATED set. */
  isCreated(): Promise<UserTaskAssert>;
  /** Assert at least one selected task is COMPLETED. */
  isCompleted(): Promise<UserTaskAssert>;
  /** Assert at least one selected task is assigned to `assignee`. */
  hasAssignee(assignee: string): Promise<UserTaskAssert>;
  /** Assert at least one selected task offers the candidate group `group`. */
  hasCandidateGroup(group: string): Promise<UserTaskAssert>;
}

/** The non-lifecycle selectors the read model accepts, resolved from a {@link UserTaskSelector}. */
interface TaskFilter {
  readonly processInstanceKey?: string;
}

/** Render a selector for a failure headline. */
function describeSelector(selector: UserTaskSelector): string {
  const parts: string[] = [];
  if (selector.instance !== undefined) parts.push(`instance ${formatValue(selector.instance)}`);
  if (selector.elementId !== undefined) parts.push(`elementId ${formatValue(selector.elementId)}`);
  return parts.length === 0 ? "any user task" : parts.join(" / ");
}

/** Resolve the selector's instance (if any) to the read model's `processInstanceKey`
 *  filter. Delegates instance resolution to the shared resolver, which throws an
 *  intent-revealing `AssertionError` when the instance cannot be resolved. */
function toFilter(engine: EngineReadModel, selector: UserTaskSelector): TaskFilter {
  if (selector.instance === undefined) return {};
  return { processInstanceKey: resolveInstanceKey(engine, selector.instance) };
}

/** Narrow a set of rows to the selector's element id (a no-op when none is given). */
function narrow(rows: readonly UserTaskRow[], selector: UserTaskSelector): UserTaskRow[] {
  if (selector.elementId === undefined) return rows.slice();
  return rows.filter((row) => row.elementId === selector.elementId);
}

/** Describe the tasks the selector actually resolves to, tagged with the lifecycle state
 *  each is in (CREATED / COMPLETED, or "other" for the intentionally-unmatched
 *  CANCELED/FAILED states), for a failure message. Purely additional read-model reads. */
async function describeActual(
  engine: EngineReadModel,
  filter: TaskFilter,
  selector: UserTaskSelector,
): Promise<string> {
  const all = narrow(await engine.searchUserTasks({ ...filter }), selector);
  if (all.length === 0) return `no user task matches ${describeSelector(selector)}`;
  const created = new Set(
    narrow(await engine.searchUserTasks({ ...filter, state: "CREATED" }), selector).map(
      (row) => row.userTaskKey,
    ),
  );
  const completed = new Set(
    narrow(await engine.searchUserTasks({ ...filter, state: "COMPLETED" }), selector).map(
      (row) => row.userTaskKey,
    ),
  );
  const rendered = all
    .map((row) => {
      const state = created.has(row.userTaskKey)
        ? "CREATED"
        : completed.has(row.userTaskKey)
          ? "COMPLETED"
          : "CANCELED|FAILED";
      return `{ userTaskKey: ${formatValue(row.userTaskKey)}, elementId: ${formatValue(row.elementId)}, state: ${state} }`;
    })
    .join(", ");
  return `matching user tasks: [${rendered}]`;
}

/** Assert over the user task(s) selected by `selector`, reading `engine`'s user-task
 *  read model. */
export function assertThatUserTask(engine: EngineReadModel, selector: UserTaskSelector): UserTaskAssert {
  const self: UserTaskAssert = {
    async isCreated(): Promise<UserTaskAssert> {
      const filter = toFilter(engine, selector);
      // `openUserTasks` is `searchUserTasks` pinned to state CREATED — the open set.
      const open = narrow(await engine.openUserTasks({ ...filter }), selector);
      if (open.length === 0) {
        failAssertion({
          message:
            `Expected a user task matching ${describeSelector(selector)} to be CREATED (open), ` +
            `but none is open. ${await describeActual(engine, filter, selector)}.`,
          operator: "assertThatUserTask.isCreated",
          diff: false,
        });
      }
      return self;
    },

    async isCompleted(): Promise<UserTaskAssert> {
      const filter = toFilter(engine, selector);
      const done = narrow(
        await engine.searchUserTasks({ ...filter, state: "COMPLETED" }),
        selector,
      );
      if (done.length === 0) {
        failAssertion({
          message:
            `Expected a user task matching ${describeSelector(selector)} to be COMPLETED, ` +
            `but none is. ${await describeActual(engine, filter, selector)}.`,
          operator: "assertThatUserTask.isCompleted",
          diff: false,
        });
      }
      return self;
    },

    async hasAssignee(assignee: string): Promise<UserTaskAssert> {
      const filter = toFilter(engine, selector);
      const existing = narrow(await engine.searchUserTasks({ ...filter }), selector);
      if (existing.length === 0) {
        failAssertion({
          message:
            `Expected a user task matching ${describeSelector(selector)} assigned to ` +
            `${formatValue(assignee)}, but ${await describeActual(engine, filter, selector)}.`,
          operator: "assertThatUserTask.hasAssignee",
          diff: false,
        });
      }
      const assigned = narrow(await engine.searchUserTasks({ ...filter, assignee }), selector);
      if (assigned.length === 0) {
        failAssertion({
          message:
            `Expected a user task matching ${describeSelector(selector)} assigned to ` +
            `${formatValue(assignee)}, but no matching task carries that assignee (the read ` +
            `model surfaces assignee only through its filter, so the actual assignee value is ` +
            `not projected on the row). ${await describeActual(engine, filter, selector)}.`,
          operator: "assertThatUserTask.hasAssignee",
          diff: false,
        });
      }
      return self;
    },

    async hasCandidateGroup(group: string): Promise<UserTaskAssert> {
      const filter = toFilter(engine, selector);
      const existing = narrow(await engine.searchUserTasks({ ...filter }), selector);
      if (existing.length === 0) {
        failAssertion({
          message:
            `Expected a user task matching ${describeSelector(selector)} offering candidate ` +
            `group ${formatValue(group)}, but ${await describeActual(engine, filter, selector)}.`,
          operator: "assertThatUserTask.hasCandidateGroup",
          diff: false,
        });
      }
      const offered = narrow(
        await engine.searchUserTasks({ ...filter, candidateGroup: group }),
        selector,
      );
      if (offered.length === 0) {
        failAssertion({
          message:
            `Expected a user task matching ${describeSelector(selector)} offering candidate ` +
            `group ${formatValue(group)}, but no matching task offers it (the read model ` +
            `surfaces candidate groups only through its filter, so the actual groups are not ` +
            `projected on the row). ${await describeActual(engine, filter, selector)}.`,
          operator: "assertThatUserTask.hasCandidateGroup",
          diff: false,
        });
      }
      return self;
    },
  };
  return self;
}
