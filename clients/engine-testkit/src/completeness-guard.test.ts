// Completeness guard for the `assertThat*` DSL (issue Magikcraft/nano-bpm#912).
//
// Ported from urban-testkit's `src/assert/completeness-guard.test.ts` when the
// Tier-A matcher source was lifted here (#894 / #895 / nanobpm/nano-ide#402) but
// its guards were left behind — retargeted at engine-testkit's own public
// surface. Unlike the urban version, it reads ONLY engine-testkit's own read
// model (`EngineReadModel`, `ProcessInstanceState`, `InstanceAssert`,
// `UserTaskAssert`), so it no longer depends on a booted Urban app: the matcher
// shapes are introspected against the in-memory `fakeEngine`. The two Urban-only
// surfaces (SQLite table, HTTP response) have no analog here and are dropped.
//
// The DSL's public types and matchers are imported through the package barrel
// (`./index.ts`), not the internal modules, so this guard ALSO fails if a public
// re-export is accidentally dropped from the barrel. The only non-barrel import
// is the in-memory `fakeEngine` test fixture (`./fixtures.ts`), which is
// deliberately excluded from the published build and is not part of the public
// surface.
//
// Every state / surface IN THE DSL'S DECLARED SCOPE must have a corresponding
// matcher, so adding a new in-scope engine state without a matcher fails CI.
// Three dimensions, and no others (we do NOT invent a dimension for a type the
// package does not define):
//
//   (a) PROCESS-INSTANCE STATE — FULL DERIVATION from `ProcessInstanceState`.
//   (b) ELEMENT-STATE KINDS    — EXPLICIT {active, completed} allowlist.
//   (c) USER-TASK STATE        — EXPLICIT {CREATED, COMPLETED} allowlist.
//
// (a) and (c)/(b) differ deliberately: where a source-of-truth union exists and
// the DSL covers ALL of it (process-instance state) the guard DERIVES from the
// union so a new member fails tsc/CI; where the DSL deliberately covers only PART
// of a surface (user-task state) or the package defines NO enum at all (element
// state) the guard uses an explicit, commented allowlist that maps EXACTLY onto
// the shipped matchers — it must never demand a matcher the DSL intentionally
// omits.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  assertThatInstance,
  assertThatUserTask,
  type InstanceAssert,
  type ProcessInstanceState,
  type UserTaskAssert,
  type UserTaskState,
} from "./index.ts";
import { fakeEngine } from "./fixtures.ts";

// ---------------------------------------------------------------------------
// (a) PROCESS-INSTANCE STATE — FULL DERIVATION.
//
// `Record<ProcessInstanceState, keyof InstanceAssert>` makes this map EXHAUSTIVE
// over the union and TYPE-CHECKED against the matcher names: adding a new
// `ProcessInstanceState` member (e.g. "SUSPENDED") without a matcher fails `tsc`
// with a missing-key error, and renaming a state matcher fails because the value
// is no longer `keyof InstanceAssert`. The guard thus tracks the union at the
// type level, not via a duplicated literal list.
const PROCESS_INSTANCE_STATE_MATCHERS = {
  ACTIVE: "isActive",
  COMPLETED: "hasCompleted",
  TERMINATED: "isTerminated",
} satisfies Record<ProcessInstanceState, keyof InstanceAssert>;

// ---------------------------------------------------------------------------
// (b) ELEMENT-STATE KINDS — EXPLICIT {active, completed} SCOPE (NOT a derivation).
//
// There is deliberately NO package-wide element-state enum to enumerate, so this
// is NOT derived from a type — `assertThatInstance` intentionally supports exactly
// two element lifecycle kinds, ACTIVE and COMPLETED, and no others. This
// commented allowlist is the DSL's intentional supported element-state scope; it
// maps exactly onto the shipped matchers and must not demand element matchers the
// DSL never builds (do NOT try to import/derive an `ElementInstanceState` type —
// none exists).
const ELEMENT_STATE_MATCHERS: Readonly<Record<"active" | "completed", readonly (keyof InstanceAssert)[]>> = {
  active: ["hasActiveElement", "hasActiveElements"],
  completed: ["hasCompletedElements"],
};

// ---------------------------------------------------------------------------
// (c) USER-TASK STATE — EXPLICIT, JUSTIFIED {CREATED, COMPLETED} ALLOWLIST.
//
// `UserTaskState` is a 4-member union (CREATED | COMPLETED | CANCELED | FAILED),
// but the DSL deliberately exposes state matchers for only the two states an
// integration test asserts on. CANCELED and FAILED are intentionally OUT OF
// SCOPE — no `isCanceled()` / `isFailed()` — so this allowlist is NOT derived
// from the full union (deriving from it would wrongly fail CI for the omitted
// states). `Partial<Record<UserTaskState, …>>` still type-checks that each
// allow-listed key is a genuine `UserTaskState` member (a typo like "CREATE"
// fails tsc) without forcing exhaustiveness. This set MUST stay in lockstep with
// the {CREATED, COMPLETED} scope declared in `user-task.ts`.
const USER_TASK_STATE_MATCHERS = {
  CREATED: "isCreated",
  COMPLETED: "isCompleted",
} satisfies Partial<Record<UserTaskState, keyof UserTaskAssert>>;

/** True when `obj` exposes `name` as a callable matcher method. Uses `Reflect.get`
 *  so no `as`-cast is needed to index by a dynamic key. */
function hasMatcher(obj: object, name: string): boolean {
  return typeof Reflect.get(obj, name) === "function";
}

// A single ACTIVE instance in an in-memory snapshot → `assertThatInstance` resolves
// a real fluent object we can introspect, with no booted engine. The completeness
// guard reasons over object SHAPE, not verdicts.
const INSTANCE_ASSERT: InstanceAssert = assertThatInstance(
  fakeEngine({ snapshot: { instances: [{ key: "pi-1", state: "Active", processId: "park" }] } }),
  "pi-1",
);

// `assertThatUserTask` builds its fluent object without reading the engine, so an
// empty fake and any selector suffice to introspect the `UserTaskAssert` shape.
const USER_TASK_ASSERT: UserTaskAssert = assertThatUserTask(fakeEngine({}), { instance: "pi-1" });

test("(a) every ProcessInstanceState member has an assertThatInstance state matcher", () => {
  // Exhaustiveness over the union is enforced at the TYPE level by the
  // `satisfies Record<ProcessInstanceState, …>` map above — a new member without
  // a matcher fails tsc, and an extra key fails as an excess property — so we do
  // NOT re-assert the literal key set here (that would duplicate the union and
  // create a second drift point). The runtime part only proves each derived
  // matcher name is a real callable method.
  for (const [state, matcher] of Object.entries(PROCESS_INSTANCE_STATE_MATCHERS)) {
    assert.ok(
      hasMatcher(INSTANCE_ASSERT, matcher),
      `assertThatInstance must expose \`${matcher}()\` for ProcessInstanceState ${state}`,
    );
  }
});

test("(b) each supported element-state kind {active, completed} has an assertThatInstance matcher", () => {
  assert.deepEqual(
    Object.keys(ELEMENT_STATE_MATCHERS).sort(),
    ["active", "completed"],
    "the DSL's supported element-state scope is exactly {active, completed}",
  );
  for (const [kind, matchers] of Object.entries(ELEMENT_STATE_MATCHERS)) {
    for (const matcher of matchers) {
      assert.ok(
        hasMatcher(INSTANCE_ASSERT, matcher),
        `assertThatInstance must expose \`${matcher}()\` for element-state kind ${kind}`,
      );
    }
  }
});

test("(c) each allow-listed user-task state {CREATED, COMPLETED} has an assertThatUserTask matcher", () => {
  assert.deepEqual(
    Object.keys(USER_TASK_STATE_MATCHERS).sort(),
    ["COMPLETED", "CREATED"],
    "the DSL's supported user-task state scope is exactly {CREATED, COMPLETED} (CANCELED/FAILED are intentionally omitted)",
  );
  for (const [state, matcher] of Object.entries(USER_TASK_STATE_MATCHERS)) {
    assert.ok(
      hasMatcher(USER_TASK_ASSERT, matcher),
      `assertThatUserTask must expose \`${matcher}()\` for user-task state ${state}`,
    );
  }
  // The intentionally-unsupported states must NOT have matchers — otherwise the
  // {CREATED, COMPLETED} scope declared here and in user-task.ts has drifted.
  for (const omitted of ["isCanceled", "isFailed"]) {
    assert.ok(
      !hasMatcher(USER_TASK_ASSERT, omitted),
      `assertThatUserTask must NOT expose \`${omitted}()\` — CANCELED/FAILED are out of scope by design`,
    );
  }
});
