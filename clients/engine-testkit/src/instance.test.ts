// Unit coverage for `assertThatInstance`, driven by an in-memory
// {@link EngineReadModel} fake (no booted engine). Red/Green: each matcher is
// exercised on a passing snapshot and asserted to throw an intent-revealing
// `AssertionError` on a failing one.

import { test } from "node:test";
import assert from "node:assert/strict";
import { AssertionError } from "node:assert";
import { assertThatInstance } from "./instance.ts";
import { byKey, byProcessId } from "./selectors.ts";
import { fakeEngine } from "./fixtures.ts";

/** Assert `fn` throws an `AssertionError` whose message contains every needle. */
function expectFailure(fn: () => unknown, needles: string[]): void {
  try {
    fn();
  } catch (err) {
    assert.ok(err instanceof AssertionError, `expected AssertionError, got ${String(err)}`);
    for (const needle of needles) {
      assert.ok(
        err.message.includes(needle),
        `expected message to include ${JSON.stringify(needle)}, got: ${err.message}`,
      );
    }
    return;
  }
  assert.fail("expected the matcher to throw, but it did not");
}

test("lifecycle: isActive / hasCompleted / isTerminated match the snapshot state", () => {
  const engine = fakeEngine({
    snapshot: {
      instances: [
        { key: "pi-1", state: "Active", processId: "order" },
        { key: "pi-2", state: "Completed", processId: "order" },
        { key: "pi-3", state: "Terminated", processId: "order" },
      ],
    },
  });
  assertThatInstance(engine, "pi-1").isActive();
  assertThatInstance(engine, "pi-2").hasCompleted();
  assertThatInstance(engine, "pi-3").isTerminated();

  expectFailure(() => assertThatInstance(engine, "pi-1").hasCompleted(), [
    "pi-1",
    "COMPLETED",
    "ACTIVE",
  ]);
});

test("Terminating projects to TERMINATED (parity with the engine REST projection)", () => {
  const engine = fakeEngine({ snapshot: { instances: [{ key: "pi-1", state: "Terminating" }] } });
  assertThatInstance(engine, "pi-1").isTerminated();
});

test("state matchers chain and return the same asserter", () => {
  const engine = fakeEngine({ snapshot: { instances: [{ key: "pi-1", state: "Active" }] } });
  const a = assertThatInstance(engine, "pi-1");
  assert.equal(a.isActive(), a);
});

test("selector resolution: bare key, byKey, byProcessId, and the single-ACTIVE default", () => {
  const engine = fakeEngine({
    snapshot: {
      instances: [
        { key: "pi-1", state: "Active", processId: "order" },
        { key: "pi-2", state: "Completed", processId: "ship" },
      ],
    },
  });
  assertThatInstance(engine, "pi-1").isActive();
  assertThatInstance(engine, byKey("pi-1")).isActive();
  assertThatInstance(engine, byProcessId("order")).isActive();
  // Exactly one ACTIVE instance → the no-selector default resolves to it.
  assertThatInstance(engine).isActive();
});

test("selector resolution fails loudly: unknown key, ambiguous default, missing processId", () => {
  const twoActive = fakeEngine({
    snapshot: {
      instances: [
        { key: "pi-1", state: "Active", processId: "order" },
        { key: "pi-2", state: "Active", processId: "ship" },
      ],
    },
  });
  expectFailure(() => assertThatInstance(twoActive, "ghost").isActive(), ["no instance with key", "ghost"]);
  expectFailure(() => assertThatInstance(twoActive).isActive(), ["ACTIVE instances", "ambiguous"]);
  expectFailure(() => assertThatInstance(twoActive, byProcessId("nope")).isActive(), [
    "no instance with processId",
    "nope",
  ]);
});

test("active elements: hasActiveElement / hasActiveElements", () => {
  const engine = fakeEngine({
    snapshot: {
      instances: [
        { key: "pi-1", state: "Active", activeElements: [{ elementId: "work" }, { elementId: "review" }] },
      ],
    },
  });
  assertThatInstance(engine, "pi-1").hasActiveElement("work").hasActiveElements("work", "review");
  expectFailure(() => assertThatInstance(engine, "pi-1").hasActiveElement("ghost"), [
    "active element",
    "ghost",
    "work",
  ]);
});

test("completed elements: read from single-instance elementStats", () => {
  const engine = fakeEngine({
    snapshot: {
      instances: [{ key: "pi-1", state: "Completed" }],
      elementStats: [
        { elementId: "s", completed: 1 },
        { elementId: "work", completed: 1 },
        { elementId: "e", completed: 1 },
        { elementId: "never", completed: 0 },
      ],
    },
  });
  assertThatInstance(engine, "pi-1").hasCompletedElements("s", "work", "e");
  expectFailure(() => assertThatInstance(engine, "pi-1").hasCompletedElements("s", "never"), [
    "completed element",
    "never",
  ]);
});

test("completed elements: refuses a multi-instance snapshot (aggregate is not per-instance)", () => {
  const engine = fakeEngine({
    snapshot: {
      instances: [
        { key: "pi-1", state: "Completed" },
        { key: "pi-2", state: "Active" },
      ],
      elementStats: [{ elementId: "s", completed: 2 }],
    },
  });
  expectFailure(() => assertThatInstance(engine, "pi-1").hasCompletedElements("s"), [
    "more than one instance",
    "unsound",
  ]);
});

test("variables: hasVariable / hasVariables subset / hasNoVariable", () => {
  const engine = fakeEngine({
    snapshot: {
      instances: [
        { key: "pi-1", state: "Active", variables: { amount: 42, currency: "EUR", nested: { ok: true } } },
      ],
    },
  });
  assertThatInstance(engine, "pi-1")
    .hasVariable("amount", 42)
    .hasVariables({ currency: "EUR", nested: { ok: true } })
    .hasNoVariable("missing");

  expectFailure(() => assertThatInstance(engine, "pi-1").hasVariable("amount", 43), ["amount", "pi-1"]);
  expectFailure(() => assertThatInstance(engine, "pi-1").hasVariable("ghost", 1), ["have variable", "ghost"]);
  expectFailure(() => assertThatInstance(engine, "pi-1").hasNoVariable("amount"), ["NO variable", "amount"]);
});

test("incidents: hasIncident (optionally narrowed) / hasNoIncident", () => {
  const withIncident = fakeEngine({
    snapshot: {
      instances: [{ key: "pi-1", state: "Active" }],
      incidents: [{ instanceKey: "pi-1", elementId: "call", reason: "connector timed out", kind: "JOB_NO_RETRIES" }],
    },
  });
  assertThatInstance(withIncident, "pi-1")
    .hasIncident()
    .hasIncident({ elementId: "call" })
    .hasIncident({ errorMessage: "timed out" });

  expectFailure(() => assertThatInstance(withIncident, "pi-1").hasIncident({ elementId: "other" }), [
    "incident matching",
  ]);
  expectFailure(() => assertThatInstance(withIncident, "pi-1").hasNoIncident(), ["no incidents", "connector timed out"]);

  const clean = fakeEngine({ snapshot: { instances: [{ key: "pi-1", state: "Active" }] } });
  assertThatInstance(clean, "pi-1").hasNoIncident();
  expectFailure(() => assertThatInstance(clean, "pi-1").hasIncident(), ["have an incident", "no incidents"]);
});

test("a vanished instance is reported, not silently passed", () => {
  // Resolution reads the snapshot once (instance present); a later matcher read
  // sees it gone. A counter-backed snapshot reproduces that race deterministically.
  let reads = 0;
  const engine = {
    snapshot: () => {
      reads += 1;
      return reads === 1 ? { instances: [{ key: "pi-1", state: "Active" }] } : { instances: [] };
    },
    searchUserTasks: () => Promise.resolve([]),
    openUserTasks: () => Promise.resolve([]),
  };
  expectFailure(() => assertThatInstance(engine, byKey("pi-1")).isActive(), [
    "pi-1",
    "no longer present",
  ]);
});
