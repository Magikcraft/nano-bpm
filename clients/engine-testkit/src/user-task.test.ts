// Unit coverage for `assertThatUserTask`, driven by an in-memory
// {@link EngineReadModel} fake that honours the same query filters a real
// adapter applies (`processInstanceKey` / `state` / `assignee` /
// `candidateGroup`). The matchers are async — each is awaited.

import { test } from "node:test";
import assert from "node:assert/strict";
import { AssertionError } from "node:assert";
import { assertThatUserTask } from "./user-task.ts";
import { byProcessId } from "./selectors.ts";
import { fakeEngine, type FakeUserTask } from "./fixtures.ts";

async function expectFailure(fn: () => Promise<unknown>, needles: string[]): Promise<void> {
  try {
    await fn();
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

const snapshot = {
  instances: [
    { key: "pi-1", state: "Active", processId: "review" },
    { key: "pi-2", state: "Active", processId: "review" },
  ],
};

function engineWith(userTasks: FakeUserTask[]) {
  return fakeEngine({ snapshot, userTasks });
}

test("isCreated: passes for an open task, fails when only a completed one exists", async () => {
  const open = engineWith([
    { userTaskKey: "ut-1", elementId: "approve", processInstanceKey: "pi-1", state: "CREATED" },
  ]);
  await assertThatUserTask(open, { instance: "pi-1", elementId: "approve" }).isCreated();

  const done = engineWith([
    { userTaskKey: "ut-1", elementId: "approve", processInstanceKey: "pi-1", state: "COMPLETED" },
  ]);
  await expectFailure(
    () => assertThatUserTask(done, { instance: "pi-1", elementId: "approve" }).isCreated(),
    ["CREATED", "elementId", "approve", "state: COMPLETED"],
  );
});

test("isCompleted: passes for a completed task, fails for an open one", async () => {
  const done = engineWith([
    { userTaskKey: "ut-1", elementId: "approve", processInstanceKey: "pi-1", state: "COMPLETED" },
  ]);
  await assertThatUserTask(done, { instance: "pi-1", elementId: "approve" }).isCompleted();

  const open = engineWith([
    { userTaskKey: "ut-1", elementId: "approve", processInstanceKey: "pi-1", state: "CREATED" },
  ]);
  await expectFailure(
    () => assertThatUserTask(open, { instance: "pi-1", elementId: "approve" }).isCompleted(),
    ["COMPLETED", "state: CREATED"],
  );
});

test("matchers chain and resolve to the same asserter", async () => {
  const engine = engineWith([
    { userTaskKey: "ut-1", elementId: "approve", processInstanceKey: "pi-1", state: "CREATED", assignee: "alice" },
  ]);
  const a = assertThatUserTask(engine, { instance: "pi-1", elementId: "approve" });
  const same = await (await a.isCreated()).hasAssignee("alice");
  assert.equal(same, a);
});

test("hasAssignee: passes for the real assignee, fails otherwise", async () => {
  const engine = engineWith([
    { userTaskKey: "ut-1", elementId: "approve", processInstanceKey: "pi-1", state: "CREATED", assignee: "alice" },
  ]);
  await assertThatUserTask(engine, { instance: "pi-1", elementId: "approve" }).hasAssignee("alice");
  await expectFailure(
    () => assertThatUserTask(engine, { instance: "pi-1", elementId: "approve" }).hasAssignee("bob"),
    ["assigned to", "bob"],
  );
});

test("hasCandidateGroup: passes for an offered group, fails otherwise", async () => {
  const engine = engineWith([
    {
      userTaskKey: "ut-1",
      elementId: "approve",
      processInstanceKey: "pi-1",
      state: "CREATED",
      candidateGroups: ["reviewers", "seniors"],
    },
  ]);
  await assertThatUserTask(engine, { instance: "pi-1", elementId: "approve" }).hasCandidateGroup("seniors");
  await expectFailure(
    () => assertThatUserTask(engine, { instance: "pi-1", elementId: "approve" }).hasCandidateGroup("admins"),
    ["candidate", "admins"],
  );
});

test("instance selector narrows to the owning instance; elementId narrows within it", async () => {
  const engine = engineWith([
    { userTaskKey: "ut-1", elementId: "approve", processInstanceKey: "pi-1", state: "CREATED" },
    { userTaskKey: "ut-2", elementId: "approve", processInstanceKey: "pi-2", state: "COMPLETED" },
  ]);
  // pi-1's approve is open; pi-2's approve is completed — the instance filter keeps them apart.
  await assertThatUserTask(engine, { instance: "pi-1", elementId: "approve" }).isCreated();
  await assertThatUserTask(engine, { instance: "pi-2", elementId: "approve" }).isCompleted();
  await expectFailure(
    () => assertThatUserTask(engine, { instance: "pi-2", elementId: "approve" }).isCreated(),
    ["CREATED"],
  );
});

test("an omitted instance matches across every instance", async () => {
  const engine = engineWith([
    { userTaskKey: "ut-1", elementId: "approve", processInstanceKey: "pi-1", state: "CREATED" },
  ]);
  await assertThatUserTask(engine, { elementId: "approve" }).isCreated();
});

test("a byProcessId selector resolves the owning instance for the task filter", async () => {
  const engine = fakeEngine({
    snapshot: { instances: [{ key: "pi-9", state: "Active", processId: "onboarding" }] },
    userTasks: [{ userTaskKey: "ut-1", elementId: "verify", processInstanceKey: "pi-9", state: "CREATED" }],
  });
  await assertThatUserTask(engine, { instance: byProcessId("onboarding"), elementId: "verify" }).isCreated();
});

test("no matching task at all yields the 'no user task matches' headline", async () => {
  const engine = engineWith([]);
  await expectFailure(
    () => assertThatUserTask(engine, { instance: "pi-1", elementId: "approve" }).isCreated(),
    ["no user task matches"],
  );
});
