// Unit tests for the studio's app-running edit gate (issue #889). Node-native:
// run with `node --experimental-strip-types --test src/lib/appRunning.test.ts`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { appIsRunning, appIsEditable } from "./appRunning.ts";

test("appIsRunning gates every non-terminal lifecycle phase", () => {
  // starting/running/stopping all hold a bound worker/schema contract, so an
  // update would break the live process — the server refuses them with 409.
  for (const status of ["starting", "running", "stopping"] as const) {
    assert.equal(appIsRunning(status), true, `status=${status}`);
    assert.equal(appIsEditable(status), false, `status=${status}`);
  }
});

test("appIsRunning allows editing only when stopped or errored", () => {
  for (const status of ["stopped", "error"] as const) {
    assert.equal(appIsRunning(status), false, `status=${status}`);
    assert.equal(appIsEditable(status), true, `status=${status}`);
  }
});

test("appIsRunning treats a missing run status as not-running (editable)", () => {
  assert.equal(appIsRunning(null), false);
  assert.equal(appIsRunning(undefined), false);
  assert.equal(appIsEditable(null), true);
});
