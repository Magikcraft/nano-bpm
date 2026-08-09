// Unit tests for the Process Explorer instance-action helpers. Node-native: run
// with `node --experimental-strip-types --test src/lib/instanceActions.test.ts`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { isCancellable, cancelConfirmMessage } from "./instanceActions.ts";

test("isCancellable is true only for a running (Active) instance", () => {
  assert.equal(isCancellable("Active"), true);
});

test("isCancellable is false for finished or transient states", () => {
  // Completed / Terminated have no tokens to discard; Terminating is already
  // being cancelled. Guards the defect class of offering Cancel on an instance
  // the engine would reject with a 404.
  for (const state of ["Completed", "Terminated", "Terminating", ""]) {
    assert.equal(isCancellable(state), false, `state=${state}`);
  }
});

test("cancelConfirmMessage names the instance and warns it is irreversible", () => {
  const msg = cancelConfirmMessage("order-fulfilment");
  assert.match(msg, /order-fulfilment/);
  assert.match(msg, /cannot be undone/i);
});
