import { test } from "node:test";
import assert from "node:assert/strict";
import {
  formatErrorSummary,
  normalizeError,
  shouldResetOnKeyChange,
} from "./routeErrorBoundaryLogic.ts";

test("normalizeError passes an Error through unchanged", () => {
  const err = new Error("boom");
  assert.equal(normalizeError(err), err);
});

test("normalizeError wraps a thrown string", () => {
  const err = normalizeError("nope");
  assert.ok(err instanceof Error);
  assert.equal(err.message, "nope");
});

test("normalizeError coerces non-Error, non-string throws", () => {
  assert.equal(normalizeError({ code: 42 }).message, "[object Object]");
  assert.equal(normalizeError(null).message, "null");
  assert.equal(normalizeError(undefined).message, "undefined");
});

test("formatErrorSummary uses the message when present", () => {
  assert.equal(formatErrorSummary(new Error("  spacey  ")), "spacey");
});

test("formatErrorSummary falls back to the error name, then a generic label", () => {
  const named = new TypeError("");
  assert.equal(formatErrorSummary(named), "TypeError");
  const anon = new Error("");
  anon.name = "";
  assert.equal(formatErrorSummary(anon), "Error");
});

test("shouldResetOnKeyChange resets only when the key changes AND an error is held", () => {
  // Navigated away from a crashed view → reset.
  assert.equal(shouldResetOnKeyChange("/explorer", "/metrics", true), true);
  // Same route, error held → no reset (retry handles same-route recovery).
  assert.equal(shouldResetOnKeyChange("/explorer", "/explorer", true), false);
  // Route changed but nothing is wrong → no-op.
  assert.equal(shouldResetOnKeyChange("/explorer", "/metrics", false), false);
});
