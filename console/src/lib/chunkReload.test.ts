import { test } from "node:test";
import assert from "node:assert/strict";
import {
  CHUNK_RELOAD_DEBOUNCE_MS,
  isStaleChunkError,
  shouldReloadForChunkError,
  withinReloadBudget,
} from "./chunkReload.ts";

test("isStaleChunkError matches the text/html MIME failure", () => {
  assert.equal(
    isStaleChunkError(
      new Error("'text/html' is not a valid JavaScript MIME type"),
    ),
    true,
  );
});

test("isStaleChunkError matches the cross-browser dynamic-import phrasings", () => {
  for (const msg of [
    "Failed to fetch dynamically imported module: https://x/console/assets/Workers-abc.js",
    "error loading dynamically imported module",
    "Importing a module script failed.",
    "Expected a JavaScript module script but the server responded with a MIME type of text/html",
    "Expected a JavaScript-or-Wasm module script but the server responded with a MIME type of text/html",
    "Loading chunk 42 failed.",
    "ChunkLoadError: Loading chunk 3 failed.",
  ]) {
    assert.equal(isStaleChunkError(new Error(msg)), true, msg);
  }
});

test("isStaleChunkError accepts a thrown string or {message} object", () => {
  assert.equal(
    isStaleChunkError("Failed to fetch dynamically imported module"),
    true,
  );
  assert.equal(
    isStaleChunkError({ message: "Importing a module script failed" }),
    true,
  );
});

test("isStaleChunkError ignores unrelated errors and non-errors", () => {
  assert.equal(isStaleChunkError(new Error("boom")), false);
  assert.equal(isStaleChunkError(new TypeError("x is not a function")), false);
  assert.equal(isStaleChunkError(null), false);
  assert.equal(isStaleChunkError(undefined), false);
  assert.equal(isStaleChunkError(42), false);
  assert.equal(isStaleChunkError(""), false);
});

test("shouldReloadForChunkError reloads a stale-chunk error with no prior reload", () => {
  const err = new Error("Failed to fetch dynamically imported module");
  assert.equal(shouldReloadForChunkError(err, null, 1_000), true);
});

test("shouldReloadForChunkError suppresses a reload within the debounce window", () => {
  const err = new Error("Failed to fetch dynamically imported module");
  const now = 1_000_000;
  const recent = now - (CHUNK_RELOAD_DEBOUNCE_MS - 1);
  assert.equal(shouldReloadForChunkError(err, recent, now), false);
});

test("shouldReloadForChunkError re-arms after the debounce window (later redeploy)", () => {
  const err = new Error("Failed to fetch dynamically imported module");
  const now = 1_000_000;
  const old = now - (CHUNK_RELOAD_DEBOUNCE_MS + 1);
  assert.equal(shouldReloadForChunkError(err, old, now), true);
});

test("shouldReloadForChunkError never reloads a non-stale error", () => {
  const err = new Error("some genuine render bug");
  assert.equal(shouldReloadForChunkError(err, null, 1_000), false);
});

test("withinReloadBudget allows a reload with no prior reload", () => {
  assert.equal(withinReloadBudget(null, 1_000), true);
});

test("withinReloadBudget suppresses within the debounce window and re-arms after", () => {
  const now = 1_000_000;
  assert.equal(
    withinReloadBudget(now - (CHUNK_RELOAD_DEBOUNCE_MS - 1), now),
    false,
  );
  assert.equal(
    withinReloadBudget(now - (CHUNK_RELOAD_DEBOUNCE_MS + 1), now),
    true,
  );
});
