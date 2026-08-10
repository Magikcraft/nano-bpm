import { test } from "node:test";
import assert from "node:assert/strict";
import { installChunkReloadBackstop } from "./lazyWithReload.ts";

test("installChunkReloadBackstop attaches only one listener even when called repeatedly", () => {
  const added: string[] = [];
  const prevWindow = (globalThis as { window?: unknown }).window;
  (globalThis as { window?: unknown }).window = {
    addEventListener: (type: string) => {
      added.push(type);
    },
  };
  try {
    installChunkReloadBackstop();
    installChunkReloadBackstop();
    installChunkReloadBackstop();
    assert.deepEqual(added, ["vite:preloadError"]);
  } finally {
    if (prevWindow === undefined) {
      delete (globalThis as { window?: unknown }).window;
    } else {
      (globalThis as { window?: unknown }).window = prevWindow;
    }
  }
});
