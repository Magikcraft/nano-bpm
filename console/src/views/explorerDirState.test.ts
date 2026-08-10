import { test } from "node:test";
import assert from "node:assert/strict";
import {
  type DirLike,
  type DirState,
  collectDirPaths,
  defaultDirOpen,
  isDirOpen,
  loadDirState,
  pruneDirState,
  serializeDirState,
  toggleDir,
} from "./explorerDirState.ts";

// `node:assert/strict`'s deepEqual compares prototypes. Every DirState the
// module produces is null-prototype (a prototype-pollution guard), so normalize
// to a plain object before comparing against plain-object expectations.
const own = (s: DirState): Record<string, boolean> => ({ ...s });

test("defaultDirOpen collapses every folder on first open", () => {
  assert.equal(defaultDirOpen(0), false);
  assert.equal(defaultDirOpen(1), false);
  assert.equal(defaultDirOpen(2), false);
  assert.equal(defaultDirOpen(3), false);
});

test("loadDirState returns an empty map for missing/blank input", () => {
  assert.deepEqual(own(loadDirState(null)), {});
  assert.deepEqual(own(loadDirState("")), {});
});

test("loadDirState tolerates malformed JSON without throwing", () => {
  assert.deepEqual(own(loadDirState("{not json")), {});
});

test("loadDirState rejects non-object JSON shapes", () => {
  assert.deepEqual(own(loadDirState("[1,2,3]")), {});
  assert.deepEqual(own(loadDirState("42")), {});
  assert.deepEqual(own(loadDirState("null")), {});
});

test("loadDirState keeps only string→boolean pairs", () => {
  const raw = JSON.stringify({
    "resources/processes": true,
    pages: false,
    "resources/forms": "yes", // wrong type, dropped
    nested: { a: 1 }, // wrong type, dropped
  });
  assert.deepEqual(own(loadDirState(raw)), {
    "resources/processes": true,
    pages: false,
  });
});

test("serializeDirState round-trips through loadDirState", () => {
  const state: DirState = { "a/b": true, c: false };
  assert.deepEqual(own(loadDirState(serializeDirState(state))), state);
});

test("dangerous folder names never pollute the prototype or throw", () => {
  // A folder can legitimately be named "__proto__"/"constructor". Persisted
  // JSON with such a key must round-trip as plain data — never hit an accessor,
  // throw, or mutate Object.prototype.
  const raw = '{"__proto__": true, "constructor": false, "safe": true}';
  const loaded = loadDirState(raw);
  assert.equal(Object.getPrototypeOf(loaded), null);
  assert.equal(isDirOpen(loaded, "__proto__", 3), true);
  assert.equal(isDirOpen(loaded, "constructor", 0), false);
  // toggle keeps the null prototype and still stores by the dangerous key.
  const toggled = toggleDir(loaded, "__proto__", 3);
  assert.equal(Object.getPrototypeOf(toggled), null);
  assert.equal(isDirOpen(toggled, "__proto__", 3), false);
});

test("isDirOpen falls back to the collapsed default when no override exists", () => {
  assert.equal(isDirOpen({}, "top", 0), false);
  assert.equal(isDirOpen({}, "deep", 3), false);
});

test("isDirOpen honours a stored override over the default", () => {
  // A shallow dir the user expanded away from the collapsed default.
  assert.equal(isDirOpen({ top: true }, "top", 0), true);
  // A deep dir the user expanded.
  assert.equal(isDirOpen({ deep: true }, "deep", 3), true);
});

test("isDirOpen ignores stale paths without error", () => {
  const state: DirState = { "gone/dir": true };
  // Looking up a different, still-present path is unaffected (default collapsed).
  assert.equal(isDirOpen(state, "present", 0), false);
});

test("toggleDir stores a divergence from the default", () => {
  // Expand a shallow (default-collapsed) dir → stored as true.
  assert.deepEqual(own(toggleDir({}, "top", 0)), { top: true });
  // Expand a deep (default-collapsed) dir → stored as true.
  assert.deepEqual(own(toggleDir({}, "deep", 3)), { deep: true });
});

test("toggleDir drops the entry when it returns to the default", () => {
  // top was expanded (divergent); toggling back to collapsed matches default → gone.
  assert.deepEqual(own(toggleDir({ top: true }, "top", 0)), {});
  // deep was expanded (divergent); toggling back to collapsed matches default → gone.
  assert.deepEqual(own(toggleDir({ deep: true }, "deep", 3)), {});
});

test("toggleDir does not mutate the input state", () => {
  const state: DirState = { a: false };
  const next = toggleDir(state, "b", 0);
  assert.deepEqual(state, { a: false });
  assert.notEqual(next, state);
});

const tree: DirLike[] = [
  {
    path: "resources",
    kind: "dir",
    children: [
      { path: "resources/processes", kind: "dir", children: [] },
      { path: "resources/forms", kind: "dir" },
      { path: "resources/readme.md", kind: "file" },
    ],
  },
  { path: "pages", kind: "dir" },
  { path: "package.json", kind: "file" },
];

test("collectDirPaths gathers every directory path, skipping files", () => {
  assert.deepEqual(
    collectDirPaths(tree),
    new Set(["resources", "resources/processes", "resources/forms", "pages"]),
  );
});

test("pruneDirState drops overrides for directories that no longer exist", () => {
  const state: DirState = {
    "resources/processes": true,
    "resources/gone": false, // renamed/deleted
    pages: false,
  };
  const pruned = pruneDirState(state, collectDirPaths(tree));
  assert.deepEqual(own(pruned), { "resources/processes": true, pages: false });
});

test("pruneDirState returns the same reference when nothing is stale", () => {
  const state: DirState = { pages: false };
  const pruned = pruneDirState(state, collectDirPaths(tree));
  assert.equal(pruned, state);
});
