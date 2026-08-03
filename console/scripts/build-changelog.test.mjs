import { test } from "node:test";
import assert from "node:assert/strict";
import {
  parseSubject,
  entryText,
  groupSubjects,
} from "./build-changelog.mjs";

test("parseSubject extracts type, scope and description", () => {
  assert.deepEqual(parseSubject("feat(console): add a panel"), {
    type: "feat",
    scope: "console",
    description: "add a panel",
  });
  assert.deepEqual(parseSubject("fix: guard a null"), {
    type: "fix",
    scope: null,
    description: "guard a null",
  });
});

test("parseSubject returns null for non-conventional subjects", () => {
  assert.equal(parseSubject("Merge branch main"), null);
  assert.equal(parseSubject("just some words"), null);
});

test("parseSubject strips trailing PR references", () => {
  assert.equal(
    parseSubject("feat(console): live consumers panel (#421)")?.description,
    "live consumers panel",
  );
});

test("parseSubject strips inline and multi parenthetical references", () => {
  assert.equal(
    parseSubject(
      "feat(console): register pack-contributed journeys (#414 Part C) (#455)",
    )?.description,
    "register pack-contributed journeys",
  );
  assert.equal(
    parseSubject("feat(console): terminal (prototype, #496)")?.description,
    "terminal",
  );
});

test("parseSubject strips bare inline references and a leading PR", () => {
  assert.equal(
    parseSubject("fix(server): PR #40 review — harden the guard")?.description,
    "review — harden the guard",
  );
  assert.equal(
    parseSubject("fix(varspill): recover the #287 regression")?.description,
    "recover the regression",
  );
});

test("entryText prefixes the scope and capitalizes", () => {
  assert.equal(
    entryText({ scope: "console", description: "add a panel" }),
    "console: Add a panel",
  );
  assert.equal(
    entryText({ scope: null, description: "guard a null" }),
    "Guard a null",
  );
});

test("groupSubjects buckets user-facing types in order and drops noise", () => {
  const groups = groupSubjects([
    "fix(a): b",
    "feat(c): d",
    "chore(e): f",
    "perf(g): h",
    "docs(i): j",
    "not a commit",
  ]);
  assert.deepEqual(
    groups.map((g) => g.type),
    ["feat", "fix", "perf"],
  );
  assert.deepEqual(groups[0].entries, ["c: D"]);
  assert.deepEqual(groups[1].entries, ["a: B"]);
});

test("groupSubjects returns [] when nothing is user-facing", () => {
  assert.deepEqual(groupSubjects(["chore: x", "docs: y", "test: z"]), []);
});
