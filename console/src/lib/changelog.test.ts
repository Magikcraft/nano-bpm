import { test } from "node:test";
import assert from "node:assert/strict";
import {
  compareVersions,
  normalizeVersion,
  hasUnseenSince,
  UNRELEASED,
  type ChangelogDoc,
} from "./changelog.ts";

test("compareVersions orders bare semver numerically", () => {
  assert.ok(compareVersions("0.0.11", "0.0.9") > 0);
  assert.ok(compareVersions("0.0.9", "0.0.11") < 0);
  assert.equal(compareVersions("1.2.3", "1.2.3"), 0);
  assert.ok(compareVersions("1.10.0", "1.9.9") > 0);
});

test("compareVersions treats Unreleased as newest", () => {
  assert.ok(compareVersions(UNRELEASED, "9.9.9") > 0);
  assert.ok(compareVersions("9.9.9", UNRELEASED) < 0);
  assert.equal(compareVersions(UNRELEASED, UNRELEASED), 0);
});

test("normalizeVersion extracts the released prefix", () => {
  assert.equal(normalizeVersion("0.0.11"), "0.0.11");
  assert.equal(normalizeVersion("v0.0.11"), "0.0.11");
  assert.equal(normalizeVersion("0.0.11-3-gabc123"), "0.0.11");
  assert.equal(normalizeVersion(null), null);
  assert.equal(normalizeVersion("dev"), null);
});

const docWith = (versions: string[]): ChangelogDoc => ({
  generatedAt: "2026-08-03T00:00:00.000Z",
  versions: versions.map((version) => ({ version, date: null, groups: [] })),
});

test("hasUnseenSince: empty history never badges", () => {
  assert.equal(hasUnseenSince(docWith([]), null), false);
  assert.equal(hasUnseenSince(null, "0.0.1"), false);
});

test("hasUnseenSince: first run badges only with real release history", () => {
  assert.equal(hasUnseenSince(docWith(["0.0.11", "0.0.10"]), null), true);
  assert.equal(hasUnseenSince(docWith([UNRELEASED]), null), false);
});

test("hasUnseenSince: badges when newest exceeds last seen", () => {
  const doc = docWith(["0.0.11", "0.0.10"]);
  assert.equal(hasUnseenSince(doc, "0.0.10"), true);
  assert.equal(hasUnseenSince(doc, "0.0.11"), false);
  assert.equal(hasUnseenSince(doc, "0.0.12"), false);
});

test("hasUnseenSince: Unreleased badges once a real version was seen", () => {
  const doc = docWith([UNRELEASED, "0.0.11"]);
  assert.equal(hasUnseenSince(doc, "0.0.11"), true);
});
