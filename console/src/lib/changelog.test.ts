import { test } from "node:test";
import assert from "node:assert/strict";
import {
  compareVersions,
  normalizeVersion,
  displayVersion,
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

test("displayVersion flags non-release builds", () => {
  // Clean tagged release: shown bare.
  assert.equal(displayVersion("0.0.11"), "0.0.11");
  assert.equal(displayVersion("v0.0.11"), "0.0.11");
  // Dirty working tree wins even when also ahead of the tag.
  assert.equal(displayVersion("0.0.11-3-g8b71af4-dirty"), "0.0.11-dirty");
  assert.equal(displayVersion("0.0.11-dirty"), "0.0.11-dirty");
  // Commits past the tag, clean tree.
  assert.equal(displayVersion("0.0.11-3-g8b71af4"), "0.0.11-dev");
  // Clean prerelease tag keeps its suffix (not a dev build).
  assert.equal(displayVersion("0.0.11-rc.1"), "0.0.11-rc.1");
  assert.equal(displayVersion("v0.0.11-rc.1"), "0.0.11-rc.1");
  // git describe measured from a prerelease tag keeps the -rc.1 context.
  assert.equal(displayVersion("0.0.11-rc.1-3-g8b71af4"), "0.0.11-rc.1-dev");
  assert.equal(
    displayVersion("0.0.11-rc.1-3-g8b71af4-dirty"),
    "0.0.11-rc.1-dirty",
  );
  // Untagged fallbacks (git describe --always / literal) shown verbatim.
  assert.equal(displayVersion("8b71af4"), "8b71af4");
  assert.equal(displayVersion("8b71af4-dirty"), "8b71af4-dirty");
  assert.equal(displayVersion("dev"), "dev");
  assert.equal(displayVersion(null), null);
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

test("hasUnseenSince: re-badges after a real release ships over a seen dev build", () => {
  // The user opened the panel on a dev build whose top section was "Unreleased"
  // (so lastSeen === UNRELEASED). A later tagged build promotes a real release
  // to the top with no Unreleased section — that release is genuinely new.
  assert.equal(hasUnseenSince(docWith(["0.0.12", "0.0.11"]), UNRELEASED), true);
  // But if the newest section is still just "Unreleased", nothing tagged is new.
  assert.equal(
    hasUnseenSince(docWith([UNRELEASED, "0.0.11"]), UNRELEASED),
    false,
  );
});
