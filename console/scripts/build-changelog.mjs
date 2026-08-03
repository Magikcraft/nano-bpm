// Generate the console changelog ("What's new") from the git history.
//
// Output: console/public/changelog.json — grouped by Conventional-Commit type,
// with PR/issue references and merge noise stripped, one section per released
// version tag (plus an "Unreleased" section for changes landed since the latest
// tag). Vite ships public/ into dist/, which the gateway rust-embeds, so the
// file is served at `${BASE_URL}changelog.json` (i.e. /console/changelog.json).
//
// This is build tooling, so it is plain JS (no TS import) to stay portable
// across the Node versions used by the various CI jobs that build the console.
// It is *offline-soft*, mirroring server_update.rs: any git failure (shallow
// clone without tags, git absent, detached history) degrades to an empty
// changelog and a clean exit rather than breaking the asset build.
//
// The shape written here must match ChangelogDoc in src/lib/changelog.ts.

import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// Conventional-Commit types surfaced to users, in display order. Everything
// else (chore, ci, build, test, refactor, style, revert, docs…) is internal
// noise for a product changelog and is intentionally dropped.
const GROUPS = [
  { type: "feat", title: "Features" },
  { type: "fix", title: "Fixes" },
  { type: "perf", title: "Performance" },
];

// Only real product releases: v1.2.3-style tags. Excludes the per-package npm
// tags (workflow-npm-v*, bojtos-npm-v*, nano-bernd-*) and one-off tags.
const PRODUCT_TAG_GLOB = "v[0-9]*.[0-9]*.[0-9]*";

/** Runs git, returning trimmed stdout, or null on any failure (offline-soft). */
function git(args) {
  try {
    return execFileSync("git", args, {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
  } catch {
    return null;
  }
}

/** Uppercases the first character of a string (leaves the rest untouched). */
function capitalize(s) {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
}

/**
 * Parses a commit subject into { type, scope, description } or null when it is
 * not a Conventional-Commit subject we care about. PR/issue references like
 * "(#509)" are stripped from the description.
 */
export function parseSubject(subject) {
  const m = /^(\w+)(?:\(([^)]+)\))?(!)?:\s*(.+)$/.exec(subject.trim());
  if (!m) return null;
  const [, type, scope, , rawDesc] = m;
  const description = rawDesc
    // Drop any parenthetical containing an issue/PR reference, whether a bare
    // trailing "(#455)" or an inline annotation like "(#414 Part C)" or
    // "(prototype, #496)" — the user-facing changelog carries no issue links.
    .replace(/\s*\([^)]*#\d+[^)]*\)/g, "")
    // Drop bare inline references too, e.g. "PR #40 review" or "the #287 spike".
    .replace(/\s*\bPR\s+#\d+/gi, "")
    .replace(/\s*#\d+\b/g, "")
    .replace(/\s+([.,;:])/g, "$1")
    .replace(/\s{2,}/g, " ")
    .trim();
  if (!description) return null;
  return { type, scope: scope || null, description };
}

/** Renders a parsed commit into a user-facing entry line. */
export function entryText({ scope, description }) {
  const body = capitalize(description);
  return scope ? `${scope}: ${body}` : body;
}

/**
 * Groups an array of raw commit subjects into the user-facing GROUPS, in order,
 * dropping empty groups. Returns [] when nothing user-facing is present.
 */
export function groupSubjects(subjects) {
  const byType = new Map();
  for (const subject of subjects) {
    const parsed = parseSubject(subject);
    if (!parsed) continue;
    if (!GROUPS.some((g) => g.type === parsed.type)) continue;
    if (!byType.has(parsed.type)) byType.set(parsed.type, []);
    byType.get(parsed.type).push(entryText(parsed));
  }
  return GROUPS.filter((g) => byType.has(g.type)).map((g) => ({
    type: g.type,
    title: g.title,
    entries: byType.get(g.type),
  }));
}

/** Subjects (newest-first, merges excluded) in a git range, or [] on failure. */
function subjectsInRange(range) {
  const out = git(["log", "--no-merges", "--format=%s", range]);
  if (!out) return [];
  return out.split("\n").filter(Boolean);
}

/**
 * Creation date (YYYY-MM-DD) of a tag, or null. Uses the tag's own
 * `creatordate` — the same key `buildVersions` sorts on — so an annotated or
 * late-created tag displays when it was cut, not its underlying commit's date.
 */
function tagDate(ref) {
  return git(["tag", "--list", ref, "--format=%(creatordate:short)"]);
}

/** Builds the ordered list of ChangelogVersion entries from the git history. */
export function buildVersions() {
  const tagsRaw = git([
    "tag",
    "--list",
    PRODUCT_TAG_GLOB,
    "--sort=-creatordate",
  ]);
  const tags = tagsRaw ? tagsRaw.split("\n").filter(Boolean) : [];

  const versions = [];

  // Changes landed after the most recent tag (only when there are any).
  if (tags.length > 0) {
    const unreleased = groupSubjects(subjectsInRange(`${tags[0]}..HEAD`));
    if (unreleased.length > 0) {
      versions.push({ version: "Unreleased", date: null, groups: unreleased });
    }
  }

  // Each released tag: commits from the previous (older) tag up to it. The
  // oldest tag has no predecessor, so it collects everything up to itself.
  for (let i = 0; i < tags.length; i++) {
    const tag = tags[i];
    const older = tags[i + 1];
    const range = older ? `${older}..${tag}` : tag;
    const groups = groupSubjects(subjectsInRange(range));
    if (groups.length === 0) continue; // maintenance-only release — omit
    versions.push({
      version: tag.replace(/^v/, ""),
      date: tagDate(tag),
      groups,
    });
  }

  return versions;
}

function main() {
  const here = dirname(fileURLToPath(import.meta.url));
  const outPath = join(here, "..", "public", "changelog.json");
  const doc = {
    generatedAt: new Date().toISOString(),
    versions: buildVersions(),
  };
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, `${JSON.stringify(doc, null, 2)}\n`);
  const total = doc.versions.reduce(
    (n, v) => n + v.groups.reduce((m, g) => m + g.entries.length, 0),
    0,
  );
  console.log(
    `build-changelog: wrote ${doc.versions.length} version(s), ${total} entries to public/changelog.json`,
  );
}

// Only run when invoked directly, so the pure helpers can be unit-tested.
if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main();
}
