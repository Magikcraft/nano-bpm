// Types + pure helpers for the console changelog ("What's new").
//
// The changelog JSON itself is generated at build time from the git history by
// `console/scripts/build-changelog.mjs` (grouped by Conventional-Commit type,
// PR/issue references stripped) and served as a static asset at
// `${BASE_URL}changelog.json`. This module only describes that document's shape
// and the small amount of client-side logic the UI needs — deciding whether
// there is anything newer than the version the user last acknowledged, so the
// sidebar can wear an unobtrusive "new" dot.

/** One Conventional-Commit type bucket within a version (e.g. Features, Fixes). */
export interface ChangelogGroup {
  /** The Conventional-Commit type this group collects (e.g. `feat`, `fix`). */
  type: string;
  /** Human-facing heading (e.g. "Features"). */
  title: string;
  /** Rendered, reference-free entry lines, newest-first as authored. */
  entries: string[];
}

/** A single released (or in-progress "Unreleased") version and its changes. */
export interface ChangelogVersion {
  /** Bare semver (e.g. "0.0.11") or the literal "Unreleased". */
  version: string;
  /** ISO date (YYYY-MM-DD) the tag was created, or null for "Unreleased". */
  date: string | null;
  /** Non-empty, user-facing groups in a stable display order. */
  groups: ChangelogGroup[];
}

/** The whole document as served to the console. */
export interface ChangelogDoc {
  /** ISO timestamp the file was generated (for cache-busting / display). */
  generatedAt: string;
  /** Versions newest-first; "Unreleased" (if present) always leads. */
  versions: ChangelogVersion[];
}

/** The literal used for changes landed after the latest release tag. */
export const UNRELEASED = "Unreleased";

/**
 * Compares two bare semver strings numerically. "Unreleased" sorts above any
 * real version. Returns >0 when `a` is newer than `b`, <0 when older, 0 equal.
 * Non-numeric / malformed inputs sort as the lowest possible version so a bad
 * value never suppresses the badge for a genuinely newer release.
 */
export function compareVersions(a: string, b: string): number {
  if (a === b) return 0;
  if (a === UNRELEASED) return 1;
  if (b === UNRELEASED) return -1;
  const parse = (v: string): number[] =>
    v
      .replace(/^v/, "")
      .split(".")
      .map((p) => Number.parseInt(p, 10))
      .map((n) => (Number.isFinite(n) ? n : -1));
  const pa = parse(a);
  const pb = parse(b);
  const len = Math.max(pa.length, pb.length);
  for (let i = 0; i < len; i++) {
    const d = (pa[i] ?? 0) - (pb[i] ?? 0);
    if (d !== 0) return d > 0 ? 1 : -1;
  }
  return 0;
}

/** Normalizes a running-gateway version to the changelog's bare-semver space. */
export function normalizeVersion(v: string | null | undefined): string | null {
  if (!v) return null;
  // Gateway versions can be git-describe builds like "0.0.11-3-gabc123" or
  // carry a leading "v"; the changelog keys on the released "0.0.11" prefix.
  const m = v.trim().match(/^v?(\d+\.\d+\.\d+)/);
  return m ? m[1] : null;
}

/**
 * A short, friendly label for the running gateway version, for the sidebar
 * chrome. The gateway is stamped with `git describe --tags --always --dirty`
 * (server/build.rs), so a value can carry build metadata past its released
 * `x.y.z` prefix. This collapses that to a legible tag while still flagging a
 * non-release build so a local dev binary is distinguishable at a glance:
 *   - `0.0.11`                      -> `0.0.11`        (clean tagged release)
 *   - `0.0.11-3-g8b71af4-dirty`     -> `0.0.11-dirty`  (uncommitted changes)
 *   - `0.0.11-3-g8b71af4`           -> `0.0.11-dev`    (commits past the tag)
 *   - `0.0.11-rc.1`                 -> `0.0.11-rc.1`   (clean prerelease tag)
 *   - `8b71af4` / `dev`             -> shown verbatim  (untagged fallback)
 * The full, unabbreviated build id stays available on the Topology page and via
 * `c8 nano status`.
 */
export function displayVersion(v: string | null | undefined): string | null {
  if (!v) return null;
  const raw = v.trim();
  const bare = normalizeVersion(raw);
  if (!bare) return raw; // non-semver (bare sha / "dev") — show verbatim
  if (/-dirty$/.test(raw)) return `${bare}-dirty`; // dirty working tree
  if (/-\d+-g[0-9a-f]+/.test(raw)) return `${bare}-dev`; // ahead of the tag
  return raw.replace(/^v/, ""); // clean release (keeps prerelease suffixes)
}

/**
 * Is there a changelog entry newer than what the user last acknowledged?
 *
 * `lastSeen` is the bare version the user most recently opened the panel at
 * (persisted in localStorage). When it is null/absent (first run) we do NOT
 * badge on an empty history but DO badge as soon as any real version exists, so
 * a fresh install with releases still invites a look without nagging on a blank
 * changelog. An "Unreleased" section counts as unseen only once the user has
 * seen at least one real version (avoids a permanent dot on dev builds with no
 * tags yet).
 */
export function hasUnseenSince(
  doc: ChangelogDoc | null | undefined,
  lastSeen: string | null,
): boolean {
  const versions = doc?.versions ?? [];
  if (versions.length === 0) return false;
  const newest = versions[0].version;
  // First run: invite a look only if there is real release history — never nag
  // on a dev build whose sole section is "Unreleased".
  if (!lastSeen) return versions.some((v) => v.version !== UNRELEASED);
  // Upgrade from a dev build: if the user last opened the panel when the top
  // section was "Unreleased" (persisted as lastSeen), a subsequent tagged build
  // whose newest section is a real release IS new to them. A plain
  // compareVersions would mis-order this (Unreleased sorts above any semver) and
  // wrongly suppress the badge, so handle it explicitly.
  if (lastSeen === UNRELEASED) return newest !== UNRELEASED;
  return compareVersions(newest, lastSeen) > 0;
}
