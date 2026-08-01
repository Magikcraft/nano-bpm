// Pure helpers for persisting the collapsed/expanded state of directories in
// the project-workspace file explorer (see ProjectWorkspace.tsx → FileBrowser /
// TreeNode).
//
// The tree used to keep each folder's open/closed flag in per-node React state
// seeded only by depth, so it was thrown away on every remount (navigation,
// live refetch, reload). These side-effect-free helpers let the browser persist
// that state per project in localStorage; they live here so the console's
// node:test unit suite can cover the persist/restore + default-fallback logic
// directly, without a renderer.

/**
 * Directories whose open/closed state the user has changed away from the
 * default. Keyed by the directory's project-relative `path`; the value is
 * whether it is open. Directories absent from the map fall back to the
 * depth-based default (see {@link defaultDirOpen}), so the map only ever stores
 * *divergent* folders and stays compact.
 */
export type DirState = Record<string, boolean>;

/**
 * Directories at depth 0 and 1 (top two levels) default to open; everything
 * deeper defaults to collapsed. This preserves the original first-open UX.
 */
export const DEFAULT_OPEN_DEPTH = 2;

/** The default open/closed state for a directory at `depth`, absent any override. */
export function defaultDirOpen(depth: number): boolean {
  return depth < DEFAULT_OPEN_DEPTH;
}

/**
 * Parse a persisted {@link DirState} from its localStorage string. Tolerates
 * missing, malformed, or wrongly-typed JSON by returning an empty map — a
 * corrupt entry must never break the explorer. Only string→boolean pairs are
 * kept; anything else in the object is ignored.
 */
export function loadDirState(raw: string | null): DirState {
  if (!raw) return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return {};
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return {};
  }
  const out: DirState = {};
  for (const [key, value] of Object.entries(parsed)) {
    if (typeof value === "boolean") out[key] = value;
  }
  return out;
}

/** Serialize a {@link DirState} for localStorage. */
export function serializeDirState(state: DirState): string {
  return JSON.stringify(state);
}

/**
 * Resolve whether the directory at `path`/`depth` is open: its stored override
 * if present, else the depth-based default. A stale `path` (a folder that no
 * longer exists) simply never gets looked up, so it is ignored without error.
 */
export function isDirOpen(
  state: DirState,
  path: string,
  depth: number,
): boolean {
  const override = state[path];
  return typeof override === "boolean" ? override : defaultDirOpen(depth);
}

/**
 * Flip the open/closed state of the directory at `path`/`depth`, returning a new
 * {@link DirState}. When the new state equals the depth default the entry is
 * dropped rather than stored, keeping the persisted map to divergent folders
 * only.
 */
export function toggleDir(
  state: DirState,
  path: string,
  depth: number,
): DirState {
  const nextOpen = !isDirOpen(state, path, depth);
  const next = { ...state };
  if (nextOpen === defaultDirOpen(depth)) {
    delete next[path];
  } else {
    next[path] = nextOpen;
  }
  return next;
}

/** A directory-tree node, structurally compatible with the generated `FileNode`. */
export interface DirLike {
  path: string;
  kind: "dir" | "file";
  children?: DirLike[];
}

/** Collect the project-relative paths of every directory in a file tree. */
export function collectDirPaths(
  nodes: readonly DirLike[],
  acc: Set<string> = new Set(),
): Set<string> {
  for (const node of nodes) {
    if (node.kind === "dir") {
      acc.add(node.path);
      if (node.children) collectDirPaths(node.children, acc);
    }
  }
  return acc;
}

/**
 * Drop overrides for directories that no longer exist in the current tree, so
 * renamed/deleted folders don't accumulate stale entries in localStorage.
 * Returns the same reference when nothing was pruned, so callers can skip a
 * redundant state update / write.
 */
export function pruneDirState(
  state: DirState,
  validPaths: Set<string>,
): DirState {
  const kept: DirState = {};
  let changed = false;
  for (const [key, value] of Object.entries(state)) {
    if (validPaths.has(key)) {
      kept[key] = value;
    } else {
      changed = true;
    }
  }
  return changed ? kept : state;
}
