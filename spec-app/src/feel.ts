// Shared FEEL scope model (ADR 0029 §5).
//
// The type-walking used by *both* the manifest completer (variable-path
// autocomplete) and the validator (wrong-path diagnostics), so authoring and
// validation resolve `body.<path>` against the trigger's `bodyType` the same
// way and can never disagree.

import { DOMAIN_PRIMITIVES } from "./symbol-index.ts";

const PRIMITIVES = new Set<string>(DOMAIN_PRIMITIVES);

export interface FeelFieldDef {
  type?: string;
  list?: boolean;
}

function typeRecord(manifest: unknown, id: string): Record<string, unknown> | undefined {
  const types = (manifest as { types?: Record<string, unknown> })?.types;
  const t = types?.[id];
  return t && typeof t === "object" ? (t as Record<string, unknown>) : undefined;
}

/** Whether `id` names a declared domain type (not a primitive / unknown). */
export function isDeclaredType(manifest: unknown, id: string | undefined): boolean {
  return id != null && typeRecord(manifest, id) !== undefined;
}

/** Declared fields of a domain type id (empty when the id is unknown/absent). */
export function fieldsOf(manifest: unknown, typeId: string | undefined): Record<string, FeelFieldDef> {
  if (!typeId) return {};
  const t = typeRecord(manifest, typeId);
  const fields = t && t.fields;
  return fields && typeof fields === "object"
    ? (fields as Record<string, FeelFieldDef>)
    : {};
}

/** Outcome of resolving a dotted `body`-rooted path against the scope type. */
export type PathResolution =
  /** The full path resolves to a declared field. */
  | { kind: "ok"; type?: string; list?: boolean }
  /** Just `body` — the root, no segments to resolve. */
  | { kind: "root" }
  /** A segment is not a field of the (known) type at that point. */
  | { kind: "unknown"; segment: string }
  /** The walk passed through a primitive `json`/unknown type — can't verify. */
  | { kind: "indeterminate" };

/**
 * Resolve `segs` (the path after `body`) against `bodyType`, walking nested
 * declared types. Descending into a list of a declared type follows FEEL's list
 * projection (`body.items.name`). The walk is deliberately conservative: it only
 * reports `unknown` when a segment is definitively absent from a *declared* type
 * at that point, and reports `indeterminate` (never a false error) once it hits a
 * `json` field or an undeclared type whose shape it cannot know.
 */
export function resolveBodyPath(
  manifest: unknown,
  bodyType: string | undefined,
  segs: string[],
): PathResolution {
  if (segs.length === 0) return { kind: "root" };
  let curType = bodyType;
  for (let i = 0; i < segs.length; i++) {
    if (!isDeclaredType(manifest, curType)) return { kind: "indeterminate" };
    const f = fieldsOf(manifest, curType)[segs[i]];
    if (!f) return { kind: "unknown", segment: segs[i] };
    if (i === segs.length - 1) return { kind: "ok", type: f.type, list: f.list };
    // Not the last segment — we must descend one level.
    if (f.type === "json") return { kind: "indeterminate" }; // dynamic shape
    if (f.type != null && PRIMITIVES.has(f.type)) {
      // A scalar has no members: the next segment cannot resolve.
      return { kind: "unknown", segment: segs[i + 1] };
    }
    if (!isDeclaredType(manifest, f.type)) return { kind: "indeterminate" };
    curType = f.type;
  }
  return { kind: "indeterminate" };
}

/**
 * Extract the `body`-rooted dotted paths in a FEEL expression, as segment arrays
 * *excluding* the leading `body`. Conservative on purpose: it matches only a
 * standalone `body` identifier followed by one or more `.field` accessors, and
 * skips anything followed by `[` (indexing) or `(` (a call) — so complex FEEL
 * never yields a spurious path to flag.
 */
export function bodyPaths(feel: string): string[][] {
  const out: string[][] = [];
  const re = /(?<![\w.])body((?:\.[A-Za-z_]\w*)+)(?![\w([])/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(feel)) !== null) {
    out.push(m[1].split(".").filter(Boolean));
  }
  return out;
}
