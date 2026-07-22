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

/**
 * A neutral variable-scope node (ADR 0029 §5). An editor-agnostic tree the
 * console maps onto its FEEL editor's variable shape (e.g. dmn-js / feel-editor
 * `Variable`), so the type-in-scope logic stays here (tested) and the editor
 * wiring stays thin. `entries` are the fields of a nested declared type.
 */
export interface ScopeVar {
  name: string;
  /** The field's declared type or primitive — a short hint for the editor. */
  type?: string;
  list?: boolean;
  entries?: ScopeVar[];
}

/**
 * The fields of `typeId` as a scope tree, recursing into nested declared types
 * (lists included — FEEL projects a list of records). Cycles in the nominal type
 * graph are broken by tracking the types on the current path, so a self- or
 * mutually-recursive type resolves one level deep without looping.
 */
export function scopeVarsForType(manifest: unknown, typeId: string | undefined): ScopeVar[] {
  const walk = (id: string | undefined, seen: ReadonlySet<string>): ScopeVar[] => {
    if (!id || seen.has(id) || !isDeclaredType(manifest, id)) return [];
    const next = new Set(seen).add(id);
    return Object.entries(fieldsOf(manifest, id)).map(([name, f]) => {
      const v: ScopeVar = { name, type: f.type, list: f.list === true };
      const entries = walk(f.type, next);
      if (entries.length > 0) v.entries = entries;
      return v;
    });
  };
  return walk(typeId, new Set());
}

/**
 * The variable scope for a decision's input-expression FEEL: the fields of the
 * domain type bound to `decisionId` in `bindings[]` (ADR 0029 §5). Returns
 * `undefined` when the decision has no binding, or the binding's type is not a
 * declared type — callers then contribute no domain variables (never a wrong scope).
 */
export function decisionScope(manifest: unknown, decisionId: string | undefined): ScopeVar[] | undefined {
  return bindingScope(manifest, "decision", decisionId);
}

/**
 * The variable scope for a process's FEEL (component/service-task inputs, gateway
 * conditions): the fields of the domain type bound to `processId` in `bindings[]`
 * — the process as the motion of a typed domain object (ADR 0030). Returns
 * `undefined` when the process has no binding or the bound type is not declared.
 */
export function processScope(manifest: unknown, processId: string | undefined): ScopeVar[] | undefined {
  return bindingScope(manifest, "process", processId);
}

/**
 * The domain-type scope bound to a model id in `bindings[]` (ADR 0029 §5). `key`
 * is the binding discriminator (`decision` / `process` / `form`); shared so every
 * scope entry point resolves bindings identically.
 */
function bindingScope(
  manifest: unknown,
  key: "decision" | "process" | "form",
  id: string | undefined,
): ScopeVar[] | undefined {
  if (!id) return undefined;
  const bindings = (manifest as { bindings?: unknown })?.bindings;
  if (!Array.isArray(bindings)) return undefined;
  const binding = bindings.find(
    (b) => b && typeof b === "object" && (b as Record<string, unknown>)[key] === id,
  ) as { type?: unknown } | undefined;
  const typeId = typeof binding?.type === "string" ? binding.type : undefined;
  if (!isDeclaredType(manifest, typeId)) return undefined;
  return scopeVarsForType(manifest, typeId);
}
