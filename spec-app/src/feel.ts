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
 * A component output mapping: a service task's `taskType` (the worker seam,
 * ADR 0022) and the process variable one of its output mappings writes into.
 * The console extracts these from the diagram; `componentOutputScope` types them.
 */
export interface ComponentOutput {
  taskType: string;
  target: string;
}

/**
 * The declared domain type a worker writes as its result (ADR 0033 §3), resolved
 * by a component's `taskType`. Returns `undefined` when no worker matches, the
 * worker declares no `outputType`, or that type is not declared — callers then
 * leave the output variable untyped (never a wrong scope).
 */
export function outputTypeForTaskType(manifest: unknown, taskType: string | undefined): string | undefined {
  if (!taskType) return undefined;
  const workers = (manifest as { workers?: unknown })?.workers;
  if (!Array.isArray(workers)) return undefined;
  const worker = workers.find(
    (w) => w && typeof w === "object" && (w as { taskType?: unknown }).taskType === taskType,
  ) as { outputType?: unknown } | undefined;
  const typeId = typeof worker?.outputType === "string" ? worker.outputType : undefined;
  return isDeclaredType(manifest, typeId) ? typeId : undefined;
}

/**
 * The variable scope contributed by a process's component outputs (ADR 0033 §3):
 * each output-mapped process variable typed by the domain type its worker
 * declares (`workers[].outputType`). This is the "component output → typed
 * process variable → next component input" continuity — a task placed after a
 * component autocompletes on the result's fields. Outputs whose worker declares
 * no (declared) `outputType` are skipped; a variable written by more than one
 * component keeps the first typed occurrence.
 */
export function componentOutputScope(manifest: unknown, outputs: readonly ComponentOutput[]): ScopeVar[] {
  const out: ScopeVar[] = [];
  const seen = new Set<string>();
  for (const o of outputs) {
    if (!o || !o.target || seen.has(o.target)) continue;
    const typeId = outputTypeForTaskType(manifest, o.taskType);
    if (!typeId) continue;
    seen.add(o.target);
    const v: ScopeVar = { name: o.target, type: typeId };
    const entries = scopeVarsForType(manifest, typeId);
    if (entries.length > 0) v.entries = entries;
    out.push(v);
  }
  return out;
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

// ---------------------------------------------------------------------------
// The `data.query` App-tier FEEL builtin (ADR 0024 §5).
//
// `data.query` reads an App datasource from *App-tier* FEEL — the expressions
// the App runtime evaluates outside the engine's replayable path (I/O trigger
// actions, forms, App-side decisions). It is deliberately NOT an engine FEEL
// builtin: the engine's FEEL (conditions, I/O mappings, correlation-key
// evaluation) must stay pure and deterministic so a journal replay never
// diverges, and the engine must stay untyped/Zeebe-pure with the datasource as
// App-owned state-at-rest. A trigger action computes its value App-side and
// hands the engine a literal, so determinism is preserved.
//
// Because FEEL evaluation is synchronous and a datasource read is async, the
// runtime resolves `data.query(...)` calls by pre-resolution (collect the
// calls, run them read-only through the datasource gateway, substitute the
// results, then evaluate the expression) — the same shape as the form option
// binding (ADR 0024 §5). This module supplies the *contract* (signature for
// editor surfacing) and a pure call extractor (for alias validation and, later,
// that pre-resolution); the runtime wiring lands in a follow-up.
// ---------------------------------------------------------------------------

/** The `data.query` builtin's editor-facing signature descriptor. */
export interface FeelFunctionSignature {
  /** The callable name as written in FEEL. */
  name: string;
  /** One-line human hint for autocomplete/detail. */
  detail: string;
  /** The accepted call forms, most-specific first. */
  forms: string[];
  /** A longer description for signature/documentation surfaces. */
  doc: string;
}

/**
 * The `data.query` builtin contract (ADR 0024 §5). Read-only: it reads from a
 * datasource, never writes. Two forms — an explicit alias, or the default
 * source (`data.default`) when the alias is omitted.
 */
export const DATA_QUERY: FeelFunctionSignature = {
  name: "data.query",
  detail: "read a datasource (App-tier, read-only)",
  forms: [`data.query(source, sql)`, `data.query(sql)`],
  doc:
    "Read rows from an App datasource. `source` names a declared `data.sources` " +
    "alias; omit it to use `data.default`. Read-only, and available only in " +
    "App-tier FEEL (trigger actions, forms) — never in engine FEEL, which stays " +
    "deterministic (ADR 0024 §5).",
};

/** A `data.query(...)` call site found in a FEEL expression. */
export interface DataQueryCall {
  /**
   * The datasource alias named as the first argument in the two-argument form
   * `data.query("alias", "SELECT …")`. `null` for the single-argument form
   * `data.query("SELECT …")` (which uses `data.default`) or when the first
   * argument is not a plain string literal (dynamic — unverifiable, never a
   * false error), mirroring the conservative stance of `bodyPaths`.
   */
  source: string | null;
  /** Offset of the `data.query` occurrence in the expression. */
  index: number;
}

// `data.query(` with tolerant whitespace around the dot and before the paren.
const DATA_QUERY_CALL = /\bdata\s*\.\s*query\s*\(/g;
// A FEEL double-quoted string literal (with escapes) at the current position.
const STRING_LITERAL = /^\s*"((?:[^"\\]|\\.)*)"\s*([,)])/;

/**
 * Extract the `data.query(...)` call sites in a FEEL expression. Conservative on
 * purpose: it reports the alias only for the two-argument form whose first
 * argument is a plain string literal; the single-argument (default-source) form
 * and any dynamic first argument yield `source: null` so validation never flags
 * a call it cannot verify.
 */
export function dataQueryCalls(feel: string): DataQueryCall[] {
  const out: DataQueryCall[] = [];
  DATA_QUERY_CALL.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = DATA_QUERY_CALL.exec(feel)) !== null) {
    const rest = feel.slice(m.index + m[0].length);
    const lit = STRING_LITERAL.exec(rest);
    // Two-argument form (`"alias" ,`) names the source; single-argument
    // (`"sql" )`) uses the default; anything else is dynamic/unverifiable.
    const source = lit && lit[2] === "," ? lit[1] : null;
    out.push({ source, index: m.index });
  }
  return out;
}
