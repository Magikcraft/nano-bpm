// Runtime pre-resolution of the App-tier `data.query()` FEEL builtin (ADR 0024 §5).
//
// `data.query()` reads an App datasource from App-tier FEEL. FEEL evaluates
// *synchronously* (both the engine's Rust FEEL and the App's `@bpmn-io/feelin`),
// but a datasource read is async — so the App cannot resolve `data.query()`
// inside the evaluation loop. Instead it **pre-resolves**: scan the expression
// for `data.query(...)` calls, run each one read-only through the datasource
// gateway (ADR 0024 phase 2), bind the result rows to a fresh context variable,
// and rewrite the call to reference that variable. The rewritten expression then
// evaluates synchronously with the bound rows in scope — the same collect →
// run → substitute shape as the §5 form option binding.
//
// This module owns the *parsing* and *rewrite* (pure, tested here); the caller
// supplies the async `resolve` (the gateway) and evaluates the rewritten
// expression. It is deliberately conservative: only statically-resolvable calls
// (string-literal arguments) are pre-resolved up-front — a dynamic argument
// (`data.query("app", "… " + string(x))`) depends on runtime data and cannot be
// resolved before evaluation, so it is left untouched (a follow-up).

/** The prefix for a generated binding variable a `data.query(...)` call becomes. */
export const DATA_QUERY_BINDING_PREFIX = "__dq";

/** A `data.query(...)` call site located in a FEEL expression. */
export interface DataQueryCallSpan {
  /** `[start, end)` offsets of the whole `data.query(...)` call in the source. */
  span: [number, number];
  /**
   * The datasource alias — the first argument's string literal in the
   * two-argument form `data.query("alias", "SELECT …")`. `null` for the
   * single-argument (default-source) form or a dynamic first argument.
   */
  source: string | null;
  /**
   * The SQL — the last argument's string literal, when it is statically
   * resolvable. `null` when the SQL argument is dynamic (a non-literal FEEL
   * expression) and so cannot be pre-resolved.
   */
  sql: string | null;
  /** Whether the call can be pre-resolved up-front (`sql` is a literal). */
  static: boolean;
}

/**
 * Read a parenthesised, comma-separated argument list starting at `open` (the
 * index of `(`), returning the top-level argument slices (trimmed, raw FEEL
 * text) and the index of the matching `)`. String-literal and nesting aware, so
 * commas/parens inside `"…"`, `'…'`, `(…)`, `[…]` or `{…}` don't split. Returns
 * `null` when the parentheses are unbalanced (a mid-edit expression).
 */
function readArgs(feel: string, open: number): { end: number; args: string[] } | null {
  const args: string[] = [];
  let depth = 0;
  let argStart = open + 1;
  let quote: '"' | "'" | null = null;
  for (let i = open + 1; i < feel.length; i++) {
    const ch = feel[i];
    if (quote) {
      if (ch === "\\") i++; // skip escaped char
      else if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") quote = ch;
    else if (ch === "(" || ch === "[" || ch === "{") depth++;
    else if (ch === ")" || ch === "]" || ch === "}") {
      if (ch === ")" && depth === 0) {
        const slice = feel.slice(argStart, i).trim();
        // A lone `)` right after `(` is a zero-arg call — no argument slice.
        if (!(args.length === 0 && slice === "")) args.push(slice);
        return { end: i, args };
      }
      depth--;
    } else if (ch === "," && depth === 0) {
      args.push(feel.slice(argStart, i).trim());
      argStart = i + 1;
    }
  }
  return null; // unbalanced
}

/** The unescaped value of a FEEL double-quoted string literal, else `null`. */
function stringLiteral(arg: string): string | null {
  const m = /^"((?:[^"\\]|\\.)*)"$/.exec(arg);
  if (!m) return null;
  return m[1].replace(/\\(["\\])/g, "$1");
}

// `data.query(` with tolerant whitespace around the dot and before the paren.
const CALL_START = /\bdata\s*\.\s*query\s*\(/g;

/**
 * Locate the `data.query(...)` call sites in a FEEL expression, with their full
 * span and statically-resolvable arguments. Conservative: an argument that is
 * not a plain string literal yields `null` for that slot (never a guess).
 */
export function scanDataQueryCalls(feel: string): DataQueryCallSpan[] {
  const out: DataQueryCallSpan[] = [];
  CALL_START.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = CALL_START.exec(feel)) !== null) {
    const open = m.index + m[0].length - 1; // index of the `(`
    const parsed = readArgs(feel, open);
    if (!parsed) continue; // unbalanced — skip
    const { end, args } = parsed;
    let source: string | null = null;
    let sql: string | null = null;
    if (args.length >= 2) {
      source = stringLiteral(args[0]);
      sql = stringLiteral(args[1]);
    } else if (args.length === 1) {
      sql = stringLiteral(args[0]); // default-source form
    }
    out.push({ span: [m.index, end + 1], source, sql, static: sql != null });
    CALL_START.lastIndex = end + 1; // resume past this call
  }
  return out;
}

/** A row set, as returned by the datasource gateway (ADR 0024 phase 2). */
export type DataQueryRows = ReadonlyArray<Record<string, unknown>>;

/**
 * Runs one `data.query(...)` read against the datasource gateway. `source` is
 * the named alias, or `null` for the default-source form (the caller maps it to
 * `data.default`). Read-only by contract (ADR 0024 §5).
 */
export type DataQueryResolver = (
  source: string | null,
  sql: string,
) => Promise<DataQueryRows>;

/** A pre-resolved expression: the rewritten FEEL plus the bound row sets. */
export interface PreResolvedExpr {
  /** The expression with each resolved `data.query(...)` call replaced by its binding. */
  expr: string;
  /** The binding variables (`__dq0`, …) → resolved rows to add to the FEEL context. */
  context: Record<string, DataQueryRows>;
}

/**
 * Pre-resolve the statically-resolvable `data.query(...)` calls in one FEEL
 * expression: run each distinct `(source, sql)` once through `resolve`, bind the
 * rows to a fresh `__dq<n>` variable, and rewrite the calls to reference it. The
 * returned `expr` evaluates synchronously once `context` is merged into the FEEL
 * data. `nextIndex` seeds the binding counter so a caller can keep names unique
 * across many expressions (see `preResolveFormSchema`).
 */
export async function preResolveDataQuery(
  feel: string,
  resolve: DataQueryResolver,
  nextIndex = 0,
): Promise<PreResolvedExpr & { nextIndex: number }> {
  const calls = scanDataQueryCalls(feel).filter((c) => c.static);
  if (calls.length === 0) return { expr: feel, context: {}, nextIndex };

  const context: Record<string, DataQueryRows> = {};
  const byKey = new Map<string, string>(); // (source\0sql) → binding name
  let n = nextIndex;

  // Assign binding names first (dedup identical queries), then resolve in
  // parallel, so a form reusing one query hits the gateway once.
  const toResolve: { key: string; name: string; source: string | null; sql: string }[] = [];
  const bindings = calls.map((c) => {
    const key = `${c.source ?? ""}\u0000${c.sql}`;
    let name = byKey.get(key);
    if (!name) {
      name = `${DATA_QUERY_BINDING_PREFIX}${n++}`;
      byKey.set(key, name);
      toResolve.push({ key, name, source: c.source, sql: c.sql as string });
    }
    return { span: c.span, name };
  });

  await Promise.all(
    toResolve.map(async (r) => {
      context[r.name] = await resolve(r.source, r.sql);
    }),
  );

  // Rewrite right-to-left so earlier spans keep their offsets.
  let expr = feel;
  for (const b of [...bindings].sort((a, b) => b.span[0] - a.span[0])) {
    expr = expr.slice(0, b.span[0]) + b.name + expr.slice(b.span[1]);
  }
  return { expr, context, nextIndex: n };
}

/** A per-expression pre-resolution failure, located by a schema JSON path. */
export interface DataQueryError {
  /** Dotted/indexed path to the offending schema property (e.g. `components.0.conditional.hide`). */
  path: string;
  message: string;
}

/** The result of pre-resolving a whole form schema's `data.query(...)` calls. */
export interface PreResolvedForm {
  /** A deep copy of the schema with resolved calls rewritten to their bindings. */
  schema: unknown;
  /** The bound row sets to seed as the form's initial data (the FEEL context). */
  data: Record<string, DataQueryRows>;
  /** Per-expression resolution errors; the call is bound to `[]` so the form still renders. */
  errors: DataQueryError[];
}

const errMsg = (e: unknown): string =>
  e instanceof Error ? e.message : typeof e === "string" ? e : String(e);

// Non-global guard (a global-flag regex is stateful under `.test()`).
const HAS_DATA_QUERY = /\bdata\s*\.\s*query\s*\(/;

/**
 * Pre-resolve every `=`-prefixed FEEL expression in a form-js schema that
 * contains a `data.query(...)` call (ADR 0024 §5). Walks the schema, rewrites
 * each such expression in place (a deep copy — the input is untouched), and
 * returns the bound row sets to seed as the form's initial data so form-js's
 * synchronous evaluation resolves the bindings from context. Distinct queries
 * across the whole form share one binding and one gateway call. A failing query
 * is bound to an empty list and reported in `errors`, so an unbound field never
 * breaks the whole preview.
 */
export async function preResolveFormSchema(
  schema: unknown,
  resolve: DataQueryResolver,
): Promise<PreResolvedForm> {
  const copy = JSON.parse(JSON.stringify(schema));
  const data: Record<string, DataQueryRows> = {};
  const errors: DataQueryError[] = [];
  const nameByKey = new Map<string, string>();
  const fetchByKey = new Map<string, { source: string | null; sql: string; path: string }>();
  let counter = 0;

  const rewriteExpr = (expr: string, path: string): string => {
    const calls = scanDataQueryCalls(expr).filter((c) => c.static);
    if (calls.length === 0) return expr;
    const bindings = calls.map((c) => {
      const key = `${c.source ?? ""}\u0000${c.sql}`;
      let name = nameByKey.get(key);
      if (!name) {
        name = `${DATA_QUERY_BINDING_PREFIX}${counter++}`;
        nameByKey.set(key, name);
        fetchByKey.set(key, { source: c.source, sql: c.sql as string, path });
      }
      return { span: c.span, name };
    });
    let out = expr;
    for (const b of [...bindings].sort((a, b) => b.span[0] - a.span[0])) {
      out = out.slice(0, b.span[0]) + b.name + out.slice(b.span[1]);
    }
    return out;
  };

  const walk = (node: unknown, path: string): unknown => {
    if (typeof node === "string") {
      return node.startsWith("=") && HAS_DATA_QUERY.test(node) ? rewriteExpr(node, path) : node;
    }
    if (Array.isArray(node)) {
      for (let i = 0; i < node.length; i++) node[i] = walk(node[i], `${path}.${i}`);
      return node;
    }
    if (node && typeof node === "object") {
      const o = node as Record<string, unknown>;
      for (const k of Object.keys(o)) o[k] = walk(o[k], path ? `${path}.${k}` : k);
      return o;
    }
    return node;
  };
  const rewritten = walk(copy, "");

  // Fetch each distinct query once (in parallel); a failure binds [] + reports.
  await Promise.all(
    [...fetchByKey.entries()].map(async ([key, f]) => {
      const name = nameByKey.get(key) as string;
      try {
        data[name] = await resolve(f.source, f.sql);
      } catch (e) {
        data[name] = [];
        errors.push({ path: f.path, message: errMsg(e) });
      }
    }),
  );

  return { schema: rewritten, data, errors };
}
