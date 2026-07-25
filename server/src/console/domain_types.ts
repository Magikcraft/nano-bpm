// Urban domain-type reifier — ADR 0029 §4.1 + §6 (spike).
//
// The datasource schema is the *spine* of the domain model: a table is already a
// declared record type, so `DataSource.schema()`'s `TableMeta` reifies straight
// into a named TypeScript record. This module turns those tables into a generated
// `domain.d.ts` (the "models directory" makers asked for — but generated, never
// hand-edited, so it can't drift from the DB), and the worker SDK + App loader
// type against it. Types are a compile/boot-time contract only: they are erased
// at `deno compile`, and the shipped App is still untyped JSON on the wire
// (ADR 0029 §3) — the engine stays Zeebe-pure.
//
//   import { emitDomainDts } from "./domain-types.ts";
//   const tables = await (await openDataSource("app")).schema();
//   await Deno.writeTextFile(".nanobpm/domain.d.ts", emitDomainDts(tables));
//
// then a worker is typed end-to-end:
//
//   import type { DomainTables } from "./.nanobpm/domain.d.ts";
//   const rows = await db.query<DomainTables["customers"]>("SELECT * FROM customers");
//   rows[0].tier          // string | null — checked while authoring
//
// The manifest `types` registry (ADR 0029 §4.2, transient/non-persisted shapes)
// is the *other* source: `emitDomainTypeRegistry` renders it as a `DomainTypes`
// map keyed by type id, and `emitDomainModel` composes the table spine + the
// registry into the single generated file.
//
// This module is a **pure emitter**: it imports only the `TableMeta`/`ColumnMeta`
// *types* from the data SDK (a type-only import, erased at runtime), so it is
// materialised verbatim next to `data-cli.ts` as `.nanobpm/domain-types.ts` and
// the data CLI's `domaintypes` op imports `emitDomainDts` from it. Opening the
// datasource, running `schema()`, and writing the file are the CLI op's job
// (`data_cli.ts`), which already owns the datasource seam and the file writer.

import type { ColumnMeta, TableMeta } from "./data_sdk.ts";

/** The generated file's basename, written under a project's `.nanobpm/`. */
export const DOMAIN_DTS = "domain.d.ts";

/**
 * Map a SQLite declared column type to a TypeScript field type. SQLite is
 * dynamically typed but assigns each column a *type affinity* from its declared
 * name (the CREATE TABLE text), so we apply the affinity rules
 * (https://sqlite.org/datatype3.html §3.1) with two maker-friendly overrides the
 * DB Manager's type list implies: `BOOLEAN` → `boolean` and date/time types →
 * `string` (ISO text), matching how Urban surfaces them.
 */
export function sqliteAffinityToTs(declaredType: string): string {
  const t = declaredType.toUpperCase();
  if (t.length === 0) return "unknown"; // NONE affinity — opaque
  if (t.includes("BOOL")) return "boolean";
  if (t.includes("DATE") || t.includes("TIME")) return "string";
  if (t.includes("INT")) return "number";
  if (t.includes("CHAR") || t.includes("CLOB") || t.includes("TEXT")) return "string";
  if (t.includes("BLOB")) return "Uint8Array";
  if (t.includes("REAL") || t.includes("FLOA") || t.includes("DOUB")) return "number";
  return "number"; // NUMERIC affinity
}

/** A TS identifier is safe to use bare as a property name / type name. */
function isIdent(name: string): boolean {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(name);
}

/** PascalCase a table name into an interface name, sanitised to an identifier. */
export function interfaceName(table: string): string {
  const pascal = table
    .replace(/[^A-Za-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : ""))
    .join("");
  const safe = pascal.replace(/[^A-Za-z0-9_$]/g, "");
  return /^[A-Za-z_$]/.test(safe) ? safe : `T_${safe}`;
}

/** The TS field type for a column: nullable columns widen with `| null`. */
function fieldType(col: ColumnMeta): string {
  const base = sqliteAffinityToTs(col.type);
  // A NOT NULL column (or the INTEGER PRIMARY KEY rowid alias) is never null.
  return col.notNull || col.primaryKey ? base : `${base} | null`;
}

/** One `export interface` block for a table under an explicit interface name. */
function tableInterfaceNamed(name: string, t: TableMeta): string {
  const fields = t.columns
    .map((c) => {
      const key = isIdent(c.name) ? c.name : JSON.stringify(c.name);
      const pk = c.primaryKey ? " (primary key)" : "";
      return `  /** ${c.type || "?"}${pk} */\n  ${key}: ${fieldType(c)};`;
    })
    .join("\n");
  return `export interface ${name} {\n${fields}\n}`;
}

/** One `export interface` block for a table (name derived from the table). */
function tableInterface(t: TableMeta): string {
  return tableInterfaceNamed(interfaceName(t.name), t);
}

/**
 * Emit the full `domain.d.ts` from a datasource's tables: one `interface` per
 * table plus a `DomainTables` lookup keyed by the *raw* table name (the stable
 * wire identifier), so callers write `DomainTables["customers"]`.
 */
export function emitDomainDts(tables: TableMeta[]): string {
  const header =
    "// AUTO-GENERATED by nanobpmn from the datasource schema (ADR 0029 §4.1).\n" +
    "// Do not edit — regenerated from the live tables. Erased at `deno compile`.\n" +
    "// eslint-disable\n";
  if (tables.length === 0) {
    return `${header}\nexport interface DomainTables {}\n`;
  }
  const interfaces = tables.map(tableInterface).join("\n\n");
  const map = tables
    .map((t) => `  ${JSON.stringify(t.name)}: ${interfaceName(t.name)};`)
    .join("\n");
  return `${header}\n${interfaces}\n\n/** Every table keyed by its wire name — index it for a row type. */\nexport interface DomainTables {\n${map}\n}\n`;
}

/** One datasource's introspected tables, paired with its manifest alias. */
export interface SourceSchema {
  source: string;
  tables: TableMeta[];
}

/**
 * The interface name for a table in a multi-source union: the source alias is
 * PascalCased and prefixed to the table's interface name so tables that share a
 * name across datasources (e.g. `app.customers` and `analytics.customers`) get
 * distinct, collision-free interfaces (`AppCustomers`, `AnalyticsCustomers`).
 */
function prefixedInterfaceName(source: string, table: string): string {
  return `${interfaceName(source)}${interfaceName(table)}`;
}

/**
 * Emit `domain.d.ts` for an App that declares *multiple* datasources: one
 * `interface` per (source, table), a `DomainSources` map keyed by alias then
 * wire table name, and a `DomainTables` alias to the default source so the
 * single-source convention (`DomainTables["customers"]`) keeps working. When
 * there is one source (or none) this is byte-identical to `emitDomainDts` so the
 * common case stays stable.
 */
export function emitDomainDtsForSources(
  sources: SourceSchema[],
  defaultSource?: string,
): string {
  if (sources.length <= 1) {
    return emitDomainDts(sources[0]?.tables ?? []);
  }
  const header =
    "// AUTO-GENERATED by nanobpmn from the datasource schemas (ADR 0029 §4.1/§6).\n" +
    "// Do not edit — regenerated from the live tables. Erased at `deno compile`.\n" +
    "// eslint-disable\n";

  const interfaces = sources
    .flatMap((s) =>
      s.tables.map((t) =>
        tableInterfaceNamed(prefixedInterfaceName(s.source, t.name), t)
      )
    )
    .join("\n\n");

  const sourceMap = sources
    .map((s) => {
      if (s.tables.length === 0) return `  ${JSON.stringify(s.source)}: {};`;
      const tableMap = s.tables
        .map((t) =>
          `    ${JSON.stringify(t.name)}: ${prefixedInterfaceName(s.source, t.name)};`
        )
        .join("\n");
      return `  ${JSON.stringify(s.source)}: {\n${tableMap}\n  };`;
    })
    .join("\n");

  const def = defaultSource && sources.some((s) => s.source === defaultSource)
    ? defaultSource
    : sources[0].source;

  return `${header}\n${interfaces}\n\n` +
    `/** Every datasource keyed by alias, then table by wire name. */\n` +
    `export interface DomainSources {\n${sourceMap}\n}\n\n` +
    `/** The default datasource's tables — index by wire name. */\n` +
    `export type DomainTables = DomainSources[${JSON.stringify(def)}];\n`;
}

// --- manifest `types` registry (ADR 0029 §4.2) ------------------------------

/** One field of a declared domain type in the manifest `types` registry. */
export interface DomainFieldDef {
  /** A primitive keyword or the id of another registry type (nominal ref). */
  type: string;
  optional?: boolean;
  list?: boolean;
}

/** A declared (transient) domain record type from the manifest `types` block. */
export interface DomainTypeDef {
  name?: string;
  match?: string;
  table?: string;
  fields: Record<string, DomainFieldDef>;
}

/** The manifest `types` registry: type id → declared record type. */
export type DomainTypeRegistry = Record<string, DomainTypeDef>;

/** Manifest primitive field keywords → TS types (ADR 0029 §4.2 / schema). */
const PRIMITIVE_TS: Record<string, string> = {
  string: "string",
  number: "number",
  integer: "number",
  boolean: "boolean",
  date: "string",
  datetime: "string",
  json: "unknown",
};

/**
 * The TS type for a registry field. A primitive keyword maps directly (and takes
 * precedence over an identically named type, per the schema); anything else is a
 * nominal reference to another registry type, emitted as an indexed access into
 * `DomainTypes` (self-contained, so no top-level interface names can collide with
 * table interfaces). An unresolved reference degrades to `unknown`. `list` wraps
 * the base type in `[]`.
 */
function fieldTsType(field: DomainFieldDef, ids: Set<string>): string {
  const base = PRIMITIVE_TS[field.type] ??
    (ids.has(field.type) ? `DomainTypes[${JSON.stringify(field.type)}]` : "unknown");
  return field.list ? `${base}[]` : base;
}

/**
 * Emit the `DomainTypes` block for the manifest `types` registry: an inline map
 * keyed by type id, so a worker/form types against `DomainTypes["taxSubmission"]`
 * exactly as it types against `DomainTables["customers"]`. Returns `""` when the
 * registry is empty, so the table spine stays byte-identical when no types are
 * declared. Field references resolve to `DomainTypes[<id>]`; `optional` widens
 * the key with `?`.
 */
export function emitDomainTypeRegistry(types: DomainTypeRegistry): string {
  const ids = new Set(Object.keys(types));
  if (ids.size === 0) return "";
  const entries = Object.entries(types)
    .map(([id, def]) => {
      const fields = Object.entries(def.fields ?? {})
        .map(([fname, field]) => {
          const key = isIdent(fname) ? fname : JSON.stringify(fname);
          const opt = field.optional ? "?" : "";
          return `    ${key}${opt}: ${fieldTsType(field, ids)};`;
        })
        .join("\n");
      const body = fields.length > 0 ? `{\n${fields}\n  }` : "{}";
      return `  ${JSON.stringify(id)}: ${body};`;
    })
    .join("\n");
  return `/** Declared (transient) domain types from the manifest \`types\` registry (ADR 0029 §4.2), keyed by id. */\nexport interface DomainTypes {\n${entries}\n}\n`;
}

/**
 * Compose the full `domain.d.ts`: the datasource table spine (every source,
 * ADR 0029 §6) followed by the manifest `types` registry (§4.2). This is the
 * single entry point the `domaintypes` op uses.
 */
export function emitDomainModel(
  sources: SourceSchema[],
  defaultSource: string | undefined,
  types: DomainTypeRegistry,
): string {
  const spine = emitDomainDtsForSources(sources, defaultSource);
  const registry = emitDomainTypeRegistry(types);
  return registry ? `${spine}\n${registry}` : spine;
}

// --- domain bindings: the typed data-object accessor (ADR 0029 §6) ----------

/** The generated bindings file's basename — the typed `openDomain()` accessor
 * materialised next to `domain.d.ts` under a project's `.nanobpm/`. */
export const DOMAIN_BINDINGS = "domain.ts";

/** The primary-key column for a table's `Table` gateway: its first declared PK
 * column, or `id` when the table declares none. */
function primaryKeyOf(t: TableMeta): string {
  return t.columns.find((c) => c.primaryKey)?.name ?? "id";
}

/**
 * Emit `domain.ts`: a typed data-object accessor over the App's default
 * datasource, so a worker writes `db.orders.insert({...})` / `db.orders.get(id)`
 * instead of hand-writing SQL (ADR 0029 §6 — the Delphi data-module idea). The
 * generic `Table<T>` runtime lives in `data-sdk.ts`; this generated module only
 * binds each table name to its row type (from `domain.d.ts`) and primary key, so
 * it imports nothing but the sibling SDK (relative) and a type-only `domain.d.ts`
 * — staying dual-runtime (Node + Deno) and erased at `deno compile`. When the App
 * declares multiple datasources the accessor reflects the default one; `db.raw`
 * is the escape hatch (and `openDataSource(name)` reaches the others).
 */
export function emitDomainBindings(
  sources: SourceSchema[],
  defaultSource?: string,
): string {
  const header =
    "// AUTO-GENERATED by nanobpmn from the datasource schema (ADR 0029 §4.1/§6).\n" +
    "// The typed data-object layer: `openDomain()` → one `Table<T>` per table, so\n" +
    "// workers manipulate typed rows instead of hand-writing SQL. Do not edit —\n" +
    "// regenerated from the live tables. Erased to plain JS at `deno compile`.\n" +
    "// eslint-disable\n";

  const multi = sources.length > 1;
  const def = defaultSource && sources.some((s) => s.source === defaultSource)
    ? defaultSource
    : sources[0]?.source;
  const primary = sources.find((s) => s.source === def);
  const tables = primary?.tables ?? [];

  // Interface names must match `domain.d.ts` exactly (prefixed when multi-source).
  const typeName = (table: string) =>
    multi ? prefixedInterfaceName(primary!.source, table) : interfaceName(table);
  const propKey = (table: string) =>
    isIdent(table) ? table : JSON.stringify(table);

  const typeImports = [...new Set(tables.map((t) => typeName(t.name)))].sort();
  const importTypes = typeImports.length > 0
    ? `import type { ${typeImports.join(", ")} } from "./${DOMAIN_DTS}";\n`
    : "";

  const fields = tables
    .map((t) => `  readonly ${propKey(t.name)}: Table<${typeName(t.name)}>;`)
    .join("\n");
  const binds = tables
    .map((t) =>
      `    ${propKey(t.name)}: raw.table<${typeName(t.name)}>(` +
      `${JSON.stringify(t.name)}, ${JSON.stringify(primaryKeyOf(t))}),`
    )
    .join("\n");

  const domainIface = `export interface Domain {\n` +
    `  /** The underlying datasource handle — the raw-SQL escape hatch. */\n` +
    `  readonly raw: DataSource;\n` +
    (fields ? `${fields}\n` : "") +
    `  /** Close the underlying connection. */\n  close(): void;\n}`;

  const openFn = `/**\n` +
    ` * Open the App's ${multi ? `default (\`${def}\`) ` : ""}datasource as a typed domain: each table is\n` +
    ` * a \`Table<T>\` gateway (\`db.orders.insert(...)\`, \`db.orders.get(id)\`), with the\n` +
    ` * raw \`DataSource\` at \`db.raw\` for anything the gateway doesn't cover.\n` +
    ` */\n` +
    `export async function openDomain(source?: string): Promise<Domain> {\n` +
    `  const raw = await openDataSource(source);\n` +
    `  return {\n    raw,\n` +
    (binds ? `${binds}\n` : "") +
    `    close: () => raw.close(),\n  };\n}`;

  return `${header}\n` +
    `import { openDataSource, type DataSource, type Table } from "./data-sdk.ts";\n` +
    importTypes +
    `\n${domainIface}\n\n${openFn}\n\nexport type { Table };\n`;
}

// --- worker bindings: typed job workers keyed by taskType (ADR 0033 §3) ------

/** The generated worker-IO type map's basename — the `taskType → {in,out}` map
 * materialised next to `domain.d.ts` under a project's `.nanobpm/`. */
export const WORKER_BINDINGS_DTS = "workers.d.ts";

/** The generated typed-`defineWorker` wrapper's basename — re-exports the worker
 * SDK and overrides `defineWorker` with a taskType-keyed typed signature. */
export const WORKER_BINDINGS_TS = "workers.ts";

/** One `workers[]` declaration the codegen reads: the join `taskType` plus the
 * declared input/output domain-type ids (ADR 0033 §3). */
export interface WorkerBindingDecl {
  taskType: string;
  inputType?: string;
  outputType?: string;
}

/** The TS type expression for a worker's declared input/output type id: an index
 * into the `DomainTypes` registry when the id is declared, else `undefined` (the
 * caller omits the entry so the taskType falls back to `WorkerVars`). */
function typeRefFor(id: string | undefined, declared: Set<string>): string | undefined {
  return id != null && declared.has(id) ? `DomainTypes[${JSON.stringify(id)}]` : undefined;
}

/**
 * Emit `workers.d.ts`: the machine-readable bridge from the process model to the
 * worker type system (ADR 0033 §3). Each declared worker's `taskType` maps to the
 * TS type of its input payload (`job.variables`, from `inputType`) and its result
 * (from `outputType`), resolved against the `DomainTypes` registry in
 * `domain.d.ts`. Only entries whose type is actually declared are emitted; a
 * taskType with no declared type is absent and falls back to `WorkerVars` in the
 * typed `defineWorker`. Imports `DomainTypes` type-only (so it erases at compile
 * and stays dual-runtime) — and only when at least one entry references it, so an
 * app with no declared types still yields a valid (empty) map.
 */
export function emitWorkerBindings(
  workers: WorkerBindingDecl[],
  declaredTypeIds: Iterable<string>,
): string {
  const declared = new Set(declaredTypeIds);
  const propKey = (t: string) => JSON.stringify(t);
  const inputs: string[] = [];
  const outputs: string[] = [];
  for (const w of workers) {
    if (typeof w?.taskType !== "string" || w.taskType.length === 0) continue;
    const inRef = typeRefFor(w.inputType, declared);
    if (inRef) inputs.push(`  ${propKey(w.taskType)}: ${inRef};`);
    const outRef = typeRefFor(w.outputType, declared);
    if (outRef) outputs.push(`  ${propKey(w.taskType)}: ${outRef};`);
  }

  const header =
    "// AUTO-GENERATED by nanobpmn from the App manifest (ADR 0033 §3).\n" +
    "// The bridge from the process model to the worker type system: each declared\n" +
    "// worker's `taskType` maps to the TS type of its input payload (`job.variables`)\n" +
    "// and result, so the typed `defineWorker` types a handler by its job type. Do\n" +
    "// not edit — regenerated from the manifest. Erased to plain JS at compile.\n" +
    "// eslint-disable\n";

  const needsRegistry = inputs.length > 0 || outputs.length > 0;
  const importTypes = needsRegistry
    ? `import type { DomainTypes } from "./${DOMAIN_DTS}";\n`
    : "";

  const inputsIface = inputs.length > 0
    ? `export interface WorkerInputs {\n${inputs.join("\n")}\n}\n`
    : `export interface WorkerInputs {}\n`;
  const outputsIface = outputs.length > 0
    ? `export interface WorkerOutputs {\n${outputs.join("\n")}\n}\n`
    : `export interface WorkerOutputs {}\n`;

  return `${header}\n` +
    importTypes +
    `\n/** Untyped fallback for a job whose worker declares no input/output type. */\n` +
    `export type WorkerVars = Record<string, unknown>;\n\n` +
    `/** Input payload (\`job.variables\`) per declared worker, keyed by \`taskType\`. */\n` +
    inputsIface +
    `\n/** Output payload (worker result) per declared worker, keyed by \`taskType\`. */\n` +
    outputsIface;
}

/**
 * The static typed-`defineWorker` wrapper (`workers.ts`). It re-exports the whole
 * worker SDK and overrides `defineWorker` with a signature that keys off the
 * `type:` string literal: when a job type is present in the generated
 * `WorkerInputs`/`WorkerOutputs` (ADR 0033 §3), the handler's `job.variables` and
 * result are typed from the declared domain type; otherwise they fall back to
 * `WorkerVars`. The body is a pass-through — the wire stays untyped JSON (ADR 0029
 * §3). This file never changes with the schema, so it is written verbatim (unlike
 * the regenerated `workers.d.ts`). Dual-runtime: only type-level constructs +
 * `export *` + a type assertion, so Node's strip-only mode accepts it (ADR 0036).
 */
export function emitWorkerBindingsRuntime(): string {
  return "// AUTO-GENERATED by nanobpmn (ADR 0033 §3): the typed `defineWorker`.\n" +
    "// Re-exports the worker SDK and overrides `defineWorker` with a taskType-keyed\n" +
    "// typed signature (job.variables + result typed from the worker's declared\n" +
    "// input/output domain type). Erased to a pass-through at runtime. Do not edit.\n" +
    "// eslint-disable\n\n" +
    `import { defineWorker as defineWorkerRaw } from "./worker-sdk.ts";\n` +
    `import type { WorkerOptions } from "./worker-sdk.ts";\n` +
    `import type { WorkerInputs, WorkerOutputs, WorkerVars } from "./${WORKER_BINDINGS_DTS}";\n\n` +
    `export * from "./worker-sdk.ts";\n\n` +
    `type InFor<K extends string> = K extends keyof WorkerInputs ? WorkerInputs[K] : WorkerVars;\n` +
    `type OutFor<K extends string> = K extends keyof WorkerOutputs ? WorkerOutputs[K] : WorkerVars;\n\n` +
    `/**\n` +
    ` * Typed \`defineWorker\`: the handler's \`job.variables\` and result are typed from\n` +
    ` * the worker's declared \`inputType\`/\`outputType\` (ADR 0033 §3), keyed off the\n` +
    ` * \`type\` string literal. A job type with no declared type falls back to WorkerVars.\n` +
    ` */\n` +
    `export function defineWorker<K extends string>(\n` +
    `  opts: { type: K } & WorkerOptions<InFor<K> & object, OutFor<K> & object>,\n` +
    `): void {\n` +
    `  defineWorkerRaw(opts as unknown as WorkerOptions);\n` +
    `}\n`;
}
