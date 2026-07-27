// Urban domain-type reifier — ADR 0029 §4.1 + §6 (spike).
//
// The datasource schema is the *spine* of the domain model: a table is already a
// declared record type, so `DataSource.schema()`'s `TableMeta` reifies straight
// into a named TypeScript record. This module turns those tables into a generated
// `domain-rows.d.ts` (the "models directory" makers asked for — but generated, never
// hand-edited, so it can't drift from the DB), and the worker SDK + App loader
// type against it. Types are a compile/boot-time contract only: they are erased
// at `deno compile`, and the shipped App is still untyped JSON on the wire
// (ADR 0029 §3) — the engine stays Zeebe-pure.
//
//   import { emitDomainDts } from "./domain-types.ts";
//   const tables = await (await openDataSource("app")).schema();
//   await Deno.writeTextFile("nano-generated/domain-rows.d.ts", emitDomainDts(tables));
//
// then a worker is typed end-to-end:
//
//   import type { DomainTables } from "./nano-generated/domain-rows.d.ts";
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
// materialised verbatim next to `data-cli.ts` as `nano-generated/domain-types.ts` and
// the data CLI's `domaintypes` op imports `emitDomainDts` from it. Opening the
// datasource, running `schema()`, and writing the file are the CLI op's job
// (`data_cli.ts`), which already owns the datasource seam and the file writer.

import type { ColumnMeta, TableMeta } from "./data_sdk.ts";

/**
 * The per-project generated-SDK directory. Visible (not dot-hidden) so makers
 * can read the generated accessors. Single source of truth for the TS side;
 * the Rust scaffolder (`projects.rs::GEN_DIR`) must agree.
 */
export const GEN_DIR = "nano-generated";

/** The generated file's basename, written under a project's `nano-generated/`. */
// Distinct stem from the runtime accessor `domain.ts`: TypeScript otherwise pairs
// `domain-rows.d.ts` as the *declaration of* `domain.ts`, so the accessor importing its
// own row types reads as a circular self-import (TS2303/TS2459) under a project
// `tsconfig.json` `paths` map. `-rows` also avoids colliding with the emitter
// module `domain-types.ts`. See the scaffolder's `PROJECT_TSCONFIG_JSON`.
export const DOMAIN_DTS = "domain-rows.d.ts";

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
 * Emit the full `domain-rows.d.ts` from a datasource's tables: one `interface` per
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
 * Emit `domain-rows.d.ts` for an App that declares *multiple* datasources: one
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
 * Compose the full `domain-rows.d.ts`: the datasource table spine (every source,
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
 * materialised next to `domain-rows.d.ts` under a project's `nano-generated/`. */
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
 * binds each table name to its row type (from `domain-rows.d.ts`) and primary key, so
 * it imports nothing but the sibling SDK (relative) and a type-only `domain-rows.d.ts`
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

  // Interface names must match `domain-rows.d.ts` exactly (prefixed when multi-source).
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
 * materialised next to `domain-rows.d.ts` under a project's `nano-generated/`. */
// Distinct stem from the runtime wrapper `workers.ts` (same pairing hazard as
// `DOMAIN_DTS`): a `worker-io.d.ts` reads as the declaration of `workers.ts`.
export const WORKER_BINDINGS_DTS = "worker-io.d.ts";

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
 * Emit `worker-io.d.ts`: the machine-readable bridge from the process model to the
 * worker type system (ADR 0033 §3). Each declared worker's `taskType` maps to the
 * TS type of its input payload (`job.variables`, from `inputType`) and its result
 * (from `outputType`), resolved against the `DomainTypes` registry in
 * `domain-rows.d.ts`. Only entries whose type is actually declared are emitted; a
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
  const taskTypes = [
    ...new Set(
      workers
        .map((w) => w?.taskType)
        .filter((t): t is string => typeof t === "string" && t.length > 0),
    ),
  ];
  // The model-derived set of job types the typed `defineWorker` accepts. A finite
  // union when workers are declared (so `type:` autocompletes and rejects typos);
  // `string` for an app with none, so `defineWorker` stays usable pre-declaration.
  const taskTypeUnion = taskTypes.length > 0
    ? taskTypes.map((t) => JSON.stringify(t)).join(" | ")
    : "string";
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
    `/** Every declared worker \`taskType\` (ADR 0033 §3): the model-derived set the\n` +
    ` * typed \`defineWorker\` accepts, so \`type\` autocompletes and rejects unknown jobs. */\n` +
    `export type WorkerTaskType = ${taskTypeUnion};\n\n` +
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
 * the regenerated `worker-io.d.ts`). Dual-runtime: only type-level constructs +
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
    `import type { WorkerInputs, WorkerOutputs, WorkerTaskType, WorkerVars } from "./${WORKER_BINDINGS_DTS}";\n\n` +
    `export * from "./worker-sdk.ts";\n\n` +
    `type InFor<K extends WorkerTaskType> = K extends keyof WorkerInputs ? WorkerInputs[K] : WorkerVars;\n` +
    `type OutFor<K extends WorkerTaskType> = K extends keyof WorkerOutputs ? WorkerOutputs[K] : WorkerVars;\n\n` +
    `/**\n` +
    ` * Typed \`defineWorker\`: \`type\` is constrained to the model's declared job types\n` +
    ` * (\`WorkerTaskType\`, ADR 0033 §3) so it autocompletes and rejects unknown jobs,\n` +
    ` * and the handler's \`job.variables\` + result are typed from the worker's declared\n` +
    ` * \`inputType\`/\`outputType\`. A declared job type with no domain type falls back to\n` +
    ` * WorkerVars.\n` +
    ` */\n` +
    `export function defineWorker<K extends WorkerTaskType>(\n` +
    `  opts: { type: K } & WorkerOptions<InFor<K> & object, OutFor<K> & object>,\n` +
    `): void {\n` +
    `  defineWorkerRaw(opts as unknown as WorkerOptions);\n` +
    `}\n`;
}

// --- message-carried envelopes (ADR 0040 slice 2) --------------------------

/** The generated message-payload type map's basename. Distinct stem from the
 * runtime wrapper `messages.ts` (same pairing hazard as `DOMAIN_DTS`): a
 * `message-io.d.ts` reads as the declaration of `messages.ts`. */
export const MESSAGE_BINDINGS_DTS = "message-io.d.ts";

/** The generated typed-`publishMessage` wrapper's basename — re-exports the
 * worker SDK's `publishMessage` with a messageName-keyed typed signature. */
export const MESSAGE_BINDINGS_TS = "messages.ts";

/** One message binding the codegen reads: the message `name` plus the declared
 * envelope type ids (ADR 0040 slice 2). `inputType` is the *received* payload —
 * i.e. what a `publishMessage` caller sends. `outputType` is reserved. */
export interface MessageBindingDecl {
  messageName: string;
  inputType?: string;
  outputType?: string;
}

/**
 * Emit `message-io.d.ts`: the machine-readable bridge from the process model's
 * `bpmn:message` envelopes to the message type system (ADR 0040 slice 2). Each
 * named message contributes to the `MessageName` union (so `publishMessage`
 * autocompletes and rejects unknown messages); a message whose envelope declares
 * an `inputType` also maps its name to that payload type in `MessagePayloads`, so
 * `publishMessage(name, { variables })` is typed. Only entries whose type is
 * actually declared are emitted; a message with no declared type is absent from
 * `MessagePayloads` and its payload falls back to `MessageVars`. Imports
 * `DomainTypes` type-only, and only when at least one entry references it.
 */
export function emitMessageBindings(
  messages: MessageBindingDecl[],
  declaredTypeIds: Iterable<string>,
): string {
  const declared = new Set(declaredTypeIds);
  const propKey = (m: string) => JSON.stringify(m);
  const names = [
    ...new Set(
      messages
        .map((m) => m?.messageName)
        .filter((n): n is string => typeof n === "string" && n.length > 0),
    ),
  ];
  // The model-derived set of message names the typed `publishMessage` accepts. A
  // finite union when messages are declared; `string` for a model with none, so
  // `publishMessage` stays usable pre-declaration.
  const nameUnion = names.length > 0
    ? names.map((n) => JSON.stringify(n)).join(" | ")
    : "string";
  const payloads: string[] = [];
  for (const m of messages) {
    if (typeof m?.messageName !== "string" || m.messageName.length === 0) continue;
    const ref = typeRefFor(m.inputType, declared);
    if (ref) payloads.push(`  ${propKey(m.messageName)}: ${ref};`);
  }

  const header =
    "// AUTO-GENERATED by nanobpmn from the process model (ADR 0040 slice 2).\n" +
    "// The bridge from a `bpmn:message`'s data envelope to the message type system:\n" +
    "// each named message maps to the TS type of its payload, so the typed\n" +
    "// `publishMessage` types a call by its message name. Do not edit — regenerated\n" +
    "// from the model. Erased to plain JS at compile.\n" +
    "// eslint-disable\n";

  const needsRegistry = payloads.length > 0;
  const importTypes = needsRegistry
    ? `import type { DomainTypes } from "./${DOMAIN_DTS}";\n`
    : "";

  const payloadsIface = payloads.length > 0
    ? `export interface MessagePayloads {\n${payloads.join("\n")}\n}\n`
    : `export interface MessagePayloads {}\n`;

  return `${header}\n` +
    importTypes +
    `\n/** Untyped fallback payload for a message that declares no envelope type. */\n` +
    `export type MessageVars = Record<string, unknown>;\n\n` +
    `/** Every declared message \`name\` (ADR 0040 slice 2): the model-derived set the\n` +
    ` * typed \`publishMessage\` accepts, so \`name\` autocompletes and rejects unknown messages. */\n` +
    `export type MessageName = ${nameUnion};\n\n` +
    `/** Payload (\`variables\`) per declared message, keyed by \`name\`. */\n` +
    payloadsIface;
}

/**
 * The static typed-`publishMessage` wrapper (`messages.ts`). It re-exports the
 * worker SDK's `publishMessage` (and its option/result types) and overrides
 * `publishMessage` with a signature that keys off the `name` string literal: when
 * a message is present in the generated `MessagePayloads` (ADR 0040 slice 2), the
 * call's `variables` are typed from the declared domain type; otherwise they fall
 * back to `MessageVars`. The body is a pass-through — the wire stays untyped JSON.
 * This file never changes with the model, so it is written verbatim (unlike the
 * regenerated `message-io.d.ts`). Dual-runtime: only type-level constructs + a
 * type assertion, so Node's strip-only mode accepts it (ADR 0036).
 */
export function emitMessageBindingsRuntime(): string {
  return "// AUTO-GENERATED by nanobpmn (ADR 0040 slice 2): the typed `publishMessage`.\n" +
    "// Re-exports the worker SDK's `publishMessage` and overrides it with a\n" +
    "// messageName-keyed typed signature (variables typed from the message's declared\n" +
    "// envelope). Erased to a pass-through at runtime. Do not edit.\n" +
    "// eslint-disable\n\n" +
    `import { publishMessage as publishMessageRaw } from "./worker-sdk.ts";\n` +
    `import type { PublishMessageOptions, PublishMessageResult } from "./worker-sdk.ts";\n` +
    `import type { MessageName, MessagePayloads, MessageVars } from "./${MESSAGE_BINDINGS_DTS}";\n\n` +
    `export type { PublishMessageOptions, PublishMessageResult } from "./worker-sdk.ts";\n\n` +
    `type PayloadFor<K extends MessageName> = K extends keyof MessagePayloads ? MessagePayloads[K] : MessageVars;\n\n` +
    `/**\n` +
    ` * Typed \`publishMessage\`: \`name\` is constrained to the model's declared message\n` +
    ` * names (\`MessageName\`, ADR 0040 slice 2) so it autocompletes and rejects unknown\n` +
    ` * messages, and \`variables\` are typed from the message's declared envelope. A\n` +
    ` * declared message with no domain type falls back to MessageVars.\n` +
    ` */\n` +
    `export function publishMessage<K extends MessageName>(\n` +
    `  name: K,\n` +
    `  opts?: PublishMessageOptions<PayloadFor<K> & object>,\n` +
    `): Promise<PublishMessageResult> {\n` +
    `  return publishMessageRaw(name, opts as unknown as PublishMessageOptions);\n` +
    `}\n`;
}

// --- composed motion shapes: the fuse composition algebra (ADR 0040 §9/§10) ---
//
// A composed shape is a named, model-scoped declaration authored in the Modeller
// and carried in the `.bpmn` as a `nano:shape` (see `console/src/lib/shapeCarrier.ts`
// and the Rust scan `envelope_scan.rs`). It composes existing fused entities — DB
// tables, manifest `types`, and other shapes — via four ordered operations:
//
//   carry(ref)                 spread every field of `ref`
//   project(ref, fields, via)  spread only the named fields of `ref`
//   extend(name, type, ...)    add a process-authored field
//   reference(name, ref, ...)  nest (or spread) another shape
//
// Resolution folds the ops, in author (XML) order, into a flat `DomainTypeDef`, so
// a composed shape enters the same `DomainTypes` registry a manifest `type` does —
// every downstream consumer (envelope pickers, `defineWorker`, `publishMessage`,
// FEEL scopes) types against `DomainTypes["ApprovedOrder"]` with no new codegen
// path. Broken shapes are omitted (degrade to untyped) and reported as diagnostics.

/** One composition operation of a `nano:shape`, in author (XML) order. */
export type ShapeOp =
  | { op: "carry"; ref: string }
  | { op: "project"; ref: string; fields: string[]; via?: string }
  | { op: "extend"; name: string; type: string; optional?: boolean; list?: boolean }
  | { op: "reference"; name: string; ref: string; spread?: boolean; list?: boolean };

/** A composed motion-shape declaration lifted from the model (ADR 0040 §9). */
export interface ShapeDecl {
  /** The fuse identity; also the `DomainTypes` key the resolved shape lands under. */
  id: string;
  /** Optional human label (`nano:shape name`). */
  name?: string;
  /** The defining process id, for the `model:<processId>` provenance tag. */
  process?: string;
  /** The ordered composition operations. */
  ops: ShapeOp[];
  /** Model-level metadata (`nano:meta`), free-form key/value (ADR 0040 §5). */
  meta?: Record<string, string>;
}

/** A scan/resolve-time problem with a shape, surfaced like the `workers[]` drift
 * warning (never a silent merge). A shape with any `error` diagnostic is omitted
 * from the fuse; a `warning` (e.g. a deliberate field shadow) still resolves. */
export interface ShapeDiagnostic {
  /** The offending shape id. */
  shape: string;
  kind:
    | "unresolved-reference"
    | "reference-cycle"
    | "field-conflict"
    | "unknown-field"
    | "duplicate-id"
    | "same-id-collision";
  severity: "error" | "warning";
  message: string;
}

/** The result of resolving the project's shapes against the leaf fuse. */
export interface ShapeResolution {
  /** The resolved shapes as registry entries, keyed by shape id, ready to fold
   * into the manifest `types` registry before `emitDomainModel`. */
  types: DomainTypeRegistry;
  diagnostics: ShapeDiagnostic[];
}

/** An entity the fuse can resolve a `carry`/`project`/`reference` against: its
 * field map, plus (DB tables only) its FK columns for `via`-path validation. */
interface FuseEntity {
  fields: Record<string, DomainFieldDef>;
  /** FK column name → referenced table, for `project via` validation. */
  fks?: Record<string, string>;
}

/** Map one datasource column to a domain field. A nullable column widens to
 * `optional` (the composed shape is a motion snapshot: an absent value reads as
 * undefined, not SQL NULL). The manifest keyword is inferred from the column's
 * SQLite affinity so it flows through `fieldTsType` like any declared field;
 * affinities with no keyword (BLOB/opaque) fall back to `json`. */
function columnToField(col: ColumnMeta): DomainFieldDef {
  const ts = sqliteAffinityToTs(col.type);
  const keyword = ts === "string" || ts === "number" || ts === "boolean" ? ts : "json";
  const nullable = !(col.notNull || col.primaryKey);
  return nullable ? { type: keyword, optional: true } : { type: keyword };
}

/** Build the leaf entity index the shape fold resolves references against: every
 * datasource table (by raw wire name, and by `source.table` to disambiguate a
 * name shared across sources) and every manifest `type` (by id). A raw table name
 * shared across sources keeps the first (default-source-first) binding; the
 * qualified `source.table` alias is always unambiguous. */
function leafEntityIndex(
  sources: SourceSchema[],
  types: DomainTypeRegistry,
): Map<string, FuseEntity> {
  const index = new Map<string, FuseEntity>();
  for (const s of sources) {
    for (const t of s.tables) {
      const fields: Record<string, DomainFieldDef> = {};
      const fks: Record<string, string> = {};
      for (const c of t.columns) fields[c.name] = columnToField(c);
      for (const fk of t.foreignKeys ?? []) fks[fk.column] = fk.refTable;
      const entity: FuseEntity = { fields, fks };
      index.set(`${s.source}.${t.name}`, entity);
      if (!index.has(t.name)) index.set(t.name, entity);
    }
  }
  for (const [id, def] of Object.entries(types)) {
    if (!index.has(id)) index.set(id, { fields: { ...def.fields } });
  }
  return index;
}

/** Whether a manifest field keyword resolves to a primitive (vs a nominal ref). */
function isPrimitiveKeyword(type: string): boolean {
  return Object.prototype.hasOwnProperty.call(PRIMITIVE_TS, type);
}

/** The shape ids a shape references (carry/project/reference targets, and an
 * `extend` whose type names a shape) — the edges of the shape dependency graph. */
function referencedShapeIds(shape: ShapeDecl, shapeIds: Set<string>): string[] {
  const refs = new Set<string>();
  for (const op of shape.ops) {
    if (op.op === "carry" || op.op === "project" || op.op === "reference") {
      if (shapeIds.has(op.ref)) refs.add(op.ref);
    } else if (op.op === "extend" && shapeIds.has(op.type)) {
      refs.add(op.type);
    }
  }
  return [...refs];
}

/** Structural equality of two domain field defs (`type` + normalized optional/list
 * flags), so a differing property insertion order does not read as a conflict. */
function sameFieldDef(a: DomainFieldDef, b: DomainFieldDef): boolean {
  return (
    a.type === b.type &&
    !!a.optional === !!b.optional &&
    !!a.list === !!b.list
  );
}

/**
 * Resolve the project's composed shapes into `DomainTypeDef`s and diagnostics
 * (ADR 0040 §10). Leaves (DB tables, manifest types) fuse first; shapes resolve in
 * dependency order so a shape can carry another shape regardless of declaration
 * order. A shape in a reference cycle, or one that names an unresolvable id, is
 * omitted (degrades to untyped) with an `error` diagnostic; a deliberate field
 * shadow with a differing type resolves with a `field-conflict` warning.
 */
export function resolveShapes(
  shapes: ShapeDecl[],
  types: DomainTypeRegistry,
  sources: SourceSchema[] = [],
): ShapeResolution {
  const diagnostics: ShapeDiagnostic[] = [];
  const resolved: DomainTypeRegistry = {};
  if (shapes.length === 0) return { types: resolved, diagnostics };

  const index = leafEntityIndex(sources, types);
  // Duplicate shape ids are fuse-identity collisions: since resolution keys by id,
  // a later declaration would silently shadow an earlier one. Report every id that
  // appears more than once and omit all of its declarations from resolution.
  const idCounts = new Map<string, number>();
  for (const s of shapes) if (s.id) idCounts.set(s.id, (idCounts.get(s.id) ?? 0) + 1);
  const duplicated = new Set<string>();
  for (const [id, n] of idCounts) {
    if (n > 1) {
      duplicated.add(id);
      diagnostics.push({
        shape: id,
        kind: "duplicate-id",
        severity: "error",
        message: `shape id "${id}" is declared ${n} times; ids are fuse identities and must be unique`,
      });
    }
  }
  const byId = new Map<string, ShapeDecl>();
  for (const s of shapes) if (s.id && !duplicated.has(s.id)) byId.set(s.id, s);
  const shapeIds = new Set(byId.keys());

  // Cycle detection over the shape-only dependency graph (DFS three-colour). Every
  // shape on a back edge is failed; the reported path aids the maker's fix.
  const failed = new Set<string>();
  const colour = new Map<string, 0 | 1 | 2>(); // 0=unvisited 1=on-stack 2=done
  const visit = (id: string, stack: string[]): void => {
    colour.set(id, 1);
    stack.push(id);
    for (const dep of referencedShapeIds(byId.get(id)!, shapeIds)) {
      const c = colour.get(dep) ?? 0;
      if (c === 1) {
        const cycle = stack.slice(stack.indexOf(dep)).concat(dep);
        for (const n of cycle) {
          if (!failed.has(n)) {
            failed.add(n);
            diagnostics.push({
              shape: n,
              kind: "reference-cycle",
              severity: "error",
              message: `shape "${n}" is part of a reference cycle: ${cycle.join(" → ")}`,
            });
          }
        }
      } else if (c === 0) {
        visit(dep, stack);
      }
    }
    stack.pop();
    colour.set(id, 2);
  };
  for (const id of byId.keys()) if ((colour.get(id) ?? 0) === 0) visit(id, []);

  // Topological resolution: resolve a shape only after its (non-failed) shape
  // dependencies, adding each resolved shape to the index so later shapes see it.
  const done = new Set<string>();
  const resolveOne = (shape: ShapeDecl): void => {
    if (done.has(shape.id) || failed.has(shape.id)) return;
    // Resolve shape dependencies first (they may themselves be pending).
    for (const dep of referencedShapeIds(shape, shapeIds)) {
      const d = byId.get(dep);
      if (d && !done.has(dep) && !failed.has(dep)) resolveOne(d);
    }

    const fields: Record<string, DomainFieldDef> = {};
    let broken = false;
    const addField = (name: string, field: DomainFieldDef): void => {
      const existing = fields[name];
      if (existing && !sameFieldDef(existing, field)) {
        diagnostics.push({
          shape: shape.id,
          kind: "field-conflict",
          severity: "warning",
          message:
            `field "${name}" is contributed twice with differing types; the later value wins (author-order fold)`,
        });
      }
      fields[name] = field; // last (author-order) wins
    };
    const lookup = (ref: string): FuseEntity | undefined => index.get(ref);
    const unresolved = (ref: string): void => {
      broken = true;
      diagnostics.push({
        shape: shape.id,
        kind: "unresolved-reference",
        severity: "error",
        message: `shape "${shape.id}" references unknown entity "${ref}"`,
      });
    };

    for (const op of shape.ops) {
      switch (op.op) {
        case "carry": {
          const e = lookup(op.ref);
          if (!e) { unresolved(op.ref); break; }
          for (const [k, f] of Object.entries(e.fields)) addField(k, f);
          break;
        }
        case "project": {
          const e = lookup(op.ref);
          if (!e) { unresolved(op.ref); break; }
          for (const fname of op.fields) {
            const f = e.fields[fname];
            if (!f) {
              broken = true;
              diagnostics.push({
                shape: shape.id,
                kind: "unknown-field",
                severity: "error",
                message: `projected field "${fname}" is not a field of "${op.ref}"`,
              });
              continue;
            }
            addField(fname, f);
          }
          if (op.via) validateVia(shape.id, op.via, index, diagnostics);
          break;
        }
        case "extend": {
          if (!isPrimitiveKeyword(op.type) && !index.has(op.type)) {
            broken = true;
            diagnostics.push({
              shape: shape.id,
              kind: "unresolved-reference",
              severity: "error",
              message:
                `extend field "${op.name}" has type "${op.type}", which is neither a scalar keyword nor a fused entity`,
            });
            break;
          }
          const field: DomainFieldDef = { type: op.type };
          if (op.optional) field.optional = true;
          if (op.list) field.list = true;
          addField(op.name, field);
          break;
        }
        case "reference": {
          const e = lookup(op.ref);
          if (!e) { unresolved(op.ref); break; }
          if (op.spread) {
            for (const [k, f] of Object.entries(e.fields)) addField(k, f);
          } else {
            const field: DomainFieldDef = { type: op.ref };
            if (op.list) field.list = true;
            addField(op.name, field);
          }
          break;
        }
      }
    }

    // same-id collision: the shape id shadows a leaf entity it does not source.
    const sourcesRef = new Set(
      shape.ops.flatMap((o) => (o.op === "extend" ? [] : [o.ref])),
    );
    if (index.has(shape.id) && !sourcesRef.has(shape.id)) {
      diagnostics.push({
        shape: shape.id,
        kind: "same-id-collision",
        severity: "error",
        message:
          `shape id "${shape.id}" collides with an existing fused entity it does not compose`,
      });
      broken = true;
    }

    done.add(shape.id);
    if (broken) {
      failed.add(shape.id);
      return;
    }
    const def: DomainTypeDef = { name: shape.name, fields };
    resolved[shape.id] = def;
    // Later shapes may carry this one; expose its resolved fields to the index.
    index.set(shape.id, { fields });
  };
  for (const s of shapes) if (s.id && !duplicated.has(s.id)) resolveOne(s);

  return { types: resolved, diagnostics };
}

/** Validate a `project via` FK path (`Entity.column[.column...]`): the leading
 * entity must resolve and carry the named column (an FK column on a DB table, or
 * any field on a manifest type/shape). Deeper hops are validated leniently — the
 * FK target is followed when the leading entity is a DB table with that FK. */
function validateVia(
  shape: string,
  via: string,
  index: Map<string, FuseEntity>,
  diagnostics: ShapeDiagnostic[],
): void {
  const parts = via.split(".");
  if (parts.length < 2) {
    diagnostics.push({
      shape,
      kind: "unknown-field",
      severity: "warning",
      message: `via path "${via}" is not of the form Entity.column`,
    });
    return;
  }
  let entity = index.get(parts[0]);
  if (!entity) {
    diagnostics.push({
      shape,
      kind: "unknown-field",
      severity: "warning",
      message: `via path "${via}" starts at unknown entity "${parts[0]}"`,
    });
    return;
  }
  for (let i = 1; i < parts.length; i++) {
    const col = parts[i];
    const hasField = Object.prototype.hasOwnProperty.call(entity.fields, col);
    const fkTarget = entity.fks?.[col];
    if (!hasField && !fkTarget) {
      diagnostics.push({
        shape,
        kind: "unknown-field",
        severity: "warning",
        message: `via path "${via}" hops through unknown column "${col}"`,
      });
      return;
    }
    // Follow the FK to the next entity when we can; otherwise stop (lenient).
    const next = fkTarget ? index.get(fkTarget) : undefined;
    if (!next) return;
    entity = next;
  }
}
