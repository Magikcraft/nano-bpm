// Deno unit tests for the Urban domain-type reifier (ADR 0029 §4.1 spike).
//
// CI cannot run Deno, so these are run locally with:
//   deno test --allow-read --allow-write --allow-env server/src/console/domain_types_test.ts
//
// They cover the SQLite-affinity → TS mapping, interface-name sanitising, the
// `domain.d.ts` emitter, and a full schema() → emit → write roundtrip against a
// temp manifest + SQLite db.

import { assertEquals, assertStringIncludes } from "jsr:@std/assert@1";
import type { TableMeta } from "./data_sdk.ts";
import {
  DOMAIN_BINDINGS,
  DOMAIN_DTS,
  emitDomainBindings,
  emitDomainDts,
  emitDomainDtsForSources,
  emitDomainModel,
  emitDomainTypeRegistry,
  emitWorkerBindings,
  emitWorkerBindingsRuntime,
  interfaceName,
  sqliteAffinityToTs,
  WORKER_BINDINGS_DTS,
  WORKER_BINDINGS_TS,
} from "./domain_types.ts";

Deno.test("sqliteAffinityToTs applies affinity rules + Urban overrides", () => {
  assertEquals(sqliteAffinityToTs("INTEGER"), "number");
  assertEquals(sqliteAffinityToTs("BIGINT"), "number");
  assertEquals(sqliteAffinityToTs("TEXT"), "string");
  assertEquals(sqliteAffinityToTs("VARCHAR(255)"), "string");
  assertEquals(sqliteAffinityToTs("REAL"), "number");
  assertEquals(sqliteAffinityToTs("DOUBLE"), "number");
  assertEquals(sqliteAffinityToTs("NUMERIC"), "number");
  assertEquals(sqliteAffinityToTs("BLOB"), "Uint8Array");
  assertEquals(sqliteAffinityToTs(""), "unknown"); // NONE affinity
  // Maker-friendly overrides the DB Manager's type list implies:
  assertEquals(sqliteAffinityToTs("BOOLEAN"), "boolean");
  assertEquals(sqliteAffinityToTs("TIMESTAMP"), "string");
  assertEquals(sqliteAffinityToTs("DATE"), "string");
});

Deno.test("interfaceName PascalCases and sanitises to an identifier", () => {
  assertEquals(interfaceName("customers"), "Customers");
  assertEquals(interfaceName("order_items"), "OrderItems");
  assertEquals(interfaceName("2fa-tokens"), "T_2faTokens");
});

Deno.test("emitDomainDts renders interfaces + a DomainTables map", () => {
  const tables: TableMeta[] = [
    {
      name: "customers",
      indexes: [], foreignKeys: [],
      columns: [
        { name: "id", type: "INTEGER", primaryKey: true, notNull: false },
        { name: "name", type: "TEXT", primaryKey: false, notNull: true },
        { name: "tier", type: "TEXT", primaryKey: false, notNull: false },
        { name: "active", type: "BOOLEAN", primaryKey: false, notNull: true },
      ],
    },
  ];
  const dts = emitDomainDts(tables);
  assertStringIncludes(dts, "export interface Customers {");
  assertStringIncludes(dts, "id: number;"); // PK ⇒ not null
  assertStringIncludes(dts, "name: string;"); // NOT NULL ⇒ not null
  assertStringIncludes(dts, "tier: string | null;"); // nullable widens
  assertStringIncludes(dts, "active: boolean;");
  assertStringIncludes(dts, 'export interface DomainTables {');
  assertStringIncludes(dts, '"customers": Customers;');
});

Deno.test("emitDomainDts quotes non-identifier column names", () => {
  const dts = emitDomainDts([
    {
      name: "t",
      indexes: [], foreignKeys: [],
      columns: [{ name: "first name", type: "TEXT", primaryKey: false, notNull: true }],
    },
  ]);
  assertStringIncludes(dts, '"first name": string;');
});

Deno.test("emitDomainDts handles an empty schema", () => {
  assertStringIncludes(emitDomainDts([]), "export interface DomainTables {}");
});

Deno.test("emitDomainDtsForSources is byte-identical to emitDomainDts for one source", () => {
  const tables: TableMeta[] = [{
    name: "customers",
    indexes: [], foreignKeys: [],
    columns: [
      { name: "id", type: "INTEGER", primaryKey: true, notNull: false },
      { name: "name", type: "TEXT", primaryKey: false, notNull: true },
    ],
  }];
  assertEquals(
    emitDomainDtsForSources([{ source: "app", tables }], "app"),
    emitDomainDts(tables),
  );
  // No sources at all also degrades to the empty single-source output.
  assertEquals(emitDomainDtsForSources([], "app"), emitDomainDts([]));
});

Deno.test("emitDomainDtsForSources unions sources + resolves name collisions", () => {
  const customers: TableMeta[] = [{
    name: "customers",
    indexes: [], foreignKeys: [],
    columns: [{ name: "id", type: "INTEGER", primaryKey: true, notNull: false }],
  }];
  const events: TableMeta[] = [{
    name: "customers", // same table name in a different source
    indexes: [], foreignKeys: [],
    columns: [{ name: "at", type: "TIMESTAMP", primaryKey: false, notNull: true }],
  }];
  const dts = emitDomainDtsForSources(
    [{ source: "app", tables: customers }, { source: "analytics", tables: events }],
    "analytics",
  );
  // Collision-free, source-prefixed interfaces.
  assertStringIncludes(dts, "export interface AppCustomers {");
  assertStringIncludes(dts, "export interface AnalyticsCustomers {");
  // A DomainSources map keyed by alias then wire table name.
  assertStringIncludes(dts, "export interface DomainSources {");
  assertStringIncludes(dts, '"app": {');
  assertStringIncludes(dts, '"customers": AppCustomers;');
  assertStringIncludes(dts, '"analytics": {');
  assertStringIncludes(dts, '"customers": AnalyticsCustomers;');
  // DomainTables aliases the declared default source.
  assertStringIncludes(dts, 'export type DomainTables = DomainSources["analytics"];');
});

Deno.test("emitDomainDtsForSources emits an empty object for a source with no tables", () => {
  const dts = emitDomainDtsForSources(
    [
      { source: "app", tables: [{ name: "t", indexes: [], foreignKeys: [], columns: [{ name: "id", type: "INTEGER", primaryKey: true, notNull: false }] }] },
      { source: "empty", tables: [] },
    ],
    "app",
  );
  assertStringIncludes(dts, '"empty": {};');
  // Falls back to the first source when the default is unknown.
  const dts2 = emitDomainDtsForSources(
    [
      { source: "a", tables: [] },
      { source: "b", tables: [] },
    ],
    "missing",
  );
  assertStringIncludes(dts2, 'export type DomainTables = DomainSources["a"];');
});

Deno.test("emitDomainTypeRegistry renders declared types with refs, optional + list", () => {
  const dts = emitDomainTypeRegistry({
    taxLine: { fields: { amount: { type: "number" }, note: { type: "string", optional: true } } },
    taxSubmission: {
      name: "Tax Submission",
      fields: {
        filedAt: { type: "datetime" },
        active: { type: "boolean", optional: true },
        blob: { type: "json" },
        lines: { type: "taxLine", list: true },
        ref: { type: "unknownType" }, // unresolved → unknown
      },
    },
  });
  assertStringIncludes(dts, "export interface DomainTypes {");
  assertStringIncludes(dts, '"taxLine": {');
  assertStringIncludes(dts, "amount: number;");
  assertStringIncludes(dts, "note?: string;"); // optional widens the key
  assertStringIncludes(dts, '"taxSubmission": {');
  assertStringIncludes(dts, "filedAt: string;"); // datetime → ISO string
  assertStringIncludes(dts, "active?: boolean;");
  assertStringIncludes(dts, "blob: unknown;"); // json → unknown
  assertStringIncludes(dts, 'lines: DomainTypes["taxLine"][];'); // ref + list
  assertStringIncludes(dts, "ref: unknown;"); // unresolved ref degrades
});

Deno.test("emitDomainTypeRegistry is empty for an empty registry", () => {
  assertEquals(emitDomainTypeRegistry({}), "");
});

Deno.test("emitDomainModel appends the registry to the table spine", () => {
  const tables: TableMeta[] = [{
    name: "customers",
    indexes: [], foreignKeys: [],
    columns: [{ name: "id", type: "INTEGER", primaryKey: true, notNull: false }],
  }];
  // No declared types → byte-identical to the table-only spine.
  assertEquals(
    emitDomainModel([{ source: "app", tables }], "app", {}),
    emitDomainDtsForSources([{ source: "app", tables }], "app"),
  );
  // With declared types → both DomainTables and DomainTypes are present.
  const full = emitDomainModel(
    [{ source: "app", tables }],
    "app",
    { note: { fields: { text: { type: "string" } } } },
  );
  assertStringIncludes(full, "export interface DomainTables {");
  assertStringIncludes(full, "export interface DomainTypes {");
  assertStringIncludes(full, '"note": {');
});

Deno.test("schema() → emit → write roundtrip (the CLI op's path)", async () => {
  const root = await Deno.makeTempDir();
  await Deno.writeTextFile(
    `${root}/nano.app.json`,
    JSON.stringify({
      data: { default: "app", sources: { app: { driver: "sqlite", url: "file:./app.db" } } },
    }),
  );
  const cwd = `${root}/workers/gen`;
  await Deno.mkdir(cwd, { recursive: true });

  const { openDataSource } = await import("./data_sdk.ts");
  const db = await openDataSource("app", { cwd });
  await db.exec(
    "CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT NOT NULL, tier TEXT, credit REAL)",
  );
  const tables = await db.schema();
  db.close();

  // Mirror the data-cli `domaintypes` op: emit from schema() and write.
  const text = emitDomainDts(tables);
  const outDir = `${root}/.nanobpm`;
  await Deno.mkdir(outDir, { recursive: true });
  const path = `${outDir}/${DOMAIN_DTS}`;
  await Deno.writeTextFile(path, text);

  assertEquals(tables.length, 1);
  const written = await Deno.readTextFile(path);
  assertEquals(written, text);
  assertStringIncludes(written, "export interface Customers {");
  assertStringIncludes(written, "id: number;");
  assertStringIncludes(written, "tier: string | null;");
  assertStringIncludes(written, "credit: number | null;");
  assertStringIncludes(written, '"customers": Customers;');

  await Deno.remove(root, { recursive: true });
});

Deno.test("emitDomainBindings renders openDomain with a typed Table per table", () => {
  const tables: TableMeta[] = [
    {
      name: "orders",
      columns: [
        { name: "id", type: "INTEGER", notNull: true, primaryKey: true },
        { name: "customer_id", type: "INTEGER", notNull: true, primaryKey: false },
        { name: "status", type: "TEXT", notNull: false, primaryKey: false },
      ],
      indexes: [],
      foreignKeys: [],
    },
  ];
  const out = emitDomainBindings([{ source: "app", tables }], "app");
  // Imports only the sibling SDK (relative) + a type-only domain.d.ts — no
  // jsr:/https: so it survives the Node fallback loader (ADR 0036).
  assertStringIncludes(out, 'import { openDataSource, type DataSource, type Table } from "./data-sdk.ts";');
  assertStringIncludes(out, `import type { Orders } from "./${DOMAIN_DTS}";`);
  assertStringIncludes(out, "export interface Domain {");
  assertStringIncludes(out, "readonly raw: DataSource;");
  assertStringIncludes(out, "readonly orders: Table<Orders>;");
  assertStringIncludes(out, "export async function openDomain(source?: string): Promise<Domain> {");
  // The gateway is bound with the table's real primary key.
  assertStringIncludes(out, 'orders: raw.table<Orders>("orders", "id"),');
  assertStringIncludes(out, "close: () => raw.close(),");
  assertEquals(DOMAIN_BINDINGS, "domain.ts");
});

Deno.test("emitDomainBindings uses the first declared PK (not always id)", () => {
  const tables: TableMeta[] = [
    {
      name: "sessions",
      columns: [
        { name: "token", type: "TEXT", notNull: true, primaryKey: true },
        { name: "user_id", type: "INTEGER", notNull: true, primaryKey: false },
      ],
      indexes: [],
      foreignKeys: [],
    },
  ];
  const out = emitDomainBindings([{ source: "app", tables }], "app");
  assertStringIncludes(out, 'sessions: raw.table<Sessions>("sessions", "token"),');
});

Deno.test("emitDomainBindings handles an empty schema (raw-only Domain)", () => {
  const out = emitDomainBindings([{ source: "app", tables: [] }], "app");
  assertStringIncludes(out, "export interface Domain {");
  assertStringIncludes(out, "readonly raw: DataSource;");
  assertStringIncludes(out, "export async function openDomain(");
  // No table imports and no Table fields.
  assertEquals(out.includes(`from "./${DOMAIN_DTS}"`), false);
  assertEquals(out.includes("raw.table<"), false);
});

Deno.test("Table<T> CRUD roundtrip via the data SDK (ADR 0029 §6)", async () => {
  const root = await Deno.makeTempDir();
  await Deno.writeTextFile(
    `${root}/nano.app.json`,
    JSON.stringify({
      data: { default: "app", sources: { app: { driver: "sqlite", url: "file:./app.db" } } },
    }),
  );
  const { openDataSource } = await import("./data_sdk.ts");
  const db = await openDataSource("app", { cwd: root });
  try {
    await db.exec(
      "CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT NOT NULL, qty INTEGER NOT NULL, status TEXT)",
    );
    const orders = db.table<
      { id: number; item: string; qty: number; status: string | null }
    >("orders", "id");

    // insert returns the new rowid; get fetches it back typed.
    const id = Number(await orders.insert({ item: "Widget", qty: 3, status: "received" }));
    assertEquals(id, 1);
    const row = await orders.get(id);
    assertEquals(row?.item, "Widget");
    assertEquals(row?.qty, 3);

    // update by pk, then re-read.
    assertEquals(await orders.update(id, { status: "fulfilled" }), 1);
    assertEquals((await orders.get(id))?.status, "fulfilled");

    // find/count on an equality filter.
    await orders.insert({ item: "Gadget", qty: 12, status: "received" });
    assertEquals((await orders.find({ status: "received" })).length, 1);
    assertEquals(await orders.count(), 2);
    assertEquals((await orders.findOne({ item: "Gadget" }))?.qty, 12);

    // delete by pk.
    assertEquals(await orders.delete(id), 1);
    assertEquals(await orders.count(), 1);
    assertEquals(await orders.get(id), undefined);
  } finally {
    db.close();
    await Deno.remove(root, { recursive: true });
  }
});

// Regression: the generated `domain.d.ts` emits row types as `interface`s, and
// an interface (unlike an inline type literal) has no implicit string index
// signature — so it is NOT assignable to `Record<string, unknown>`. The Table
// bound must therefore be `T extends object` (ADR 0029 §6.1), or every worker
// that imports the reified `domain.ts` fails `deno check` with TS2344. This test
// passes an `interface` through `db.table<T>()`, so the harness's own type-check
// is the guard (a regression to `T extends Row` would fail to compile here).
interface RegOrderRow {
  id: number;
  item: string;
  qty: number;
  status: string | null;
}

Deno.test("Table<T> accepts an interface row type (ADR 0029 §6.1 — the domain.d.ts shape)", async () => {
  const root = await Deno.makeTempDir();
  await Deno.writeTextFile(
    `${root}/nano.app.json`,
    JSON.stringify({
      data: { default: "app", sources: { app: { driver: "sqlite", url: "file:./app.db" } } },
    }),
  );
  const { openDataSource } = await import("./data_sdk.ts");
  const db = await openDataSource("app", { cwd: root });
  try {
    await db.exec(
      "CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT NOT NULL, qty INTEGER NOT NULL, status TEXT)",
    );
    // The `interface` type argument is the crux — this line would not compile
    // under a `T extends Row` bound.
    const orders = db.table<RegOrderRow>("orders", "id");
    const id = Number(await orders.insert({ item: "Widget", qty: 3, status: "received" }));
    assertEquals((await orders.get(id))?.item, "Widget");
    assertEquals(await orders.update(id, { status: "done" }), 1);
    assertEquals((await orders.findOne({ status: "done" }))?.id, id);
    assertEquals(await orders.count({ item: "Widget" }), 1);
  } finally {
    db.close();
    await Deno.remove(root, { recursive: true });
  }
});

Deno.test("emitWorkerBindings maps taskType → declared input/output types (ADR 0033 §3)", () => {
  const out = emitWorkerBindings(
    [
      { taskType: "save-order", inputType: "orderRequest", outputType: "savedOrder" },
      { taskType: "review-order", inputType: "savedOrder" }, // input only
      { taskType: "noop" }, // no declared types → absent
      { taskType: "ghost", inputType: "undeclared" }, // undeclared id → absent
    ],
    ["orderRequest", "savedOrder"],
  );
  assertStringIncludes(out, `import type { DomainTypes } from "./${DOMAIN_DTS}";`);
  assertStringIncludes(out, "export interface WorkerInputs {");
  assertStringIncludes(out, `"save-order": DomainTypes["orderRequest"];`);
  assertStringIncludes(out, `"review-order": DomainTypes["savedOrder"];`);
  assertStringIncludes(out, "export interface WorkerOutputs {");
  assertStringIncludes(out, `"save-order": DomainTypes["savedOrder"];`);
  // Type-less and undeclared-type workers never appear.
  assertEquals(out.includes('"noop"'), false);
  assertEquals(out.includes('"ghost"'), false);
  // review-order has no output → not in WorkerOutputs.
  assertEquals(out.split(`"review-order"`).length, 2); // exactly one occurrence
});

Deno.test("emitWorkerBindings with no declared worker types is a valid empty map", () => {
  const out = emitWorkerBindings([{ taskType: "noop" }], []);
  assertStringIncludes(out, "export interface WorkerInputs {}");
  assertStringIncludes(out, "export interface WorkerOutputs {}");
  assertStringIncludes(out, "export type WorkerVars = Record<string, unknown>;");
  // No registry import when nothing references it.
  assertEquals(out.includes("import type { DomainTypes }"), false);
});

Deno.test("emitWorkerBindingsRuntime is a taskType-keyed typed defineWorker wrapper", () => {
  const out = emitWorkerBindingsRuntime();
  assertStringIncludes(out, `export * from "./worker-sdk.ts";`);
  assertStringIncludes(out, `import type { WorkerInputs, WorkerOutputs, WorkerVars } from "./${WORKER_BINDINGS_DTS}";`);
  assertStringIncludes(out, "export function defineWorker<K extends string>(");
  assertStringIncludes(out, "opts: { type: K } & WorkerOptions<InFor<K> & object, OutFor<K> & object>,");
  // Node strip-only safety (ADR 0036): no TS parameter properties/enums.
  assertEquals(out.includes("constructor(private"), false);
  assertEquals(out.includes("enum "), false);
  // File basenames are stable.
  assertEquals(WORKER_BINDINGS_TS, "workers.ts");
  assertEquals(WORKER_BINDINGS_DTS, "workers.d.ts");
});
