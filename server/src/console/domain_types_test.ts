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
  DOMAIN_DTS,
  emitDomainDts,
  emitDomainDtsForSources,
  emitDomainModel,
  emitDomainTypeRegistry,
  interfaceName,
  sqliteAffinityToTs,
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
