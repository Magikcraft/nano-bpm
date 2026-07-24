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
//   import { openDataSource } from "@nanobpm/data";
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
// is the *other* source; merging it in is the documented follow-up (see the ADR
// delta) — this spike reifies the table spine.

import type { ColumnMeta, TableMeta } from "./data_sdk.ts";
import { openDataSource } from "./data_sdk.ts";

// Runtime adapter: the host calls that differ between Deno (native `Deno.*`) and
// Node (`node:fs`). Mirrors data_sdk.ts so this file degrades to Node >= 22.6
// (ADR 0036); only the file writer needs it — the emitter is pure.
interface DomainRuntime {
  mkdir(path: string): Promise<void>;
  writeTextFile(path: string, data: string): Promise<void>;
}
const RT: DomainRuntime = ((): DomainRuntime => {
  const g = globalThis as unknown as {
    Deno?: {
      mkdir(p: string, o: { recursive: boolean }): Promise<void>;
      writeTextFile(p: string, d: string): Promise<void>;
    };
  };
  if (g.Deno) {
    const d = g.Deno;
    return {
      mkdir: (p) => d.mkdir(p, { recursive: true }),
      writeTextFile: (p, data) => d.writeTextFile(p, data),
    };
  }
  return {
    mkdir: async (p) => {
      await (await import("node:fs/promises")).mkdir(p, { recursive: true });
    },
    writeTextFile: async (p, data) => {
      await (await import("node:fs/promises")).writeFile(p, data, "utf8");
    },
  };
})();

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

/** One `export interface` block for a table. */
function tableInterface(t: TableMeta): string {
  const fields = t.columns
    .map((c) => {
      const key = isIdent(c.name) ? c.name : JSON.stringify(c.name);
      const pk = c.primaryKey ? " (primary key)" : "";
      return `  /** ${c.type || "?"}${pk} */\n  ${key}: ${fieldType(c)};`;
    })
    .join("\n");
  return `export interface ${interfaceName(t.name)} {\n${fields}\n}`;
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

/**
 * Generate a project's `domain.d.ts` from a live datasource: introspect the
 * schema, emit the types, and write `<outDir>/domain.d.ts`. Returns the emitted
 * text. `source` is the datasource alias (default when omitted, per ADR 0024).
 */
export async function generateDomainDts(
  opts: { source?: string; outDir: string; cwd?: string },
): Promise<{ path: string; text: string; tables: number }> {
  const db = await openDataSource(opts.source, opts.cwd ? { cwd: opts.cwd } : undefined);
  try {
    const tables = await db.schema();
    const text = emitDomainDts(tables);
    await RT.mkdir(opts.outDir);
    const path = `${opts.outDir.replace(/\/+$/, "")}/${DOMAIN_DTS}`;
    await RT.writeTextFile(path, text);
    return { path, text, tables: tables.length };
  } finally {
    db.close();
  }
}
