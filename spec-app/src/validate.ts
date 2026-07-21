// The fail-closed manifest validator (ADR 0027 §4).
//
// Two stages, shape-first:
//   1. Schema validation against nano-app.schema.json (Ajv, draft 2020-12).
//   2. Cross-reference rules — every id the manifest names must resolve, either
//      within the manifest (connections, llm, datasources) or against the
//      project symbol index (processes, messages, decisions).
//
// If the manifest fails shape validation we return those errors and stop: a
// structurally broken manifest can't be meaningfully cross-referenced. Every
// diagnostic carries a JSON Pointer to the offending node so the console can
// place an inline marker (and the compile/boot gates can print a precise error).

import Ajv2020 from "ajv/dist/2020.js";
import schema from "../nano-app.schema.json" with { type: "json" };
import type { SymbolIndex } from "./symbol-index.ts";
import { DOMAIN_PRIMITIVES } from "./symbol-index.ts";

export interface Diagnostic {
  severity: "error";
  /** JSON Pointer (RFC 6901) to the offending node. */
  pointer: string;
  message: string;
  /** Stable code: "schema" for shape errors, else the cross-reference rule id. */
  code: string;
}

export interface ValidationResult {
  ok: boolean;
  diagnostics: Diagnostic[];
}

// strictRequired is disabled for the `oneOf: [{required:["start"]},…]` idiom
// where the properties are declared on the parent schema (see scripts/validate.mjs).
const ajv = new Ajv2020({ allErrors: true, strict: true, strictRequired: false });
const validateSchema = ajv.compile(schema as object);

function schemaDiagnostics(errors: unknown): Diagnostic[] {
  const list = (errors as any[]) || [];
  return list.map((e) => {
    let pointer = e.instancePath || "";
    let message = e.message || "is invalid";
    if (e.keyword === "required" && e.params?.missingProperty) {
      pointer = `${pointer}/${e.params.missingProperty}`;
      message = `is required`;
    } else if (e.keyword === "additionalProperties" && e.params?.additionalProperty) {
      pointer = `${pointer}/${e.params.additionalProperty}`;
      message = `is not a known property`;
    }
    return { severity: "error" as const, pointer: pointer || "/", message, code: "schema" };
  });
}

function crossReferenceDiagnostics(manifest: any, index?: SymbolIndex): Diagnostic[] {
  const diags: Diagnostic[] = [];
  const push = (pointer: string, message: string, code: string) =>
    diags.push({ severity: "error", pointer, message, code });

  const sourceNames = new Set(Object.keys(manifest.data?.sources ?? {}));
  const connectionNames = new Set(Object.keys(manifest.connections ?? {}));
  const llmNames = new Set(Object.keys(manifest.llm ?? {}));

  const processIds = index && new Set(index.processes.map((p) => p.id));
  const messageNames = index && new Set(index.messages);
  const decisionIds = index && new Set(index.decisions.map((d) => d.id));

  // data.default names a declared source.
  if (manifest.data?.default != null && !sourceNames.has(manifest.data.default)) {
    push("/data/default", `datasource "${manifest.data.default}" is not declared in data.sources`, "unknown-datasource");
  }

  // triggers[].connection / .auth reference a declared connection; action targets resolve.
  const triggers: any[] = manifest.triggers ?? [];
  triggers.forEach((t, i) => {
    if (t.connection != null && !connectionNames.has(t.connection)) {
      push(`/triggers/${i}/connection`, `connection "${t.connection}" is not declared in connections`, "unknown-connection");
    }
    if (typeof t.auth === "string") {
      const ref = t.auth.includes(":") ? t.auth.slice(t.auth.indexOf(":") + 1) : t.auth;
      if (!connectionNames.has(ref)) {
        push(`/triggers/${i}/auth`, `auth references connection "${ref}", which is not declared in connections`, "unknown-connection");
      }
    }
    if (t.action?.start != null && processIds && !processIds.has(t.action.start)) {
      push(`/triggers/${i}/action/start`, `no deployed process has id "${t.action.start}"`, "unknown-process");
    }
    if (t.action?.message != null && messageNames && !messageNames.has(t.action.message)) {
      push(`/triggers/${i}/action/message`, `no model declares a message named "${t.action.message}"`, "unknown-message");
    }
  });

  // surfaces.chat.agent names a declared llm.
  const agent = manifest.surfaces?.chat?.agent;
  if (agent != null && !llmNames.has(agent)) {
    push("/surfaces/chat/agent", `chat agent "${agent}" is not declared in llm`, "unknown-llm");
  }

  // workers[].llm names a declared llm.
  const workers: any[] = manifest.workers ?? [];
  workers.forEach((w, i) => {
    if (w.llm != null && !llmNames.has(w.llm)) {
      push(`/workers/${i}/llm`, `worker llm "${w.llm}" is not declared in llm`, "unknown-llm");
    }
  });

  // llm[].output.decision names a deployed decision.
  for (const [name, binding] of Object.entries(manifest.llm ?? {}) as [string, any][]) {
    const decision = binding?.output?.decision;
    if (decision != null && decisionIds && !decisionIds.has(decision)) {
      push(`/llm/${name}/output/decision`, `no model declares a decision with id "${decision}"`, "unknown-decision");
    }
  }

  // types[].fields[].type is a primitive or resolves to another declared type id
  // (nominal — ADR 0029 §4 / ADR 0031). This runs without an index (intra-manifest).
  const primitives = new Set<string>(DOMAIN_PRIMITIVES);
  const typeIds = new Set(Object.keys(manifest.types ?? {}));
  for (const [id, t] of Object.entries(manifest.types ?? {}) as [string, any][]) {
    for (const [fieldKey, f] of Object.entries(t?.fields ?? {}) as [string, any][]) {
      const ft = f?.type;
      if (typeof ft === "string" && !primitives.has(ft) && !typeIds.has(ft)) {
        push(
          `/types/${id}/fields/${fieldKey}/type`,
          `field type "${ft}" is neither a primitive nor a declared domain type`,
          "unknown-type",
        );
      }
    }
  }

  return diags;
}

/**
 * Validate a manifest fail-closed. Pass the project `index` to enable the
 * model-resolving rules (start/message/decision); omit it for a manifest-only
 * lint (schema + intra-manifest references). Returns `ok:false` with the
 * diagnostics whenever anything fails.
 */
export function validateManifest(manifest: unknown, index?: SymbolIndex): ValidationResult {
  const valid = validateSchema(manifest);
  if (!valid) {
    return { ok: false, diagnostics: schemaDiagnostics(validateSchema.errors) };
  }
  const diagnostics = crossReferenceDiagnostics(manifest, index);
  return { ok: diagnostics.length === 0, diagnostics };
}
