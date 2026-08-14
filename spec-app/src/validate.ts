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
import { bodyPaths, dataQueryCalls, isDeclaredType, resolveBodyPath } from "./feel.ts";

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
  const typeIds = new Set(Object.keys(manifest.types ?? {}));

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
    // bodyType names a declared domain type — the FEEL scope for this trigger's
    // action expressions (ADR 0029 §5). Intra-manifest, runs without an index.
    if (t.bodyType != null && !typeIds.has(t.bodyType)) {
      push(`/triggers/${i}/bodyType`, `bodyType "${t.bodyType}" is not a declared domain type`, "unknown-type");
    }
    // With a resolvable bodyType in scope, the action's FEEL fields must only
    // reference `body` paths that exist in that type — wrong paths become
    // diagnostics instead of runtime nulls (ADR 0029 §5). The path walker is
    // shared with the completer so autocomplete and validation cannot disagree.
    if (t.bodyType != null && isDeclaredType(manifest, t.bodyType)) {
      for (const field of ["variables", "correlationKey"] as const) {
        const expr = t.action?.[field];
        if (typeof expr !== "string") continue;
        for (const segs of bodyPaths(expr)) {
          const res = resolveBodyPath(manifest, t.bodyType, segs);
          if (res.kind === "unknown") {
            push(
              `/triggers/${i}/action/${field}`,
              `body path "body.${segs.join(".")}" has no field "${res.segment}" in domain type "${t.bodyType}"`,
              "unknown-path",
            );
          }
        }
      }
    }
    // `data.query("alias", …)` in an action's App-tier FEEL must name a declared
    // datasource (ADR 0024 §5) — the same bind-to-the-alias rule as a form's
    // dataSource. Intra-manifest (no index), and independent of bodyType: the
    // read is App-tier, so it is sound here (unlike engine FEEL). The default
    // form `data.query(sql)` names no alias and is covered by `data.default`.
    for (const field of ["variables", "correlationKey"] as const) {
      const expr = t.action?.[field];
      if (typeof expr !== "string") continue;
      for (const call of dataQueryCalls(expr)) {
        if (call.source != null && !sourceNames.has(call.source)) {
          push(
            `/triggers/${i}/action/${field}`,
            `data.query datasource "${call.source}" is not declared in data.sources`,
            "unknown-datasource",
          );
        }
      }
    }
  });
  const agent = manifest.surfaces?.chat?.agent;
  if (agent != null && !llmNames.has(agent)) {
    push("/surfaces/chat/agent", `chat agent "${agent}" is not declared in llm`, "unknown-llm");
  }

  // surfaces.pages.sourceName names a declared datasource (ADR 0027 §4): the page
  // runtime reads /app/data/<source> from this alias, so an unknown one is a
  // dangling reference just like data.default or a form field's dataSource.
  const pagesSource = manifest.surfaces?.pages?.sourceName;
  if (pagesSource != null && !sourceNames.has(pagesSource)) {
    push(
      "/surfaces/pages/sourceName",
      `pages datasource "${pagesSource}" is not declared in data.sources`,
      "unknown-datasource",
    );
  }

  // workers[].llm names a declared llm; workers[].outputType names a declared type.
  const workers: any[] = manifest.workers ?? [];
  const wiredTaskTypes = new Set(
    workers.map((w) => w?.taskType).filter((t): t is string => typeof t === "string" && t.length > 0),
  );
  workers.forEach((w, i) => {
    if (w.llm != null && !llmNames.has(w.llm)) {
      push(`/workers/${i}/llm`, `worker llm "${w.llm}" is not declared in llm`, "unknown-llm");
    }
    if (w.inputType != null && !typeIds.has(w.inputType)) {
      push(`/workers/${i}/inputType`, `inputType "${w.inputType}" is not a declared domain type`, "unknown-type");
    }
    if (w.outputType != null && !typeIds.has(w.outputType)) {
      push(`/workers/${i}/outputType`, `outputType "${w.outputType}" is not a declared domain type`, "unknown-type");
    }
  });

  // A task type declared external must NOT also be hosted in workers[] — it is one or the other.
  const externalTaskTypes: any[] = manifest.externalTaskTypes ?? [];
  externalTaskTypes.forEach((t, i) => {
    if (typeof t === "string" && wiredTaskTypes.has(t)) {
      push(
        `/externalTaskTypes/${i}`,
        `task type "${t}" is declared external but also wired in workers[] — an external task cannot be app-hosted`,
        "external-task-conflict",
      );
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

  // bindings[] declare the domain type in scope for a form's / decision's FEEL
  // (ADR 0029 §5). The bound model (form id or decision id) must resolve against
  // the project index, and the type must be a declared domain type. This puts a
  // type in scope for that model's expressions the same way trigger.bodyType does
  // for a trigger's action.
  const formIds = index && new Set(index.forms.map((f) => f.id));
  const bindings: any[] = manifest.bindings ?? [];
  bindings.forEach((b, i) => {
    if (b.form != null && formIds && !formIds.has(b.form)) {
      push(`/bindings/${i}/form`, `no model declares a form with id "${b.form}"`, "unknown-form");
    }
    if (b.decision != null && decisionIds && !decisionIds.has(b.decision)) {
      push(`/bindings/${i}/decision`, `no model declares a decision with id "${b.decision}"`, "unknown-decision");
    }
    if (b.process != null && processIds && !processIds.has(b.process)) {
      push(`/bindings/${i}/process`, `no deployed process has id "${b.process}"`, "unknown-process");
    }
    if (b.type != null && !typeIds.has(b.type)) {
      push(`/bindings/${i}/type`, `type "${b.type}" is not a declared domain type`, "unknown-type");
    }
  });

  // Form-field datasource bindings (ADR 0024 §5): a choice field's `dataSource`
  // must name a declared datasource — bind to the alias, never a driver — so the
  // form survives the SQLite→Postgres flip. The field's `query` shape is the
  // maker's; only the alias resolves here (columns are checked at run time by
  // the datasource, ADR 0024 phase 2).
  if (index) {
    for (const form of index.forms) {
      for (const field of form.fields) {
        const ds = field.dataSource;
        if (!ds) continue;
        const at = `/forms/${form.id}/fields/${field.key}/dataSource`;
        if (!sourceNames.has(ds.source)) {
          push(
            `${at}/source`,
            `datasource "${ds.source}" is not declared in data.sources`,
            "unknown-datasource",
          );
        }
      }
    }
  }

  // instanceTracking[]: `activeStatuses`/`terminalStatuses` select rows by their
  // `statusField`, so declaring statuses without the column to read them against
  // is incoherent — the runtime would have no field to filter on and would poll
  // every row. Flag it here rather than let the mismatch surface as a silent
  // full-table scan. The two selectors are also mutually exclusive: one is an
  // allow-list (fail-closed), the other an exclusion-list (fail-open).
  const tracking: any[] = manifest.instanceTracking ?? [];
  tracking.forEach((t, i) => {
    if (t?.activeStatuses != null && t?.statusField == null) {
      push(
        `/instanceTracking/${i}/activeStatuses`,
        "activeStatuses requires statusField (the column those statuses are read from)",
        "instance-tracking-incoherent",
      );
    }
    if (t?.terminalStatuses != null && t?.statusField == null) {
      push(
        `/instanceTracking/${i}/terminalStatuses`,
        "terminalStatuses requires statusField (the column those statuses are read from)",
        "instance-tracking-incoherent",
      );
    }
    if (t?.activeStatuses != null && t?.terminalStatuses != null) {
      push(
        `/instanceTracking/${i}/terminalStatuses`,
        "activeStatuses and terminalStatuses are mutually exclusive (allow-list vs. exclusion-list); set only one",
        "instance-tracking-incoherent",
      );
    }
  });

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
