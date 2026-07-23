// The project symbol index (ADR 0029 phase 1).
//
// Parses a project's BPMN / DMN / form-js files into a symbol table of the ids
// and shapes the manifest references. It is the single enumeration source: the
// console App panels bind pickers to it, and the manifest validator resolves
// every cross-reference against it ("id ∈ index", ADR 0027 §4).
//
// Parsing uses the same moddle/form-js model the editors use, so the index can
// never disagree with what a maker actually drew.

/// <reference path="./moddle-shims.d.ts" />
import BpmnModdle from "bpmn-moddle";
import DmnModdle from "dmn-moddle";
import zeebe from "zeebe-bpmn-moddle/resources/zeebe.json" with { type: "json" };
import { readFieldDataBinding, type FormFieldDataBinding } from "./form-data-binding.ts";

/** A model file to index. `kind` selects the parser; `text` is the file body. */
export interface ModelFile {
  path: string;
  kind: "bpmn" | "dmn" | "form";
  text: string;
}

export interface UserTaskSymbol {
  id: string;
  name?: string;
  /** zeebe:formDefinition formId, if the task references a form. */
  formId?: string;
}

export interface ProcessSymbol {
  id: string;
  name?: string;
  executable: boolean;
  /** Message names on message start events (targets of action.message that start an instance). */
  messageStartEvents: string[];
  userTasks: UserTaskSymbol[];
  /** zeebe:taskDefinition types of service tasks (worker taskTypes). */
  serviceTaskTypes: string[];
}

export interface DecisionSymbol {
  id: string;
  name?: string;
}

export interface FormFieldSymbol {
  key: string;
  type: string;
  /** Datasource binding (ADR 0024 §5), when the field declares one. */
  dataSource?: FormFieldDataBinding;
}

export interface FormSymbol {
  id: string;
  fields: FormFieldSymbol[];
}

/** The primitive field types a domain type (ADR 0029 §4 / ADR 0031) may use. */
export type DomainPrimitive =
  | "string"
  | "number"
  | "integer"
  | "boolean"
  | "date"
  | "datetime"
  | "json";

export const DOMAIN_PRIMITIVES: readonly DomainPrimitive[] = [
  "string",
  "number",
  "integer",
  "boolean",
  "date",
  "datetime",
  "json",
];

export interface InferredField {
  key: string;
  type: DomainPrimitive;
}

/**
 * A candidate domain record inferred from a form's fields — the ADR 0029 §4
 * on-ramp: the maker either promotes it into the `types` registry or binds it
 * to a datasource table. Inference is heuristic (form keys are free strings),
 * so it is a suggestion, never a silently-invented schema.
 */
export interface InferredRecord {
  /** Candidate type id — the source form's id. */
  id: string;
  source: "form";
  sourcePath: string;
  fields: InferredField[];
}

export interface SymbolIndex {
  processes: ProcessSymbol[];
  /** All declared bpmn:message names (targets of action.message). */
  messages: string[];
  decisions: DecisionSymbol[];
  forms: FormSymbol[];
  /** Candidate domain records inferred from forms (ADR 0029 §4 promotion on-ramp). */
  inferredRecords: InferredRecord[];
  /** Non-fatal problems encountered while parsing a model file. */
  parseErrors: { path: string; message: string }[];
}

type MEl = { $type?: string; [k: string]: any };

function extensionValues(el: MEl): MEl[] {
  return (el.extensionElements && el.extensionElements.values) || [];
}

function findExt(el: MEl, type: string): MEl | undefined {
  return extensionValues(el).find((v) => v.$type === type);
}

/** Depth-first walk of flowElements (handles sub-processes and ad-hoc scopes). */
function walkFlowElements(container: MEl, visit: (el: MEl) => void): void {
  for (const fe of (container.flowElements as MEl[]) || []) {
    visit(fe);
    if (fe.flowElements) walkFlowElements(fe, visit);
  }
}

async function indexBpmn(model: ModelFile, index: SymbolIndex): Promise<void> {
  const moddle = new BpmnModdle({ zeebe });
  const { rootElement } = await moddle.fromXML(model.text);
  const roots: MEl[] = (rootElement && rootElement.rootElements) || [];

  for (const root of roots) {
    if (root.$type === "bpmn:Message" && typeof root.name === "string") {
      if (!index.messages.includes(root.name)) index.messages.push(root.name);
    }
  }

  for (const root of roots) {
    if (root.$type !== "bpmn:Process") continue;
    const proc: ProcessSymbol = {
      id: root.id,
      name: root.name,
      executable: root.isExecutable !== false,
      messageStartEvents: [],
      userTasks: [],
      serviceTaskTypes: [],
    };
    walkFlowElements(root, (fe) => {
      switch (fe.$type) {
        case "bpmn:UserTask": {
          const form = findExt(fe, "zeebe:FormDefinition");
          proc.userTasks.push({ id: fe.id, name: fe.name, formId: form?.formId });
          break;
        }
        case "bpmn:ServiceTask":
        case "bpmn:BusinessRuleTask":
        case "bpmn:ScriptTask":
        case "bpmn:SendTask": {
          const td = findExt(fe, "zeebe:TaskDefinition");
          if (td && typeof td.type === "string" && !proc.serviceTaskTypes.includes(td.type)) {
            proc.serviceTaskTypes.push(td.type);
          }
          break;
        }
        case "bpmn:StartEvent": {
          for (const def of (fe.eventDefinitions as MEl[]) || []) {
            if (def.$type === "bpmn:MessageEventDefinition" && def.messageRef?.name) {
              proc.messageStartEvents.push(def.messageRef.name);
            }
          }
          break;
        }
      }
    });
    index.processes.push(proc);
  }
}

async function indexDmn(model: ModelFile, index: SymbolIndex): Promise<void> {
  const moddle = new DmnModdle();
  const { rootElement } = await moddle.fromXML(model.text);
  for (const drg of (rootElement && rootElement.drgElement) || []) {
    if (drg.$type === "dmn:Decision") {
      index.decisions.push({ id: drg.id, name: drg.name });
    }
  }
}

function collectFormFields(components: MEl[], out: FormFieldSymbol[]): void {
  for (const c of components || []) {
    if (typeof c.key === "string" && typeof c.type === "string") {
      const field: FormFieldSymbol = { key: c.key, type: c.type };
      const binding = readFieldDataBinding(c);
      if (binding) field.dataSource = binding;
      out.push(field);
    }
    // Layout components (groups, dynamic lists) nest their own components.
    if (Array.isArray(c.components)) collectFormFields(c.components as MEl[], out);
  }
}

/**
 * Map a form-js component `type` to a domain primitive (ADR 0029 §4). Heuristic
 * and deliberately conservative — anything not clearly numeric/boolean/temporal
 * falls back to `string`, and the maker confirms on promotion.
 */
export function formTypeToPrimitive(formType: string): DomainPrimitive {
  switch (formType) {
    case "number":
      return "number";
    case "checkbox":
      return "boolean";
    case "datetime":
      return "datetime";
    default:
      return "string";
  }
}

function indexForm(model: ModelFile, index: SymbolIndex): void {
  const doc = JSON.parse(model.text) as MEl;
  const fields: FormFieldSymbol[] = [];
  collectFormFields((doc.components as MEl[]) || [], fields);
  index.forms.push({ id: doc.id, fields });
  if (typeof doc.id === "string" && fields.length > 0) {
    index.inferredRecords.push({
      id: doc.id,
      source: "form",
      sourcePath: model.path,
      fields: fields.map((f) => ({ key: f.key, type: formTypeToPrimitive(f.type) })),
    });
  }
}

/**
 * Build the symbol index from a project's model files. Parse failures are
 * collected in `parseErrors` rather than thrown, so one malformed model does not
 * blind the index to the rest of the project.
 */
export async function buildSymbolIndex(models: ModelFile[]): Promise<SymbolIndex> {
  const index: SymbolIndex = {
    processes: [],
    messages: [],
    decisions: [],
    forms: [],
    inferredRecords: [],
    parseErrors: [],
  };
  for (const model of models) {
    try {
      if (model.kind === "bpmn") await indexBpmn(model, index);
      else if (model.kind === "dmn") await indexDmn(model, index);
      else if (model.kind === "form") indexForm(model, index);
    } catch (err) {
      index.parseErrors.push({
        path: model.path,
        message: err instanceof Error ? err.message : String(err),
      });
    }
  }
  return index;
}

/** Classify a model file by extension (helper for callers listing a project dir). */
export function modelKindOf(path: string): ModelFile["kind"] | undefined {
  if (path.endsWith(".bpmn")) return "bpmn";
  if (path.endsWith(".dmn")) return "dmn";
  if (path.endsWith(".form")) return "form";
  return undefined;
}
