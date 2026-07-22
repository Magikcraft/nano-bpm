// Urban components — element templates that turn a generic BPMN task into a
// typed, configured component with its own properties panel and palette entry
// (ADR 0033). This is the Delphi-component analog for the process canvas: the
// element template is a component's *design-time face*; its `taskDefinition`
// type is the seam to a runtime `workers[].taskType` (ADR 0022), and its
// input/output properties are FEEL fields that will be scoped to the bound
// domain type (ADR 0029, generalising the DMN injection shipped in #197).
//
// SPIKE SCOPE: this is a small bundled sample set proving the load → palette →
// properties-panel → apply loop end to end. In the shipped design these come
// from installed component packs (ADR 0007) and the project, not this constant.

/** The Zeebe (Camunda 8 / "Cloud") element-template JSON shape. Typed loosely —
 *  the authoritative schema is `@camunda/zeebe-element-templates-json-schema`,
 *  which the modeler's validator enforces at `elementTemplates.set()`. */
export interface ElementTemplate {
  $schema?: string;
  id: string;
  name: string;
  description?: string;
  appliesTo: string[];
  elementType?: { value: string };
  properties: Array<Record<string, unknown>>;
}

const SCHEMA =
  "https://unpkg.com/@camunda/zeebe-element-templates-json-schema@0.44.0/resources/schema.json";

/**
 * Read Thermostat — mirrors ADR 0022's `read-thermostat` worker. A service task
 * pre-bound to that task type, with one FEEL input (`room`) and one output
 * variable.
 */
const readThermostat: ElementTemplate = {
  $schema: SCHEMA,
  id: "io.nanobpm.urban.read-thermostat",
  name: "Read Thermostat",
  description: "Read a room's current temperature (Urban component).",
  appliesTo: ["bpmn:Task"],
  elementType: { value: "bpmn:ServiceTask" },
  properties: [
    {
      type: "Hidden",
      value: "read-thermostat",
      binding: { type: "zeebe:taskDefinition:type" },
    },
    {
      label: "Room",
      type: "String",
      feel: "optional",
      binding: { type: "zeebe:input", name: "room" },
    },
    {
      label: "Result variable",
      type: "String",
      value: "temperature",
      binding: { type: "zeebe:output", source: "= temperature" },
    },
  ],
};

/**
 * Classify (LLM) — mirrors ADR 0022's (E) LLM-as-worker. A service task bound to
 * the `classify` task type with a FEEL `text` input and a categorised output.
 */
const classifyLlm: ElementTemplate = {
  $schema: SCHEMA,
  id: "io.nanobpm.urban.classify-llm",
  name: "Classify (LLM)",
  description: "Classify text with an LLM job worker (Urban component).",
  appliesTo: ["bpmn:Task"],
  elementType: { value: "bpmn:ServiceTask" },
  properties: [
    {
      type: "Hidden",
      value: "classify",
      binding: { type: "zeebe:taskDefinition:type" },
    },
    {
      label: "Text",
      type: "String",
      feel: "required",
      binding: { type: "zeebe:input", name: "text" },
    },
    {
      label: "Categories (comma separated)",
      type: "String",
      binding: { type: "zeebe:input", name: "categories" },
    },
    {
      label: "Result variable",
      type: "String",
      value: "category",
      binding: { type: "zeebe:output", source: "= category" },
    },
  ],
};

/** The bundled sample components surfaced in the palette and template chooser. */
export const urbanComponents: ElementTemplate[] = [readThermostat, classifyLlm];
