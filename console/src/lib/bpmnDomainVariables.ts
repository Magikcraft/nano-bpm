// Maps the manifest's domain-type binding for a process (ADR 0030 duality) onto
// the variable shape bpmn-js's FEEL editor autocompletes against. A process is
// the motion of a typed domain object, so its bound type scopes every FEEL
// expression in the diagram — element-template/component inputs, gateway
// conditions, output mappings. The type-in-scope resolution lives in the schema
// package (tested); this only translates its neutral scope tree and keeps the
// BpmnModeler wiring thin.

import { processScope, componentOutputScope, type ComponentOutput } from "@nanobpm/nano-app-schema";
import { toFeelVariables, type FeelVariable } from "./feelVariables";

export type { ComponentOutput };

/**
 * The FEEL variables in scope for a process's expressions, derived from its
 * `bindings[]` domain type (ADR 0030). Empty when the process has no bound type
 * — the maker then sees only bpmn-js's own extracted process variables.
 */
export function processFeelVariables(manifest: unknown, processId: string | undefined): FeelVariable[] {
  const scope = processScope(manifest, processId);
  return scope ? toFeelVariables(scope) : [];
}

/**
 * The FEEL variables contributed by the diagram's component outputs (ADR 0033
 * §3): each output-mapped process variable typed by the domain type its worker
 * declares (`workers[].outputType`). `outputs` are the `{ taskType, target }`
 * pairs the BpmnModeler extracts from service-task output mappings. Empty when no
 * output maps to a worker with a declared `outputType`.
 */
export function componentOutputFeelVariables(manifest: unknown, outputs: readonly ComponentOutput[]): FeelVariable[] {
  return toFeelVariables(componentOutputScope(manifest, outputs));
}
