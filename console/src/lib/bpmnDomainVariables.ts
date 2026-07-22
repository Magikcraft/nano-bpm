// Maps the manifest's domain-type binding for a process (ADR 0030 duality) onto
// the variable shape bpmn-js's FEEL editor autocompletes against. A process is
// the motion of a typed domain object, so its bound type scopes every FEEL
// expression in the diagram — element-template/component inputs, gateway
// conditions, output mappings. The type-in-scope resolution lives in the schema
// package (tested); this only translates its neutral scope tree and keeps the
// BpmnModeler wiring thin.

import { processScope } from "@nanobpm/nano-app-schema";
import { toFeelVariables, type FeelVariable } from "./feelVariables";

/**
 * The FEEL variables in scope for a process's expressions, derived from its
 * `bindings[]` domain type (ADR 0030). Empty when the process has no bound type
 * — the maker then sees only bpmn-js's own extracted process variables.
 */
export function processFeelVariables(manifest: unknown, processId: string | undefined): FeelVariable[] {
  const scope = processScope(manifest, processId);
  return scope ? toFeelVariables(scope) : [];
}
