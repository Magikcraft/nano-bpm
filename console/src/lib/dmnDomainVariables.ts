// Maps the manifest's domain-type binding for a decision (ADR 0029 §5) onto the
// variable shape dmn-js's FEEL editor (`@bpmn-io/feel-editor`) autocompletes
// against. The type-in-scope resolution lives in the schema package (tested);
// this only translates its neutral scope tree into feel-editor variables and
// keeps the DmnModeler wiring thin.

import { decisionScope } from "@nanobpm/nano-app-schema";
import { toFeelVariables, type FeelVariable } from "./feelVariables";

export type { FeelVariable };

/**
 * The FEEL variables in scope for a decision's input expressions, derived from
 * its `bindings[]` domain type (ADR 0029 §5). Empty when the decision has no
 * bound type — the maker then sees only dmn-js's own inferred variables.
 */
export function decisionFeelVariables(manifest: unknown, decisionId: string | undefined): FeelVariable[] {
  const scope = decisionScope(manifest, decisionId);
  return scope ? toFeelVariables(scope) : [];
}
