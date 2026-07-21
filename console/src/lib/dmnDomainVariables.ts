// Maps the manifest's domain-type binding for a decision (ADR 0029 §5) onto the
// variable shape dmn-js's FEEL editor (`@bpmn-io/feel-editor`) autocompletes
// against. The type-in-scope resolution lives in the schema package (tested);
// this only translates its neutral scope tree into feel-editor `Variable`s and
// keeps the DmnModeler wiring thin.

import { decisionScope, type ScopeVar } from "@nanobpm/nano-app-schema";

/**
 * A feel-editor variable. `entries` drive nested path completion
 * (`customer.address.city`); `isList` follows FEEL list projection.
 */
export interface FeelVariable {
  name: string;
  /** Short type hint shown beside the suggestion. */
  detail?: string;
  isList?: boolean;
  entries?: FeelVariable[];
}

function toFeelVariables(vars: ScopeVar[]): FeelVariable[] {
  return vars.map((v) => {
    const out: FeelVariable = { name: v.name };
    if (v.type) out.detail = v.type;
    if (v.list) out.isList = true;
    if (v.entries && v.entries.length > 0) out.entries = toFeelVariables(v.entries);
    return out;
  });
}

/**
 * The FEEL variables in scope for a decision's input expressions, derived from
 * its `bindings[]` domain type (ADR 0029 §5). Empty when the decision has no
 * bound type — the maker then sees only dmn-js's own inferred variables.
 */
export function decisionFeelVariables(manifest: unknown, decisionId: string | undefined): FeelVariable[] {
  const scope = decisionScope(manifest, decisionId);
  return scope ? toFeelVariables(scope) : [];
}
