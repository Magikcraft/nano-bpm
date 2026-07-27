// Translates the schema package's neutral domain scope tree (`ScopeVar`, tested
// there) into the variable shape the `@bpmn-io/feel-editor` autocompletes
// against. Shared by the DMN (decision-scoped) and BPMN (process-scoped) FEEL
// injectors so both surfaces speak the same variable vocabulary.

import { type ScopeVar } from "@nanobpm/nano-app-schema";

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

/** Maps a domain scope tree onto feel-editor variables (recursively). */
export function toFeelVariables(vars: ScopeVar[]): FeelVariable[] {
  return vars.map((v) => {
    const out: FeelVariable = { name: v.name };
    if (v.type) out.detail = v.type;
    if (v.list) out.isList = true;
    if (v.entries && v.entries.length > 0)
      out.entries = toFeelVariables(v.entries);
    return out;
  });
}
