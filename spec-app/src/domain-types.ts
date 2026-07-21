// The resolved domain-type view (ADR 0029 §4 / ADR 0031). Unions the two type
// sources the console offers a maker: the *declared* `types` registry in the
// manifest, and the *inferred* candidate records the symbol index derives from
// forms (the promotion on-ramp). A third source — datasource tables via the
// 0024 schema() runtime — will join here once that runtime exists.
//
// This is the enumeration the App panels bind type pickers to, kept in one place
// so authoring and validation cannot disagree (the ADR 0029 §1 "one index" rule
// extended to types).

import type { InferredRecord, SymbolIndex } from "./symbol-index.ts";

export interface ResolvedField {
  key: string;
  /** A primitive, or the id of another declared type (nominal). */
  type: string;
  optional: boolean;
  list: boolean;
}

export interface ResolvedDomainType {
  id: string;
  name?: string;
  /** Identity discipline; "nominal" today (the structural escape hatch is reserved). */
  match: "nominal" | "structural";
  /** Datasource table this type binds to as its rest projection, if any. */
  table?: string;
  fields: ResolvedField[];
}

export interface DomainTypeResolution {
  /** Types declared in the manifest `types` registry. */
  declared: ResolvedDomainType[];
  /** Form-inferred candidates not already declared — a maker may promote these. */
  inferred: InferredRecord[];
}

/**
 * Resolve the domain types a maker can reference. Pass the project `index` to
 * include form-inferred candidates; omit it for the declared registry alone.
 */
export function resolveDomainTypes(manifest: unknown, index?: SymbolIndex): DomainTypeResolution {
  const types = (manifest as any)?.types ?? {};
  const declared: ResolvedDomainType[] = Object.entries(types).map(
    ([id, t]: [string, any]) => ({
      id,
      name: t?.name,
      match: t?.match === "structural" ? "structural" : "nominal",
      table: t?.table,
      fields: Object.entries(t?.fields ?? {}).map(([key, f]: [string, any]) => ({
        key,
        type: f?.type,
        optional: f?.optional === true,
        list: f?.list === true,
      })),
    }),
  );

  const declaredIds = new Set(declared.map((d) => d.id));
  const inferred = (index?.inferredRecords ?? []).filter((r) => !declaredIds.has(r.id));

  return { declared, inferred };
}
