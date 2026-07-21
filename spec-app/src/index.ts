// Public entry point for the Urban App manifest library: the JSON-Schema-derived
// AppManifest types, the project symbol index (ADR 0029), and the fail-closed
// validator (ADR 0027 §4). Consumed by the console App panels and the Deno App
// loader/compile gate.

export * from "./symbol-index.ts";
export * from "./domain-types.ts";
export * from "./validate.ts";
export type { AppManifest } from "../gen/nano-app.d.ts";
