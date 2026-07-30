// Typed data envelopes — the code-first expression of a nano:shape.
//
// An envelope declares a named, typed payload contract IN CODE. It carries two
// things at once:
//   - a runtime schema (ordered fields + scalar types), which the model emitter
//     LIFTS into the BPMN as a `nano:shape` (under `nano:shapes` on the process)
//     plus the `io.nanobpm.dataEnvelope.in/out` `zeebe:property` on the service
//     task / message — exactly the carrier the console derives worker I/O from
//     (server/src/console/envelope_scan.rs); and
//   - a phantom TypeScript type, inferred from the field spec, so `run`/`task`
//     handlers and message payloads are statically typed at the call site.
//
// This is what makes code-first EJECTABLE to model-first: the generated `.bpmn`
// already carries the typed shapes + envelope wiring the modeller and the Fused
// Domain Model (ADR 0040) expect, so opening it in the Modeller loses nothing.
//
//   const PrReviewRoundIn = envelope("PrReviewRoundIn", {
//     prUrl: "string", repo: "string", prNumber: "integer",
//     prompt: "string", round: "integer",
//     answer: { type: "string", optional: true },
//   });
//   // PrReviewRoundIn.type  ≅  { prUrl: string; repo: string; prNumber: number;
//   //                            prompt: string; round: number; answer?: string }
import { assertIdent } from "./xml.js";
const SCALARS = new Set([
    "string",
    "integer",
    "number",
    "boolean",
    "datetime",
]);
function normaliseField(name, spec) {
    if (spec === null || (typeof spec !== "string" && typeof spec !== "object")) {
        throw new Error(`envelope field "${name}": must be a scalar type or a { type, optional?, list? } object`);
    }
    const raw = typeof spec === "string" ? { type: spec } : spec;
    if (!SCALARS.has(raw.type)) {
        throw new Error(`envelope field "${name}": unknown type "${raw.type}" (expected one of ${[...SCALARS].join(", ")})`);
    }
    return {
        name,
        type: raw.type,
        optional: typeof spec === "object" && spec.optional === true,
        list: typeof spec === "object" && spec.list === true,
    };
}
/**
 * Declare a typed data envelope. `name` becomes the `nano:shape` id lifted into
 * the model (must be a valid BPMN identifier); `fields` declares the payload.
 * The returned envelope's `type` phantom carries the inferred TS payload type.
 */
export function envelope(name, fields) {
    assertIdent("envelope name", name);
    const list = Object.entries(fields).map(([k, v]) => normaliseField(k, v));
    if (list.length === 0)
        throw new Error(`envelope "${name}" declares no fields`);
    for (const f of list)
        assertIdent("envelope field name", f.name);
    // `type` is phantom: never read at runtime; the cast keeps the value shape
    // minimal while the declared type carries the inference.
    return { name, fields: list, type: undefined };
}
