// Optional diagram-interchange (DI) generation for the code-first surfaces.
//
// The authoring surfaces emit *semantic* BPMN — process, flow nodes, sequence
// flows, messages — with no `bpmndi:` diagram: the engine runs the semantic model
// and DI is an authoring-time concern only (a derived flow has no diagram of its
// own). This module layers bpmn-io's `bpmn-auto-layout` on top so a derived model
// opens with a rendered, auto-laid-out diagram in a modeller/viewer, which makes
// code-first flows visually inspectable, reviewable, and round-trippable.
//
// `bpmn-auto-layout` is an *optional peer dependency*: the runtime client stays
// dependency-free and the layout engine is loaded lazily, only when you ask for a
// diagram. Install it alongside this package (`npm i bpmn-auto-layout`) to use
// these helpers.
//
// DI is best-effort and *regenerable* — the semantic model (`declarativeToBpmn`)
// stays authoritative. Re-run layout whenever the flow changes rather than
// hand-editing the generated diagram.

import type { DeclarativeFlow } from "./types.js";
import { declarativeToBpmn } from "./declarative.js";

/**
 * Generate BPMN diagram interchange (DI) for a BPMN XML string using bpmn-io's
 * `bpmn-auto-layout`, returning the same model with an auto-laid-out diagram
 * (`bpmndi:BPMNDiagram`). Works on DI-less *or* already-laid-out input, and
 * preserves `zeebe:` extension elements (task definitions, message
 * subscriptions) through the round-trip.
 *
 * Requires the optional peer dependency `bpmn-auto-layout` to be installed; a
 * clear error is thrown if it is missing.
 */
export async function layoutBpmn(bpmnXml: string): Promise<string> {
  let layoutProcess: typeof import("bpmn-auto-layout").layoutProcess;
  try {
    ({ layoutProcess } = await import("bpmn-auto-layout"));
  } catch (cause) {
    throw new Error(
      'layoutBpmn requires the optional peer dependency "bpmn-auto-layout". ' +
        "Install it to generate diagram layout: npm i bpmn-auto-layout",
      { cause },
    );
  }
  const result = await layoutProcess(bpmnXml);
  // Version-shape normalization: bpmn-auto-layout >= 1.4 (and `main`) resolves to
  // `{ xml, warnings }`; 1.3.x resolves to the laid-out XML string directly.
  return typeof result === "string" ? result : result.xml;
}

/**
 * Derive an executable BPMN model from a declarative flow AND auto-generate its
 * diagram (DI), so the model opens rendered in a modeller/viewer. Convenience
 * over `layoutBpmn(declarativeToBpmn(flow))`; async because layout is async. The
 * semantic model stays authoritative — see `declarativeToBpmn` for the DI-less
 * model the engine actually runs.
 *
 * Requires the optional peer dependency `bpmn-auto-layout` to be installed.
 */
export function declarativeToLayoutedBpmn(flow: DeclarativeFlow): Promise<string> {
  return layoutBpmn(declarativeToBpmn(flow));
}
