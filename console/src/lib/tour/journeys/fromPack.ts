// Pack-contributed journeys: turning a manifest `TourSpec` into a runnable
// `Journey` (ADR 0049 §7, #414 Part C).
//
// This is the single place structural validation happens. The wire format is
// deliberately free-form (`Extension.tours` is `Array<{[k: string]: unknown}>`,
// mirroring how `components` is carried) precisely so the step union is not typed
// in three places — Rust, OpenAPI, and here. The host is a carrier; this module is
// the validator; `jwulf/nano-ide`'s `npm run validate` is the same rule set applied
// at authoring time so a pack author gets told at publish rather than by a step
// vanishing in someone else's browser.
//
// A pack ships DATA, never code, so the two things a journey normally expresses as
// functions arrive as names instead: `precondition`/`successWhen` name a gate from
// a closed set, and a handoff's verification can only be "is something polling this
// job type". Everything unrecognised is dropped rather than guessed at.

import type { Extension } from "../../../gen";
import { registerJourney } from "../registry.ts";
import {
  hasCluster,
  hasJsRuntime,
  hasProject,
  hasTraces,
  isPolling,
} from "../preconditions.ts";
import type {
  Journey,
  Precondition,
  Predicate,
  Step,
  TourContext,
} from "../types";

/** The closed gate vocabulary, mapped to the console's own precondition library. */
const GATES: Record<string, Precondition> = {
  hasJsRuntime,
  hasProject,
  hasCluster,
  hasTraces,
};

/** Gates that can return "repair", and therefore require a repair step. */
const REPAIRABLE = new Set(["hasJsRuntime", "hasCluster"]);

const STEP_KINDS = new Set(["spotlight", "note", "handoff"]);
const PROFILES = new Set(["studio", "observe"]);

/** ADR 0049 caps a journey at five steps: one needing a detour is split. */
const MAX_STEPS = 5;

/**
 * Namespaced id for a pack journey.
 *
 * **Load-bearing, not cosmetic.** `registerJourney` is keyed by id and last write
 * wins, so an unprefixed pack tour declaring `id: "overview"` would *replace* a
 * built-in journey — silently, and for every user with that pack installed.
 * Prefixing makes collision impossible and makes provenance legible in analytics
 * and in a `?tour=` deep link.
 */
export function packJourneyId(packId: string, tourId: string): string {
  return `pack:${packId}:${tourId}`;
}

const str = (v: unknown): string | undefined =>
  typeof v === "string" && v.trim() ? v : undefined;

/**
 * Dev-only warning.
 *
 * `import.meta.env` is injected by Vite and simply does not exist when this module
 * is imported by a Node unit test — reading `.DEV` off it there throws, which would
 * turn every "reject malformed input" path into a crash. Hence the guarded read.
 */
function isDev(): boolean {
  try {
    return Boolean(import.meta.env?.DEV);
  } catch {
    return false;
  }
}

function warn(message: string): void {
  // A malformed pack tour is the pack author's bug: degrade quietly for the user,
  // loudly for the developer.
  if (isDev()) console.warn(`[tour] pack tour ignored — ${message}`);
}

function gateFor(raw: unknown, where: string): Precondition | null | undefined {
  if (raw === undefined) return undefined;
  const name = str(raw);
  const gate = name ? GATES[name] : undefined;
  if (!gate) {
    warn(`${where}: unknown gate ${JSON.stringify(raw)}`);
    return null;
  }
  return gate;
}

/**
 * Build one step, or `null` when it is malformed.
 *
 * `nested` marks a repair replacement: a repair step does not itself repair, so a
 * nested `repair` is dropped rather than honoured — that matches the authoring
 * contract and stops a pack nesting arbitrarily deep.
 */
function stepFrom(raw: unknown, where: string, nested: boolean): Step | null {
  if (typeof raw !== "object" || raw === null) {
    warn(`${where}: step is not an object`);
    return null;
  }
  const o = raw as Record<string, unknown>;
  const id = str(o.id);
  const title = str(o.title);
  const body = str(o.body);
  if (!id || !title || !body) {
    warn(`${where}: step needs id, title and body`);
    return null;
  }
  const kind = str(o.kind) ?? "spotlight";
  if (!STEP_KINDS.has(kind)) {
    warn(`${where}/${id}: unknown step kind ${kind}`);
    return null;
  }
  const route = str(o.route);
  if (route && !route.startsWith("/")) {
    warn(`${where}/${id}: route must be an absolute path`);
    return null;
  }

  const gate = gateFor(o.precondition, `${where}/${id}`);
  if (gate === null) return null;

  let repair: Step | undefined;
  if (o.repair !== undefined) {
    if (nested) {
      warn(`${where}/${id}: a repair step cannot itself have a repair`);
    } else {
      repair = stepFrom(o.repair, where, true) ?? undefined;
    }
  }
  // A gate that can demand a repair, with none authored, would silently drop the
  // step at resolution time — costing the user exactly the hint they needed. The
  // check is on the manifest gate NAME (`hasJsRuntime`), not the precondition's
  // kebab id (`has-js-runtime`), because the manifest name is the pack's vocabulary.
  const gateName = str(o.precondition);
  if (gateName && REPAIRABLE.has(gateName) && !repair) {
    warn(`${where}/${id}: gated on ${gateName} but authors no repair step`);
    return null;
  }

  const base = {
    id,
    title,
    body,
    ...(route ? { route } : {}),
    ...(gate ? { precondition: gate } : {}),
    ...(repair ? { repair } : {}),
    ...(o.optional === true ? { optional: true } : {}),
  };

  if (kind === "note") return { ...base, kind: "note" };

  if (kind === "handoff") {
    const copy = str(o.copy);
    if (!copy) {
      warn(`${where}/${id}: a handoff step needs something to copy`);
      return null;
    }
    const jobType = str(o.verifyPollingJobType);
    return {
      ...base,
      kind: "handoff",
      copy,
      ...(str(o.copyLabel) ? { copyLabel: str(o.copyLabel)! } : {}),
      // The only verification expressible without code: auto-advance once an
      // external worker is seen polling that job type (#404 supplies `consumers`;
      // absent, `isPolling` is false and the step stays self-reported).
      ...(jobType ? { verify: isPolling(jobType) as Predicate } : {}),
    };
  }

  const selector = str(o.selector);
  if (!selector) {
    warn(`${where}/${id}: a spotlight step needs a selector`);
    return null;
  }
  const side = str(o.side) as "top" | "right" | "bottom" | "left" | undefined;
  const align = str(o.align) as "start" | "center" | "end" | undefined;
  return {
    ...base,
    kind: "spotlight",
    selector,
    ...(side ? { side } : {}),
    ...(align ? { align } : {}),
  };
}

/**
 * Build a `Journey` from one manifest tour, or `null` when it is unusable.
 *
 * Whole-journey rejection is reserved for defects that make it meaningless (no id,
 * no title, no blurb for its card, no valid steps, over the step cap). A single bad
 * step drops just that step — a pack should not lose a whole journey to one typo,
 * and the remaining steps still teach something.
 */
export function journeyFromPackTour(
  packId: string,
  raw: unknown,
): Journey | null {
  if (typeof raw !== "object" || raw === null) {
    warn(`${packId}: tour is not an object`);
    return null;
  }
  const o = raw as Record<string, unknown>;
  const tourId = str(o.id);
  const title = str(o.title);
  const blurb = str(o.blurb);
  if (!tourId || !title || !blurb) {
    warn(`${packId}: tour needs id, title and blurb`);
    return null;
  }
  const where = `${packId}/${tourId}`;

  const rawSteps = Array.isArray(o.steps) ? o.steps : [];
  if (rawSteps.length === 0) {
    warn(`${where}: no steps`);
    return null;
  }
  if (rawSteps.length > MAX_STEPS) {
    warn(`${where}: ${rawSteps.length} steps exceeds the cap of ${MAX_STEPS}`);
    return null;
  }

  const seen = new Set<string>();
  const steps: Step[] = [];
  for (const rawStep of rawSteps) {
    const step = stepFrom(rawStep, where, false);
    if (!step) continue;
    if (seen.has(step.id)) {
      warn(`${where}: duplicate step id ${step.id}`);
      continue;
    }
    seen.add(step.id);
    steps.push(step);
  }
  if (steps.length === 0) {
    warn(`${where}: no usable steps survived validation`);
    return null;
  }

  const profiles = (Array.isArray(o.profiles) ? o.profiles : [])
    .map((p) => str(p))
    .filter((p): p is string => !!p && PROFILES.has(p));

  const preconditions: Precondition[] = [];
  for (const g of Array.isArray(o.preconditions) ? o.preconditions : []) {
    const gate = gateFor(g, where);
    if (gate) preconditions.push(gate);
    else if (gate === null) return null; // an unknown gate would silently widen reach
  }

  const successGate = gateFor(o.successWhen, where);
  if (successGate === null) return null;

  return {
    id: packJourneyId(packId, tourId),
    title,
    blurb,
    // Empty ⇒ studio only: the conservative default, since most pack capabilities
    // are authoring surfaces the lean operator build does not ship.
    profiles:
      profiles.length > 0 ? (profiles as Journey["profiles"]) : ["studio"],
    ...(preconditions.length > 0 ? { preconditions } : {}),
    steps,
    // A gate reads as a predicate here: "is it satisfied". Absent ⇒ the journey is
    // orientation-only and records completion without claiming an outcome, exactly
    // as the console's own overview journey does.
    successEvent: successGate
      ? (ctx: TourContext) => successGate.test(ctx) === "ok"
      : () => true,
  };
}

/**
 * Register every valid journey contributed by the installed packs.
 *
 * Idempotent: the registry is keyed by id, so re-running on each context refresh
 * replaces rather than duplicates. Returns the ids registered so the caller can
 * tell whether the offerable set changed and recompute it.
 *
 * Note there is no trust check here — the server already stripped `handoff` steps
 * from untrusted packs (and dropped any journey left empty) before this payload
 * was built, so an untrusted pack's command string never reached the browser.
 */
export function registerPackTours(extensions: Extension[]): string[] {
  const ids: string[] = [];
  for (const ext of extensions) {
    for (const raw of ext.tours ?? []) {
      const journey = journeyFromPackTour(ext.id, raw);
      if (!journey) continue;
      registerJourney(journey);
      ids.push(journey.id);
    }
  }
  return ids;
}
