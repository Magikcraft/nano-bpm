// Test-run payload scaffolding (ADR 0040 §9/§10) ----------------------------
//
// The test view drives the in-browser engine by hand: the maker types a JSON
// blob to start an instance, and another to complete each waiting job. Now that
// payload schemas travel *in the model* — a service task's `io.nanobpm.dataEnvelope`
// names a type, and pure-motion types are `nano:shape`s composed of `extend` ops
// (the flat-record case, ADR 0040 §9) carried on the process — the panel can
// scaffold those blobs instead of leaving the maker to guess field names.
//
// These helpers are deliberately pure and DOM-free (a tolerant string scan, not
// `DOMParser`) so they run identically in the browser panel and under the
// console's `node --test` unit runner. They read only the model text the panel
// already holds; the authoritative reifier remains `resolveShapes` server-side.

import { ENVELOPE_KEY } from "./dataEnvelope.ts";

/** A flat payload field lifted from a shape's `extend` op. */
export interface ScaffoldField {
  name: string;
  type: string;
  optional?: boolean;
  list?: boolean;
}

/** The model facts the scaffolder needs, parsed once from the process xml. */
export interface ModelEnvelopes {
  /** Element id → its data-envelope type refs (`in`/`out`), when declared. */
  tasks: Map<string, { in?: string; out?: string }>;
  /** Shape id → its ordered `extend` fields (empty for a non-flat shape). */
  shapes: Map<string, ScaffoldField[]>;
  /** Element ids that a start event flows directly into (union across processes). */
  startTargets: Set<string>;
  /**
   * Per-`<bpmn:process>` start targets, keyed by process id. A multi-process
   * definition keeps each process's entry points separate so `scaffoldStartVars`
   * can scope to the selected process instead of conflating them.
   */
  startTargetsByProcess: Map<string, Set<string>>;
}

const TASK_TAGS = [
  "serviceTask",
  "businessRuleTask",
  "scriptTask",
  "sendTask",
  "receiveTask",
  "userTask",
  "task",
];

/** Read an XML attribute off an opening-tag string (`id="x" type="y"`). */
function attr(tag: string, name: string): string | undefined {
  const m = new RegExp(`\\b${name}="([^"]*)"`).exec(tag);
  return m ? m[1] : undefined;
}

/**
 * Parse the process xml into the envelope + shape facts the scaffolder needs.
 * Tolerant by design: unknown constructs are ignored, a shape with non-`extend`
 * ops simply contributes the `extend` fields it does carry (refs are resolved
 * server-side, not here). Namespace prefixes are matched loosely (`*:shape`) so a
 * re-serialised model with a different `nano` prefix still parses.
 */
export function parseModelEnvelopes(xml: string): ModelEnvelopes {
  const tasks = new Map<string, { in?: string; out?: string }>();
  const shapes = new Map<string, ScaffoldField[]>();
  const startTargets = new Set<string>();
  const startTargetsByProcess = new Map<string, Set<string>>();

  // Shapes: <…:shape id="X"> … <…:extend name=".." type=".." optional=".." /> …
  const shapeRe = /<[\w-]*:?shape\b([^>]*)>([\s\S]*?)<\/[\w-]*:?shape>/g;
  const extendRe = /<[\w-]*:?extend\b([^>]*)\/?>/g;
  for (let sm = shapeRe.exec(xml); sm; sm = shapeRe.exec(xml)) {
    const id = attr(sm[1], "id");
    if (!id) continue;
    const fields: ScaffoldField[] = [];
    for (let em = extendRe.exec(sm[2]); em; em = extendRe.exec(sm[2])) {
      const name = attr(em[1], "name");
      const type = attr(em[1], "type");
      if (!name || !type) continue;
      const f: ScaffoldField = { name, type };
      if (attr(em[1], "optional") === "true") f.optional = true;
      if (attr(em[1], "list") === "true") f.list = true;
      fields.push(f);
    }
    shapes.set(id, fields);
  }

  // Task envelopes: for each task element, read the two reserved zeebe:property
  // values out of its own body (non-greedy to the matching close tag).
  for (const tag of TASK_TAGS) {
    const blockRe = new RegExp(
      `<[\\w-]*:?${tag}\\b([^>]*)>([\\s\\S]*?)<\\/[\\w-]*:?${tag}>`,
      "g",
    );
    for (let bm = blockRe.exec(xml); bm; bm = blockRe.exec(xml)) {
      const id = attr(bm[1], "id");
      if (!id) continue;
      const body = bm[2];
      const env: { in?: string; out?: string } = {};
      const inVal = propValue(body, ENVELOPE_KEY.inputType);
      const outVal = propValue(body, ENVELOPE_KEY.outputType);
      if (inVal) env.in = inVal;
      if (outVal) env.out = outVal;
      if (env.in || env.out) tasks.set(id, env);
    }
  }

  // Start entry targets, scoped per <bpmn:process> so a multi-process definition
  // keeps each process's entry points separate. A start target always lives in
  // the same process as its start event, so we scan each process body in
  // isolation and also keep a union for callers that don't care about scoping.
  const processRe = /<[\w-]*:?process\b([^>]*)>([\s\S]*?)<\/[\w-]*:?process>/g;
  let sawProcess = false;
  for (let pm = processRe.exec(xml); pm; pm = processRe.exec(xml)) {
    sawProcess = true;
    const pid = attr(pm[1], "id");
    const targets = collectStartTargets(pm[2]);
    for (const t of targets) startTargets.add(t);
    if (pid) startTargetsByProcess.set(pid, targets);
  }
  // Fallback for a fragment with no <process> wrapper: scan the whole xml.
  if (!sawProcess)
    for (const t of collectStartTargets(xml)) startTargets.add(t);

  return { tasks, shapes, startTargets, startTargetsByProcess };
}

/** Element ids that a start event flows directly into, within a scope of xml. */
function collectStartTargets(scope: string): Set<string> {
  const startIds = new Set<string>();
  const targets = new Set<string>();
  const startRe = /<[\w-]*:?startEvent\b([^>]*)>/g;
  for (let m = startRe.exec(scope); m; m = startRe.exec(scope)) {
    const id = attr(m[1], "id");
    if (id) startIds.add(id);
  }
  const flowRe = /<[\w-]*:?sequenceFlow\b([^>]*)\/?>/g;
  for (let m = flowRe.exec(scope); m; m = flowRe.exec(scope)) {
    const source = attr(m[1], "sourceRef");
    const target = attr(m[1], "targetRef");
    if (source && target && startIds.has(source)) targets.add(target);
  }
  return targets;
}

/** The value of a reserved `zeebe:property` (by its `name`) within an element body. */
function propValue(body: string, key: string): string | undefined {
  const re = new RegExp(
    `<[\\w-]*:?property\\b[^>]*\\bname="${key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"[^>]*>`,
  );
  const m = re.exec(body);
  return m ? attr(m[0], "value") : undefined;
}

/** A representative empty value for a scalar keyword; unknown/nominal → `{}`. */
function placeholder(field: ScaffoldField): unknown {
  if (field.list) return [];
  switch (field.type) {
    case "string":
    case "date":
    case "datetime":
      return "";
    case "integer":
    case "number":
      return 0;
    case "boolean":
      return false;
    case "json":
      return {};
    default:
      return {}; // a nominal reference to another type — nested shape
  }
}

/**
 * Build a pretty-printed JSON skeleton for a type id: every field of the shape
 * with a typed empty placeholder. Returns `"{}"` when the id is unknown or has
 * no `extend` fields, so a caller can always drop the result straight into a
 * textarea.
 */
export function scaffoldForType(
  model: ModelEnvelopes,
  typeId: string | undefined,
): string {
  const fields = typeId ? model.shapes.get(typeId) : undefined;
  if (!fields || fields.length === 0) return "{}";
  const obj: Record<string, unknown> = {};
  for (const f of fields) obj[f.name] = placeholder(f);
  return JSON.stringify(obj, null, 2);
}

/** The output-payload skeleton for a waiting job's element (its `out` envelope). */
export function scaffoldJobOutput(
  model: ModelEnvelopes,
  elementId: string,
): string {
  return scaffoldForType(model, model.tasks.get(elementId)?.out);
}

/**
 * The start-instance skeleton: the input envelope of the single service task the
 * start event flows into. Only a lone, unambiguous entry task is scaffolded —
 * otherwise the process input is not model-typed and we yield `"{}"`.
 *
 * When `processId` is given, scoping is restricted to that process's start
 * targets, so a multi-process definition scaffolds the selected process only
 * (and never conflates entry points across processes). With no `processId`, the
 * union across all processes is used (single-process behaviour).
 */
export function scaffoldStartVars(
  model: ModelEnvelopes,
  processId?: string,
): string {
  const scoped = processId
    ? model.startTargetsByProcess.get(processId)
    : undefined;
  const targets = [...(scoped ?? model.startTargets)];
  if (targets.length !== 1) return "{}";
  return scaffoldForType(model, model.tasks.get(targets[0])?.in);
}

// Message-publish / signal-broadcast scaffolding ----------------------------
//
// The test view drives message-catch / receive tasks and signal catch/boundary
// events by hand, mirroring the "Waiting jobs" affordance. These pure helpers
// turn an open subscription (carried on the snapshot) into the prefilled form a
// maker edits before publishing/broadcasting, so the UI never re-derives the
// mapping inline (and it stays unit-testable under `node --test`).

/** The prefilled fields for publishing a message to an open subscription. */
export interface MessagePublishForm {
  messageName: string;
  correlationKey: string;
  /** Correlation variables (JSON), defaulted empty for the maker to edit. */
  variables: string;
}

/** The prefilled fields for broadcasting a signal to an open subscription. */
export interface SignalBroadcastForm {
  signalName: string;
  /** Signal variables (JSON), defaulted empty for the maker to edit. */
  variables: string;
}

/**
 * Prefill a message-publish form from an open message subscription: the engine
 * correlates by `messageName` + `correlationKey`, so both are seeded from the
 * subscription (a missing/empty key stays an empty string). Variables default to
 * `"{}"` — a message carries no in-model payload envelope to scaffold from.
 */
export function prefillMessagePublish(sub: {
  messageName: string;
  correlationKey?: string;
}): MessagePublishForm {
  return {
    messageName: sub.messageName,
    correlationKey: sub.correlationKey ?? "",
    variables: "{}",
  };
}

/**
 * Prefill a signal-broadcast form from an open signal subscription: signals
 * correlate by **name only**, so only `signalName` is seeded. Variables default
 * to `"{}"`.
 */
export function prefillSignalBroadcast(sub: {
  signalName: string;
}): SignalBroadcastForm {
  return { signalName: sub.signalName, variables: "{}" };
}
