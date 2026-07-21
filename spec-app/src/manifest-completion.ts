// Manifest reference completions — the "picker over the index" (ADR 0029 §2).
//
// This is the engine behind Urban's manifest IntelliSense: given the manifest
// text, a cursor offset, the parsed manifest and the project symbol index, it
// decides whether the cursor sits inside a *reference* string value (e.g. the
// process id under a trigger's `action.start`) and, if so, enumerates the valid
// ids from the index/manifest. The console maps these to Monaco completions, so
// makers pick references from a list instead of hand-typing free strings — the
// core move of ADR 0029 ("typed references replace free-string ids").
//
// It is framework-free and tolerant: the manifest is usually mid-edit and may
// not be valid JSON, so we locate the cursor by a small brace-tracking scan
// rather than a strict parse.

import { DOMAIN_PRIMITIVES } from "./symbol-index.ts";
import type { SymbolIndex } from "./symbol-index.ts";
import { fieldsOf } from "./feel.ts";

/** The kinds of reference a manifest string value can be. */
export type ReferenceSite =
  | "process" // trigger action.start — a BPMN process id
  | "message" // trigger action.message — a bpmn:message name
  | "decision" // llm.<agent>.output.decision / bindings[].decision — a DMN decision id
  | "field-type" // types.<id>.fields.<key>.type — a primitive or declared type id
  | "body-type" // triggers[].bodyType — a declared domain type id (FEEL scope)
  | "binding-type" // bindings[].type — a declared domain type id (model FEEL scope)
  | "form-ref" // bindings[].form — a form-js form id
  | "datasource" // data.default — a declared datasource id
  | "agent"; // surfaces.<name>.agent / workers[].llm — a declared llm agent id

export type CandidateKind =
  | "process"
  | "message"
  | "decision"
  | "primitive"
  | "type"
  | "form"
  | "datasource"
  | "agent"
  | "variable"; // a FEEL variable-path segment (ADR 0029 §5)

export interface CompletionCandidate {
  /** The literal id/name to insert (unquoted). */
  value: string;
  kind: CandidateKind;
  /** Short human hint (e.g. a process name or "primitive"). */
  detail?: string;
}

export interface ManifestCompletion {
  site: ReferenceSite | "feel";
  /** Offset span of the string *content* (between the quotes) to replace. */
  range: { start: number; end: number };
  candidates: CompletionCandidate[];
}

/** Just the parts of the index this engine reads (keeps callers flexible). */
export type CompletionIndex = Pick<SymbolIndex, "processes" | "messages" | "decisions" | "forms">;

interface StringSite {
  /** Object-property key path to this string value (array indices omitted). */
  path: string[];
  /**
   * Fully navigable path including array element indices, so callers can walk
   * the parsed manifest to the exact node (e.g. `["triggers", 0, "bodyType"]`).
   */
  navPath: (string | number)[];
  /** True when the located string is a value (not an object key). */
  isValue: boolean;
  /** Offset of the first content char (just after the opening quote). */
  contentStart: number;
}

/**
 * Locate the string the cursor is inside, returning the object-key path to it.
 * Returns null when the cursor is not inside a string. Tolerant of invalid JSON:
 * it tracks a container stack and the pending key per object frame, ignoring
 * whitespace/other tokens it doesn't need.
 */
function locateString(text: string, offset: number): StringSite | null {
  type Frame = {
    kind: "obj" | "arr";
    // The parent object-property key under which this container sits (undefined
    // for the root and for containers that are array elements — the array
    // already contributed its key).
    enteredKey?: string;
    // For a container that is an array element: its index within the parent
    // array (undefined otherwise).
    arrayIndex?: number;
    // In an object: the key whose value is currently expected (set when a key
    // string closes, cleared on `,`). Undefined ⇒ the next string is a key.
    pendingKey?: string;
    // In an array: count of elements separated so far (the index of the element
    // currently being scanned).
    elemIndex?: number;
  };
  const stack: Frame[] = [];
  let inString = false;
  let stringStart = -1; // content start (char after opening quote)
  let stringIsKey = false;

  const top = () => stack[stack.length - 1];

  for (let i = 0; i < offset; i++) {
    const c = text[i];
    if (inString) {
      if (c === "\\") {
        i++; // skip the escaped char
        continue;
      }
      if (c === '"') {
        // String closed before the cursor — record it as key/value context.
        inString = false;
        const f = top();
        if (f && f.kind === "obj") {
          if (stringIsKey) {
            f.pendingKey = text.slice(stringStart, i);
          } else {
            f.pendingKey = undefined; // value consumed
          }
        }
      }
      continue;
    }
    switch (c) {
      case '"': {
        inString = true;
        stringStart = i + 1;
        const f = top();
        // In an object, a string is a key unless we're past a `:` (pendingKey set).
        stringIsKey = !!f && f.kind === "obj" && f.pendingKey === undefined;
        break;
      }
      case "{":
      case "[": {
        const parent = top();
        const enteredKey =
          parent && parent.kind === "obj" ? parent.pendingKey : undefined;
        const arrayIndex =
          parent && parent.kind === "arr" ? parent.elemIndex ?? 0 : undefined;
        stack.push({
          kind: c === "{" ? "obj" : "arr",
          enteredKey,
          arrayIndex,
          elemIndex: c === "[" ? 0 : undefined,
        });
        break;
      }
      case "}":
      case "]":
        stack.pop();
        break;
      case ",": {
        const f = top();
        if (f && f.kind === "obj") f.pendingKey = undefined;
        else if (f && f.kind === "arr") f.elemIndex = (f.elemIndex ?? 0) + 1;
        break;
      }
      default:
        break;
    }
  }

  if (!inString) return null;

  // Build the object-key path from the stack's enteredKeys plus the top object's
  // pending key (the immediate property being valued). navPath additionally
  // threads array element indices so callers can address the exact node.
  const path: string[] = [];
  const navPath: (string | number)[] = [];
  for (const f of stack) {
    if (f.enteredKey !== undefined) {
      path.push(f.enteredKey);
      navPath.push(f.enteredKey);
    }
    if (f.arrayIndex !== undefined) navPath.push(f.arrayIndex);
  }
  const f = top();
  const isValue = !stringIsKey;
  if (f && f.kind === "obj" && isValue && f.pendingKey !== undefined) {
    path.push(f.pendingKey);
    navPath.push(f.pendingKey);
  }
  return { path, navPath, isValue, contentStart: stringStart };
}

/** Scan forward from the cursor to the end of the current string content. */
function stringContentEnd(text: string, offset: number): number {
  for (let i = offset; i < text.length; i++) {
    const c = text[i];
    if (c === "\\") {
      i++;
      continue;
    }
    if (c === '"') return i;
  }
  return text.length;
}

/** Classify an object-key path as a reference site, or null. */
function classify(path: string[]): ReferenceSite | null {
  const last = path[path.length - 1];
  if (last === undefined) return null;
  const at = (n: number) => path[path.length - n];
  switch (last) {
    case "start":
      // triggers[].action.start
      if (at(2) === "action") return "process";
      return null;
    case "message":
      if (at(2) === "action") return "message";
      return null;
    case "decision":
      // llm.<agent>.output.decision, or bindings[].decision
      if (at(2) === "output") return "decision";
      if (at(2) === "bindings") return "decision";
      return null;
    case "type":
      // types.<id>.fields.<key>.type, or bindings[].type
      if (at(3) === "fields") return "field-type";
      if (at(2) === "bindings") return "binding-type";
      return null;
    case "form":
      // bindings[].form
      if (at(2) === "bindings") return "form-ref";
      return null;
    case "bodyType":
      // triggers[].bodyType
      if (at(2) === "triggers") return "body-type";
      return null;
    case "default":
      if (at(2) === "data") return "datasource";
      return null;
    case "agent":
      // surfaces.<name>.agent
      return "agent";
    case "llm":
      // workers[].llm
      if (at(2) === "workers") return "agent";
      return null;
    default:
      return null;
  }
}

function record(obj: unknown, key: string): Record<string, unknown> | undefined {
  const v = (obj as Record<string, unknown> | undefined)?.[key];
  return v && typeof v === "object" ? (v as Record<string, unknown>) : undefined;
}

function candidatesFor(
  site: ReferenceSite,
  manifest: unknown,
  index?: CompletionIndex,
): CompletionCandidate[] {
  switch (site) {
    case "process":
      return (index?.processes ?? []).map((p) => ({
        value: p.id,
        kind: "process" as const,
        detail: p.name,
      }));
    case "message":
      return (index?.messages ?? []).map((m) => ({ value: m, kind: "message" as const }));
    case "decision":
      return (index?.decisions ?? []).map((d) => ({
        value: d.id,
        kind: "decision" as const,
        detail: d.name,
      }));
    case "field-type": {
      const primitives = DOMAIN_PRIMITIVES.map((p) => ({
        value: p,
        kind: "primitive" as const,
        detail: "primitive",
      }));
      const types = Object.keys(record(manifest, "types") ?? {}).map((id) => ({
        value: id,
        kind: "type" as const,
        detail: "domain type",
      }));
      return [...primitives, ...types];
    }
    case "body-type":
      return Object.keys(record(manifest, "types") ?? {}).map((id) => ({
        value: id,
        kind: "type" as const,
        detail: "domain type",
      }));
    case "binding-type":
      return Object.keys(record(manifest, "types") ?? {}).map((id) => ({
        value: id,
        kind: "type" as const,
        detail: "domain type",
      }));
    case "form-ref":
      return (index?.forms ?? []).map((f) => ({
        value: f.id,
        kind: "form" as const,
      }));
    case "datasource":
      return Object.keys(record(record(manifest, "data"), "sources") ?? {}).map((id) => ({
        value: id,
        kind: "datasource" as const,
      }));
    case "agent":
      return Object.keys(record(manifest, "llm") ?? {}).map((id) => ({
        value: id,
        kind: "agent" as const,
      }));
  }
}

/** Detect the FEEL-expression field the cursor path names (ADR 0029 §5). */
function feelFieldOf(path: string[]): "variables" | "correlationKey" | null {
  const last = path[path.length - 1];
  if ((last === "variables" || last === "correlationKey") && path[path.length - 2] === "action") {
    return last;
  }
  return null;
}

/** The trigger object owning this FEEL field, resolved via the navigable path. */
function triggerOf(
  manifest: unknown,
  navPath: (string | number)[],
): Record<string, unknown> | undefined {
  const i = navPath.indexOf("triggers");
  if (i < 0 || typeof navPath[i + 1] !== "number") return undefined;
  const triggers = (manifest as { triggers?: unknown }).triggers;
  const t = Array.isArray(triggers) ? triggers[navPath[i + 1] as number] : undefined;
  return t && typeof t === "object" ? (t as Record<string, unknown>) : undefined;
}

/**
 * FEEL variable-path completion (ADR 0029 §5). Completes the dotted path segment
 * under the caret against the type in scope (the trigger's `bodyType`): `body`
 * at the root, then `body.<field>` walking declared nested domain types. The
 * replacement range is just the current segment, so completing mid-expression
 * (`= {room: body.roo|}`) rewrites only `roo`.
 */
function feelCandidates(
  manifest: unknown,
  bodyType: string | undefined,
  text: string,
  offset: number,
  contentStart: number,
): { candidates: CompletionCandidate[]; range: { start: number; end: number } } {
  // Isolate the dotted path being typed: scan left over identifier/dot chars,
  // right over the rest of the current identifier.
  let s = offset;
  while (s > contentStart && /[A-Za-z0-9_.]/.test(text[s - 1])) s--;
  let e = offset;
  while (e < text.length && /[A-Za-z0-9_]/.test(text[e])) e++;
  const segs = text.slice(s, offset).split(".");
  const seg = segs[segs.length - 1];
  const range = { start: offset - seg.length, end: e };
  const prefix = segs.slice(0, -1); // the resolved portion before the caret

  // Root position (no dot yet): offer `body`, the event-body binding.
  if (prefix.length === 0) {
    return {
      candidates: [{ value: "body", kind: "variable", detail: bodyType ?? "event body" }],
      range,
    };
  }
  // Paths must be rooted at `body`; anything else is out of the resolvable scope.
  if (prefix[0] !== "body") return { candidates: [], range };

  // Walk declared nested types from bodyType through the prefix segments. Stop at
  // unknown fields, lists (FEEL indexes those) and primitives — none expose
  // further declared paths.
  let curType = bodyType;
  for (const p of prefix.slice(1)) {
    const f = fieldsOf(manifest, curType)[p];
    if (!f || f.list) return { candidates: [], range };
    curType = f.type;
  }
  const candidates = Object.entries(fieldsOf(manifest, curType)).map(([key, f]) => ({
    value: key,
    kind: "variable" as const,
    detail: f.list ? `${f.type ?? "?"}[]` : f.type,
  }));
  return { candidates, range };
}

/**
 * The public entry point: what completions apply at `offset`, or null when the
 * cursor is not inside a recognized reference value or FEEL expression.
 */
export function manifestCompletionAt(
  text: string,
  offset: number,
  manifest: unknown,
  index?: CompletionIndex,
): ManifestCompletion | null {
  const loc = locateString(text, offset);
  if (!loc || !loc.isValue) return null;

  // FEEL fields resolve variable paths against the trigger's bodyType (§5),
  // which is distinct from whole-string reference completion.
  if (feelFieldOf(loc.path)) {
    const trigger = triggerOf(manifest, loc.navPath);
    const bodyType = typeof trigger?.bodyType === "string" ? trigger.bodyType : undefined;
    const { candidates, range } = feelCandidates(
      manifest,
      bodyType,
      text,
      offset,
      loc.contentStart,
    );
    return { site: "feel", range, candidates };
  }

  const site = classify(loc.path);
  if (!site) return null;
  return {
    site,
    range: { start: loc.contentStart, end: stringContentEnd(text, offset) },
    candidates: candidatesFor(site, manifest, index),
  };
}
