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

/** The kinds of reference a manifest string value can be. */
export type ReferenceSite =
  | "process" // trigger action.start — a BPMN process id
  | "message" // trigger action.message — a bpmn:message name
  | "decision" // llm.<agent>.output.decision — a DMN decision id
  | "field-type" // types.<id>.fields.<key>.type — a primitive or declared type id
  | "datasource" // data.default — a declared datasource id
  | "agent"; // surfaces.<name>.agent / workers[].llm — a declared llm agent id

export type CandidateKind =
  | "process"
  | "message"
  | "decision"
  | "primitive"
  | "type"
  | "datasource"
  | "agent";

export interface CompletionCandidate {
  /** The literal id/name to insert (unquoted). */
  value: string;
  kind: CandidateKind;
  /** Short human hint (e.g. a process name or "primitive"). */
  detail?: string;
}

export interface ManifestCompletion {
  site: ReferenceSite;
  /** Offset span of the string *content* (between the quotes) to replace. */
  range: { start: number; end: number };
  candidates: CompletionCandidate[];
}

/** Just the parts of the index this engine reads (keeps callers flexible). */
export type CompletionIndex = Pick<SymbolIndex, "processes" | "messages" | "decisions">;

interface StringSite {
  /** Object-property key path to this string value (array indices omitted). */
  path: string[];
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
    // In an object: the key whose value is currently expected (set when a key
    // string closes, cleared on `,`). Undefined ⇒ the next string is a key.
    pendingKey?: string;
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
        stack.push({ kind: c === "{" ? "obj" : "arr", enteredKey });
        break;
      }
      case "}":
      case "]":
        stack.pop();
        break;
      case ",": {
        const f = top();
        if (f && f.kind === "obj") f.pendingKey = undefined;
        break;
      }
      default:
        break;
    }
  }

  if (!inString) return null;

  // Build the object-key path from the stack's enteredKeys plus the top object's
  // pending key (the immediate property being valued).
  const path: string[] = [];
  for (const f of stack) if (f.enteredKey !== undefined) path.push(f.enteredKey);
  const f = top();
  const isValue = !stringIsKey;
  if (f && f.kind === "obj" && isValue && f.pendingKey !== undefined) {
    path.push(f.pendingKey);
  }
  return { path, isValue, contentStart: stringStart };
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
      // llm.<agent>.output.decision
      if (at(2) === "output") return "decision";
      return null;
    case "type":
      // types.<id>.fields.<key>.type
      if (at(3) === "fields") return "field-type";
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

/**
 * The public entry point: what reference completions apply at `offset`, or null
 * when the cursor is not inside a recognized reference value.
 */
export function manifestCompletionAt(
  text: string,
  offset: number,
  manifest: unknown,
  index?: CompletionIndex,
): ManifestCompletion | null {
  const loc = locateString(text, offset);
  if (!loc || !loc.isValue) return null;
  const site = classify(loc.path);
  if (!site) return null;
  return {
    site,
    range: { start: loc.contentStart, end: stringContentEnd(text, offset) },
    candidates: candidatesFor(site, manifest, index),
  };
}
