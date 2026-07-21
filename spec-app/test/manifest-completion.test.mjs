// Tests for the manifest reference completion engine (ADR 0029 §2) — the
// "picker over the index". Uses a `‸` caret marker in the source text: we strip
// it and pass its index as the cursor offset. `node --test`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { manifestCompletionAt } from "../src/manifest-completion.ts";

const CARET = "‸";
/** Split a marked manifest into { text, offset } at the caret. */
function at(marked) {
  const offset = marked.indexOf(CARET);
  if (offset < 0) throw new Error("no caret in test input");
  return { text: marked.replace(CARET, ""), offset };
}

const index = {
  processes: [
    { id: "heating-cycle", name: "Heating cycle" },
    { id: "triage-email", name: "Triage email" },
  ],
  messages: ["temp-reading", "override"],
  decisions: [{ id: "email-triage", name: "Email triage" }],
};

const manifest = {
  data: { sources: { app: {}, archive: {} } },
  types: { reading: { fields: {} }, schedule: { fields: {} } },
  llm: { classifier: {}, concierge: {} },
};

test("completes a process id inside a trigger action.start", () => {
  const { text, offset } = at('{ "triggers": [ { "action": { "start": "‸" } } ] }');
  const r = manifestCompletionAt(text, offset, manifest, index);
  assert.ok(r, "expected a completion");
  assert.equal(r.site, "process");
  assert.deepEqual(
    r.candidates.map((c) => c.value).sort(),
    ["heating-cycle", "triage-email"],
  );
  assert.equal(r.candidates[0].kind, "process");
});

test("completes a message name inside a trigger action.message", () => {
  const { text, offset } = at('{ "triggers": [ { "action": { "message": "te‸" } } ] }');
  const r = manifestCompletionAt(text, offset, manifest, index);
  assert.ok(r);
  assert.equal(r.site, "message");
  assert.deepEqual(r.candidates.map((c) => c.value), ["temp-reading", "override"]);
  // Range covers the partial content "te".
  assert.equal(text.slice(r.range.start, r.range.end), "te");
});

test("completes a decision id under llm.<agent>.output.decision", () => {
  const { text, offset } = at('{ "llm": { "c": { "output": { "decision": "‸" } } } }');
  const r = manifestCompletionAt(text, offset, manifest, index);
  assert.ok(r);
  assert.equal(r.site, "decision");
  assert.deepEqual(r.candidates.map((c) => c.value), ["email-triage"]);
});

test("completes primitives + declared type ids in a field type", () => {
  const { text, offset } = at(
    '{ "types": { "reading": { "fields": { "room": { "type": "‸" } } } } }',
  );
  const r = manifestCompletionAt(text, offset, manifest, index);
  assert.ok(r);
  assert.equal(r.site, "field-type");
  const byKind = (k) => r.candidates.filter((c) => c.kind === k).map((c) => c.value);
  assert.ok(byKind("primitive").includes("string"));
  assert.deepEqual(byKind("type").sort(), ["reading", "schedule"]);
});

test("completes a datasource id at data.default", () => {
  const { text, offset } = at('{ "data": { "default": "‸", "sources": { "app": {} } } }');
  const r = manifestCompletionAt(text, offset, manifest, index);
  assert.ok(r);
  assert.equal(r.site, "datasource");
  assert.deepEqual(r.candidates.map((c) => c.value).sort(), ["app", "archive"]);
});

test("completes an llm agent id at surfaces.<name>.agent and workers[].llm", () => {
  const surf = at('{ "surfaces": { "chat": { "agent": "‸" } } }');
  const rs = manifestCompletionAt(surf.text, surf.offset, manifest, index);
  assert.ok(rs);
  assert.equal(rs.site, "agent");
  assert.deepEqual(rs.candidates.map((c) => c.value).sort(), ["classifier", "concierge"]);

  const wrk = at('{ "workers": [ { "taskType": "x", "llm": "‸" } ] }');
  const rw = manifestCompletionAt(wrk.text, wrk.offset, manifest, index);
  assert.ok(rw);
  assert.equal(rw.site, "agent");
});

test("returns null for non-reference sites and for object keys", () => {
  // A value that isn't a reference site.
  const v = at('{ "id": "‸" }');
  assert.equal(manifestCompletionAt(v.text, v.offset, manifest, index), null);
  // The cursor is on a key, not a value.
  const k = at('{ "triggers": [ { "action": { "st‸": "x" } } ] }');
  assert.equal(manifestCompletionAt(k.text, k.offset, manifest, index), null);
  // `start` outside an action must not classify as a process ref.
  const s = at('{ "start": "‸" }');
  assert.equal(manifestCompletionAt(s.text, s.offset, manifest, index), null);
});

test("is tolerant of an unclosed manifest (mid-edit)", () => {
  const { text, offset } = at('{ "triggers": [ { "action": { "start": "‸');
  const r = manifestCompletionAt(text, offset, manifest, index);
  assert.ok(r);
  assert.equal(r.site, "process");
  assert.equal(r.range.end, text.length);
});

// bindings[] — the form/decision → domain-type binding (ADR 0029 §5). The type
// in scope for a model's FEEL. form and decision resolve against the index; type
// resolves against the declared registry (same as trigger.bodyType).
const bindIndex = {
  ...index,
  forms: [
    { id: "intake-form", fields: [] },
    { id: "triage-form", fields: [] },
  ],
};

test("completes a form id under bindings[].form", () => {
  const { text, offset } = at('{ "bindings": [ { "form": "‸" } ] }');
  const r = manifestCompletionAt(text, offset, manifest, bindIndex);
  assert.ok(r);
  assert.equal(r.site, "form-ref");
  assert.deepEqual(r.candidates.map((c) => c.value).sort(), ["intake-form", "triage-form"]);
  assert.equal(r.candidates[0].kind, "form");
});

test("completes a decision id under bindings[].decision", () => {
  const { text, offset } = at('{ "bindings": [ { "decision": "‸" } ] }');
  const r = manifestCompletionAt(text, offset, manifest, bindIndex);
  assert.ok(r);
  assert.equal(r.site, "decision");
  assert.deepEqual(r.candidates.map((c) => c.value), ["email-triage"]);
});

test("completes a declared type id under bindings[].type", () => {
  const { text, offset } = at('{ "bindings": [ { "form": "intake-form", "type": "‸" } ] }');
  const r = manifestCompletionAt(text, offset, manifest, bindIndex);
  assert.ok(r);
  assert.equal(r.site, "binding-type");
  assert.deepEqual(r.candidates.map((c) => c.value).sort(), ["reading", "schedule"]);
  assert.equal(r.candidates[0].kind, "type");
});

test("bindings[].form in the second element resolves via navPath index", () => {
  const { text, offset } = at(
    '{ "bindings": [ { "decision": "email-triage" }, { "form": "tri‸" } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest, bindIndex);
  assert.ok(r);
  assert.equal(r.site, "form-ref");
  assert.equal(text.slice(r.range.start, r.range.end), "tri");
});
