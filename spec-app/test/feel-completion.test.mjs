// Tests for FEEL variable-path completion (ADR 0029 §5). The cursor sits inside
// a trigger action's FEEL field (`variables`/`correlationKey`); completions come
// from the trigger's declared `bodyType` domain type. `‸` marks the caret.
import { test } from "node:test";
import assert from "node:assert/strict";
import { manifestCompletionAt } from "../src/manifest-completion.ts";

const CARET = "‸";
function at(marked) {
  const offset = marked.indexOf(CARET);
  if (offset < 0) throw new Error("no caret in test input");
  return { text: marked.replace(CARET, ""), offset };
}

// A registry with a nested type (reading.sensor -> sensor) and a list field.
const manifest = {
  types: {
    reading: {
      fields: {
        room: { type: "string" },
        temp: { type: "number" },
        sensor: { type: "sensor" },
      },
    },
    sensor: {
      fields: { id: { type: "string" }, tags: { type: "string", list: true } },
    },
  },
  triggers: [
    {
      id: "sensor",
      type: "webhook",
      bodyType: "reading",
      action: { message: "temp-reading", correlationKey: "= " },
    },
    {
      id: "morning",
      type: "cron",
      action: { start: "x", variables: "= " },
    },
  ],
};

const values = (r) => r.candidates.map((c) => c.value).sort();

test("root FEEL position offers `body` and the `data.query` builtin", () => {
  const { text, offset } = at(
    '{ "triggers": [ { "bodyType": "reading", "action": { "variables": "= ‸" } } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest);
  assert.ok(r);
  assert.equal(r.site, "feel");
  assert.deepEqual(values(r), ["body", "data.query"]);
  const body = r.candidates.find((c) => c.value === "body");
  const dq = r.candidates.find((c) => c.value === "data.query");
  assert.equal(body.kind, "variable");
  assert.equal(dq.kind, "function");
});

test("`data.` completes the `query` datasource builtin (ADR 0024 §5)", () => {
  const { text, offset } = at(
    '{ "triggers": [ { "action": { "variables": "= data.‸" } } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest);
  assert.ok(r);
  assert.equal(r.site, "feel");
  assert.deepEqual(values(r), ["query"]);
  assert.equal(r.candidates[0].kind, "function");
});

test("`body.` completes the bodyType's fields", () => {
  const { text, offset } = at(
    '{ "triggers": [ { "bodyType": "reading", "action": { "variables": "= body.‸" } } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest);
  assert.ok(r);
  assert.equal(r.site, "feel");
  assert.deepEqual(values(r), ["room", "sensor", "temp"]);
  // Empty segment: range is an insertion point at the caret.
  assert.equal(r.range.start, r.range.end);
});

test("a partial segment narrows and its range covers only the segment", () => {
  const { text, offset } = at(
    '{ "triggers": [ { "bodyType": "reading", "action": { "variables": "= body.ro‸" } } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest);
  assert.ok(r);
  assert.equal(text.slice(r.range.start, r.range.end), "ro");
  assert.ok(values(r).includes("room"));
});

test("walks a nested declared type (body.sensor.<field>)", () => {
  const { text, offset } = at(
    '{ "triggers": [ { "bodyType": "reading", "action": { "variables": "= body.sensor.‸" } } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest);
  assert.ok(r);
  assert.deepEqual(values(r), ["id", "tags"]);
});

test("a list field is a leaf: no dotted recursion through it", () => {
  const { text, offset } = at(
    '{ "triggers": [ { "bodyType": "reading", "action": { "variables": "= body.sensor.tags.‸" } } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest);
  assert.ok(r);
  assert.deepEqual(r.candidates, []);
});

test("correlationKey shares the same bodyType scope", () => {
  const { text, offset } = at(
    '{ "triggers": [ { "bodyType": "reading", "action": { "message": "m", "correlationKey": "= body.‸" } } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest);
  assert.ok(r);
  assert.deepEqual(values(r), ["room", "sensor", "temp"]);
});

test("completes mid-expression, rewriting only the caret's segment", () => {
  const { text, offset } = at(
    '{ "triggers": [ { "bodyType": "reading", "action": { "variables": "= {r: body.te‸}" } } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest);
  assert.ok(r);
  assert.equal(text.slice(r.range.start, r.range.end), "te");
  assert.ok(values(r).includes("temp"));
});

test("picks the right trigger's bodyType by array index", () => {
  // The second trigger has no bodyType — only `body` root, no fields.
  const { text, offset } = at(
    '{ "triggers": [ { "bodyType": "reading", "action": { "start": "x" } }, { "action": { "variables": "= body.‸" } } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest);
  assert.ok(r);
  assert.equal(r.site, "feel");
  assert.deepEqual(r.candidates, []); // unknown bodyType ⇒ no fields
});

test("a body path not rooted at `body` yields no field candidates", () => {
  const { text, offset } = at(
    '{ "triggers": [ { "bodyType": "reading", "action": { "variables": "= now.‸" } } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest);
  assert.ok(r);
  assert.deepEqual(r.candidates, []);
});

test("bodyType value itself completes declared domain type ids", () => {
  const { text, offset } = at(
    '{ "triggers": [ { "bodyType": "‸", "action": { "start": "x" } } ] }',
  );
  const r = manifestCompletionAt(text, offset, manifest);
  assert.ok(r);
  assert.equal(r.site, "body-type");
  assert.deepEqual(values(r), ["reading", "sensor"]);
  assert.equal(r.candidates[0].kind, "type");
});
