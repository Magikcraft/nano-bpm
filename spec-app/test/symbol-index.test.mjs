// Tests for the project symbol index (ADR 0029) — run with `node --test`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { buildSymbolIndex, modelKindOf } from "../src/symbol-index.ts";

const here = dirname(fileURLToPath(import.meta.url));
const fx = (name) => readFileSync(join(here, "fixtures", name), "utf8");

const models = [
  { path: "heating.bpmn", kind: "bpmn", text: fx("heating.bpmn") },
  { path: "triage.dmn", kind: "dmn", text: fx("triage.dmn") },
  { path: "confirm-heating.form", kind: "form", text: fx("confirm-heating.form") },
];

test("modelKindOf classifies by extension", () => {
  assert.equal(modelKindOf("a/b.bpmn"), "bpmn");
  assert.equal(modelKindOf("a/b.dmn"), "dmn");
  assert.equal(modelKindOf("a/b.form"), "form");
  assert.equal(modelKindOf("a/b.txt"), undefined);
});

test("indexes processes with tasks, service types and message starts", async () => {
  const index = await buildSymbolIndex(models);
  assert.deepEqual(index.parseErrors, []);

  const heating = index.processes.find((p) => p.id === "heating-cycle");
  assert.ok(heating, "heating-cycle process is indexed");
  assert.equal(heating.name, "Heating Cycle");
  assert.equal(heating.executable, true);
  assert.deepEqual(
    heating.userTasks.map((t) => ({ id: t.id, formId: t.formId })),
    [{ id: "Confirm", formId: "confirm-heating" }],
  );
  assert.deepEqual(heating.serviceTaskTypes, ["read-thermostat"]);

  const watch = index.processes.find((p) => p.id === "temp-watch");
  assert.ok(watch, "temp-watch process is indexed");
  assert.deepEqual(watch.messageStartEvents, ["temp-reading"]);
});

test("indexes declared messages, decisions and form fields", async () => {
  const index = await buildSymbolIndex(models);

  assert.deepEqual(index.messages, ["temp-reading"]);
  assert.deepEqual(index.decisions, [{ id: "email-triage", name: "Email Triage" }]);

  const form = index.forms.find((f) => f.id === "confirm-heating");
  assert.ok(form, "form is indexed");
  assert.deepEqual(form.fields, [
    { key: "reading.room", type: "textfield" },
    { key: "reading.targetTemp", type: "number" },
  ]);
});

test("infers a candidate domain record from a form's fields", async () => {
  const index = await buildSymbolIndex(models);
  assert.deepEqual(index.inferredRecords, [
    {
      id: "confirm-heating",
      source: "form",
      sourcePath: "confirm-heating.form",
      fields: [
        { key: "reading.room", type: "string" },
        { key: "reading.targetTemp", type: "number" },
      ],
    },
  ]);
});

test("collects parse errors without throwing on a malformed model", async () => {
  const index = await buildSymbolIndex([
    { path: "broken.form", kind: "form", text: "{ not json" },
    { path: "triage.dmn", kind: "dmn", text: fx("triage.dmn") },
  ]);
  assert.equal(index.parseErrors.length, 1);
  assert.equal(index.parseErrors[0].path, "broken.form");
  // The valid DMN is still indexed despite the broken sibling.
  assert.equal(index.decisions.length, 1);
});
