// Tests for the fail-closed manifest validator (ADR 0027 §4) — `node --test`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { buildSymbolIndex } from "../src/symbol-index.ts";
import { validateManifest } from "../src/validate.ts";

const here = dirname(fileURLToPath(import.meta.url));
const fx = (name) => readFileSync(join(here, "fixtures", name), "utf8");
const manifest = () => JSON.parse(fx("heating.nano.app.json"));

const models = [
  { path: "heating.bpmn", kind: "bpmn", text: fx("heating.bpmn") },
  { path: "triage.dmn", kind: "dmn", text: fx("triage.dmn") },
  { path: "confirm-heating.form", kind: "form", text: fx("confirm-heating.form") },
];

const codesFor = (result, pointer) =>
  result.diagnostics.filter((d) => d.pointer === pointer).map((d) => d.code);

test("a manifest whose references all resolve is valid", async () => {
  const index = await buildSymbolIndex(models);
  const result = validateManifest(manifest(), index);
  assert.deepEqual(result.diagnostics, []);
  assert.equal(result.ok, true);
});

test("schema errors fail closed with a JSON pointer, before cross-ref runs", () => {
  const m = manifest();
  delete m.id; // required
  m.triggers[0].action.start = 12345; // wrong type
  const result = validateManifest(m);
  assert.equal(result.ok, false);
  assert.ok(result.diagnostics.every((d) => d.code === "schema"));
  assert.ok(result.diagnostics.some((d) => d.pointer === "/id"));
});

test("unknown process / message / decision are rejected with pointers", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.triggers[0].action.start = "does-not-exist";
  m.triggers[1].action.message = "no-such-message";
  m.llm.classifier.output.decision = "missing-decision";
  const result = validateManifest(m, index);
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/triggers/0/action/start"), ["unknown-process"]);
  assert.deepEqual(codesFor(result, "/triggers/1/action/message"), ["unknown-message"]);
  assert.deepEqual(codesFor(result, "/llm/classifier/output/decision"), ["unknown-decision"]);
});

test("unknown connection, llm agent and datasource are rejected", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.triggers[1].auth = "hmac:ghost"; // connection "ghost" undeclared
  m.surfaces.chat.agent = "nobody"; // llm undeclared
  m.data.default = "elsewhere"; // datasource undeclared
  const result = validateManifest(m, index);
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/triggers/1/auth"), ["unknown-connection"]);
  assert.deepEqual(codesFor(result, "/surfaces/chat/agent"), ["unknown-llm"]);
  assert.deepEqual(codesFor(result, "/data/default"), ["unknown-datasource"]);
});

test("manifest-only mode (no index) skips model rules but keeps intra-manifest rules", () => {
  const m = manifest();
  m.triggers[0].action.start = "does-not-exist"; // model rule — skipped without an index
  m.surfaces.chat.agent = "nobody"; // intra-manifest rule — still enforced
  const result = validateManifest(m); // no index
  assert.equal(result.ok, false);
  assert.equal(codesFor(result, "/triggers/0/action/start").length, 0);
  assert.deepEqual(codesFor(result, "/surfaces/chat/agent"), ["unknown-llm"]);
});

// ── Domain type registry (ADR 0029 §4 / ADR 0031) ─────────────────────────────
import { resolveDomainTypes } from "../src/domain-types.ts";

test("a domain type whose field types are primitives or declared types is valid", () => {
  const m = manifest();
  m.types = {
    reading: { fields: { room: { type: "string" }, targetTemp: { type: "number" } } },
    schedule: { name: "Schedule", fields: { reading: { type: "reading", list: true } } },
  };
  const result = validateManifest(m); // intra-manifest rule, no index needed
  assert.deepEqual(
    result.diagnostics.filter((d) => d.pointer.startsWith("/types")),
    [],
  );
  assert.equal(result.ok, true);
});

test("a field type that is neither a primitive nor a declared type is rejected with a pointer", () => {
  const m = manifest();
  m.types = { reading: { fields: { room: { type: "strng" }, ref: { type: "no-such-type" } } } };
  const result = validateManifest(m);
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/types/reading/fields/room/type"), ["unknown-type"]);
  assert.deepEqual(codesFor(result, "/types/reading/fields/ref/type"), ["unknown-type"]);
});

test("resolveDomainTypes unions declared registry types with form-inferred candidates", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.types = { reading: { fields: { room: { type: "string" } } } };
  const resolution = resolveDomainTypes(m, index);

  assert.deepEqual(
    resolution.declared.map((d) => ({ id: d.id, match: d.match })),
    [{ id: "reading", match: "nominal" }],
  );
  // confirm-heating form inference remains a candidate (not shadowed by a declared type)
  assert.ok(resolution.inferred.some((r) => r.id === "confirm-heating"));
});

test("a declared type shadows its form-inferred candidate of the same id", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.types = { "confirm-heating": { fields: { room: { type: "string" } } } };
  const resolution = resolveDomainTypes(m, index);
  assert.ok(!resolution.inferred.some((r) => r.id === "confirm-heating"));
});
