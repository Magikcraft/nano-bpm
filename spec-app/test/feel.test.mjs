// Tests for the shared FEEL scope model (ADR 0029 §5): `bodyPaths` extraction
// and `resolveBodyPath` type-walking. `node --test`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { bodyPaths, resolveBodyPath, isDeclaredType, scopeVarsForType, decisionScope, processScope, outputTypeForTaskType, componentOutputScope, formTypeId, dataQueryCalls, DATA_QUERY } from "../src/feel.ts";

const manifest = {
  types: {
    reading: {
      fields: {
        room: { type: "string" },
        temp: { type: "number" },
        meta: { type: "json" },
        sensor: { type: "sensor" },
        readings: { type: "sensor", list: true },
      },
    },
    sensor: {
      fields: { id: { type: "string" }, tags: { type: "string", list: true } },
    },
  },
};

test("bodyPaths extracts standalone body-rooted dotted paths", () => {
  assert.deepEqual(bodyPaths("= body.room"), [["room"]]);
  assert.deepEqual(bodyPaths("= {a: body.temp, b: body.sensor.id}"), [
    ["temp"],
    ["sensor", "id"],
  ]);
});

test("bodyPaths ignores non-standalone, member-access, indexed and called forms", () => {
  assert.deepEqual(bodyPaths("= somebody.x"), []); // not standalone
  assert.deepEqual(bodyPaths("= x.body.y"), []); // member access, not the root
  assert.deepEqual(bodyPaths("= body.items[1]"), []); // indexing
  assert.deepEqual(bodyPaths("= body.thing(1)"), []); // call
  assert.deepEqual(bodyPaths("= body"), []); // bare root, no path
});

test("resolveBodyPath resolves declared fields and nested types", () => {
  assert.equal(resolveBodyPath(manifest, "reading", []).kind, "root");
  assert.equal(resolveBodyPath(manifest, "reading", ["room"]).kind, "ok");
  assert.equal(resolveBodyPath(manifest, "reading", ["sensor", "id"]).kind, "ok");
  // list of a declared type follows FEEL list projection.
  assert.equal(resolveBodyPath(manifest, "reading", ["readings", "id"]).kind, "ok");
});

test("resolveBodyPath flags a missing segment as unknown", () => {
  assert.deepEqual(resolveBodyPath(manifest, "reading", ["nope"]), {
    kind: "unknown",
    segment: "nope",
  });
  assert.deepEqual(resolveBodyPath(manifest, "reading", ["sensor", "bad"]), {
    kind: "unknown",
    segment: "bad",
  });
  // A scalar has no members: descending past it is a wrong path.
  assert.deepEqual(resolveBodyPath(manifest, "reading", ["temp", "x"]), {
    kind: "unknown",
    segment: "x",
  });
});

test("resolveBodyPath stays indeterminate through json and unknown scope", () => {
  assert.equal(resolveBodyPath(manifest, "reading", ["meta", "anything"]).kind, "indeterminate");
  assert.equal(resolveBodyPath(manifest, undefined, ["room"]).kind, "indeterminate");
  assert.equal(resolveBodyPath(manifest, "no-such-type", ["room"]).kind, "indeterminate");
});

test("isDeclaredType distinguishes registry types from primitives/unknowns", () => {
  assert.equal(isDeclaredType(manifest, "reading"), true);
  assert.equal(isDeclaredType(manifest, "string"), false);
  assert.equal(isDeclaredType(manifest, undefined), false);
});

test("scopeVarsForType returns fields, recursing into nested declared types", () => {
  const vars = scopeVarsForType(manifest, "reading");
  assert.deepEqual(vars.map((v) => v.name).sort(), ["meta", "readings", "room", "sensor", "temp"]);
  const room = vars.find((v) => v.name === "room");
  assert.deepEqual({ ...room }, { name: "room", type: "string", list: false });
  // a nested declared type carries its fields as entries; lists too (FEEL projects them)
  const sensor = vars.find((v) => v.name === "sensor");
  assert.deepEqual(sensor.entries.map((e) => e.name).sort(), ["id", "tags"]);
  const readings = vars.find((v) => v.name === "readings");
  assert.equal(readings.list, true);
  assert.deepEqual(readings.entries.map((e) => e.name).sort(), ["id", "tags"]);
  // primitives / json carry no entries
  assert.equal(vars.find((v) => v.name === "temp").entries, undefined);
  assert.equal(vars.find((v) => v.name === "meta").entries, undefined);
});

test("scopeVarsForType breaks cycles in the nominal type graph", () => {
  const m = { types: { node: { fields: { next: { type: "node" }, label: { type: "string" } } } } };
  const vars = scopeVarsForType(m, "node");
  assert.deepEqual(vars.map((v) => v.name).sort(), ["label", "next"]);
  const next = vars.find((v) => v.name === "next");
  // `next` is the same type already on the path — it does not expand (no loop),
  // but is still offered as a variable with its type hint.
  assert.deepEqual({ ...next }, { name: "next", type: "node", list: false });
  assert.equal(next.entries, undefined);
});

test("decisionScope resolves the type bound to a decision, else undefined", () => {
  const m = {
    types: manifest.types,
    bindings: [{ decision: "triage", type: "reading" }, { form: "f", type: "reading" }],
  };
  const vars = decisionScope(m, "triage");
  assert.deepEqual(vars.map((v) => v.name).sort(), ["meta", "readings", "room", "sensor", "temp"]);
  // no binding for this decision → undefined (contribute no domain variables)
  assert.equal(decisionScope(m, "other"), undefined);
  // a binding whose type isn't declared → undefined
  const m2 = { types: manifest.types, bindings: [{ decision: "d", type: "ghost" }] };
  assert.equal(decisionScope(m2, "d"), undefined);
  assert.equal(decisionScope(m, undefined), undefined);
});

test("processScope resolves the type bound to a process, else undefined (ADR 0030)", () => {
  const m = {
    types: manifest.types,
    bindings: [{ process: "order-cycle", type: "reading" }, { decision: "d", type: "reading" }],
  };
  const vars = processScope(m, "order-cycle");
  assert.deepEqual(vars.map((v) => v.name).sort(), ["meta", "readings", "room", "sensor", "temp"]);
  // a process id with no binding, an undeclared type, and a missing id → undefined
  assert.equal(processScope(m, "other"), undefined);
  const m2 = { types: manifest.types, bindings: [{ process: "p", type: "ghost" }] };
  assert.equal(processScope(m2, "p"), undefined);
  assert.equal(processScope(m, undefined), undefined);
});

test("outputTypeForTaskType resolves a worker's declared output type (ADR 0033 §3)", () => {
  const m = {
    types: manifest.types,
    workers: [
      { taskType: "read-thermostat", handler: "x", outputType: "reading" },
      { taskType: "classify", llm: "c", outputType: "ghost" }, // undeclared type → undefined
      { taskType: "noisy", handler: "y" }, // no outputType → undefined
    ],
  };
  assert.equal(outputTypeForTaskType(m, "read-thermostat"), "reading");
  assert.equal(outputTypeForTaskType(m, "classify"), undefined);
  assert.equal(outputTypeForTaskType(m, "noisy"), undefined);
  assert.equal(outputTypeForTaskType(m, "no-such-worker"), undefined);
  assert.equal(outputTypeForTaskType(m, undefined), undefined);
});

test("componentOutputScope types each output-mapped variable by its worker (ADR 0033 §3)", () => {
  const m = {
    types: manifest.types,
    workers: [
      { taskType: "read-thermostat", handler: "x", outputType: "reading" },
      { taskType: "classify", llm: "c" }, // untyped worker
    ],
  };
  const scope = componentOutputScope(m, [
    { taskType: "read-thermostat", target: "current" },
    { taskType: "classify", target: "category" }, // untyped → skipped
    { taskType: "read-thermostat", target: "current" }, // duplicate target → kept once
    { taskType: "read-thermostat", target: "" }, // empty target → skipped
  ]);
  assert.deepEqual(scope.map((v) => v.name), ["current"]);
  assert.equal(scope[0].type, "reading");
  assert.deepEqual(scope[0].entries.map((e) => e.name).sort(), ["meta", "readings", "room", "sensor", "temp"]);
  // no outputs, or all untyped → empty scope
  assert.deepEqual(componentOutputScope(m, []), []);
});

test("formTypeId resolves the declared type bound to a form, else undefined (ADR 0033 §6)", () => {
  const m = {
    types: manifest.types,
    bindings: [
      { form: "order-form", type: "reading" },
      { decision: "order-form", type: "sensor" }, // same id, different discriminator → ignored
      { form: "ghost-form", type: "ghost" }, // undeclared type → undefined
    ],
  };
  assert.equal(formTypeId(m, "order-form"), "reading");
  assert.equal(formTypeId(m, "ghost-form"), undefined);
  assert.equal(formTypeId(m, "no-such-form"), undefined);
  assert.equal(formTypeId(m, undefined), undefined);
  assert.equal(formTypeId({ types: manifest.types }, "order-form"), undefined); // no bindings
});

// --- data.query builtin (ADR 0024 §5) --------------------------------------

test("dataQueryCalls extracts the alias from the two-argument form", () => {
  const calls = dataQueryCalls('= data.query("app", "SELECT * FROM t")');
  assert.equal(calls.length, 1);
  assert.equal(calls[0].source, "app");
});

test("dataQueryCalls returns null source for the default-source form", () => {
  const calls = dataQueryCalls('= data.query("SELECT 1")');
  assert.deepEqual(calls.map((c) => c.source), [null]);
});

test("dataQueryCalls is conservative: dynamic first arg is unverifiable (null)", () => {
  assert.deepEqual(dataQueryCalls("= data.query(src, sql)").map((c) => c.source), [null]);
  assert.deepEqual(dataQueryCalls("= data.query(body.alias, \"x\")").map((c) => c.source), [null]);
});

test("dataQueryCalls finds multiple calls and tolerates whitespace", () => {
  const calls = dataQueryCalls('= { a: data . query ( "app" , "x" ), b: data.query("warehouse","y") }');
  assert.deepEqual(calls.map((c) => c.source), ["app", "warehouse"]);
  assert.ok(calls[0].index < calls[1].index);
});

test("dataQueryCalls ignores lookalikes (data.queryFoo, other.query)", () => {
  assert.deepEqual(dataQueryCalls('= data.queryFoo("app","x")'), []);
  assert.deepEqual(dataQueryCalls('= other.query("app","x")'), []);
});

test("DATA_QUERY exposes the read-only App-tier contract", () => {
  assert.equal(DATA_QUERY.name, "data.query");
  assert.deepEqual(DATA_QUERY.forms, ["data.query(source, sql)", "data.query(sql)"]);
});
