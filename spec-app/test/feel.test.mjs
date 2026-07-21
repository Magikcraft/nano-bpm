// Tests for the shared FEEL scope model (ADR 0029 §5): `bodyPaths` extraction
// and `resolveBodyPath` type-walking. `node --test`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { bodyPaths, resolveBodyPath, isDeclaredType } from "../src/feel.ts";

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
