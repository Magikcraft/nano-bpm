// Unit tests for the Page Composer schema + serializer (ADR 0042 §1).
// Node-native: `node --experimental-strip-types --test src/lib/pageComposer.test.ts`.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  emptyPage,
  parsePageDoc,
  PAGE_SCHEMA_VERSION,
  type PageDoc,
} from "./pageSchema.ts";
import {
  fromPageDoc,
  toPageDoc,
  ROOT_ID,
  makeNode,
  type CraftState,
} from "./pageComposer.ts";

// The witness page: urban-pr-review's Submit + Converging screen (ADR 0042 Context).
const witness: PageDoc = {
  schemaVersion: PAGE_SCHEMA_VERSION,
  title: "PR Review Convergence",
  nodes: [
    {
      type: "text",
      id: "h1",
      props: { text: "PR Review Convergence", variant: "heading" },
    },
    {
      type: "actionForm",
      id: "submit",
      props: {
        title: "Submit PR",
        submitLabel: "Submit PR",
        action: { kind: "startProcess", process: "convergence-loop" },
        fields: [
          { key: "pr", label: "owner/repo#123 or PR URL", type: "text" },
        ],
      },
    },
    {
      type: "dataGrid",
      id: "list",
      props: {
        title: "Converging",
        data: { kind: "datasource", source: "app", table: "pull_requests" },
        columns: [
          { field: "pr_key", header: "PR" },
          { field: "status", header: "Status" },
        ],
      },
    },
  ],
};

test("parsePageDoc accepts the witness page unchanged", () => {
  const r = parsePageDoc(witness);
  assert.ok(r.ok, r.ok ? "" : r.errors.join("; "));
  assert.deepEqual(r.doc, witness);
});

test("emptyPage is a valid, empty page", () => {
  const r = parsePageDoc(emptyPage("Home"));
  assert.ok(r.ok);
  assert.equal(r.ok && r.doc.nodes.length, 0);
});

test("parsePageDoc rejects an unknown node type", () => {
  const r = parsePageDoc({
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "x",
    nodes: [{ type: "chart", id: "c1", props: {} }],
  });
  assert.ok(!r.ok);
  assert.match(
    (r as { errors: string[] }).errors.join(" "),
    /not a known node type/,
  );
});

test("parsePageDoc rejects a duplicate node id", () => {
  const r = parsePageDoc({
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "x",
    nodes: [
      { type: "text", id: "dup", props: { text: "a", variant: "body" } },
      { type: "text", id: "dup", props: { text: "b", variant: "body" } },
    ],
  });
  assert.ok(!r.ok);
  assert.match((r as { errors: string[] }).errors.join(" "), /duplicated/);
});

test("parsePageDoc coerces missing/bad props to canonical defaults", () => {
  const r = parsePageDoc({
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "x",
    nodes: [{ type: "text", id: "t", props: { variant: "nope" } }],
  });
  assert.ok(r.ok);
  assert.deepEqual(r.ok && r.doc.nodes[0], {
    type: "text",
    id: "t",
    props: { text: "", variant: "body" },
  });
});

test("wrong schemaVersion is an error", () => {
  const r = parsePageDoc({ schemaVersion: "9.9", title: "x", nodes: [] });
  assert.ok(!r.ok);
});

test("Craft.js round-trip: fromPageDoc → toPageDoc is identity", () => {
  const state = fromPageDoc(witness);
  // ROOT canvas orders the three children.
  assert.deepEqual(state[ROOT_ID].nodes, ["h1", "submit", "list"]);
  assert.equal(state["submit"].parent, ROOT_ID);
  const back = toPageDoc(state, witness.title);
  assert.deepEqual(back, witness);
});

test("toPageDoc skips unknown Craft components + preserves child order", () => {
  const state: CraftState = {
    [ROOT_ID]: {
      type: { resolvedName: "PageCanvas" },
      isCanvas: true,
      nodes: ["a", "ghost", "b"],
    },
    a: {
      type: { resolvedName: "TextNode" },
      props: { text: "A", variant: "body" },
    },
    ghost: { type: { resolvedName: "UnknownWidget" }, props: {} },
    b: {
      type: { resolvedName: "TextNode" },
      props: { text: "B", variant: "sub" },
    },
  };
  const doc = toPageDoc(state, "T");
  assert.deepEqual(
    doc.nodes.map((n) => n.id),
    ["a", "b"],
  );
});

test("makeNode produces a validatable node of the requested type", () => {
  for (const type of ["text", "actionForm", "dataGrid"] as const) {
    const n = makeNode(type);
    const r = parsePageDoc({
      schemaVersion: PAGE_SCHEMA_VERSION,
      title: "x",
      nodes: [n],
    });
    assert.ok(r.ok, r.ok ? "" : r.errors.join("; "));
    assert.equal(n.type, type);
  }
});
