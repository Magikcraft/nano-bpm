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

// The full v2 witness: filtered/tabbed grid with row actions (cancel), a lazy
// nested child grid, and a conditional escalation-answer form (ADR 0042 v2).
const v2Grid: PageDoc = {
  schemaVersion: PAGE_SCHEMA_VERSION,
  title: "PRs",
  nodes: [
    {
      type: "dataGrid",
      id: "list",
      props: {
        title: "Pull Requests",
        data: {
          kind: "datasource",
          source: "app",
          table: "pull_requests",
          filter: [{ field: "status", eq: "converging" }],
          orderBy: { field: "updated_at", dir: "desc" },
        },
        columns: [{ field: "pr_key", header: "PR" }],
        rowKey: "pr_key",
        tabs: [
          {
            label: "Active",
            filter: [
              {
                field: "status",
                in: ["converging", "waiting_review", "escalated"],
              },
            ],
          },
          {
            label: "History",
            filter: [{ field: "status", in: ["converged", "abandoned"] }],
          },
        ],
        rowActions: [
          {
            label: "Cancel",
            confirm: "Cancel this PR review?",
            action: { kind: "cancelProcess", keyField: "process_key" },
          },
        ],
        detail: {
          linkField: "url",
          fields: [{ field: "repo", label: "Repo" }],
          children: [
            {
              title: "Rounds",
              source: "app",
              table: "rounds",
              parentField: "pr_key",
              childField: "pr_key",
              columns: [{ field: "round_no", header: "#" }],
              orderBy: { field: "round_no", dir: "asc" },
              lazyField: {
                field: "transcript",
                label: "Transcript",
                lazy: true,
              },
            },
          ],
          form: {
            showWhenField: "open_escalation_id",
            title: "Answer escalation",
            promptField: "open_escalation_question",
            inputKey: "answer",
            inputLabel: "Your answer",
            submitLabel: "Send answer",
            action: {
              kind: "publishMessage",
              message: "escalation-answered",
              correlationKeyField: "pr_key",
            },
          },
        },
        refreshMs: 5000,
      },
    },
  ],
};

test("parsePageDoc accepts the full v2 dataGrid (filters, tabs, actions, detail)", () => {
  const r = parsePageDoc(v2Grid);
  assert.ok(r.ok, r.ok ? "" : r.errors.join("; "));
  assert.deepEqual(r.doc, v2Grid);
});

test("v2 dataGrid survives a Craft.js round-trip unchanged", () => {
  const state = fromPageDoc(v2Grid);
  const back = toPageDoc(state, v2Grid.title);
  assert.deepEqual(back, v2Grid);
});

test("parsePageDoc drops a row action with an unknown kind", () => {
  const r = parsePageDoc({
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "x",
    nodes: [
      {
        type: "dataGrid",
        id: "g",
        props: {
          data: { kind: "datasource", source: "app", table: "t" },
          columns: [{ field: "id", header: "ID" }],
          rowActions: [
            { label: "Bad", action: { kind: "wat" } },
            {
              label: "Cancel",
              action: { kind: "cancelProcess", keyField: "k" },
            },
          ],
        },
      },
    ],
  });
  assert.ok(r.ok, r.ok ? "" : r.errors.join("; "));
  const grid = r.ok && r.doc.nodes[0];
  assert.equal(
    grid && grid.type === "dataGrid" && grid.props.rowActions?.length,
    1,
  );
});

test("parsePageDoc omits an empty detail rather than storing a hollow object", () => {
  const r = parsePageDoc({
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "x",
    nodes: [
      {
        type: "dataGrid",
        id: "g",
        props: {
          data: { kind: "datasource", source: "app", table: "t" },
          columns: [],
          detail: { fields: [], children: [] },
        },
      },
    ],
  });
  assert.ok(r.ok);
  const grid = r.ok && r.doc.nodes[0];
  assert.equal(
    grid && grid.type === "dataGrid" && grid.props.detail,
    undefined,
  );
});
