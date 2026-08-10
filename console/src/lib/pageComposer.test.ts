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
  loadPageJson,
  reconcileGridColumns,
  serializePageNodes,
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

// ── nav node (regression: Nano Workforce pages opened blank + "unsaved" because
// the Console schema lacked `nav`, which urban's renderer supports — issue #705) ──

const navWitness: PageDoc = {
  schemaVersion: PAGE_SCHEMA_VERSION,
  title: "Nav witness",
  nodes: [
    {
      type: "nav",
      id: "topbar",
      props: {
        variant: "bar",
        title: "Nano Workforce",
        items: [
          { label: "Convergence", page: "home" },
          { label: "Docs", href: "https://nanobpm.io", icon: "📖" },
        ],
      },
    },
    {
      type: "nav",
      id: "side",
      props: { variant: "rail", items: "auto" },
    },
  ],
};

test("parsePageDoc accepts a nav page (bar+rail, explicit items and auto) unchanged", () => {
  const r = parsePageDoc(navWitness);
  assert.ok(r.ok, r.ok ? "" : r.errors.join("; "));
  assert.deepEqual(r.doc, navWitness);
});

test("Craft.js round-trip preserves a nav node", () => {
  const state = fromPageDoc(navWitness);
  assert.deepEqual(state[ROOT_ID].nodes, ["topbar", "side"]);
  assert.equal(state["topbar"].parent, ROOT_ID);
  const back = toPageDoc(state, navWitness.title);
  assert.deepEqual(back, navWitness);
});

test("nav item: page wins over href, empty items are dropped (matches urban navLink)", () => {
  const r = parsePageDoc({
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "x",
    nodes: [
      {
        type: "nav",
        id: "n",
        props: {
          variant: "bogus", // → defaults to "bar"
          items: [
            { label: "Both", page: "home", href: "https://x.test" }, // page wins
            { label: "Docs", href: "https://docs.test" }, // external kept
            { label: "", href: "" }, // no label + no target → dropped entirely
          ],
        },
      },
    ],
  });
  assert.ok(r.ok, r.ok ? "" : r.errors.join("; "));
  const nav = r.ok && r.doc.nodes[0];
  assert.deepEqual(nav, {
    type: "nav",
    id: "n",
    props: {
      variant: "bar",
      items: [
        { label: "Both", page: "home" },
        { label: "Docs", href: "https://docs.test" },
      ],
    },
  });
});

test("nav item: an unsafe href scheme is dropped (only http(s) is persisted)", () => {
  const r = parsePageDoc({
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "x",
    nodes: [
      {
        type: "nav",
        id: "n",
        props: {
          items: [
            { label: "XSS", href: "javascript:alert(1)" }, // scheme stripped
            { label: "Rel", href: "/relative/path" }, // non-http(s) stripped
          ],
        },
      },
    ],
  });
  assert.ok(r.ok, r.ok ? "" : r.errors.join("; "));
  const nav = r.ok && r.doc.nodes[0];
  // Both hrefs are unsafe/non-http(s) → dropped, leaving bare-label items (they
  // still have a label, so they are not removed as empty).
  assert.deepEqual(nav, {
    type: "nav",
    id: "n",
    props: {
      variant: "bar",
      items: [{ label: "XSS" }, { label: "Rel" }],
    },
  });
});

// ── load-path data-loss guard (issue #705): a page that fails to parse must NOT
// yield a blank editable doc — a blank + Save would overwrite the real file. ──

test("loadPageJson: blank string is a new empty page (ok)", () => {
  const r = loadPageJson("   ", "Home");
  assert.ok(r.ok);
  assert.equal(r.ok && r.doc.nodes.length, 0);
  assert.equal(r.ok && r.doc.title, "Home");
});

test("loadPageJson: an unknown node type is a load error, NOT a blank doc", () => {
  const raw = JSON.stringify({
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "Real page",
    nodes: [{ type: "chart", id: "c1", props: {} }],
  });
  const r = loadPageJson(raw, "Home");
  assert.ok(!r.ok, "must not silently succeed with a blank page");
  assert.match(
    (r as { errors: string[] }).errors.join(" "),
    /not a known node type/,
  );
});

test("loadPageJson: invalid JSON is a load error, NOT a blank doc", () => {
  const r = loadPageJson("{ not json", "Home");
  assert.ok(!r.ok);
  assert.match((r as { errors: string[] }).errors.join(" "), /not valid JSON/);
});

test("loadPageJson: a valid nav page loads (regression for Nano Workforce)", () => {
  const r = loadPageJson(JSON.stringify(navWitness), "x");
  assert.ok(r.ok, r.ok ? "" : (r as { errors: string[] }).errors.join("; "));
  assert.deepEqual(r.ok && r.doc, navWitness);
});

// The dirty signal is DERIVED from serializePageNodes, not from raw Craft change
// events. Guards the false-"unsaved" failure mode: loading a page (and the initial
// canvas mount) fire onNodesChange, but the node serialization is unchanged, so the
// composer must not report a pristine load as an edit.
test("serializePageNodes: loading a page is not an edit (baseline matches itself)", () => {
  const state = fromPageDoc(navWitness);
  const baseline = serializePageNodes(state);
  // Re-deriving from the same loaded state (what onNodesChange sees for a load /
  // mount echo) yields the identical baseline → no false dirty.
  assert.equal(serializePageNodes(fromPageDoc(navWitness)), baseline);
});

test("serializePageNodes: title differences do NOT read as a canvas edit", () => {
  // Title lives outside the node list (it is signalled separately), so a title-only
  // change must not flip the node-derived dirty signal.
  const a = serializePageNodes(fromPageDoc({ ...navWitness, title: "A" }));
  const b = serializePageNodes(fromPageDoc({ ...navWitness, title: "B" }));
  assert.equal(a, b);
});

test("serializePageNodes: a genuine node change IS detected as an edit", () => {
  const baseline = serializePageNodes(fromPageDoc(navWitness));
  const edited = fromPageDoc(navWitness);
  // Append a real node — what a palette "+ Text" drop does.
  const added = makeNode("text");
  edited[added.id] = {
    type: { resolvedName: "TextNode" },
    isCanvas: false,
    props: added.props as Record<string, unknown>,
    displayName: "TextNode",
    custom: {},
    hidden: false,
    nodes: [],
    linkedNodes: {},
    parent: ROOT_ID,
  };
  edited[ROOT_ID].nodes = [...(edited[ROOT_ID].nodes ?? []), added.id];
  assert.notEqual(serializePageNodes(edited), baseline);
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
  for (const type of ["text", "nav", "actionForm", "dataGrid"] as const) {
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

test("parsePageDoc keeps a processExplorer column link and round-trips it", () => {
  const doc = {
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "x",
    nodes: [
      {
        type: "dataGrid" as const,
        id: "g",
        props: {
          title: "",
          data: { kind: "datasource" as const, source: "app", table: "t" },
          columns: [
            {
              field: "status",
              header: "Status",
              link: {
                kind: "processExplorer" as const,
                keyField: "process_key",
              },
            },
          ],
        },
      },
    ],
  };
  const r = parsePageDoc(doc);
  assert.ok(r.ok, r.ok ? "" : r.errors.join("; "));
  const grid = r.ok && r.doc.nodes[0];
  const col =
    grid && grid.type === "dataGrid" ? grid.props.columns[0] : undefined;
  assert.deepEqual(col?.link, {
    kind: "processExplorer",
    keyField: "process_key",
  });
  // Survives a Craft.js round-trip unchanged.
  const back = toPageDoc(fromPageDoc(r.ok ? r.doc : doc), doc.title);
  const backGrid = back.nodes[0];
  assert.deepEqual(
    backGrid.type === "dataGrid" ? backGrid.props.columns[0].link : undefined,
    { kind: "processExplorer", keyField: "process_key" },
  );
});

const peLink = { kind: "processExplorer" as const, keyField: "k" };

test("reconcileGridColumns preserves a link across a field rename (edit keeps count)", () => {
  const prev = [
    { field: "status", header: "Status", link: peLink },
    { field: "url", header: "URL" },
  ];
  // The user renamed the first column's `field`; count is unchanged.
  const out = reconcileGridColumns(prev, [
    { field: "state", header: "Status" },
    { field: "url", header: "URL" },
  ]);
  assert.deepEqual(out[0], { field: "state", header: "Status", link: peLink });
  assert.equal(out[1].link, undefined);
});

test("reconcileGridColumns preserves links by identity when a column is deleted", () => {
  const prev = [
    { field: "a", header: "A" },
    { field: "status", header: "Status", link: peLink },
    { field: "b", header: "B" },
  ];
  // Deleted the first column — indices shift, so a positional match would
  // mis-attach the link to `a`. Identity match keeps it on `status`.
  const out = reconcileGridColumns(prev, [
    { field: "status", header: "Status" },
    { field: "b", header: "B" },
  ]);
  assert.deepEqual(out[0], { field: "status", header: "Status", link: peLink });
  assert.equal(out[1].link, undefined);
});

test("reconcileGridColumns drops a link when the delete match is ambiguous", () => {
  // Two columns share `field`+`header`; only one carries a link. On delete we
  // can't tell which survivor should keep it, so we drop rather than mis-attach.
  const prev = [
    { field: "dup", header: "Dup", link: peLink },
    { field: "dup", header: "Dup" },
    { field: "c", header: "C" },
  ];
  const out = reconcileGridColumns(prev, [
    { field: "dup", header: "Dup" },
    { field: "c", header: "C" },
  ]);
  assert.equal(out[0].link, undefined);
  assert.equal(out[1].link, undefined);
});

test("parsePageDoc drops a column link with an unknown kind or empty keyField", () => {
  const r = parsePageDoc({
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "x",
    nodes: [
      {
        type: "dataGrid",
        id: "g",
        props: {
          data: { kind: "datasource", source: "app", table: "t" },
          columns: [
            { field: "a", header: "A", link: { kind: "wat", keyField: "k" } },
            {
              field: "b",
              header: "B",
              link: { kind: "processExplorer", keyField: "" },
            },
          ],
        },
      },
    ],
  });
  assert.ok(r.ok, r.ok ? "" : r.errors.join("; "));
  const grid = r.ok && r.doc.nodes[0];
  const cols = grid && grid.type === "dataGrid" ? grid.props.columns : [];
  assert.equal(cols[0]?.link, undefined);
  assert.equal(cols[1]?.link, undefined);
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
