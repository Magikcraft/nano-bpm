// Round-trip tests for the C1 mobile-first responsive vocabulary the Page
// Composer authors (epic #1005, unit C3). Each test proves an author-composed
// responsive prop survives `parsePageDoc` and the Craft.js
// `fromPageDoc`→`toPageDoc` serializer identity — i.e. it round-trips through
// `*.page.json` without being dropped or defaulted away.
// Node-native: `node --experimental-strip-types --test src/lib/pageSchema.responsive.test.ts`.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  parsePageDoc,
  PAGE_SCHEMA_VERSION,
  COLUMN_MOBILE_PRIORITIES,
  NAV_VARIANTS,
  NAV_OVERFLOW_MODES,
  NAV_ITEM_GROUPS,
  DATA_GRID_MOBILE_PRESENTATIONS,
  LAYOUT_MOBILE_VARIANTS,
  type PageDoc,
} from "./pageSchema.ts";
import { fromPageDoc, toPageDoc } from "./pageComposer.ts";

/** Assert that a page both parses unchanged and survives the Craft serializer
 * round-trip (`fromPageDoc`→`toPageDoc`). This is the exact path a Composer edit
 * takes on save, so anything that survives it can be authored end-to-end. */
function assertRoundTrips(doc: PageDoc): void {
  const parsed = parsePageDoc(doc);
  assert.ok(parsed.ok, parsed.ok ? "" : parsed.errors.join("; "));
  assert.deepEqual(parsed.doc, doc);
  const back = toPageDoc(fromPageDoc(doc), doc.title);
  // The page-level `layout` is not a Craft node prop, so the serializer drops it
  // (the composer re-splices it from its own state). Compare the node tree here
  // and cover `layout` separately via `parsePageDoc` above.
  assert.deepEqual(doc.layout ? { ...back, layout: doc.layout } : back, doc);
}

test("column.mobile { priority, label } round-trips", () => {
  const doc: PageDoc = {
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "Grid mobile columns",
    nodes: [
      {
        type: "dataGrid",
        id: "grid",
        props: {
          title: "Instances",
          data: { kind: "datasource", source: "app", table: "instances" },
          columns: [
            {
              field: "key",
              header: "Key",
              mobile: { priority: "primary", label: "#" },
            },
            { field: "status", header: "Status", mobile: { priority: "chip" } },
            { field: "notes", header: "Notes", mobile: { priority: "hidden" } },
          ],
        },
      },
    ],
  };
  assertRoundTrips(doc);
});

test("every column.mobile priority is accepted", () => {
  for (const priority of COLUMN_MOBILE_PRIORITIES) {
    const doc: PageDoc = {
      schemaVersion: PAGE_SCHEMA_VERSION,
      title: "priority",
      nodes: [
        {
          type: "dataGrid",
          id: "g",
          props: {
            title: "T",
            data: { kind: "datasource", source: "app", table: "t" },
            columns: [{ field: "f", header: "F", mobile: { priority } }],
          },
        },
      ],
    };
    assertRoundTrips(doc);
  }
});

test("nav variant cards + overflow menu + item groups round-trip", () => {
  const doc: PageDoc = {
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "Launcher nav",
    nodes: [
      {
        type: "nav",
        id: "nav",
        props: {
          variant: "cards",
          title: "Menu",
          overflow: "menu",
          items: [
            { label: "Home", page: "home", group: "primary" },
            { label: "Settings", page: "settings", group: "secondary" },
          ],
        },
      },
    ],
  };
  assertRoundTrips(doc);
});

test("every nav variant / overflow / item group enum value is accepted", () => {
  for (const variant of NAV_VARIANTS) {
    for (const overflow of NAV_OVERFLOW_MODES) {
      for (const group of NAV_ITEM_GROUPS) {
        const doc: PageDoc = {
          schemaVersion: PAGE_SCHEMA_VERSION,
          title: "nav enums",
          nodes: [
            {
              type: "nav",
              id: "n",
              props: {
                variant,
                title: "N",
                overflow,
                items: [{ label: "Item", page: "p", group }],
              },
            },
          ],
        };
        assertRoundTrips(doc);
      }
    }
  }
});

test("nav item group defaults away when omitted (runtime applies primary)", () => {
  const doc: PageDoc = {
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "default group",
    nodes: [
      {
        type: "nav",
        id: "n",
        props: {
          variant: "bar",
          title: "N",
          items: [{ label: "Item", page: "p" }],
        },
      },
    ],
  };
  // No `group` key persisted — the runtime applies the `primary` default.
  assertRoundTrips(doc);
  const parsed = parsePageDoc(doc);
  assert.ok(parsed.ok);
  const nav = parsed.ok && parsed.doc.nodes[0];
  assert.ok(nav && nav.type === "nav" && Array.isArray(nav.props.items));
  assert.equal(
    nav && nav.type === "nav" && Array.isArray(nav.props.items)
      ? "group" in nav.props.items[0]
      : true,
    false,
  );
});

test("dataGrid.props.mobile.presentation round-trips (table + cards)", () => {
  for (const presentation of DATA_GRID_MOBILE_PRESENTATIONS) {
    const doc: PageDoc = {
      schemaVersion: PAGE_SCHEMA_VERSION,
      title: "grid mobile presentation",
      nodes: [
        {
          type: "dataGrid",
          id: "g",
          props: {
            title: "T",
            data: { kind: "datasource", source: "app", table: "t" },
            columns: [{ field: "f", header: "F" }],
            mobile: { presentation },
          },
        },
      ],
    };
    assertRoundTrips(doc);
  }
});

test("page-level layout.mobile round-trips for every variant", () => {
  for (const mobile of LAYOUT_MOBILE_VARIANTS) {
    const doc: PageDoc = {
      schemaVersion: PAGE_SCHEMA_VERSION,
      title: "page layout",
      nodes: [
        { type: "text", id: "t", props: { text: "Hi", variant: "body" } },
      ],
      layout: { mobile },
    };
    const parsed = parsePageDoc(doc);
    assert.ok(parsed.ok, parsed.ok ? "" : parsed.errors.join("; "));
    assert.deepEqual(parsed.doc, doc);
  }
});

test("unknown responsive enum values are dropped, not persisted", () => {
  const doc = {
    schemaVersion: PAGE_SCHEMA_VERSION,
    title: "bogus",
    nodes: [
      {
        type: "nav",
        id: "n",
        props: {
          variant: "spaceship",
          title: "N",
          overflow: "carousel",
          items: [{ label: "Item", page: "p", group: "tertiary" }],
        },
      },
      {
        type: "dataGrid",
        id: "g",
        props: {
          title: "T",
          data: { kind: "datasource", source: "app", table: "t" },
          columns: [{ field: "f", header: "F", mobile: { priority: "huge" } }],
          mobile: { presentation: "hologram" },
        },
      },
    ],
    layout: { mobile: "teleport" },
  };
  const parsed = parsePageDoc(doc);
  assert.ok(parsed.ok, parsed.ok ? "" : parsed.errors.join("; "));
  assert.ok(parsed.ok);
  const nav = parsed.doc.nodes[0];
  assert.ok(nav.type === "nav");
  assert.equal(nav.props.variant, "bar"); // fell back to the default
  assert.equal("overflow" in nav.props, false);
  assert.ok(Array.isArray(nav.props.items));
  assert.equal("group" in nav.props.items[0], false);
  const grid = parsed.doc.nodes[1];
  assert.ok(grid.type === "dataGrid");
  assert.equal("mobile" in grid.props, false);
  assert.equal("mobile" in grid.props.columns[0], false);
  assert.equal("layout" in parsed.doc, false);
});
