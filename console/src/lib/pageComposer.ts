// Pure serializer between Craft.js editor state and the owned `page.json` schema
// (ADR 0042 §1). `PageComposer.tsx` is a thin React shell over these functions; the
// Craft.js `<Editor>` holds the live tree, and on save we `toPageDoc(query.serialize())`
// down to the persisted contract, and on open we `fromPageDoc()` back into Craft.js's
// serialized form. Keeping this pure (no React, no Craft.js runtime) makes the
// round-trip unit-testable and the persisted artifact tool-independent.

import {
  defaultProps,
  emptyPage,
  type GridColumn,
  type PageDoc,
  type PageNode,
  type PageNodeType,
  PAGE_NODE_TYPES,
  PAGE_SCHEMA_VERSION,
  parsePageDoc,
} from "./pageSchema.ts";

/** The result of loading a persisted `page.json` string for the composer. A blank
 *  string is a NEW page (`ok` with an empty doc). Non-empty content that fails to
 *  parse or validate is `ok:false` with human-readable errors — the caller must NOT
 *  fall back to a blank editable canvas (a blank + Save would overwrite the file). */
export type LoadPageResult =
  { ok: true; doc: PageDoc } | { ok: false; errors: string[] };

/**
 * Pure decision for opening a `page.json` in the composer. Kept React/Craft-free so
 * the "load vs. surface-an-error" rule (the data-loss guard) is unit-testable.
 *  - blank/whitespace → a fresh empty page titled `title` (new file).
 *  - not valid JSON → `ok:false` (do not blank the canvas).
 *  - valid JSON but schema-invalid (e.g. an unknown/newer node type) → `ok:false`.
 *  - valid → `ok:true` with the canonicalized doc.
 */
export function loadPageJson(text: string, title: string): LoadPageResult {
  const trimmed = text.trim();
  if (!trimmed) return { ok: true, doc: emptyPage(title) };
  let value: unknown;
  try {
    value = JSON.parse(trimmed);
  } catch (e) {
    return {
      ok: false,
      errors: [
        `page.json is not valid JSON: ${e instanceof Error ? e.message : String(e)}`,
      ],
    };
  }
  const parsed = parsePageDoc(value);
  if (!parsed.ok) return { ok: false, errors: parsed.errors };
  return { ok: true, doc: parsed.doc };
}

/**
 * Reconcile a grid's edited `field`/`header` rows (from the string-cell
 * ListEditor) back onto the structured `GridColumn[]`, carrying over each
 * column's non-editable `link`. The ListEditor mutates one column at a time:
 *  - add/edit keeps order + count, so existing columns keep their index and a
 *    positional match preserves a link across a `field` rename.
 *  - delete shortens the list and shifts indices, so we instead match on the
 *    stable `field`+`header` identity. Columns may legitimately share a
 *    `field`, so a link is only carried over when EXACTLY ONE previous column
 *    matches — an ambiguous (or absent) match drops the link rather than risk
 *    re-attaching it to the wrong neighbour.
 */
export function reconcileGridColumns(
  prevCols: GridColumn[],
  rows: { field?: string; header?: string }[],
): GridColumn[] {
  const deleted = rows.length < prevCols.length;
  return rows.map((r, i) => {
    const field = r.field ?? "";
    const header = r.header ?? "";
    let prev: GridColumn | undefined;
    if (deleted) {
      const matches = prevCols.filter(
        (c) => c.field === field && (c.header ?? "") === header,
      );
      prev = matches.length === 1 ? matches[0] : undefined;
    } else {
      prev = prevCols[i];
    }
    const col: GridColumn = { field, header };
    return prev?.link ? { ...col, link: prev.link } : col;
  });
}

/** The Craft.js resolver name for our root canvas. */
export const ROOT_ID = "ROOT";
/** The single Craft.js component that hosts the ordered node list (a canvas). */
export const PAGE_CANVAS_NAME = "PageCanvas";

/** Craft.js `resolvedName` ↔ our node type. The composer registers its components
 * under these names, so the mapping is identity for the leaf nodes. */
const CRAFT_NAME: Record<PageNodeType, string> = {
  text: "TextNode",
  nav: "NavNode",
  actionForm: "ActionFormNode",
  dataGrid: "DataGridNode",
  prose: "ProseNode",
  button: "ButtonNode",
};
const TYPE_BY_CRAFT: Record<string, PageNodeType> = Object.fromEntries(
  PAGE_NODE_TYPES.map((t) => [CRAFT_NAME[t], t]),
);

/** A minimal view of one Craft.js serialized node (the fields we read/write). */
export interface CraftNode {
  type: { resolvedName: string } | string;
  isCanvas?: boolean;
  props?: Record<string, unknown>;
  displayName?: string;
  custom?: Record<string, unknown>;
  parent?: string | null;
  hidden?: boolean;
  nodes?: string[];
  linkedNodes?: Record<string, string>;
}

export type CraftState = Record<string, CraftNode>;

function resolvedName(t: CraftNode["type"]): string {
  return typeof t === "string" ? t : t.resolvedName;
}

/**
 * Serialize a Craft.js editor state (the object form of `query.serialize()`) down
 * to the owned `page.json`. Reads the ROOT canvas's children in order, mapping each
 * to a `PageNode`; unknown component types are skipped (a page never carries a node
 * the runtime renderer can't render).
 */
export function toPageDoc(state: CraftState, title: string): PageDoc {
  const root = state[ROOT_ID];
  const childIds = root?.nodes ?? [];
  const nodes: PageNode[] = [];
  for (const id of childIds) {
    const cn = state[id];
    if (!cn) continue;
    const type = TYPE_BY_CRAFT[resolvedName(cn.type)];
    if (!type) continue;
    nodes.push({ type, id, props: cn.props ?? {} } as PageNode);
  }
  // Round-trip through the validator so persisted pages are always canonical.
  const parsed = parsePageDoc({
    schemaVersion: PAGE_SCHEMA_VERSION,
    title,
    nodes,
  });
  return parsed.ok
    ? parsed.doc
    : { schemaVersion: PAGE_SCHEMA_VERSION, title, nodes };
}

/**
 * Canonical serialization of a Craft.js state's ordered node list. This is the
 * single source of truth for "does the canvas differ from what was loaded?": the
 * composer captures this at load time and compares against it on every Craft
 * `onNodesChange` to decide dirtiness, rather than treating each change event
 * (which also fires for the programmatic load and the initial mount) as a user
 * edit. Deriving dirtiness this way makes the signal deterministic and immune to
 * load/mount echo — no timing guesswork. Title is intentionally excluded: title
 * edits are signalled separately by the title input, and excluding it keeps the
 * comparison independent of async title state.
 */
export function serializePageNodes(state: CraftState): string {
  return JSON.stringify(toPageDoc(state, "").nodes);
}

/**
 * Inflate a `page.json` into a Craft.js serialized state so the composer can open an
 * existing page. Produces a ROOT canvas whose ordered children are the page's nodes.
 */
export function fromPageDoc(doc: PageDoc): CraftState {
  const state: CraftState = {
    [ROOT_ID]: {
      type: { resolvedName: PAGE_CANVAS_NAME },
      isCanvas: true,
      props: {},
      displayName: PAGE_CANVAS_NAME,
      custom: {},
      hidden: false,
      nodes: doc.nodes.map((n) => n.id),
      linkedNodes: {},
      parent: null,
    },
  };
  for (const n of doc.nodes) {
    state[n.id] = {
      type: { resolvedName: CRAFT_NAME[n.type] },
      isCanvas: false,
      props: n.props as Record<string, unknown>,
      displayName: CRAFT_NAME[n.type],
      custom: {},
      hidden: false,
      nodes: [],
      linkedNodes: {},
      parent: ROOT_ID,
    };
  }
  return state;
}

/** A fresh node id (stable enough for a session; Craft.js reassigns on drop). */
export function newNodeId(type: PageNodeType): string {
  return `${type}-${Math.random().toString(36).slice(2, 8)}`;
}

/** Build a `PageNode` with default props for a palette drop. */
export function makeNode(type: PageNodeType): PageNode {
  return { type, id: newNodeId(type), props: defaultProps(type) } as PageNode;
}
