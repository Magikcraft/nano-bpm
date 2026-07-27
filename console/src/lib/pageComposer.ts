// Pure serializer between Craft.js editor state and the owned `page.json` schema
// (ADR 0042 §1). `PageComposer.tsx` is a thin React shell over these functions; the
// Craft.js `<Editor>` holds the live tree, and on save we `toPageDoc(query.serialize())`
// down to the persisted contract, and on open we `fromPageDoc()` back into Craft.js's
// serialized form. Keeping this pure (no React, no Craft.js runtime) makes the
// round-trip unit-testable and the persisted artifact tool-independent.

import {
  defaultProps,
  type PageDoc,
  type PageNode,
  type PageNodeType,
  PAGE_NODE_TYPES,
  PAGE_SCHEMA_VERSION,
  parsePageDoc,
} from "./pageSchema.ts";

/** The Craft.js resolver name for our root canvas. */
export const ROOT_ID = "ROOT";

/** The single Craft.js component that hosts the ordered node list (a canvas). */
export const PAGE_CANVAS_NAME = "PageCanvas";

/** Craft.js `resolvedName` ↔ our node type. The composer registers its components
 * under these names, so the mapping is identity for the leaf nodes. */
const CRAFT_NAME: Record<PageNodeType, string> = {
  text: "TextNode",
  actionForm: "ActionFormNode",
  dataGrid: "DataGridNode",
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
