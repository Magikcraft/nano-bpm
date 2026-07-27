// The owned Page Composer schema (ADR 0042 §1). This is the persisted `page.json`
// contract — deliberately independent of Craft.js's internal serialized format so
// the authoring canvas can be swapped without a data migration, exactly as the
// form-js *schema* is independent of the form-js *editor*. The runtime renderer
// (App-side) reads only this shape and never imports Craft.js.
//
// v1 is a titled, ordered, flat list of typed nodes (no nesting — see ADR 0042
// Increment 2). Every rule here is pure and unit-testable.

export const PAGE_SCHEMA_VERSION = "1.0" as const;

/** A field on an `actionForm` (a labelled input contributing one start variable). */
export interface ActionFormField {
  /** The variable key the input value is submitted under (verbatim, ADR 0042 OQ). */
  key: string;
  label: string;
  /** v1 supports only free-text inputs. */
  type: "text";
}

/** An `actionForm`'s submit binding — v1 starts a process (ADR 0026 §1 `start`). */
export interface StartProcessAction {
  kind: "startProcess";
  /** A manifest process id (typed by the fuse's process registry, ADR 0040). */
  process: string;
}

/** A `dataGrid`'s data binding — v1 reads a datasource table (the rest bank, 0024). */
export interface DatasourceBinding {
  kind: "datasource";
  /** The datasource alias (default `app`). */
  source: string;
  /** A table/entity id from the fuse. */
  table: string;
}

export interface GridColumn {
  /** A column name on the bound table/entity. */
  field: string;
  header: string;
}

export type TextVariant = "heading" | "body" | "sub";

export interface TextNode {
  type: "text";
  id: string;
  props: { text: string; variant: TextVariant };
}

export interface ActionFormNode {
  type: "actionForm";
  id: string;
  props: {
    title: string;
    submitLabel: string;
    action: StartProcessAction;
    fields: ActionFormField[];
  };
}

export interface DataGridNode {
  type: "dataGrid";
  id: string;
  props: {
    title: string;
    data: DatasourceBinding;
    columns: GridColumn[];
  };
}

export type PageNode = TextNode | ActionFormNode | DataGridNode;
export type PageNodeType = PageNode["type"];

export const PAGE_NODE_TYPES: PageNodeType[] = ["text", "actionForm", "dataGrid"];

export interface PageDoc {
  schemaVersion: typeof PAGE_SCHEMA_VERSION;
  title: string;
  nodes: PageNode[];
}

/** A fresh, empty page. */
export function emptyPage(title = "Untitled page"): PageDoc {
  return { schemaVersion: PAGE_SCHEMA_VERSION, title, nodes: [] };
}

/** Default props for a newly-dropped node of each type (used by the palette). */
export function defaultProps(type: PageNodeType): PageNode["props"] {
  switch (type) {
    case "text":
      return { text: "Text", variant: "body" };
    case "actionForm":
      return {
        title: "Action",
        submitLabel: "Submit",
        action: { kind: "startProcess", process: "" },
        fields: [{ key: "input", label: "Input", type: "text" }],
      };
    case "dataGrid":
      return { title: "Data", data: { kind: "datasource", source: "app", table: "" }, columns: [] };
  }
}

function isRecord(x: unknown): x is Record<string, unknown> {
  return typeof x === "object" && x !== null && !Array.isArray(x);
}

/**
 * Validate an arbitrary value as a `PageDoc`, returning the parsed doc or a list
 * of human-readable errors. The Console's Page Composer and its serializer go
 * through this on open/save so a malformed page fails loudly rather than
 * rendering half a screen. (The App-side runtime renderer trusts the persisted
 * `page.json` and does not re-validate.)
 */
export function parsePageDoc(value: unknown): { ok: true; doc: PageDoc } | { ok: false; errors: string[] } {
  const errors: string[] = [];
  if (!isRecord(value)) return { ok: false, errors: ["page.json must be an object"] };
  if (value.schemaVersion !== PAGE_SCHEMA_VERSION) {
    errors.push(`schemaVersion must be "${PAGE_SCHEMA_VERSION}"`);
  }
  const title = typeof value.title === "string" ? value.title : "";
  if (typeof value.title !== "string") errors.push("title must be a string");
  const rawNodes = Array.isArray(value.nodes) ? value.nodes : null;
  if (!rawNodes) errors.push("nodes must be an array");

  const nodes: PageNode[] = [];
  const seen = new Set<string>();
  for (const [i, raw] of (rawNodes ?? []).entries()) {
    if (!isRecord(raw)) {
      errors.push(`nodes[${i}] must be an object`);
      continue;
    }
    const id = typeof raw.id === "string" ? raw.id : "";
    if (!id) errors.push(`nodes[${i}].id must be a non-empty string`);
    else if (seen.has(id)) errors.push(`nodes[${i}].id "${id}" is duplicated`);
    seen.add(id);
    const type = raw.type;
    const props = isRecord(raw.props) ? raw.props : {};
    switch (type) {
      case "text":
        nodes.push({
          type: "text",
          id,
          props: {
            text: typeof props.text === "string" ? props.text : "",
            variant: (props.variant === "heading" || props.variant === "sub" ? props.variant : "body"),
          },
        });
        break;
      case "actionForm": {
        const action = isRecord(props.action) ? props.action : {};
        const fields = Array.isArray(props.fields) ? props.fields : [];
        nodes.push({
          type: "actionForm",
          id,
          props: {
            title: typeof props.title === "string" ? props.title : "",
            submitLabel: typeof props.submitLabel === "string" ? props.submitLabel : "Submit",
            action: { kind: "startProcess", process: typeof action.process === "string" ? action.process : "" },
            fields: fields.filter(isRecord).map((f) => ({
              key: typeof f.key === "string" ? f.key : "",
              label: typeof f.label === "string" ? f.label : "",
              type: "text",
            })),
          },
        });
        break;
      }
      case "dataGrid": {
        const data = isRecord(props.data) ? props.data : {};
        const columns = Array.isArray(props.columns) ? props.columns : [];
        nodes.push({
          type: "dataGrid",
          id,
          props: {
            title: typeof props.title === "string" ? props.title : "",
            data: {
              kind: "datasource",
              source: typeof data.source === "string" ? data.source : "app",
              table: typeof data.table === "string" ? data.table : "",
            },
            columns: columns.filter(isRecord).map((c) => ({
              field: typeof c.field === "string" ? c.field : "",
              header: typeof c.header === "string" ? c.header : "",
            })),
          },
        });
        break;
      }
      default:
        errors.push(`nodes[${i}].type "${String(type)}" is not a known node type`);
    }
  }

  if (errors.length) return { ok: false, errors };
  return { ok: true, doc: { schemaVersion: PAGE_SCHEMA_VERSION, title, nodes } };
}
