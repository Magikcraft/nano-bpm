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

/** An equality filter on a datasource column (whitelisted against the schema at
 * runtime, so it can never inject SQL). */
export interface ColumnFilter {
  field: string;
  eq: string;
}

/** A grid's sort order (a single column, ascending or descending). */
export interface GridOrder {
  field: string;
  dir: "asc" | "desc";
}

/** A `dataGrid`'s data binding — reads a datasource table (the rest bank, 0024),
 * optionally filtered and ordered (v2, ADR 0042 Increment). */
export interface DatasourceBinding {
  kind: "datasource";
  /** The datasource alias (default `app`). */
  source: string;
  /** A table/entity id from the fuse. */
  table: string;
  /** Optional equality filters, ANDed together. */
  filter?: ColumnFilter[];
  /** Optional single-column sort. */
  orderBy?: GridOrder;
}

export interface GridColumn {
  /** A column name on the bound table/entity. */
  field: string;
  header: string;
}

/** A tab over a single grid — selecting it swaps the active row filter
 * (client-side) without changing the bound table (e.g. Converging vs History). */
export interface GridTab {
  label: string;
  filter: ColumnFilter[];
}

/** A row-scoped `startProcess` (variables are bound from the row's fields). */
export interface RowStartProcessAction {
  kind: "startProcess";
  process: string;
  /** Static extra variables merged into the started instance. */
  variables?: Record<string, string>;
}

/** Cancel the process instance whose key lives in the row field `keyField`. */
export interface CancelProcessAction {
  kind: "cancelProcess";
  keyField: string;
}

/** Publish a message correlated on the row field `correlationKeyField`, carrying
 * the values typed into the (optional) prompt input plus any static `variables`. */
export interface PublishMessageAction {
  kind: "publishMessage";
  message: string;
  correlationKeyField: string;
  variables?: Record<string, string>;
}

export type RowActionBinding =
  RowStartProcessAction | CancelProcessAction | PublishMessageAction;

/** A button rendered on every grid row, firing a row-bound action. */
export interface RowAction {
  label: string;
  action: RowActionBinding;
  /** Optional `confirm()` text shown before the action fires. */
  confirm?: string;
  /** Only render the button when this row field is truthy. */
  showWhenField?: string;
}

/** A field shown in a row's expandable detail. `lazy` fetches it on expand. */
export interface DetailField {
  field: string;
  label: string;
  lazy?: boolean;
}

/** A grid nested inside a row's detail, filtered by a parent-row field
 * (`childField` = parent[`parentField`]) — e.g. a PR's rounds. */
export interface ChildGrid {
  title?: string;
  source: string;
  table: string;
  parentField: string;
  childField: string;
  columns: GridColumn[];
  orderBy?: GridOrder;
  /** A per-child-row lazy field (fetched on row expand), e.g. a transcript. */
  lazyField?: DetailField;
}

/** A conditional action form inside a row's detail — shown only when the parent
 * row field `showWhenField` is truthy (e.g. an open escalation). Publishes a
 * message to resume the process. */
export interface DetailForm {
  showWhenField: string;
  title?: string;
  /** A parent field rendered as the prompt above the input. */
  promptField?: string;
  inputKey: string;
  inputLabel: string;
  submitLabel: string;
  action: PublishMessageAction;
}

/** A per-row expandable detail: an optional external link, scalar fields, nested
 * child grids, and one conditional form. */
export interface DetailSpec {
  linkField?: string;
  fields?: DetailField[];
  children?: ChildGrid[];
  form?: DetailForm;
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
    /** Tabs that swap the active filter over the same table. */
    tabs?: GridTab[];
    /** The row field carrying a stable identity (defaults to `id`). */
    rowKey?: string;
    /** Per-row action buttons (cancel / publishMessage / startProcess). */
    rowActions?: RowAction[];
    /** A per-row expandable detail (child grids, lazy fields, conditional form). */
    detail?: DetailSpec;
    /** Auto-refresh interval in ms (0/omitted disables). */
    refreshMs?: number;
  };
}

export type PageNode = TextNode | ActionFormNode | DataGridNode;
export type PageNodeType = PageNode["type"];

export const PAGE_NODE_TYPES: PageNodeType[] = [
  "text",
  "actionForm",
  "dataGrid",
];

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
      return {
        title: "Data",
        data: { kind: "datasource", source: "app", table: "" },
        columns: [],
      };
  }
}

function isRecord(x: unknown): x is Record<string, unknown> {
  return typeof x === "object" && x !== null && !Array.isArray(x);
}

const str = (x: unknown, fallback = ""): string =>
  typeof x === "string" ? x : fallback;

function parseColumns(raw: unknown): GridColumn[] {
  return (Array.isArray(raw) ? raw : [])
    .filter(isRecord)
    .map((c) => ({ field: str(c.field), header: str(c.header) }))
    .filter((c) => c.field !== "");
}

function parseFilter(raw: unknown): ColumnFilter[] {
  return (Array.isArray(raw) ? raw : [])
    .filter(isRecord)
    .map((f) => ({ field: str(f.field), eq: str(f.eq) }))
    .filter((f) => f.field !== "");
}

function parseOrder(raw: unknown): GridOrder | undefined {
  if (!isRecord(raw) || typeof raw.field !== "string" || !raw.field)
    return undefined;
  return { field: raw.field, dir: raw.dir === "desc" ? "desc" : "asc" };
}

function parseStaticVars(raw: unknown): Record<string, string> | undefined {
  if (!isRecord(raw)) return undefined;
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(raw))
    if (typeof v === "string") out[k] = v;
  return Object.keys(out).length ? out : undefined;
}

function parseRowAction(raw: unknown): RowAction | null {
  if (!isRecord(raw)) return null;
  const a = isRecord(raw.action) ? raw.action : {};
  let action: RowActionBinding;
  switch (a.kind) {
    case "cancelProcess":
      if (typeof a.keyField !== "string" || !a.keyField) return null;
      action = { kind: "cancelProcess", keyField: a.keyField };
      break;
    case "publishMessage":
      if (
        typeof a.message !== "string" ||
        !a.message ||
        typeof a.correlationKeyField !== "string" ||
        !a.correlationKeyField
      )
        return null;
      action = {
        kind: "publishMessage",
        message: a.message,
        correlationKeyField: a.correlationKeyField,
        ...(parseStaticVars(a.variables)
          ? { variables: parseStaticVars(a.variables) }
          : {}),
      };
      break;
    case "startProcess":
      if (typeof a.process !== "string" || !a.process) return null;
      action = {
        kind: "startProcess",
        process: a.process,
        ...(parseStaticVars(a.variables)
          ? { variables: parseStaticVars(a.variables) }
          : {}),
      };
      break;
    default:
      return null;
  }
  return {
    label: str(raw.label, "Action"),
    action,
    ...(typeof raw.confirm === "string" ? { confirm: raw.confirm } : {}),
    ...(typeof raw.showWhenField === "string" && raw.showWhenField
      ? { showWhenField: raw.showWhenField }
      : {}),
  };
}

function parseDetailFields(raw: unknown): DetailField[] {
  return (Array.isArray(raw) ? raw : [])
    .filter(isRecord)
    .map((f) => ({
      field: str(f.field),
      label: str(f.label),
      ...(f.lazy === true ? { lazy: true } : {}),
    }))
    .filter((f) => f.field !== "");
}

function parseChildGrids(raw: unknown): ChildGrid[] {
  const out: ChildGrid[] = [];
  for (const c of Array.isArray(raw) ? raw : []) {
    if (!isRecord(c)) continue;
    if (
      typeof c.table !== "string" ||
      !c.table ||
      typeof c.parentField !== "string" ||
      !c.parentField ||
      typeof c.childField !== "string" ||
      !c.childField
    )
      continue;
    const order = parseOrder(c.orderBy);
    const lazyField = parseDetailFields(c.lazyField ? [c.lazyField] : [])[0];
    out.push({
      ...(typeof c.title === "string" ? { title: c.title } : {}),
      source: str(c.source, "app"),
      table: c.table,
      parentField: c.parentField,
      childField: c.childField,
      columns: parseColumns(c.columns),
      ...(order ? { orderBy: order } : {}),
      ...(lazyField ? { lazyField } : {}),
    });
  }
  return out;
}

function parseDetailForm(raw: unknown): DetailForm | undefined {
  if (!isRecord(raw)) return undefined;
  const a = isRecord(raw.action) ? raw.action : {};
  if (
    typeof raw.showWhenField !== "string" ||
    !raw.showWhenField ||
    typeof raw.inputKey !== "string" ||
    !raw.inputKey ||
    typeof a.message !== "string" ||
    !a.message ||
    typeof a.correlationKeyField !== "string" ||
    !a.correlationKeyField
  )
    return undefined;
  return {
    showWhenField: raw.showWhenField,
    ...(typeof raw.title === "string" ? { title: raw.title } : {}),
    ...(typeof raw.promptField === "string" && raw.promptField
      ? { promptField: raw.promptField }
      : {}),
    inputKey: raw.inputKey,
    inputLabel: str(raw.inputLabel, raw.inputKey),
    submitLabel: str(raw.submitLabel, "Submit"),
    action: {
      kind: "publishMessage",
      message: a.message,
      correlationKeyField: a.correlationKeyField,
      ...(parseStaticVars(a.variables)
        ? { variables: parseStaticVars(a.variables) }
        : {}),
    },
  };
}

function parseDetail(raw: unknown): DetailSpec | undefined {
  if (!isRecord(raw)) return undefined;
  const fields = parseDetailFields(raw.fields);
  const children = parseChildGrids(raw.children);
  const form = parseDetailForm(raw.form);
  const linkField =
    typeof raw.linkField === "string" && raw.linkField
      ? raw.linkField
      : undefined;
  if (!fields.length && !children.length && !form && !linkField)
    return undefined;
  return {
    ...(linkField ? { linkField } : {}),
    ...(fields.length ? { fields } : {}),
    ...(children.length ? { children } : {}),
    ...(form ? { form } : {}),
  };
}

/**
 * Validate an arbitrary value as a `PageDoc`, returning the parsed doc or a list
 * of human-readable errors. The Console's Page Composer and its serializer go
 * through this on open/save so a malformed page fails loudly rather than
 * rendering half a screen. (The App-side runtime renderer trusts the persisted
 * `page.json` and does not re-validate.)
 */
export function parsePageDoc(
  value: unknown,
): { ok: true; doc: PageDoc } | { ok: false; errors: string[] } {
  const errors: string[] = [];
  if (!isRecord(value))
    return { ok: false, errors: ["page.json must be an object"] };
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
            variant:
              props.variant === "heading" || props.variant === "sub"
                ? props.variant
                : "body",
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
            submitLabel:
              typeof props.submitLabel === "string"
                ? props.submitLabel
                : "Submit",
            action: {
              kind: "startProcess",
              process: typeof action.process === "string" ? action.process : "",
            },
            fields: fields
              .filter(isRecord)
              .map((f) => ({
                key: typeof f.key === "string" ? f.key : "",
                label: typeof f.label === "string" ? f.label : "",
                type: "text" as const,
              }))
              .filter((f) => f.key !== ""),
          },
        });
        break;
      }
      case "dataGrid": {
        const data = isRecord(props.data) ? props.data : {};
        const filter = parseFilter(data.filter);
        const orderBy = parseOrder(data.orderBy);
        const tabs = (Array.isArray(props.tabs) ? props.tabs : [])
          .filter(isRecord)
          .map((t) => ({
            label: str(t.label, "Tab"),
            filter: parseFilter(t.filter),
          }));
        const rowActions = (
          Array.isArray(props.rowActions) ? props.rowActions : []
        )
          .map(parseRowAction)
          .filter((a): a is RowAction => a !== null);
        const detail = parseDetail(props.detail);
        const refreshMs =
          typeof props.refreshMs === "number" && props.refreshMs > 0
            ? props.refreshMs
            : undefined;
        nodes.push({
          type: "dataGrid",
          id,
          props: {
            title: str(props.title),
            data: {
              kind: "datasource",
              source: str(data.source, "app"),
              table: str(data.table),
              ...(filter.length ? { filter } : {}),
              ...(orderBy ? { orderBy } : {}),
            },
            columns: parseColumns(props.columns),
            ...(tabs.length ? { tabs } : {}),
            ...(typeof props.rowKey === "string" && props.rowKey
              ? { rowKey: props.rowKey }
              : {}),
            ...(rowActions.length ? { rowActions } : {}),
            ...(detail ? { detail } : {}),
            ...(refreshMs ? { refreshMs } : {}),
          },
        });
        break;
      }
      default:
        errors.push(
          `nodes[${i}].type "${String(type)}" is not a known node type`,
        );
    }
  }

  if (errors.length) return { ok: false, errors };
  return {
    ok: true,
    doc: { schemaVersion: PAGE_SCHEMA_VERSION, title, nodes },
  };
}
