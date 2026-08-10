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

/** An filter on a datasource column (whitelisted against the schema at runtime,
 * so it can never inject SQL). Either an equality (`eq`) or a set membership
 * (`in`) — set membership is what a tab like "Active = converging|waiting|escalated"
 * needs. Exactly one of `eq`/`in` should be set; `eq` wins if both are present. */
export interface ColumnFilter {
  field: string;
  eq?: string;
  in?: string[];
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

/** A structured, engine-aware link a grid column's value can carry. `kind` is a
 * discriminant so more link targets can be added without breaking existing
 * pages; the runtime renders an unrecognised kind as plain text. Today the only
 * kind is `processExplorer`: the cell text links to the Nano console's explorer
 * view for the process instance whose key is held in the row field `keyField`. */
export interface ProcessExplorerColumnLink {
  kind: "processExplorer";
  /** The row field holding the process-instance key to open in the explorer. */
  keyField: string;
}

export type GridColumnLink = ProcessExplorerColumnLink;

/** The set of supported column-link kinds (single source of truth for the
 * schema parser and the Studio editor's link-kind picker). */
export const GRID_COLUMN_LINK_KINDS = ["processExplorer"] as const;

export interface GridColumn {
  /** A column name on the bound table/entity. */
  field: string;
  header: string;
  /** An optional structured link the cell value becomes (e.g. a process-explorer
   * deep link). This is the only per-column link mechanism the parser preserves
   * — `parseColumns` keeps `field`, `header`, and `link` and drops anything
   * else. (Not to be confused with `DetailSpec.linkField`, which is a
   * detail-panel concern, unrelated to grid columns.) */
  link?: GridColumnLink;
}

/** A tab over a single grid — selecting it swaps the active row filter
 * (client-side) without changing the bound table (e.g. Converging vs History). */
export interface GridTab {
  label: string;
  filter: ColumnFilter[];
}

/** A row-scoped `startProcess`. Only the static `variables` below are sent; the
 * runtime does not read row fields into the started instance. */
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

/** A field shown in a row's expandable detail. `lazy` defers its *display* until
 * the row is expanded (the value is already fetched with the row, not lazily loaded). */
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
  /** A per-child-row field whose display is deferred until the row is expanded
   * (already fetched with the row, not lazily loaded), e.g. a transcript. */
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

/** One entry in a `nav` node. Either an internal `page` link (a page id) or an
 *  external `href` (an http(s) URL); a `page` wins if both are set. `label` is the
 *  visible text and `icon` an optional leading glyph. Mirrors urban's `navLink`. */
export interface NavItem {
  label: string;
  /** Internal page id to link to (renders as `#/<page>`). */
  page?: string;
  /** External URL (http/https) — used only when `page` is absent. */
  href?: string;
  /** Optional leading icon/glyph. */
  icon?: string;
}

export type NavVariant = "bar" | "rail";

/** A navigation node: a top `bar` or side `rail` linking the app's pages. `items`
 *  is either the literal string `"auto"` (enumerate every page at render time) or an
 *  explicit ordered list. Matches urban's `renderNav`/`fillNav` contract exactly so
 *  the Composer and the runtime renderer never disagree. */
export interface NavNode {
  type: "nav";
  id: string;
  props: {
    variant: NavVariant;
    /** Optional heading shown before the links. */
    title?: string;
    /** `"auto"` = link every page; otherwise an explicit ordered list. */
    items: "auto" | NavItem[];
  };
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

export type PageNode = TextNode | NavNode | ActionFormNode | DataGridNode;
export type PageNodeType = PageNode["type"];

export const PAGE_NODE_TYPES: PageNodeType[] = [
  "text",
  "nav",
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
    case "nav":
      return { variant: "bar", title: "Navigation", items: "auto" };
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

function parseColumnLink(raw: unknown): GridColumnLink | undefined {
  if (!isRecord(raw)) return undefined;
  const kind = raw.kind;
  // Guard against unknown kinds via the single source of truth so the parser
  // can't drift from GRID_COLUMN_LINK_KINDS; each known kind then validates its
  // own fields below.
  if (
    typeof kind !== "string" ||
    !GRID_COLUMN_LINK_KINDS.some((k) => k === kind)
  )
    return undefined;
  if (kind === "processExplorer") {
    const keyField = str(raw.keyField);
    if (keyField === "") return undefined;
    return { kind: "processExplorer", keyField };
  }
  return undefined;
}

function parseColumns(raw: unknown): GridColumn[] {
  return (Array.isArray(raw) ? raw : [])
    .filter(isRecord)
    .map((c) => {
      const link = parseColumnLink(c.link);
      const col: GridColumn = { field: str(c.field), header: str(c.header) };
      return link ? { ...col, link } : col;
    })
    .filter((c) => c.field !== "");
}

function parseFilter(raw: unknown): ColumnFilter[] {
  const out: ColumnFilter[] = [];
  for (const f of Array.isArray(raw) ? raw : []) {
    if (!isRecord(f) || typeof f.field !== "string" || !f.field) continue;
    if (Array.isArray(f.in)) {
      const values = f.in.filter((v): v is string => typeof v === "string");
      if (values.length) out.push({ field: f.field, in: values });
    } else if (typeof f.eq === "string") {
      out.push({ field: f.field, eq: f.eq });
    }
  }
  return out;
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
      case "nav": {
        const variant = props.variant === "rail" ? "rail" : "bar";
        const rawItems = props.items;
        let items: "auto" | NavItem[];
        if (rawItems === "auto" || rawItems === undefined) {
          items = "auto";
        } else if (Array.isArray(rawItems)) {
          items = rawItems
            .filter(isRecord)
            .map((it): NavItem => {
              const page = typeof it.page === "string" ? it.page : "";
              // Mirror urban's navLink: an external link is only honoured when it
              // is an http(s) URL. Canonicalize here so an unsafe scheme (e.g.
              // `javascript:`) can never be persisted through the composer.
              const href =
                typeof it.href === "string" && /^https?:\/\//i.test(it.href)
                  ? it.href
                  : "";
              const label = typeof it.label === "string" ? it.label : "";
              const icon = typeof it.icon === "string" ? it.icon : "";
              return {
                label,
                // `page` wins over `href` (mirrors urban's navLink precedence).
                ...(page ? { page } : href ? { href } : {}),
                ...(icon ? { icon } : {}),
              };
            })
            // Drop fully-empty items (no label and no target): they render as
            // nothing, so they are noise in the persisted document.
            .filter(
              (it) => it.label !== "" || it.page != null || it.href != null,
            );
        } else {
          items = "auto";
        }
        nodes.push({
          type: "nav",
          id,
          props: {
            variant,
            ...(typeof props.title === "string" ? { title: props.title } : {}),
            items,
          },
        });
        break;
      }
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
