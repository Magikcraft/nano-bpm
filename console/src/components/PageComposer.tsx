// The Urban Page Composer (ADR 0042 §2) — a Craft.js WYSIWYG surface that authors a
// screen from the data-aware components (text / actionForm / dataGrid / prose /
// button / nav) and emits the owned `page.json` (the serializer + schema live in
// `../lib/pageComposer.ts`). The editable set is kept in lockstep with the runtime
// renderer's `RENDERERS` (nano-ide urban) — see the drift-guard test (issue #843).
//
// Craft.js is the canvas only; nothing here is persisted in Craft.js's internal
// format — `getPageJson()` serializes down to `page.json` and `setPageJson()` reflates
// it. The data/action pickers are fed by the fuse (`entities` = datasource tables,
// `processes` = manifest processes) so authoring a screen is typed, matching the
// ShapeComposer's use of the same registry.
import {
  forwardRef,
  useImperativeHandle,
  useRef,
  useState,
  type ReactElement,
} from "react";
import {
  Editor,
  Element,
  Frame,
  useEditor,
  useNode,
  type UserComponent,
} from "@craftjs/core";
import type { ComposerEntity } from "../lib/shapeComposer";
import {
  GRID_COLUMN_LINK_KINDS,
  asGridColumnLinkKind,
  type ActionFormField,
  type ButtonModal,
  type ButtonVariant,
  type DatasourceBinding,
  type GridColumn,
  type GridColumnLink,
  type GridColumnLinkKind,
  type NavItem,
  type TextVariant,
} from "../lib/pageSchema";
import {
  fromPageDoc,
  loadPageJson,
  reconcileGridColumns,
  serializePageNodes,
  toPageDoc,
  type CraftState,
} from "../lib/pageComposer";

export interface PageComposerHandle {
  /** Serialize the canvas down to the owned `page.json`. */
  getPageJson(): string;
  /**
   * Load a `page.json` into the canvas. Returns `{ ok: true }` on success (including
   * a blank/new page). Returns `{ ok: false, errors }` when non-empty content fails to
   * parse or validate — in that case the canvas is left untouched (NOT reset to blank),
   * so the caller can surface the errors and block a save that would overwrite the file.
   */
  setPageJson(text: string): { ok: true } | { ok: false; errors: string[] };
}

interface PageComposerProps {
  onChange?: () => void;
  /** Datasource tables/entities (fuse) for the `dataGrid` picker. */
  entities?: ComposerEntity[];
  /** Manifest process ids (fuse) for the `actionForm` picker. */
  processes?: string[];
}

// ── the three data-aware components (rendered on the canvas) ─────────────────

const useSelectableRef = () => {
  // Attach the drag/select connectors so a node is clickable on the canvas.
  const {
    connectors: { connect, drag },
  } = useNode();
  return (el: HTMLElement | null) => {
    if (el) connect(drag(el));
  };
};

const TextNode: UserComponent<{ text: string; variant: TextVariant }> = ({
  text,
  variant,
}) => {
  const ref = useSelectableRef();
  const cls =
    variant === "heading"
      ? "pc-heading"
      : variant === "sub"
        ? "pc-sub"
        : "pc-body";
  return (
    <div ref={ref} className={`pc-node ${cls}`}>
      {text || <span className="opacity-40">Text</span>}
    </div>
  );
};
TextNode.craft = {
  displayName: "TextNode",
  props: { text: "Text", variant: "body" },
};

const NavNode: UserComponent<{
  variant: "bar" | "rail";
  title?: string;
  items:
    "auto" | { label: string; page?: string; href?: string; icon?: string }[];
}> = ({ variant, title, items }) => {
  const ref = useSelectableRef();
  const auto = items === "auto";
  const list = auto ? [] : items;
  return (
    <nav
      ref={ref}
      className={`pc-node pc-nav ${variant === "rail" ? "pc-rail" : "pc-bar"}`}
    >
      {title != null && title !== "" && (
        <div className="pc-nav-title">{title}</div>
      )}
      <div className="pc-nav-items">
        {auto ? (
          <span className="pc-nav-auto opacity-40">
            (auto: links every page)
          </span>
        ) : list.length ? (
          list.map((it, i) => (
            <span key={i} className="pc-nav-link">
              {it.icon ? <span className="pc-nav-icon">{it.icon}</span> : null}
              {it.label || it.page || it.href || "(item)"}
            </span>
          ))
        ) : (
          <span className="pc-nav-empty opacity-40">No items</span>
        )}
      </div>
    </nav>
  );
};
NavNode.craft = {
  displayName: "NavNode",
  props: { variant: "bar", title: "Navigation", items: "auto" },
};

const ActionFormNode: UserComponent<{
  title: string;
  submitLabel: string;
  action: { kind: "startProcess"; process: string };
  fields: ActionFormField[];
}> = ({ title, submitLabel, action, fields }) => {
  const ref = useSelectableRef();
  return (
    <div ref={ref} className="pc-node pc-card">
      {title && <div className="pc-card-title">{title}</div>}
      {(fields ?? []).map((f) => (
        <div key={f.key} className="pc-field">
          <label>{f.label || f.key}</label>
          <input disabled placeholder={f.label || f.key} />
        </div>
      ))}
      <button className="pc-btn" disabled>
        {submitLabel || "Submit"}
      </button>
      <div className="pc-bind">
        → start <code>{action?.process || "(pick a process)"}</code>
      </div>
    </div>
  );
};
ActionFormNode.craft = {
  displayName: "ActionFormNode",
  props: {
    title: "Action",
    submitLabel: "Submit",
    action: { kind: "startProcess", process: "" },
    fields: [{ key: "input", label: "Input", type: "text" }],
  },
};

const DataGridNode: UserComponent<{
  title: string;
  data: { kind: "datasource"; source: string; table: string };
  columns: GridColumn[];
}> = ({ title, data, columns }) => {
  const ref = useSelectableRef();
  return (
    <div ref={ref} className="pc-node pc-card">
      {title && <div className="pc-card-title">{title}</div>}
      <table className="pc-grid">
        <thead>
          <tr>
            {(columns ?? []).map((c) => (
              <th key={c.field}>{c.header || c.field}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          <tr>
            <td
              colSpan={Math.max(columns?.length ?? 1, 1)}
              className="opacity-40"
            >
              rows from {data?.table || "(pick a table)"}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  );
};
DataGridNode.craft = {
  displayName: "DataGridNode",
  props: {
    title: "Data",
    data: { kind: "datasource", source: "app", table: "" },
    columns: [],
  },
};

const ProseNode: UserComponent<{
  title: string;
  data: DatasourceBinding;
  header?: string;
  body?: string;
}> = ({ title, data, header, body }) => {
  const ref = useSelectableRef();
  return (
    <div ref={ref} className="pc-node pc-card">
      {title && <div className="pc-card-title">{title}</div>}
      <div className="pc-prose-preview">
        {header ? (
          <div className="pc-prose-head opacity-60">{header}</div>
        ) : null}
        <div className="opacity-40">
          {body ? (
            <>
              markdown from <code>{body}</code>
            </>
          ) : (
            "(pick a body field)"
          )}{" "}
          · rows from <code>{data?.table || "(pick a table)"}</code>
        </div>
      </div>
    </div>
  );
};
ProseNode.craft = {
  displayName: "ProseNode",
  props: {
    title: "Prose",
    data: { kind: "datasource", source: "app", table: "" },
    header: "",
    body: "",
  },
};

const ButtonNode: UserComponent<{
  label: string;
  variant?: ButtonVariant;
  modal?: ButtonModal;
}> = ({ label, variant, modal }) => {
  const ref = useSelectableRef();
  return (
    <div ref={ref} className="pc-node pc-buttonrow">
      <button
        className={`pc-btn${variant === "ghost" ? " pc-btn-ghost" : ""}`}
        disabled
      >
        {label || "Open"}
      </button>
      {modal ? (
        <span className="pc-bind opacity-40">→ opens a copy modal</span>
      ) : null}
    </div>
  );
};
ButtonNode.craft = {
  displayName: "ButtonNode",
  props: { label: "Open" },
};

// The root canvas that hosts the ordered node list.
const PageCanvas: UserComponent<{ children?: React.ReactNode }> = ({
  children,
}) => {
  const {
    connectors: { connect },
  } = useNode();
  return (
    <div
      ref={(el) => {
        if (el) connect(el);
      }}
      className="pc-canvas"
    >
      {children}
    </div>
  );
};
PageCanvas.craft = { displayName: "PageCanvas" };

const RESOLVER = {
  PageCanvas,
  TextNode,
  NavNode,
  ActionFormNode,
  DataGridNode,
  ProseNode,
  ButtonNode,
};

// ── palette (click-to-add) ───────────────────────────────────────────────────

function Palette(): ReactElement {
  const { actions, query } = useEditor();
  const add = (el: ReactElement) => {
    const tree = query.parseReactElement(el).toNodeTree();
    actions.addNodeTree(tree, "ROOT");
  };
  return (
    <div className="pc-palette">
      <div className="pc-palette-title">Components</div>
      <button
        className="pc-palette-item"
        onClick={() =>
          add(<Element is={TextNode} text="Text" variant="body" />)
        }
      >
        + Text
      </button>
      <button
        className="pc-palette-item"
        onClick={() =>
          add(
            <Element
              is={NavNode}
              variant="bar"
              title="Navigation"
              items="auto"
            />,
          )
        }
      >
        + Nav
      </button>
      <button
        className="pc-palette-item"
        onClick={() =>
          add(
            <Element
              is={ActionFormNode}
              title="Action"
              submitLabel="Submit"
              action={{ kind: "startProcess", process: "" }}
              fields={[{ key: "input", label: "Input", type: "text" }]}
            />,
          )
        }
      >
        + Action form
      </button>
      <button
        className="pc-palette-item"
        onClick={() =>
          add(
            <Element
              is={DataGridNode}
              title="Data"
              data={{ kind: "datasource", source: "app", table: "" }}
              columns={[]}
            />,
          )
        }
      >
        + Data grid
      </button>
      <button
        className="pc-palette-item"
        onClick={() =>
          add(
            <Element
              is={ProseNode}
              title="Prose"
              data={{ kind: "datasource", source: "app", table: "" }}
              header=""
              body=""
            />,
          )
        }
      >
        + Prose
      </button>
      <button
        className="pc-palette-item"
        onClick={() => add(<Element is={ButtonNode} label="Open" />)}
      >
        + Button
      </button>
    </div>
  );
}

// ── settings panel (edits the selected node's props) ─────────────────────────

/** The composer's table pickers offer entity ids qualified as `source.table`
 * (see ProjectWorkspace's entity loader), but the persisted `dataGrid` binding —
 * and the runtime route `/app/data/<source>/<table>` — needs them split into a
 * bare `source` and a bare `table`. These two helpers keep the picker's display
 * value (qualified) and the stored binding (split) in sync. */
function qualifyTable(data?: { source?: string; table?: string }): string {
  if (!data?.table) return "";
  return data.source ? `${data.source}.${data.table}` : data.table;
}

function splitQualifiedTable(value: string): { source: string; table: string } {
  const dot = value.indexOf(".");
  if (dot <= 0) return { source: "app", table: value };
  return { source: value.slice(0, dot), table: value.slice(dot + 1) };
}

function Settings({
  entities,
  processes,
}: {
  entities: ComposerEntity[];
  processes: string[];
}): ReactElement {
  const { selectedId, name, props, actions } = useEditor((state, query) => {
    const id = Array.from(state.events.selected)[0];
    return {
      selectedId: id,
      name: id ? query.node(id).get().data.displayName : undefined,
      props: id
        ? (query.node(id).get().data.props as Record<string, unknown>)
        : undefined,
    };
  });
  if (!selectedId || !props) {
    return (
      <div className="pc-settings">
        <div className="pc-empty">Select a component to edit it.</div>
      </div>
    );
  }
  const set = (key: string, value: unknown) =>
    actions.setProp(selectedId, (p: Record<string, unknown>) => {
      p[key] = value;
    });

  const tables = entities.filter((e) => e.kind === "table");
  const tableFields = (table: string) =>
    tables.find((t) => t.id === table)?.fields ?? [];

  return (
    <div className="pc-settings">
      <div className="pc-settings-title">{name}</div>

      {name === "TextNode" && (
        <>
          <Row label="Text">
            <input
              value={String(props.text ?? "")}
              onChange={(e) => set("text", e.target.value)}
            />
          </Row>
          <Row label="Variant">
            <select
              value={String(props.variant ?? "body")}
              onChange={(e) => set("variant", e.target.value)}
            >
              <option value="heading">heading</option>
              <option value="body">body</option>
              <option value="sub">sub</option>
            </select>
          </Row>
        </>
      )}

      {name === "NavNode" && (
        <>
          <Row label="Variant">
            <select
              value={String(props.variant ?? "bar")}
              onChange={(e) => set("variant", e.target.value)}
            >
              <option value="bar">bar</option>
              <option value="rail">rail</option>
            </select>
          </Row>
          <Row label="Title">
            <input
              value={String(props.title ?? "")}
              onChange={(e) => set("title", e.target.value)}
            />
          </Row>
          <Row label="Items">
            <select
              value={props.items === "auto" ? "auto" : "manual"}
              onChange={(e) =>
                set("items", e.target.value === "auto" ? "auto" : [])
              }
            >
              <option value="auto">auto (every page)</option>
              <option value="manual">manual list</option>
            </select>
          </Row>
          {props.items !== "auto" && (
            <ListEditor
              label="Links"
              rows={
                ((props.items as NavItem[]) ?? []) as unknown as Record<
                  string,
                  string
                >[]
              }
              columns={[
                { key: "label", label: "label" },
                { key: "page", label: "page" },
                { key: "href", label: "href" },
                { key: "icon", label: "icon" },
              ]}
              onChange={(rows) =>
                set(
                  "items",
                  rows.map((r) => ({
                    label: r.label ?? "",
                    // `page` wins over `href`; only keep the set ones so the
                    // persisted item matches urban's navLink precedence.
                    ...(r.page
                      ? { page: r.page }
                      : r.href
                        ? { href: r.href }
                        : {}),
                    ...(r.icon ? { icon: r.icon } : {}),
                  })),
                )
              }
            />
          )}
        </>
      )}

      {name === "ActionFormNode" && (
        <>
          <Row label="Title">
            <input
              value={String(props.title ?? "")}
              onChange={(e) => set("title", e.target.value)}
            />
          </Row>
          <Row label="Submit label">
            <input
              value={String(props.submitLabel ?? "")}
              onChange={(e) => set("submitLabel", e.target.value)}
            />
          </Row>
          <Row label="Start process">
            <input
              list="pc-processes"
              value={String(
                (props.action as { process?: string })?.process ?? "",
              )}
              onChange={(e) =>
                set("action", { kind: "startProcess", process: e.target.value })
              }
            />
            <datalist id="pc-processes">
              {processes.map((p) => (
                <option key={p} value={p} />
              ))}
            </datalist>
          </Row>
          <ListEditor
            label="Fields"
            rows={
              ((props.fields as ActionFormField[]) ?? []) as unknown as Record<
                string,
                string
              >[]
            }
            columns={[
              { key: "key", label: "key" },
              { key: "label", label: "label" },
            ]}
            onChange={(rows) =>
              set(
                "fields",
                rows.map((r) => ({
                  key: r.key ?? "",
                  label: r.label ?? "",
                  type: "text",
                })),
              )
            }
          />
        </>
      )}

      {name === "DataGridNode" && (
        <>
          <Row label="Title">
            <input
              value={String(props.title ?? "")}
              onChange={(e) => set("title", e.target.value)}
            />
          </Row>
          <Row label="Table">
            <input
              list="pc-tables"
              value={qualifyTable(
                props.data as { source?: string; table?: string },
              )}
              onChange={(e) =>
                set("data", {
                  kind: "datasource",
                  ...splitQualifiedTable(e.target.value),
                })
              }
            />
            <datalist id="pc-tables">
              {tables.map((t) => (
                <option key={t.id} value={t.id} />
              ))}
            </datalist>
          </Row>
          <ListEditor
            label="Columns"
            rows={
              ((props.columns as GridColumn[]) ?? []) as unknown as Record<
                string,
                string
              >[]
            }
            columns={[
              { key: "field", label: "field" },
              { key: "header", label: "header" },
            ]}
            suggestions={tableFields(
              qualifyTable(props.data as { source?: string; table?: string }),
            )}
            onChange={(rows) => {
              // Preserve the structured `link` an existing column carries — it
              // isn't editable in this string-cell ListEditor (see the
              // ColumnLinks editor below), so rebuilding from field/header alone
              // would silently drop it. `reconcileGridColumns` is the pure,
              // unit-tested rule (positional on add/edit, `field`+`header`
              // identity on delete, dropping ambiguous matches).
              const prevCols =
                (props.columns as GridColumn[] | undefined) ?? [];
              set("columns", reconcileGridColumns(prevCols, rows));
            }}
          />
          <ColumnLinks
            columns={(props.columns as GridColumn[]) ?? []}
            fields={tableFields(
              qualifyTable(props.data as { source?: string; table?: string }),
            )}
            onChange={(cols) => set("columns", cols)}
          />
          <GridAdvanced key={selectedId} props={props} set={set} />
        </>
      )}

      {name === "ProseNode" && (
        <>
          <Row label="Title">
            <input
              value={String(props.title ?? "")}
              onChange={(e) => set("title", e.target.value)}
            />
          </Row>
          <Row label="Table">
            <input
              list="pc-tables"
              value={qualifyTable(
                props.data as { source?: string; table?: string },
              )}
              onChange={(e) => {
                // Preserve the binding's filter/orderBy when the table changes;
                // only the source/table split is edited here (matches dataGrid).
                const prev =
                  (props.data as Record<string, unknown> | undefined) ?? {};
                set("data", {
                  ...prev,
                  kind: "datasource",
                  ...splitQualifiedTable(e.target.value),
                });
              }}
            />
            <datalist id="pc-tables">
              {tables.map((t) => (
                <option key={t.id} value={t.id} />
              ))}
            </datalist>
          </Row>
          <Row label="Header template">
            <input
              value={String(props.header ?? "")}
              placeholder="Round {{round}} · {{status}}"
              onChange={(e) => set("header", e.target.value)}
            />
          </Row>
          <Row label="Body field">
            <input
              list="pc-prose-fields"
              value={String(props.body ?? "")}
              placeholder="findings"
              onChange={(e) => set("body", e.target.value)}
            />
            <datalist id="pc-prose-fields">
              {tableFields(
                qualifyTable(props.data as { source?: string; table?: string }),
              ).map((f) => (
                <option key={f} value={f} />
              ))}
            </datalist>
          </Row>
          <Row label="Empty text">
            <input
              value={String(props.empty ?? "")}
              onChange={(e) =>
                set("empty", e.target.value === "" ? undefined : e.target.value)
              }
            />
          </Row>
          <Row label="Measure (ch)">
            <input
              type="number"
              min={40}
              max={100}
              value={props.measure === undefined ? "" : String(props.measure)}
              onChange={(e) =>
                set(
                  "measure",
                  e.target.value === "" ? undefined : Number(e.target.value),
                )
              }
            />
          </Row>
          <Row label="Collapsible">
            <input
              type="checkbox"
              checked={props.collapsible === true}
              onChange={(e) =>
                set("collapsible", e.target.checked ? true : undefined)
              }
            />
          </Row>
          {props.collapsible === true && (
            <Row label="Start collapsed">
              <input
                type="checkbox"
                checked={props.defaultCollapsed === true}
                onChange={(e) =>
                  set("defaultCollapsed", e.target.checked ? true : undefined)
                }
              />
            </Row>
          )}
          <Row label="Refresh (ms)">
            <input
              type="number"
              min={0}
              value={
                props.refreshMs === undefined ? "" : String(props.refreshMs)
              }
              onChange={(e) =>
                set(
                  "refreshMs",
                  e.target.value === "" ? undefined : Number(e.target.value),
                )
              }
            />
          </Row>
          <BindingAdvanced key={selectedId} props={props} set={set} />
        </>
      )}

      {name === "ButtonNode" && (
        <>
          <Row label="Label">
            <input
              value={String(props.label ?? "")}
              onChange={(e) => set("label", e.target.value)}
            />
          </Row>
          <Row label="Variant">
            <select
              value={props.variant === "ghost" ? "ghost" : "default"}
              onChange={(e) =>
                set("variant", e.target.value === "ghost" ? "ghost" : undefined)
              }
            >
              <option value="default">default</option>
              <option value="ghost">ghost</option>
            </select>
          </Row>
          <ButtonModalEditor
            modal={props.modal as ButtonModal | undefined}
            onChange={(m) => set("modal", m)}
          />
        </>
      )}

      <button
        className="pc-btn-danger"
        onClick={() => actions.delete(selectedId)}
      >
        Remove component
      </button>
    </div>
  );
}

function Row({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}): ReactElement {
  return (
    <label className="pc-row">
      <span>{label}</span>
      {children}
    </label>
  );
}

/** An escape-hatch JSON editor for a `dataGrid`'s v2 knobs (filters, orderBy,
 * tabs, rowKey, rowActions, detail, refreshMs). The structured pickers above
 * cover the common case (table + columns); this exposes the relational/action
 * surface (ADR 0042 v2) without a bespoke widget per nested shape. It parses on
 * change and only commits valid JSON, so a mid-edit typo never corrupts props. */
function GridAdvanced({
  props,
  set,
}: {
  props: Record<string, unknown>;
  set: (key: string, value: unknown) => void;
}): ReactElement {
  const advancedKeys = [
    "tabs",
    "rowKey",
    "rowActions",
    "detail",
    "refreshMs",
  ] as const;
  const current = () => {
    const out: Record<string, unknown> = {};
    const data = (props.data as Record<string, unknown>) ?? {};
    if (data.filter) out.filter = data.filter;
    if (data.orderBy) out.orderBy = data.orderBy;
    for (const k of advancedKeys) if (props[k] !== undefined) out[k] = props[k];
    return JSON.stringify(out, null, 2);
  };
  const [text, setText] = useState(current);
  const [err, setErr] = useState<string | null>(null);
  const commit = (value: string) => {
    setText(value);
    if (!value.trim()) {
      setErr(null);
      const data = { ...((props.data as Record<string, unknown>) ?? {}) };
      delete data.filter;
      delete data.orderBy;
      set("data", data);
      for (const k of advancedKeys) set(k, undefined);
      return;
    }
    try {
      const parsed = JSON.parse(value) as Record<string, unknown>;
      setErr(null);
      const data = { ...((props.data as Record<string, unknown>) ?? {}) };
      data.filter = parsed.filter;
      data.orderBy = parsed.orderBy;
      if (parsed.filter === undefined) delete data.filter;
      if (parsed.orderBy === undefined) delete data.orderBy;
      set("data", data);
      for (const k of advancedKeys) set(k, parsed[k]);
    } catch (e) {
      setErr(String((e as Error).message));
    }
  };
  return (
    <div className="pc-list">
      <div className="pc-row">
        <span>Advanced (filters, tabs, row actions, detail)</span>
      </div>
      <textarea
        className="pc-advanced"
        rows={10}
        value={text}
        onChange={(e) => commit(e.target.value)}
        spellCheck={false}
      />
      {err && <div className="pc-advanced-err">Invalid JSON: {err}</div>}
    </div>
  );
}

/** A JSON escape-hatch for a `prose` node's data binding (filter + orderBy) — the
 * same relational surface `GridAdvanced` exposes for a grid, so a param-scoped
 * (`eqParam`) or filtered/ordered prose list is authorable without a bespoke
 * widget. Parses on change and only commits valid JSON. */
function BindingAdvanced({
  props,
  set,
}: {
  props: Record<string, unknown>;
  set: (key: string, value: unknown) => void;
}): ReactElement {
  const current = () => {
    const out: Record<string, unknown> = {};
    const data = (props.data as Record<string, unknown>) ?? {};
    if (data.filter) out.filter = data.filter;
    if (data.orderBy) out.orderBy = data.orderBy;
    return JSON.stringify(out, null, 2);
  };
  const [text, setText] = useState(current);
  const [err, setErr] = useState<string | null>(null);
  const commit = (value: string) => {
    setText(value);
    const data = { ...((props.data as Record<string, unknown>) ?? {}) };
    if (!value.trim()) {
      setErr(null);
      delete data.filter;
      delete data.orderBy;
      set("data", data);
      return;
    }
    try {
      const parsed = JSON.parse(value) as Record<string, unknown>;
      setErr(null);
      if (parsed.filter === undefined) delete data.filter;
      else data.filter = parsed.filter;
      if (parsed.orderBy === undefined) delete data.orderBy;
      else data.orderBy = parsed.orderBy;
      set("data", data);
    } catch (e) {
      setErr(String((e as Error).message));
    }
  };
  return (
    <div className="pc-list">
      <div className="pc-row">
        <span>Advanced (filter, orderBy)</span>
      </div>
      <textarea
        className="pc-advanced"
        rows={8}
        value={text}
        onChange={(e) => commit(e.target.value)}
        spellCheck={false}
      />
      {err && <div className="pc-advanced-err">Invalid JSON: {err}</div>}
    </div>
  );
}

/** Edits a `button` node's optional copy modal. All four fields are optional; when
 * every field is blank the modal is omitted entirely (a bare button — matching the
 * schema, which drops an all-empty modal). */
function ButtonModalEditor({
  modal,
  onChange,
}: {
  modal: ButtonModal | undefined;
  onChange: (modal: ButtonModal | undefined) => void;
}): ReactElement {
  const fields: { key: keyof ButtonModal; label: string }[] = [
    { key: "title", label: "Modal title" },
    { key: "description", label: "Description" },
    { key: "copyLabel", label: "Copy button label" },
    { key: "copyText", label: "Copy text" },
  ];
  const update = (key: keyof ButtonModal, value: string) => {
    const next: ButtonModal = { ...(modal ?? {}) };
    if (value === "") delete next[key];
    else next[key] = value;
    onChange(Object.keys(next).length ? next : undefined);
  };
  return (
    <div className="pc-list">
      <div className="pc-row">
        <span>Copy modal (optional)</span>
      </div>
      {fields.map((f) =>
        f.key === "copyText" || f.key === "description" ? (
          <Row key={f.key} label={f.label}>
            <textarea
              rows={3}
              value={String(modal?.[f.key] ?? "")}
              onChange={(e) => update(f.key, e.target.value)}
            />
          </Row>
        ) : (
          <Row key={f.key} label={f.label}>
            <input
              value={String(modal?.[f.key] ?? "")}
              onChange={(e) => update(f.key, e.target.value)}
            />
          </Row>
        ),
      )}
    </div>
  );
}

/** A tiny repeated-row editor for `fields`/`columns`. */
function ListEditor({
  label,
  rows,
  columns,
  suggestions,
  onChange,
}: {
  label: string;
  rows: Record<string, string>[];
  columns: { key: string; label: string }[];
  suggestions?: string[];
  onChange: (rows: Record<string, string>[]) => void;
}): ReactElement {
  const update = (i: number, key: string, value: string) => {
    const next = rows.map((r, j) => (j === i ? { ...r, [key]: value } : r));
    onChange(next);
  };
  const listId = `pc-sugg-${label}`;
  return (
    <div className="pc-list">
      <div className="pc-row">
        <span>{label}</span>
      </div>
      {rows.map((r, i) => (
        <div key={i} className="pc-list-row">
          {columns.map((c) => (
            <input
              key={c.key}
              placeholder={c.label}
              list={c.key === "field" && suggestions ? listId : undefined}
              value={r[c.key] ?? ""}
              onChange={(e) => update(i, c.key, e.target.value)}
            />
          ))}
          <button
            className="pc-btn-danger"
            onClick={() => onChange(rows.filter((_, j) => j !== i))}
          >
            ×
          </button>
        </div>
      ))}
      {suggestions && (
        <datalist id={listId}>
          {suggestions.map((s) => (
            <option key={s} value={s} />
          ))}
        </datalist>
      )}
      <button
        className="pc-palette-item"
        onClick={() =>
          onChange([
            ...rows,
            Object.fromEntries(columns.map((c) => [c.key, ""])),
          ])
        }
      >
        + add
      </button>
    </div>
  );
}

/** Build the `link` for a newly selected picker value. Exhaustive over
 * `GridColumnLinkKind` so adding a kind to `GRID_COLUMN_LINK_KINDS` is a
 * compile error here until it's handled (rather than silently clearing the
 * link). `undefined` kind = the "No link" option. */
function buildColumnLink(
  kind: GridColumnLinkKind | undefined,
  prev: GridColumnLink | undefined,
  fields: string[],
): GridColumnLink | undefined {
  if (kind === undefined) return undefined;
  switch (kind) {
    case "processExplorer":
      return {
        kind: "processExplorer",
        // Default the key field to the first suggested column when the author
        // hasn't picked one — a link with an empty keyField is intentionally
        // dropped on save/round-trip, so pre-filling keeps a just-selected
        // link from vanishing.
        keyField:
          (prev?.kind === "processExplorer" ? prev.keyField : "") ||
          fields[0] ||
          "",
      };
    default: {
      const _exhaustive: never = kind;
      return _exhaustive;
    }
  }
}

/** Human labels for the link-kind picker. A `Record` over the union keeps this
 * exhaustive: a new kind won't compile until it's labelled. */
const LINK_KIND_LABELS: Record<GridColumnLinkKind, string> = {
  processExplorer: "Process explorer",
};

/** Per-column structured-link editor for a `dataGrid`'s columns. The string-cell
 * ListEditor above owns field/header; this owns each column's optional `link`.
 * Today the only link kind is `processExplorer` — the cell value becomes a deep
 * link to the Nano console's explorer view for the process instance whose key is
 * held in the chosen row field (`keyField`). Selecting "None" clears the link. */
function ColumnLinks({
  columns,
  fields,
  onChange,
}: {
  columns: GridColumn[];
  fields: string[];
  onChange: (columns: GridColumn[]) => void;
}): ReactElement | null {
  if (!columns.length) return null;
  const setLink = (i: number, link: GridColumnLink | undefined) => {
    onChange(
      columns.map((c, j) => {
        if (j !== i) return c;
        if (!link) {
          return { field: c.field, header: c.header };
        }
        return { ...c, link };
      }),
    );
  };
  const listId = "pc-link-keyfields";
  return (
    <div className="pc-list">
      <div className="pc-row">
        <span>Column links</span>
      </div>
      {columns.map((c, i) => (
        <div key={`${c.field || "__col"}_${i}`} className="pc-list-row">
          <span className="pc-col-name">
            {c.header || c.field || `#${i + 1}`}
          </span>
          <select
            value={c.link?.kind ?? ""}
            onChange={(e) =>
              setLink(
                i,
                buildColumnLink(
                  asGridColumnLinkKind(e.target.value),
                  c.link,
                  fields,
                ),
              )
            }
          >
            <option value="">No link</option>
            {GRID_COLUMN_LINK_KINDS.map((k) => (
              <option key={k} value={k}>
                {LINK_KIND_LABELS[k]}
              </option>
            ))}
          </select>
          {c.link?.kind === "processExplorer" && (
            <input
              list={listId}
              placeholder="key field"
              value={c.link.keyField}
              onChange={(e) =>
                setLink(i, {
                  kind: "processExplorer",
                  keyField: e.target.value,
                })
              }
            />
          )}
        </div>
      ))}
      <datalist id={listId}>
        {fields.map((f) => (
          <option key={f} value={f} />
        ))}
      </datalist>
    </div>
  );
}

// ── the imperative bridge (get/set page.json from inside <Editor>) ───────────
const Bridge = forwardRef<
  PageComposerHandle,
  {
    title: string;
    onTitleChange: (t: string) => void;
    baselineRef: React.MutableRefObject<string | null>;
  }
>(function Bridge({ title, onTitleChange, baselineRef }, ref) {
  const { query, actions } = useEditor();
  useImperativeHandle(
    ref,
    () => ({
      getPageJson(): string {
        const state = JSON.parse(query.serialize()) as CraftState;
        const doc = toPageDoc(state, title);
        return JSON.stringify(doc, null, 2);
      },
      setPageJson(
        text: string,
      ): { ok: true } | { ok: false; errors: string[] } {
        const res = loadPageJson(text, title);
        if (!res.ok) {
          // Non-empty content that failed to parse/validate. Leave the canvas
          // untouched and report — never silently blank (a blank + Save would
          // overwrite the real file). The pure decision lives in loadPageJson.
          return { ok: false, errors: res.errors };
        }
        // Preserve the loaded page's title so a round-trip save doesn't clobber it.
        onTitleChange(res.doc.title);
        // Capture the loaded canvas as the pristine baseline BEFORE deserialize.
        // Dirtiness is then derived by comparing the live canvas against this, so
        // the load's own onNodesChange (and the initial mount) never reads as an
        // edit — deterministic, no timing guard needed.
        const state = fromPageDoc(res.doc);
        baselineRef.current = serializePageNodes(state);
        actions.deserialize(JSON.stringify(state));
        return { ok: true };
      },
    }),
    [query, actions, title, onTitleChange, baselineRef],
  );
  return null;
});

// ── the exported surface ─────────────────────────────────────────────────────

const PageComposer = forwardRef<PageComposerHandle, PageComposerProps>(
  function PageComposer({ onChange, entities = [], processes = [] }, ref) {
    const [title, setTitle] = useState("Page");
    // Pristine baseline (serialized node list) captured at load time. Dirtiness is
    // derived by comparing the live canvas against this on every Craft change, so
    // the programmatic load and the initial mount — which both fire onNodesChange —
    // are never misreported as user edits. `null` until the first load: while null,
    // there is nothing to be dirty against, so changes are ignored.
    const baselineRef = useRef<string | null>(null);
    return (
      <div className="pc-root">
        <style>{PAGE_COMPOSER_CSS}</style>
        <Editor
          resolver={RESOLVER}
          onNodesChange={(query) => {
            const baseline = baselineRef.current;
            if (baseline == null) return;
            const state = JSON.parse(query.serialize()) as CraftState;
            if (serializePageNodes(state) === baseline) return;
            onChange?.();
          }}
        >
          <Bridge
            ref={ref}
            title={title}
            onTitleChange={setTitle}
            baselineRef={baselineRef}
          />
          <div className="pc-toolbar">
            <label className="pc-row">
              <span>Page title</span>
              <input
                className="pc-title-input"
                value={title}
                onChange={(e) => {
                  setTitle(e.target.value);
                  onChange?.();
                }}
              />
            </label>
          </div>
          <div className="pc-layout">
            <Palette />
            <div className="pc-frame">
              <Frame>
                <Element is={PageCanvas} canvas />
              </Frame>
            </div>
            <Settings entities={entities} processes={processes} />
          </div>
        </Editor>
      </div>
    );
  },
);

export default PageComposer;

const PAGE_COMPOSER_CSS = `
.pc-root { height:100%; display:flex; flex-direction:column; }
.pc-toolbar { padding:.5rem .75rem; border-bottom:1px solid var(--color-edge,#d0d0d8); }
.pc-toolbar .pc-row { display:flex; align-items:center; gap:.5rem; }
.pc-title-input { flex:1; font:inherit; padding:.3rem .5rem; }
.pc-layout { display:grid; grid-template-columns:12rem 1fr 18rem; flex:1; min-height:0; }
.pc-palette, .pc-settings { border-color:var(--color-edge,#d0d0d8); padding:.75rem; overflow:auto; font-size:.85rem; }
.pc-palette { border-right:1px solid var(--color-edge,#d0d0d8); }
.pc-settings { border-left:1px solid var(--color-edge,#d0d0d8); }
.pc-palette-title, .pc-settings-title { font-weight:600; opacity:.7; margin-bottom:.5rem; text-transform:uppercase; font-size:.7rem; letter-spacing:.04em; }
.pc-palette-item { display:block; width:100%; text-align:left; padding:.4rem .5rem; margin-bottom:.35rem; border:1px solid var(--color-edge,#d0d0d8); border-radius:.35rem; background:transparent; color:inherit; cursor:pointer; }
.pc-palette-item:hover { background:rgba(120,120,160,.12); }
.pc-frame { overflow:auto; padding:1.25rem; background:rgba(120,120,160,.05); }
.pc-canvas { min-height:100%; display:flex; flex-direction:column; gap:.5rem; }
.pc-node { padding:.5rem .65rem; border:1px dashed transparent; border-radius:.4rem; cursor:pointer; }
.pc-node:hover { border-color:var(--color-edge,#c0c0c8); }
.pc-heading { font-size:1.4rem; font-weight:650; }
.pc-sub { opacity:.7; }
.pc-card { border:1px solid var(--color-edge,#d0d0d8); border-radius:.5rem; padding:.75rem; }
.pc-card-title { font-weight:600; margin-bottom:.5rem; }
.pc-nav { border:1px solid var(--color-edge,#d0d0d8); border-radius:.5rem; padding:.5rem .65rem; }
.pc-nav.pc-bar { display:flex; align-items:center; gap:.6rem; flex-wrap:wrap; }
.pc-nav.pc-rail { display:flex; flex-direction:column; gap:.35rem; max-width:14rem; }
.pc-nav-title { font-weight:650; }
.pc-nav-items { display:flex; gap:.35rem; flex-wrap:wrap; }
.pc-nav.pc-rail .pc-nav-items { flex-direction:column; }
.pc-nav-link { display:inline-flex; align-items:center; gap:.35rem; padding:.25rem .55rem; border-radius:.35rem; background:rgba(120,120,160,.12); font-size:.85rem; }
.pc-nav-icon { opacity:.8; }
.pc-field { display:flex; flex-direction:column; gap:.15rem; margin-bottom:.4rem; }
.pc-field label { font-size:.75rem; opacity:.7; }
.pc-field input { padding:.3rem .4rem; border:1px solid var(--color-edge,#d0d0d8); border-radius:.3rem; background:transparent; color:inherit; }
.pc-btn { padding:.35rem .7rem; border:0; border-radius:.35rem; background:#3b5bdb; color:#fff; }
.pc-bind { font-size:.72rem; opacity:.6; margin-top:.4rem; }
.pc-grid { width:100%; border-collapse:collapse; font-size:.8rem; }
.pc-grid th, .pc-grid td { text-align:left; padding:.25rem .4rem; border-bottom:1px solid var(--color-edge,#e0e0e8); }
.pc-row { display:flex; flex-direction:column; gap:.2rem; margin-bottom:.5rem; }
.pc-row > span { font-size:.72rem; opacity:.7; }
.pc-row input, .pc-row select, .pc-list input { padding:.3rem .4rem; border:1px solid var(--color-edge,#d0d0d8); border-radius:.3rem; background:transparent; color:inherit; width:100%; }
.pc-list-row { display:flex; gap:.3rem; margin-bottom:.3rem; }
.pc-btn-danger { border:1px solid #e0b4b4; color:#c0392b; background:transparent; border-radius:.3rem; padding:.25rem .5rem; cursor:pointer; margin-top:.5rem; }
.pc-advanced { width:100%; font:12px/1.4 ui-monospace,monospace; padding:.4rem; border:1px solid var(--color-edge,#d0d0d8); border-radius:.3rem; background:transparent; color:inherit; resize:vertical; }
.pc-advanced-err { color:#c0392b; font-size:.72rem; margin-top:.25rem; }
.pc-empty { opacity:.55; }
`;
