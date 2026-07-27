// The Urban Page Composer (ADR 0042 §2) — a Craft.js WYSIWYG surface that authors a
// screen from three data-aware components (text / actionForm / dataGrid) and emits
// the owned `page.json` (the serializer + schema live in `../lib/pageComposer.ts`).
//
// Craft.js is the canvas only; nothing here is persisted in Craft.js's internal
// format — `getPageJson()` serializes down to `page.json` and `setPageJson()` reflates
// it. The data/action pickers are fed by the fuse (`entities` = datasource tables,
// `processes` = manifest processes) so authoring a screen is typed, matching the
// ShapeComposer's use of the same registry.
import {
  forwardRef,
  useImperativeHandle,
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
  emptyPage,
  parsePageDoc,
  type ActionFormField,
  type GridColumn,
  type PageDoc,
  type TextVariant,
} from "../lib/pageSchema";
import { fromPageDoc, toPageDoc, type CraftState } from "../lib/pageComposer";

export interface PageComposerHandle {
  /** Serialize the canvas down to the owned `page.json`. */
  getPageJson(): string;
  /** Load a `page.json` (or empty when the string is blank) into the canvas. */
  setPageJson(text: string): void;
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

const RESOLVER = { PageCanvas, TextNode, ActionFormNode, DataGridNode };

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
            onChange={(rows) =>
              set(
                "columns",
                rows.map((r) => ({
                  field: r.field ?? "",
                  header: r.header ?? "",
                })),
              )
            }
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

// ── the imperative bridge (get/set page.json from inside <Editor>) ───────────

const Bridge = forwardRef<
  PageComposerHandle,
  { title: string; onTitleChange: (t: string) => void }
>(function Bridge({ title, onTitleChange }, ref) {
  const { query, actions } = useEditor();
  useImperativeHandle(
    ref,
    () => ({
      getPageJson(): string {
        const state = JSON.parse(query.serialize()) as CraftState;
        const doc = toPageDoc(state, title);
        return JSON.stringify(doc, null, 2);
      },
      setPageJson(text: string): void {
        let doc: PageDoc = emptyPage(title);
        const trimmed = text.trim();
        if (trimmed) {
          try {
            const parsed = parsePageDoc(JSON.parse(trimmed));
            if (parsed.ok) doc = parsed.doc;
          } catch {
            // Invalid/partial JSON (mid-edit, a merge conflict): fall back to an
            // empty page rather than throwing and crashing the editor pane.
          }
        }
        // Preserve the loaded page's title so a round-trip save doesn't clobber it.
        onTitleChange(doc.title);
        actions.deserialize(JSON.stringify(fromPageDoc(doc)));
      },
    }),
    [query, actions, title, onTitleChange],
  );
  return null;
});

// ── the exported surface ─────────────────────────────────────────────────────

const PageComposer = forwardRef<PageComposerHandle, PageComposerProps>(
  function PageComposer({ onChange, entities = [], processes = [] }, ref) {
    const [title, setTitle] = useState("Page");
    return (
      <div className="pc-root">
        <style>{PAGE_COMPOSER_CSS}</style>
        <Editor resolver={RESOLVER} onNodesChange={() => onChange?.()}>
          <Bridge ref={ref} title={title} onTitleChange={setTitle} />
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
.pc-empty { opacity:.55; }
`;
