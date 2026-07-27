import { useEffect, useMemo, useRef, useState } from "react";

import type { ShapeDiagnostic } from "../gen";
import type { MetaEntry, ShapeDecl, ShapeOp } from "../lib/shapeCarrier";
import {
  addOp,
  addShape,
  changeOpKind,
  entityById,
  extractShapeFields,
  moveOp,
  newOp,
  OP_KINDS,
  removeOp,
  removeShape,
  replaceOp,
  SCALAR_KEYWORDS,
  shapeEntities,
  toggleProjectField,
  uniqueShapeId,
  updateOp,
  updateShape,
  type ComposerEntity,
  type OpKind,
  type ResolvedField,
} from "../lib/shapeComposer";

/** The resolved domain text + diagnostics from one preview round-trip. */
export interface ShapePreview {
  text: string;
  diagnostics: ShapeDiagnostic[];
}

export interface ShapeComposerProps {
  /// The process's composed shapes, read from the model (undoable source of truth).
  shapes: ShapeDecl[];
  /// The process's model-level metadata (`nano:meta`), read from the model (ADR
  /// 0040 §5) — the undoable source of truth for the metadata editor.
  meta: MetaEntry[];
  /// The fuse leaf entities available as `ref`/`type` targets (tables + manifest
  /// types + the *other* composed shapes), for the pickers.
  entities: ComposerEntity[];
  /// Persist an edited shape set back to the model (writeShapes — one undoable command).
  onShapesChange: (shapes: ShapeDecl[]) => void;
  /// Persist edited model-level metadata back to the model (writeMeta — one undoable command).
  onMetaChange: (meta: MetaEntry[]) => void;
  /// Resolve `shapes` + `meta` server-side for the live field preview + diagnostics.
  preview: (shapes: ShapeDecl[], meta: MetaEntry[]) => Promise<ShapePreview>;
  /// Dismiss the composer.
  onClose: () => void;
}

const SELECT_CLASS =
  "min-w-0 rounded border border-edge-strong bg-bg-subtle px-1.5 py-0.5 text-xs text-fg";
const INPUT_CLASS =
  "min-w-0 rounded border border-edge-strong bg-bg-subtle px-1.5 py-0.5 text-xs text-fg";
const ICON_BTN =
  "rounded px-1 text-xs text-fg-faint hover:bg-hover disabled:opacity-30 disabled:hover:bg-transparent";

/** A short human gloss of what each op contributes, shown under its row. */
const OP_HINT: Record<OpKind, string> = {
  carry: "Spread every field of the entity into the shape.",
  project: "Spread only the picked fields (optionally reached via a FK path).",
  extend: "Add a new field with a scalar type or a nominal entity type.",
  reference:
    "Embed the entity under a name (spread its fields, or keep it nominal).",
};

/**
 * The composed motion-shape composer (ADR 0040 §9/§10) — the dedicated authoring
 * surface for the four-operation carry/project/extend/reference algebra. It edits
 * the process's `nano:shape` set (controlled via `shapes`/`onShapesChange`, each
 * change an undoable modeling command) and shows a live server-resolved field
 * preview + inline diagnostics for the selected shape, debounced so a burst of
 * edits collapses to one round-trip.
 */
export default function ShapeComposer({
  shapes,
  meta,
  entities,
  onShapesChange,
  onMetaChange,
  preview,
  onClose,
}: ShapeComposerProps) {
  const [selected, setSelected] = useState(0);
  const [result, setResult] = useState<ShapePreview | null>(null);
  const [previewing, setPreviewing] = useState(false);

  // Keep the selection in range as shapes are added/removed.
  const sel = Math.min(selected, Math.max(0, shapes.length - 1));
  const shape = shapes[sel];

  // Debounced live preview: a burst of edits collapses to one round-trip, and a
  // stale response (superseded by a newer edit) is dropped via the request id.
  const reqId = useRef(0);
  const shapesKey = JSON.stringify(shapes);
  const metaKey = JSON.stringify(meta);
  useEffect(() => {
    if (shapes.length === 0) {
      // Advance the request id so any preview already in flight fails its
      // `id === reqId.current` guard and can't repopulate `result` after the
      // drawer emptied.
      reqId.current++;
      setResult(null);
      setPreviewing(false);
      return;
    }
    const id = ++reqId.current;
    setPreviewing(true);
    const t = setTimeout(() => {
      preview(shapes, meta)
        .then((r) => {
          if (id === reqId.current) setResult(r);
        })
        .catch(() => {
          if (id === reqId.current) setResult(null);
        })
        .finally(() => {
          if (id === reqId.current) setPreviewing(false);
        });
    }, 350);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [shapesKey, metaKey]);

  // Entities offerable as a `ref` (any entity but the shape itself, to avoid a
  // trivially circular reference) and as a nominal `extend` type (manifest types +
  // other shapes only — a nominal table ref would emit `unknown`, ADR 0040 §10).
  // The *other* composed shapes are entities too (with their preview-resolved
  // fields), so a shape can be built by composing sibling shapes.
  const allEntities = useMemo(
    () => [...entities, ...shapeEntities(shapes, result?.text)],
    [entities, shapes, result?.text],
  );
  const refEntities = useMemo(
    () => allEntities.filter((e) => e.id !== shape?.id),
    [allEntities, shape?.id],
  );
  const nominalEntities = useMemo(
    () => refEntities.filter((e) => e.kind !== "table"),
    [refEntities],
  );

  const setOps = (ops: ShapeOp[]) =>
    onShapesChange(updateShape(shapes, sel, { ops }));

  const diagnostics = result?.diagnostics ?? [];
  const shapeDiags = diagnostics.filter((d) => d.shape === shape?.id);
  const resolvedFields: ResolvedField[] | null =
    result && shape ? extractShapeFields(result.text, shape.id) : null;
  const hasError = (id: string) =>
    diagnostics.some((d) => d.shape === id && d.severity === "error");

  return (
    <div className="flex h-full w-full flex-col bg-panel text-fg">
      <div className="flex items-center justify-between border-b border-edge px-3 py-2">
        <div>
          <h3 className="text-sm font-semibold">Composed shapes</h3>
          <p className="text-[11px] text-fg-faint">
            Author a motion shape by composing fused entities (ADR 0040).
          </p>
        </div>
        <button
          onClick={onClose}
          className="rounded p-1 text-fg-faint hover:bg-hover"
          title="Close"
        >
          ✕
        </button>
      </div>

      <div className="flex min-h-0 flex-1">
        {/* Shape list */}
        <aside className="flex w-44 shrink-0 flex-col border-r border-edge">
          <div className="min-h-0 flex-1 overflow-y-auto p-1.5">
            {shapes.length === 0 && (
              <p className="px-1 py-2 text-[11px] text-fg-faint">
                No shapes yet.
              </p>
            )}
            {shapes.map((s, i) => (
              <button
                key={i}
                onClick={() => setSelected(i)}
                className={`flex w-full items-center justify-between rounded px-2 py-1 text-left text-xs ${
                  i === sel
                    ? "bg-accent/10 text-accent-strong"
                    : "text-fg-muted hover:bg-hover"
                }`}
              >
                <span className="truncate">
                  {s.name || s.id || "(unnamed)"}
                </span>
                {hasError(s.id) && (
                  <span className="text-danger" title="Has an error diagnostic">
                    ●
                  </span>
                )}
              </button>
            ))}
          </div>
          <button
            onClick={() => {
              const id = uniqueShapeId(shapes.map((s) => s.id));
              onShapesChange(addShape(shapes, id));
              setSelected(shapes.length);
            }}
            className="m-1.5 rounded border border-dashed border-edge-strong px-2 py-1 text-xs font-medium text-accent hover:bg-accent/10"
          >
            + Add shape
          </button>
        </aside>

        {/* Selected shape editor + preview */}
        <div className="min-w-0 flex-1 overflow-y-auto">
          {!shape ? (
            <div className="p-6 text-xs text-fg-faint">
              Add a shape to start composing.
            </div>
          ) : (
            <div className="flex flex-col gap-3 p-3">
              {/* Identity */}
              <div className="flex items-center gap-2">
                <label className="text-[11px] text-fg-faint">Id</label>
                <input
                  className={INPUT_CLASS}
                  value={shape.id}
                  onChange={(e) =>
                    onShapesChange(
                      updateShape(shapes, sel, { id: e.target.value }),
                    )
                  }
                  placeholder="ApprovedOrder"
                />
                <label className="text-[11px] text-fg-faint">Name</label>
                <input
                  className={`${INPUT_CLASS} flex-1`}
                  value={shape.name ?? ""}
                  onChange={(e) =>
                    onShapesChange(
                      updateShape(shapes, sel, {
                        name: e.target.value || undefined,
                      }),
                    )
                  }
                  placeholder="Approved order"
                />
                <button
                  onClick={() => {
                    onShapesChange(removeShape(shapes, sel));
                    setSelected(Math.max(0, sel - 1));
                  }}
                  className="rounded px-2 py-0.5 text-xs text-danger hover:bg-danger/10"
                >
                  Delete
                </button>
              </div>

              {/* Op rows */}
              <div className="flex flex-col gap-2">
                {shape.ops.map((op, i) => (
                  <OpRow
                    key={i}
                    op={op}
                    index={i}
                    count={shape.ops.length}
                    refEntities={refEntities}
                    nominalEntities={nominalEntities}
                    entities={allEntities}
                    onChange={(patch) => setOps(updateOp(shape.ops, i, patch))}
                    onRetype={(kind) =>
                      setOps(replaceOp(shape.ops, i, changeOpKind(op, kind)))
                    }
                    onToggleField={(f) =>
                      setOps(toggleProjectField(shape.ops, i, f))
                    }
                    onMove={(dir) => setOps(moveOp(shape.ops, i, dir))}
                    onRemove={() => setOps(removeOp(shape.ops, i))}
                  />
                ))}
              </div>

              {/* Add op */}
              <div className="flex items-center gap-1.5">
                <span className="text-[11px] text-fg-faint">Add op:</span>
                {OP_KINDS.map((k) => (
                  <button
                    key={k}
                    onClick={() => setOps(addOp(shape.ops, newOp(k)))}
                    className="rounded border border-edge-strong px-2 py-0.5 text-xs text-fg-muted hover:border-accent hover:text-accent"
                  >
                    {k}
                  </button>
                ))}
              </div>

              {/* Live preview + diagnostics */}
              <div className="mt-1 rounded-md border border-edge bg-inset p-2">
                <div className="mb-1 flex items-center justify-between">
                  <span className="text-[11px] font-medium text-fg-muted">
                    Resolved fields{previewing ? " …" : ""}
                  </span>
                </div>
                {resolvedFields === null ? (
                  <p className="text-[11px] text-fg-faint">
                    {shapeDiags.some((d) => d.severity === "error")
                      ? "Does not resolve — fix the errors below."
                      : previewing
                        ? "Resolving…"
                        : "No preview yet."}
                  </p>
                ) : resolvedFields.length === 0 ? (
                  <p className="text-[11px] text-fg-faint">
                    Resolves to an empty shape.
                  </p>
                ) : (
                  <ul className="font-mono text-[11px] text-fg-muted">
                    {resolvedFields.map((f) => (
                      <li key={f.name}>
                        {f.name}
                        {f.optional ? "?" : ""}:{" "}
                        <span className="text-accent">{f.type}</span>
                      </li>
                    ))}
                  </ul>
                )}
                {shapeDiags.length > 0 && (
                  <ul className="mt-2 flex flex-col gap-1">
                    {shapeDiags.map((d, i) => (
                      <li
                        key={i}
                        className={`rounded px-1.5 py-1 text-[11px] ${
                          d.severity === "error"
                            ? "bg-danger/10 text-danger"
                            : "bg-warn/10 text-warn"
                        }`}
                      >
                        <span className="font-medium">{d.kind}</span>:{" "}
                        {d.message}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      <MetaEditor meta={meta} onChange={onMetaChange} />
    </div>
  );
}

/**
 * The model-level metadata editor (ADR 0040 §5): a flat key/value list carried on
 * the process as `nano:meta` siblings of the shapes container, folded into the
 * fuse (`domain.json`) and the typed `@nanobpm/meta` accessor. Each edit is one
 * undoable modeling command; last write wins per key on the server.
 */
function MetaEditor({
  meta,
  onChange,
}: {
  meta: MetaEntry[];
  onChange: (meta: MetaEntry[]) => void;
}) {
  const setAt = (i: number, patch: Partial<MetaEntry>) =>
    onChange(meta.map((m, j) => (j === i ? { ...m, ...patch } : m)));
  const dupKey = (key: string, self: number) =>
    key.trim().length > 0 &&
    meta.some((m, j) => j !== self && m.key.trim() === key.trim());

  return (
    <div className="max-h-56 shrink-0 overflow-y-auto border-t border-edge px-3 py-2">
      <div className="mb-1 flex items-center justify-between">
        <div>
          <h4 className="text-xs font-semibold">Model metadata</h4>
          <p className="text-[11px] text-fg-faint">
            Key/value metadata carried on the process (ADR 0040 §5), typed via{" "}
            <span className="font-mono">@nanobpm/meta</span>.
          </p>
        </div>
        <button
          onClick={() => onChange([...meta, { key: "", value: "" }])}
          className="rounded border border-dashed border-edge-strong px-2 py-0.5 text-xs font-medium text-accent hover:bg-accent/10"
        >
          + Add entry
        </button>
      </div>
      {meta.length === 0 ? (
        <p className="px-1 py-1 text-[11px] text-fg-faint">No metadata.</p>
      ) : (
        <div className="flex flex-col gap-1">
          {meta.map((m, i) => (
            <div key={i} className="flex items-center gap-1.5">
              <input
                className={`${INPUT_CLASS} w-40 ${dupKey(m.key, i) ? "border-danger" : ""}`}
                value={m.key}
                onChange={(e) => setAt(i, { key: e.target.value })}
                placeholder="key"
                title={
                  dupKey(m.key, i)
                    ? "Duplicate key — last write wins"
                    : "Metadata key"
                }
              />
              <input
                className={`${INPUT_CLASS} flex-1`}
                value={m.value}
                onChange={(e) => setAt(i, { value: e.target.value })}
                placeholder="value"
              />
              <button
                className={`${ICON_BTN} text-danger`}
                onClick={() => onChange(meta.filter((_, j) => j !== i))}
                title="Remove entry"
              >
                ✕
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

interface OpRowProps {
  op: ShapeOp;
  index: number;
  count: number;
  refEntities: ComposerEntity[];
  nominalEntities: ComposerEntity[];
  entities: ComposerEntity[];
  onChange: (patch: Record<string, unknown>) => void;
  onRetype: (kind: OpKind) => void;
  onToggleField: (field: string) => void;
  onMove: (dir: -1 | 1) => void;
  onRemove: () => void;
}

/** One composition op row: a kind picker + kind-specific controls + reorder/remove. */
function OpRow({
  op,
  index,
  count,
  refEntities,
  nominalEntities,
  entities,
  onChange,
  onRetype,
  onToggleField,
  onMove,
  onRemove,
}: OpRowProps) {
  const refOptions = (
    <select
      className={SELECT_CLASS}
      value={"ref" in op ? op.ref : ""}
      onChange={(e) => onChange({ ref: e.target.value })}
    >
      <option value="">— entity —</option>
      {refEntities.map((e) => (
        <option key={e.id} value={e.id}>
          {e.id} ({e.kind})
        </option>
      ))}
    </select>
  );

  return (
    <div className="rounded-md border border-edge px-2 py-1.5">
      <div className="flex flex-wrap items-center gap-1.5">
        <select
          className={SELECT_CLASS}
          value={op.op}
          onChange={(e) => onRetype(e.target.value as OpKind)}
        >
          {OP_KINDS.map((k) => (
            <option key={k} value={k}>
              {k}
            </option>
          ))}
        </select>

        {op.op === "carry" && refOptions}

        {op.op === "project" && (
          <>
            {refOptions}
            <input
              className={`${INPUT_CLASS} w-28`}
              value={op.via ?? ""}
              onChange={(e) => onChange({ via: e.target.value || undefined })}
              placeholder="via FK path (opt)"
              title="Optional dot-path of FK columns to reach the entity"
            />
          </>
        )}

        {op.op === "extend" && (
          <>
            <input
              className={`${INPUT_CLASS} w-24`}
              value={op.name}
              onChange={(e) => onChange({ name: e.target.value })}
              placeholder="field"
            />
            <select
              className={SELECT_CLASS}
              value={op.type}
              onChange={(e) => onChange({ type: e.target.value })}
            >
              <optgroup label="scalar">
                {SCALAR_KEYWORDS.map((k) => (
                  <option key={k} value={k}>
                    {k}
                  </option>
                ))}
              </optgroup>
              {nominalEntities.length > 0 && (
                <optgroup label="entity">
                  {nominalEntities.map((e) => (
                    <option key={e.id} value={e.id}>
                      {e.id}
                    </option>
                  ))}
                </optgroup>
              )}
            </select>
            <Toggle
              label="opt"
              on={!!op.optional}
              onClick={() => onChange({ optional: !op.optional })}
            />
            <Toggle
              label="list"
              on={!!op.list}
              onClick={() => onChange({ list: !op.list })}
            />
          </>
        )}

        {op.op === "reference" && (
          <>
            <input
              className={`${INPUT_CLASS} w-24`}
              value={op.name}
              onChange={(e) => onChange({ name: e.target.value })}
              placeholder="field"
            />
            {refOptions}
            <Toggle
              label="spread"
              on={!!op.spread}
              onClick={() => onChange({ spread: !op.spread })}
            />
            <Toggle
              label="list"
              on={!!op.list}
              onClick={() => onChange({ list: !op.list })}
            />
          </>
        )}

        <div className="ml-auto flex items-center">
          <button
            className={ICON_BTN}
            disabled={index === 0}
            onClick={() => onMove(-1)}
            title="Move up"
          >
            ↑
          </button>
          <button
            className={ICON_BTN}
            disabled={index === count - 1}
            onClick={() => onMove(1)}
            title="Move down"
          >
            ↓
          </button>
          <button
            className={`${ICON_BTN} text-danger`}
            onClick={onRemove}
            title="Remove op"
          >
            ✕
          </button>
        </div>
      </div>

      {/* project: the field multi-select of the referenced entity */}
      {op.op === "project" && op.ref && (
        <ProjectFields
          fields={entityById(entities, op.ref)?.fields ?? []}
          picked={op.fields}
          onToggle={onToggleField}
        />
      )}

      <p className="mt-1 text-[11px] text-fg-faint">{OP_HINT[op.op]}</p>
    </div>
  );
}

function ProjectFields({
  fields,
  picked,
  onToggle,
}: {
  fields: string[];
  picked: string[];
  onToggle: (field: string) => void;
}) {
  if (fields.length === 0) {
    return (
      <p className="mt-1 text-[11px] text-fg-faint">
        Pick an entity to choose fields.
      </p>
    );
  }
  return (
    <div className="mt-1 flex flex-wrap gap-1">
      {fields.map((f) => {
        const on = picked.includes(f);
        return (
          <button
            key={f}
            onClick={() => onToggle(f)}
            className={`rounded px-1.5 py-0.5 text-[11px] ${
              on
                ? "bg-accent/15 text-accent-strong"
                : "border border-edge-strong text-fg-faint hover:bg-hover"
            }`}
          >
            {f}
          </button>
        );
      })}
    </div>
  );
}

function Toggle({
  label,
  on,
  onClick,
}: {
  label: string;
  on: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className={`rounded px-1.5 py-0.5 text-[11px] ${
        on
          ? "bg-accent/15 text-accent-strong"
          : "border border-edge-strong text-fg-faint hover:bg-hover"
      }`}
      title={label}
    >
      {label}
    </button>
  );
}
