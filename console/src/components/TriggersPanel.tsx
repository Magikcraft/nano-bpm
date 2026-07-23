import { useCallback, useEffect, useRef, useState } from "react";

import { Badge, Button } from "./ui";
import {
  enqueueTriggerEvent,
  getTriggerInbox,
  getTriggers,
  type SourceKindInfo,
  type TriggerInboxResponse,
  type TriggerInfo,
  type TriggersResponse,
} from "../gen";

// The Triggers panel — ADR 0025 §7 / phase 3. Two sub-surfaces over the App's
// declared `triggers[]`: **Triggers** (the sources that make the App act, tagged
// against the extensible source registry from `GET /triggers`) and **Inbox** (the
// durable at-least-once delivery status from `GET /triggers/inbox`). "Run now"
// enqueues a synthetic event through the same inbox path a real source uses, so
// testing a trigger exercises the production dispatch path.

type SubTab = "triggers" | "inbox";
const INBOX_POLL_MS = 4000;

function errMsg(e: unknown): string {
  if (e instanceof Error) return e.message;
  if (typeof e === "string") return e;
  if (e && typeof e === "object") {
    const o = e as { error?: unknown; detail?: unknown };
    if (typeof o.error === "string") return o.error;
    if (typeof o.detail === "string") return o.detail;
  }
  return String(e);
}

/** Compact one-line summary of a trigger's action block. */
function actionSummary(action: TriggerInfo["action"]): string {
  if (!action || typeof action !== "object") return "—";
  const a = action as Record<string, unknown>;
  if (typeof a.start === "string") return `start ${a.start}`;
  if (typeof a.message === "string") return `message ${a.message}`;
  const keys = Object.keys(a);
  return keys.length ? keys.join(", ") : "—";
}

export default function TriggersPanel({ name }: { name: string }) {
  const [tab, setTab] = useState<SubTab>("triggers");
  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-3 border-b border-edge bg-panel px-4 py-2">
        <span className="text-xs font-semibold uppercase tracking-wider text-fg-faint">
          Triggers
        </span>
        <div className="flex-1" />
        <nav className="flex items-center gap-1 text-sm">
          {(["triggers", "inbox"] as SubTab[]).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`rounded-md px-2.5 py-1 capitalize ${
                tab === t
                  ? "bg-accent/15 font-semibold text-accent"
                  : "text-fg-faint hover:bg-hover"
              }`}
            >
              {t}
            </button>
          ))}
        </nav>
      </div>

      <div className="min-h-0 flex-1 overflow-auto">
        {tab === "triggers" ? <TriggersTab name={name} /> : <InboxTab name={name} />}
      </div>
    </div>
  );
}

// --- Triggers ---------------------------------------------------------------

function TriggersTab({ name }: { name: string }) {
  const [data, setData] = useState<TriggersResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoadError(null);
    try {
      const r = await getTriggers({ path: { name }, throwOnError: true });
      setData(r.data);
    } catch (e) {
      setLoadError(errMsg(e));
    } finally {
      setLoading(false);
    }
  }, [name]);

  useEffect(() => {
    void load();
  }, [load]);

  if (loading) {
    return <div className="p-8 text-sm text-fg-faint">Loading triggers…</div>;
  }
  if (loadError) {
    return <div className="p-8 text-sm text-danger">Couldn’t load triggers: {loadError}</div>;
  }
  if (!data) return null;

  if (data.triggers.length === 0) {
    return (
      <div className="flex h-full items-center justify-center p-8 text-center text-sm text-fg-faint">
        <div>
          <p className="font-medium text-fg-muted">No triggers declared.</p>
          <p className="mt-1">
            Add a <code className="text-fg-muted">triggers</code> block to{" "}
            <code className="text-fg-muted">nano.app.json</code> (ADR 0025) to make this App act on a
            schedule, a webhook, or a file change.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4 p-4">
      {data.errors.length > 0 && (
        <div className="rounded-md border border-danger/30 bg-danger/10 px-3 py-2 text-xs text-danger">
          <p className="font-semibold">Source configuration errors</p>
          <ul className="mt-1 list-disc pl-4">
            {data.errors.map((e, i) => (
              <li key={i}>{e}</li>
            ))}
          </ul>
        </div>
      )}

      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="border-b border-edge text-left text-xs uppercase tracking-wider text-fg-faint">
            <th className="px-2 py-1.5 font-medium">Trigger</th>
            <th className="px-2 py-1.5 font-medium">Source</th>
            <th className="px-2 py-1.5 font-medium">Action</th>
            <th className="px-2 py-1.5 font-medium text-right">Run</th>
          </tr>
        </thead>
        <tbody>
          {data.triggers.map((t) => (
            <TriggerRow key={t.id} name={name} trigger={t} />
          ))}
        </tbody>
      </table>

      <SourceRegistry sources={data.sources} onRefresh={() => void load()} />
    </div>
  );
}

function TriggerRow({ name, trigger }: { name: string; trigger: TriggerInfo }) {
  const [open, setOpen] = useState(false);
  const [body, setBody] = useState("{}");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const run = useCallback(async () => {
    setBusy(true);
    setResult(null);
    setError(null);
    let parsed: Record<string, unknown>;
    try {
      const v = body.trim() === "" ? {} : JSON.parse(body);
      if (typeof v !== "object" || v === null || Array.isArray(v)) {
        throw new Error("body must be a JSON object");
      }
      parsed = v as Record<string, unknown>;
    } catch (e) {
      setError(`Invalid JSON: ${errMsg(e)}`);
      setBusy(false);
      return;
    }
    try {
      const r = await enqueueTriggerEvent({
        path: { name },
        body: { triggerId: trigger.id, body: parsed },
        throwOnError: true,
      });
      setResult(
        r.data.enqueued
          ? `Enqueued (inbox row ${r.data.id ?? "?"}).`
          : "Duplicate — an event with this idempotency key was already enqueued.",
      );
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }, [body, name, trigger.id]);

  return (
    <>
      <tr className="border-b border-edge/50">
        <td className="px-2 py-2 font-mono text-xs text-fg">{trigger.id}</td>
        <td className="px-2 py-2">
          {trigger.builtin ? (
            <Badge tone="accent">{trigger.type}</Badge>
          ) : trigger.recognized ? (
            <Badge tone="info" className="gap-1">
              {trigger.type} <span className="opacity-70">· pack</span>
            </Badge>
          ) : (
            <span title="Unknown source kind — a typo or a not-yet-installed pack.">
              <Badge tone="danger">{trigger.type || "?"} · unrecognized</Badge>
            </span>
          )}
        </td>
        <td className="px-2 py-2 text-xs text-fg-muted">{actionSummary(trigger.action)}</td>
        <td className="px-2 py-2 text-right">
          <Button size="sm" variant="secondary" onClick={() => setOpen((v) => !v)}>
            {open ? "Cancel" : "Run now"}
          </Button>
        </td>
      </tr>
      {open && (
        <tr className="border-b border-edge/50 bg-bg-subtle/40">
          <td colSpan={4} className="px-2 py-2">
            <div className="flex flex-col gap-2">
              <label className="text-xs text-fg-faint">
                Event body (JSON) — the action’s FEEL evaluates over{" "}
                <code className="text-fg-muted">body</code>:
              </label>
              <textarea
                value={body}
                onChange={(e) => setBody(e.target.value)}
                spellCheck={false}
                rows={4}
                className="w-full rounded-md border border-edge-strong bg-bg px-2 py-1.5 font-mono text-xs text-fg outline-none focus-visible:ring-2 focus-visible:ring-accent/60"
              />
              <div className="flex items-center gap-2">
                <Button size="sm" variant="primary" onClick={() => void run()} disabled={busy}>
                  {busy ? "Enqueuing…" : "Enqueue event"}
                </Button>
                {result && <span className="text-xs text-ok">{result}</span>}
                {error && <span className="text-xs text-danger">{error}</span>}
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

function SourceRegistry({
  sources,
  onRefresh,
}: {
  sources: SourceKindInfo[];
  onRefresh: () => void;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div className="rounded-md border border-edge">
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-2 px-3 py-2 text-left text-xs font-medium text-fg-muted hover:bg-hover"
      >
        <span className="text-fg-faint">{open ? "▾" : "▸"}</span>
        Source registry ({sources.length})
        <span className="ml-1 font-normal text-fg-faint">
          — core kinds plus every installed pack
        </span>
        <div className="flex-1" />
        <span
          role="button"
          tabIndex={0}
          onClick={(e) => {
            e.stopPropagation();
            onRefresh();
          }}
          className="rounded px-1.5 py-0.5 text-fg-faint hover:bg-hover hover:text-fg-muted"
        >
          Refresh
        </span>
      </button>
      {open && (
        <div className="flex flex-wrap gap-2 border-t border-edge px-3 py-2">
          {sources.map((s) => (
            <span
              key={s.kind}
              className="inline-flex items-center gap-1.5 rounded-md border border-edge px-2 py-1 text-xs"
              title={s.builtin ? "Compiled-in core source" : "Pack source"}
            >
              <span className="font-mono text-fg">{s.kind}</span>
              {s.displayName && <span className="text-fg-faint">{s.displayName}</span>}
              <Badge tone={s.builtin ? "accent" : "info"}>{s.builtin ? "core" : "pack"}</Badge>
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

// --- Inbox ------------------------------------------------------------------

function InboxTab({ name }: { name: string }) {
  const [data, setData] = useState<TriggerInboxResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [auto, setAuto] = useState(true);
  const timer = useRef<ReturnType<typeof setInterval> | null>(null);

  const load = useCallback(async () => {
    try {
      const r = await getTriggerInbox({ path: { name }, throwOnError: true });
      setData(r.data);
      setLoadError(null);
    } catch (e) {
      setLoadError(errMsg(e));
    } finally {
      setLoading(false);
    }
  }, [name]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (!auto) {
      if (timer.current) clearInterval(timer.current);
      timer.current = null;
      return;
    }
    timer.current = setInterval(() => void load(), INBOX_POLL_MS);
    return () => {
      if (timer.current) clearInterval(timer.current);
      timer.current = null;
    };
  }, [auto, load]);

  if (loading) {
    return <div className="p-8 text-sm text-fg-faint">Loading inbox…</div>;
  }
  if (loadError) {
    return (
      <div className="p-4">
        <div className="rounded-md border border-warn/30 bg-warn/10 px-3 py-2 text-xs text-warn">
          Inbox unavailable: {loadError}
          <p className="mt-1 text-fg-faint">
            The inbox is created on first event — run a trigger, or Run the App, to initialise it.
          </p>
        </div>
      </div>
    );
  }
  if (!data) return null;

  return (
    <div className="flex flex-col gap-4 p-4">
      <div className="flex items-center gap-3">
        <Badge tone="warn">pending {data.pending}</Badge>
        <Badge tone="ok">done {data.done}</Badge>
        <Badge tone="danger">failed {data.failed}</Badge>
        <div className="flex-1" />
        <label className="flex items-center gap-1.5 text-xs text-fg-faint">
          <input type="checkbox" checked={auto} onChange={(e) => setAuto(e.target.checked)} />
          Auto-refresh
        </label>
        <Button size="sm" variant="secondary" onClick={() => void load()}>
          Refresh
        </Button>
      </div>

      {data.recent.length === 0 ? (
        <p className="text-sm text-fg-faint">No events yet.</p>
      ) : (
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr className="border-b border-edge text-left text-xs uppercase tracking-wider text-fg-faint">
              <th className="px-2 py-1.5 font-medium">#</th>
              <th className="px-2 py-1.5 font-medium">Trigger</th>
              <th className="px-2 py-1.5 font-medium">Status</th>
              <th className="px-2 py-1.5 font-medium text-right">Attempts</th>
              <th className="px-2 py-1.5 font-medium">Last error</th>
              <th className="px-2 py-1.5 font-medium">Created</th>
            </tr>
          </thead>
          <tbody>
            {data.recent.map((r) => (
              <tr key={r.id} className="border-b border-edge/50">
                <td className="px-2 py-1.5 font-mono text-xs text-fg-faint">{r.id}</td>
                <td className="px-2 py-1.5 font-mono text-xs text-fg">{r.triggerId}</td>
                <td className="px-2 py-1.5">
                  <Badge
                    tone={
                      r.status === "done" ? "ok" : r.status === "failed" ? "danger" : "warn"
                    }
                  >
                    {r.status}
                  </Badge>
                </td>
                <td className="px-2 py-1.5 text-right font-mono text-xs text-fg-muted">
                  {r.attempts}
                </td>
                <td
                  className="max-w-[24rem] truncate px-2 py-1.5 text-xs text-danger"
                  title={r.lastError ?? ""}
                >
                  {r.lastError ?? ""}
                </td>
                <td className="px-2 py-1.5 text-xs text-fg-faint">
                  {new Date(r.createdAt).toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
