import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  getProject,
  runProject,
  stopProject,
  type AppUi,
  type RunState,
} from "../gen";
import { projectLogs, type ProjectLogLine } from "../lib/api";

// The console-integrated control surface for a supervised app (ADR 0057, issue
// #638 — Slice 3/4). Reached from the left-rail running-apps entry. This is the
// *headless* body: status + ports + Start/Stop/Restart + live logs. The
// embedded webview for UI apps (a sandboxed iframe over a reverse proxy) is a
// later slice; until then every app — UI or headless — is controlled here.

function statusTone(status: RunState["status"]): {
  label: string;
  dot: string;
  text: string;
} {
  switch (status) {
    case "running":
      return { label: "Running", dot: "bg-success", text: "text-success" };
    case "starting":
      return { label: "Starting", dot: "bg-accent", text: "text-accent" };
    case "stopping":
      return { label: "Stopping", dot: "bg-warn", text: "text-warn" };
    case "error":
      return { label: "Crashed", dot: "bg-danger", text: "text-danger" };
    default:
      return { label: "Stopped", dot: "bg-fg-muted", text: "text-fg-muted" };
  }
}

function streamTone(stream: ProjectLogLine["stream"]): string {
  if (stream === "err") return "text-danger";
  if (stream === "sys") return "text-fg-muted";
  return "text-fg";
}

export default function AppView() {
  const { name = "" } = useParams<{ name: string }>();
  const [runState, setRunState] = useState<RunState | null>(null);
  const [appUi, setAppUi] = useState<AppUi | null>(null);
  const [displayName, setDisplayName] = useState<string>(name);
  const [error, setError] = useState<string | null>(null);
  const [notFound, setNotFound] = useState(false);
  const [busy, setBusy] = useState(false);
  const [logs, setLogs] = useState<ProjectLogLine[]>([]);
  const logRef = useRef<HTMLDivElement | null>(null);

  const refresh = useCallback(async () => {
    try {
      const d = (await getProject({ path: { name }, throwOnError: true })).data;
      setRunState(d.runState);
      setAppUi(d.appUi ?? null);
      setDisplayName(d.config.displayName || d.config.name || name);
      setNotFound(false);
    } catch (e) {
      // A 404 means the project was deleted while the rail still linked it.
      if (e instanceof Error && /not found|404/i.test(e.message)) {
        setNotFound(true);
      } else {
        setError(e instanceof Error ? e.message : String(e));
      }
    }
  }, [name]);

  // Initial load whenever the routed app changes.
  useEffect(() => {
    setLogs([]);
    setError(null);
    void refresh();
  }, [refresh]);

  // Live logs for the life of this view (history replays first, then tail).
  useEffect(() => {
    if (!name) return;
    const src = projectLogs(name, (line) => {
      setLogs((prev) => {
        const next =
          prev.length > 2000 ? prev.slice(prev.length - 2000) : prev.slice();
        next.push(line);
        return next;
      });
    });
    return () => src.close();
  }, [name]);

  // Poll run state while the app is in a transient/active phase, so the badge
  // and buttons settle after an async start/stop without a manual reload.
  useEffect(() => {
    const active =
      runState &&
      (runState.status !== "stopped" || runState.compiling) &&
      runState.status !== "error";
    if (!active) return;
    const t = window.setInterval(() => void refresh(), 1500);
    return () => window.clearInterval(t);
  }, [runState, refresh]);

  useEffect(() => {
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight });
  }, [logs]);

  const start = async () => {
    setBusy(true);
    setError(null);
    setLogs([]);
    try {
      setRunState(
        (await runProject({ path: { name }, throwOnError: true })).data,
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const stop = async () => {
    setBusy(true);
    setError(null);
    try {
      setRunState(
        (await stopProject({ path: { name }, throwOnError: true })).data,
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const restart = async () => {
    setBusy(true);
    setError(null);
    try {
      await stopProject({ path: { name }, throwOnError: true });
      setLogs([]);
      setRunState(
        (await runProject({ path: { name }, throwOnError: true })).data,
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  if (notFound) {
    return (
      <div className="p-8">
        <p className="text-fg-muted">
          App <span className="font-medium text-fg">{name}</span> no longer
          exists.{" "}
          <Link to="/projects" className="text-accent underline">
            Back to Studio
          </Link>
        </p>
      </div>
    );
  }

  const status = runState?.status ?? "stopped";
  const tone = statusTone(status);
  const running = status === "running" || status === "starting";
  const headless = !appUi?.enabled || appUi?.port == null;
  const btn =
    "rounded-md px-3 py-1.5 text-sm font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-50";

  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-edge px-6 py-4">
        <div className="flex items-center gap-3">
          <span
            className={`inline-block h-2.5 w-2.5 rounded-full ${tone.dot}`}
            aria-hidden="true"
          />
          <h1 className="text-lg font-semibold text-fg">
            {appUi?.label || displayName}
          </h1>
          <span className={`text-sm ${tone.text}`}>{tone.label}</span>
          <span className="ml-auto flex items-center gap-2">
            {running ? (
              <>
                <button
                  type="button"
                  onClick={restart}
                  disabled={busy}
                  className={`${btn} border border-edge text-fg hover:bg-hover`}
                >
                  Restart
                </button>
                <button
                  type="button"
                  onClick={stop}
                  disabled={busy}
                  className={`${btn} bg-danger text-white hover:opacity-90`}
                >
                  Stop
                </button>
              </>
            ) : (
              <button
                type="button"
                onClick={start}
                disabled={busy}
                className={`${btn} bg-accent text-white hover:opacity-90`}
              >
                Start
              </button>
            )}
          </span>
        </div>
        <dl className="mt-3 flex flex-wrap gap-x-6 gap-y-1 text-xs text-fg-muted">
          <div className="flex gap-1.5">
            <dt>Project</dt>
            <dd className="font-mono text-fg">{name}</dd>
          </div>
          <div className="flex gap-1.5">
            <dt>Mode</dt>
            <dd className="text-fg">{headless ? "Headless" : "UI"}</dd>
          </div>
          {appUi?.port != null && (
            <div className="flex gap-1.5">
              <dt>UI port</dt>
              <dd className="font-mono text-fg">{appUi.port}</dd>
            </div>
          )}
          {runState?.pid != null && (
            <div className="flex gap-1.5">
              <dt>PID</dt>
              <dd className="font-mono text-fg">{runState.pid}</dd>
            </div>
          )}
        </dl>
        {!headless && (
          <p className="mt-2 text-xs text-fg-muted">
            This app declares an embedded UI on port{" "}
            <span className="font-mono">{appUi?.port}</span>. The in-console
            webview ships in a later slice; use the controls above to manage it
            for now.
          </p>
        )}
        {runState?.lastError && (
          <p className="mt-2 text-xs text-danger">{runState.lastError}</p>
        )}
        {error && <p className="mt-2 text-xs text-danger">{error}</p>}
      </header>

      <section className="flex min-h-0 flex-1 flex-col">
        <div className="border-b border-edge px-6 py-2 text-xs font-medium uppercase tracking-wide text-fg-muted">
          Logs
        </div>
        <div
          ref={logRef}
          className="min-h-0 flex-1 overflow-auto bg-inset px-6 py-3 font-mono text-xs leading-relaxed"
        >
          {logs.length === 0 ? (
            <p className="text-fg-muted">
              No output yet. Start the app to see logs.
            </p>
          ) : (
            logs.map((l, i) => (
              <div key={i} className={streamTone(l.stream)}>
                {l.text}
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}
