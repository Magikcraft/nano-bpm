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
    // Use the non-throwing client so we can inspect the HTTP status: the
    // generated client throws the raw response body (a string) on error, which
    // makes 404 (deleted project) indistinguishable from a transport failure.
    const { data, error: err, response } = await getProject({ path: { name } });
    if (response?.status === 404) {
      setNotFound(true);
      return;
    }
    if (err || !data) {
      setError(
        typeof err === "string" ? err : "Failed to load the app's status.",
      );
      return;
    }
    setRunState(data.runState);
    setAppUi(data.appUi ?? null);
    setDisplayName(data.config.displayName || data.config.name || name);
    setNotFound(false);
    setError(null);
  }, [name]);

  // Initial load whenever the routed app changes.
  useEffect(() => {
    setLogs([]);
    setError(null);
    void refresh();
  }, [refresh]);

  // Live logs for the life of this view (history replays first, then tail).
  useEffect(() => {
    if (!name || notFound) return;
    const src = projectLogs(name, (line) => {
      setLogs((prev) => {
        const next =
          prev.length > 2000 ? prev.slice(prev.length - 2000) : prev.slice();
        next.push(line);
        return next;
      });
    });
    return () => src.close();
  }, [name, notFound]);

  // Poll run state while the app is in a transient/active phase, so the badge
  // and buttons settle after an async start/stop without a manual reload.
  useEffect(() => {
    const active =
      !notFound &&
      runState &&
      (runState.status !== "stopped" || runState.compiling) &&
      runState.status !== "error";
    if (!active) return;
    const t = window.setInterval(() => void refresh(), 1500);
    return () => window.clearInterval(t);
  }, [runState, refresh, notFound]);

  useEffect(() => {
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight });
  }, [logs]);

  const start = async () => {
    setBusy(true);
    setError(null);
    setLogs([]);
    try {
      // Use the non-throwing client so a 404 (project deleted mid-session)
      // flips the view into `notFound` instead of surfacing as an opaque
      // error while polling/streaming keep running against a gone project.
      const {
        data,
        error: err,
        response,
      } = await runProject({ path: { name } });
      if (response?.status === 404) {
        setNotFound(true);
        return;
      }
      if (err || !data) {
        setError(typeof err === "string" ? err : "Failed to start the app.");
        return;
      }
      setRunState(data);
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
      const {
        data,
        error: err,
        response,
      } = await stopProject({
        path: { name },
      });
      if (response?.status === 404) {
        setNotFound(true);
        return;
      }
      if (err || !data) {
        setError(typeof err === "string" ? err : "Failed to stop the app.");
        return;
      }
      setRunState(data);
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
      const stopRes = await stopProject({ path: { name } });
      if (stopRes.response?.status === 404) {
        setNotFound(true);
        return;
      }
      if (stopRes.error) {
        setError(
          typeof stopRes.error === "string"
            ? stopRes.error
            : "Failed to stop the app.",
        );
        return;
      }
      // `stop` only *signals* termination; `run` no-ops while the phase is still
      // Starting/Running. Without waiting for the child to actually exit, the
      // run is dropped and the app is left stopped. Poll (bounded ~10s) until the
      // supervisor reports a terminal phase, then start.
      for (let i = 0; i < 40; i++) {
        const { data, response } = await getProject({ path: { name } });
        if (response?.status === 404) {
          setNotFound(true);
          return;
        }
        const s = data?.runState.status;
        if (!s || s === "stopped" || s === "error") break;
        await new Promise((r) => setTimeout(r, 250));
      }
      setLogs([]);
      const runRes = await runProject({ path: { name } });
      if (runRes.response?.status === 404) {
        setNotFound(true);
        return;
      }
      if (runRes.error || !runRes.data) {
        setError(
          typeof runRes.error === "string"
            ? runRes.error
            : "Failed to start the app.",
        );
        return;
      }
      setRunState(runRes.data);
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
  // Treat every non-terminal phase as "in flight" so the Stop/Restart controls
  // (not Start) show while the app is starting or stopping. Showing Start during
  // a `stopping` transition invites an invalid action that races the shutdown.
  const running =
    status === "running" || status === "starting" || status === "stopping";
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
