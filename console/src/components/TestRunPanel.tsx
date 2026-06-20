import { useEffect, useMemo, useRef, useState } from "react";
import NavigatedViewer from "bpmn-js/lib/NavigatedViewer";
import "bpmn-js/dist/assets/diagram-js.css";
import "bpmn-js/dist/assets/bpmn-font/css/bpmn-embedded.css";
import init, { TestEngine } from "../wasm/nanobpmn_engine";

interface ActiveEl {
  key: string;
  elementId: string;
}
interface InstanceDto {
  key: string;
  processId: string;
  state: string;
  completed: boolean;
  activeElements: ActiveEl[];
  variables: Record<string, unknown>;
}
interface JobDto {
  key: string;
  instanceKey: string;
  elementId: string;
  jobType: string;
  state: string;
  retries: number;
}
interface IncidentDto {
  key: string;
  instanceKey: string;
  elementId: string;
  kind: string;
  reason: string;
}
interface TimerDto {
  key: string;
  instanceKey: string;
  elementId: string;
  dueAt: number;
  dueInMs: number;
}
interface Snapshot {
  now: number;
  eventCount: number;
  created?: string;
  totalInstances: number;
  completedInstances: number;
  instances: InstanceDto[];
  jobs: JobDto[];
  incidents: IncidentDto[];
  timers: TimerDto[];
  activeElementIds: string[];
  incidentElementIds: string[];
}

interface Canvas {
  zoom(mode: string): void;
  addMarker(elementId: string, marker: string): void;
  removeMarker(elementId: string, marker: string): void;
}

// Lazily initialise the wasm module exactly once per page.
let wasmReady: Promise<void> | null = null;
function ensureWasm(): Promise<void> {
  if (!wasmReady) wasmReady = init().then(() => undefined);
  return wasmReady;
}

/// Read-only diagram that imports the XML once and updates token/incident
/// markers in place (no re-import, so the zoom/scroll position is preserved
/// while stepping through the simulation).
function SimDiagram({
  xml,
  activeIds,
  incidentIds,
}: {
  xml: string;
  activeIds: string[];
  incidentIds: string[];
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewerRef = useRef<NavigatedViewer | null>(null);
  const importedRef = useRef(false);
  const markedRef = useRef<{ id: string; cls: string }[]>([]);

  useEffect(() => {
    if (!containerRef.current) return;
    const viewer = new NavigatedViewer({ container: containerRef.current });
    viewerRef.current = viewer;
    importedRef.current = false;
    viewer
      .importXML(xml)
      .then(() => {
        viewer.get<Canvas>("canvas").zoom("fit-viewport");
        importedRef.current = true;
        applyMarkers();
      })
      .catch(() => {
        /* malformed XML — leave blank */
      });
    return () => {
      viewer.destroy();
      viewerRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [xml]);

  function applyMarkers() {
    const viewer = viewerRef.current;
    if (!viewer || !importedRef.current) return;
    const canvas = viewer.get<Canvas>("canvas");
    for (const { id, cls } of markedRef.current) {
      try {
        canvas.removeMarker(id, cls);
      } catch {
        /* ignore */
      }
    }
    const next: { id: string; cls: string }[] = [];
    for (const id of activeIds) next.push({ id, cls: "nano-active" });
    for (const id of incidentIds) next.push({ id, cls: "nano-incident" });
    for (const { id, cls } of next) {
      try {
        canvas.addMarker(id, cls);
      } catch {
        /* element not in this diagram */
      }
    }
    markedRef.current = next;
  }

  useEffect(() => {
    applyMarkers();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeIds, incidentIds]);

  return <div ref={containerRef} className="h-full w-full" />;
}

export default function TestRunPanel({
  xml,
  onClose,
}: {
  xml: string;
  onClose: () => void;
}) {
  const engineRef = useRef<TestEngine | null>(null);
  const [phase, setPhase] = useState<"loading" | "ready" | "error">("loading");
  const [error, setError] = useState<string | null>(null);
  const [processIds, setProcessIds] = useState<string[]>([]);
  const [process, setProcess] = useState<string>("");
  const [startVars, setStartVars] = useState("{}");
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [jobVars, setJobVars] = useState<Record<string, string>>({});
  const [advanceMs, setAdvanceMs] = useState("60000");

  function deployInto(engine: TestEngine) {
    const res = JSON.parse(engine.deploy(xml)) as { processIds: string[] };
    setProcessIds(res.processIds);
    setProcess(res.processIds[0] ?? "");
    setSnapshot(null);
    setJobVars({});
    setError(null);
  }

  useEffect(() => {
    let cancelled = false;
    ensureWasm()
      .then(() => {
        if (cancelled) return;
        const engine = new TestEngine();
        engineRef.current = engine;
        deployInto(engine);
        setPhase("ready");
      })
      .catch((e) => {
        if (cancelled) return;
        setError(String(e));
        setPhase("error");
      });
    return () => {
      cancelled = true;
      engineRef.current?.free();
      engineRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [xml]);

  function run(fn: (e: TestEngine) => string): void {
    const engine = engineRef.current;
    if (!engine) return;
    try {
      setSnapshot(JSON.parse(fn(engine)) as Snapshot);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }

  function start() {
    run((e) => e.createInstance(process, startVars || "{}"));
  }
  function completeJob(key: string) {
    const vars = jobVars[key]?.trim() || "{}";
    run((e) => e.completeJob(key, vars));
  }
  function failJob(key: string) {
    run((e) => e.failJob(key, 0, "Failed in test run"));
  }
  function advance() {
    const ms = Number(advanceMs);
    run((e) => e.advanceTime(Number.isFinite(ms) ? ms : 0));
  }
  function reset() {
    const engine = engineRef.current;
    if (!engine) return;
    try {
      deployInto(engine);
    } catch (e) {
      setError(String(e));
    }
  }

  const activeIds = snapshot?.activeElementIds ?? [];
  const incidentIds = snapshot?.incidentElementIds ?? [];
  const started = snapshot !== null;

  const allDone = useMemo(
    () =>
      started &&
      snapshot!.totalInstances > 0 &&
      snapshot!.completedInstances === snapshot!.totalInstances &&
      snapshot!.jobs.length === 0 &&
      snapshot!.incidents.length === 0,
    [started, snapshot],
  );

  return (
    <div className="flex h-full min-h-0">
      {/* Simulated diagram with live token highlighting */}
      <div className="relative min-w-0 flex-1 bg-zinc-950">
        {started ? (
          <SimDiagram xml={xml} activeIds={activeIds} incidentIds={incidentIds} />
        ) : (
          <div className="flex h-full items-center justify-center text-sm text-zinc-500">
            {phase === "loading"
              ? "Loading the in-browser engine…"
              : "Start an instance to simulate this process."}
          </div>
        )}
        {allDone && (
          <div className="pointer-events-none absolute left-1/2 top-4 -translate-x-1/2 rounded-full bg-emerald-900/80 px-4 py-1 text-xs font-medium text-emerald-200">
            ✓ All instances completed
          </div>
        )}
      </div>

      {/* Control panel */}
      <div className="flex w-96 shrink-0 flex-col border-l border-zinc-800 bg-zinc-900">
        <header className="flex items-center justify-between border-b border-zinc-800 px-4 py-2">
          <div>
            <h2 className="text-sm font-semibold">Test run</h2>
            <p className="text-xs text-zinc-500">In-browser simulation (not deployed)</p>
          </div>
          <button
            onClick={onClose}
            className="rounded-md bg-zinc-800 px-2.5 py-1 text-xs text-zinc-300 hover:bg-zinc-700"
          >
            Close
          </button>
        </header>

        {error && (
          <div className="border-b border-red-900/50 bg-red-950/60 px-4 py-2 text-xs text-red-300">
            {error}
          </div>
        )}

        <div className="min-h-0 flex-1 space-y-4 overflow-auto p-4">
          {phase === "error" && (
            <p className="text-sm text-red-300">
              Could not load the simulation engine.
            </p>
          )}

          {/* Start form */}
          <section className="space-y-2">
            <div className="flex items-center justify-between">
              <h3 className="text-xs font-semibold uppercase tracking-wide text-zinc-400">
                Start
              </h3>
              {started && (
                <button
                  onClick={reset}
                  className="text-xs text-zinc-400 hover:text-zinc-200"
                >
                  Reset
                </button>
              )}
            </div>
            <label className="block text-xs text-zinc-500">
              Process
              <select
                value={process}
                onChange={(e) => setProcess(e.target.value)}
                disabled={phase !== "ready" || processIds.length === 0}
                className="mt-1 w-full rounded border border-zinc-700 bg-zinc-950 px-2 py-1 font-mono text-xs text-zinc-200"
              >
                {processIds.length === 0 && <option value="">— none —</option>}
                {processIds.map((id) => (
                  <option key={id} value={id}>
                    {id}
                  </option>
                ))}
              </select>
            </label>
            <label className="block text-xs text-zinc-500">
              Variables (JSON)
              <textarea
                value={startVars}
                onChange={(e) => setStartVars(e.target.value)}
                rows={3}
                spellCheck={false}
                className="mt-1 w-full rounded border border-zinc-700 bg-zinc-950 px-2 py-1 font-mono text-xs text-zinc-200"
              />
            </label>
            <button
              onClick={start}
              disabled={phase !== "ready" || !process}
              className="w-full rounded-md bg-sky-700 px-3 py-1.5 text-xs font-medium text-white hover:bg-sky-600 disabled:opacity-50"
            >
              {started ? "Start another instance" : "Start instance"}
            </button>
          </section>

          {started && (
            <>
              {/* Waiting jobs */}
              <section className="space-y-2">
                <h3 className="text-xs font-semibold uppercase tracking-wide text-zinc-400">
                  Waiting jobs ({snapshot!.jobs.length})
                </h3>
                {snapshot!.jobs.length === 0 ? (
                  <p className="text-xs text-zinc-600">No jobs are waiting.</p>
                ) : (
                  snapshot!.jobs.map((job) => (
                    <div
                      key={job.key}
                      className="rounded border border-zinc-800 bg-zinc-950 p-2"
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-mono text-xs text-zinc-300">
                          {job.elementId}
                        </span>
                        <span className="rounded bg-zinc-800 px-1.5 py-0.5 font-mono text-[10px] text-zinc-400">
                          {job.jobType}
                        </span>
                      </div>
                      <textarea
                        value={jobVars[job.key] ?? "{}"}
                        onChange={(e) =>
                          setJobVars((m) => ({ ...m, [job.key]: e.target.value }))
                        }
                        rows={2}
                        spellCheck={false}
                        placeholder="output variables (JSON)"
                        className="mt-1.5 w-full rounded border border-zinc-700 bg-zinc-900 px-2 py-1 font-mono text-[11px] text-zinc-200"
                      />
                      <div className="mt-1.5 flex gap-1.5">
                        <button
                          onClick={() => completeJob(job.key)}
                          className="flex-1 rounded bg-emerald-800 px-2 py-1 text-[11px] font-medium text-emerald-100 hover:bg-emerald-700"
                        >
                          Complete
                        </button>
                        <button
                          onClick={() => failJob(job.key)}
                          className="flex-1 rounded bg-red-900 px-2 py-1 text-[11px] font-medium text-red-200 hover:bg-red-800"
                        >
                          Fail (incident)
                        </button>
                      </div>
                    </div>
                  ))
                )}
              </section>

              {/* Timers */}
              {snapshot!.timers.length > 0 && (
                <section className="space-y-2">
                  <h3 className="text-xs font-semibold uppercase tracking-wide text-zinc-400">
                    Timers ({snapshot!.timers.length})
                  </h3>
                  {snapshot!.timers.map((t) => (
                    <div
                      key={t.key}
                      className="flex items-center justify-between rounded border border-zinc-800 bg-zinc-950 px-2 py-1 text-xs"
                    >
                      <span className="font-mono text-zinc-300">{t.elementId}</span>
                      <span className="text-zinc-500">
                        due in {t.dueInMs} ms
                      </span>
                    </div>
                  ))}
                </section>
              )}

              {/* Incidents */}
              {snapshot!.incidents.length > 0 && (
                <section className="space-y-2">
                  <h3 className="text-xs font-semibold uppercase tracking-wide text-red-400">
                    Incidents ({snapshot!.incidents.length})
                  </h3>
                  {snapshot!.incidents.map((i) => (
                    <div
                      key={i.key}
                      className="rounded border border-red-900/50 bg-red-950/40 p-2 text-xs"
                    >
                      <div className="font-mono text-red-300">{i.elementId}</div>
                      <div className="text-zinc-400">
                        {i.kind}: {i.reason}
                      </div>
                    </div>
                  ))}
                </section>
              )}

              {/* Clock */}
              <section className="space-y-1.5">
                <h3 className="text-xs font-semibold uppercase tracking-wide text-zinc-400">
                  Virtual clock
                </h3>
                <div className="text-xs text-zinc-500">now = {snapshot!.now} ms</div>
                <div className="flex gap-1.5">
                  <input
                    value={advanceMs}
                    onChange={(e) => setAdvanceMs(e.target.value)}
                    className="w-24 rounded border border-zinc-700 bg-zinc-950 px-2 py-1 font-mono text-xs text-zinc-200"
                  />
                  <button
                    onClick={advance}
                    className="flex-1 rounded bg-zinc-800 px-2 py-1 text-xs text-zinc-200 hover:bg-zinc-700"
                  >
                    Advance time
                  </button>
                </div>
              </section>

              {/* Variables */}
              <section className="space-y-2">
                <h3 className="text-xs font-semibold uppercase tracking-wide text-zinc-400">
                  Instances ({snapshot!.completedInstances}/{snapshot!.totalInstances} done)
                </h3>
                {snapshot!.instances.map((inst) => (
                  <div
                    key={inst.key}
                    className="rounded border border-zinc-800 bg-zinc-950 p-2 text-xs"
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono text-zinc-400">#{inst.key}</span>
                      <span
                        className={
                          inst.state === "Completed"
                            ? "text-emerald-400"
                            : inst.state === "Terminated"
                              ? "text-red-400"
                              : "text-amber-400"
                        }
                      >
                        {inst.state}
                      </span>
                    </div>
                    <pre className="mt-1 overflow-auto whitespace-pre-wrap break-words font-mono text-[11px] text-zinc-400">
                      {JSON.stringify(inst.variables, null, 2)}
                    </pre>
                  </div>
                ))}
              </section>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
