import { useEffect, useMemo, useState } from "react";
import "bpmn-js/dist/assets/diagram-js.css";
import "bpmn-js/dist/assets/bpmn-font/css/bpmn-embedded.css";
import { useBojtos, BpmnRuntimeView } from "@nanobpm/bojtos-react";
import { TraceTimeline } from "./TraceTimeline";
import { foldSimTrace, stepFmt } from "../lib/simTrace";
import { Badge, Button, ErrorText, SectionLabel } from "./ui";

export default function TestRunPanel({
  xml,
  onClose,
}: {
  xml: string;
  onClose: () => void;
}) {
  const {
    phase,
    error,
    processIds,
    snapshot,
    events,
    createInstance,
    completeJob: completeJobCmd,
    failJob: failJobCmd,
    advanceTime,
    reset: resetEngine,
  } = useBojtos({ bpmn: xml });

  const [process, setProcess] = useState<string>("");
  const [startVars, setStartVars] = useState("{}");
  const [jobVars, setJobVars] = useState<Record<string, string>>({});
  const [advanceMs, setAdvanceMs] = useState("60000");
  const [leftView, setLeftView] = useState<"diagram" | "trace">("diagram");
  const [traceKey, setTraceKey] = useState<string | null>(null);

  // Each (re)deployment produces a fresh `processIds` array; when it changes,
  // default the process selection and clear the per-run UI state — matching the
  // original panel's `deployInto` reset.
  useEffect(() => {
    setProcess(processIds[0] ?? "");
    setJobVars({});
    setTraceKey(null);
  }, [processIds]);

  function start() {
    const snap = createInstance(process, startVars || "{}");
    if (snap?.created) setTraceKey(snap.created);
  }
  function completeJob(key: string) {
    const vars = jobVars[key]?.trim() || "{}";
    completeJobCmd(key, vars);
  }
  function failJob(key: string) {
    failJobCmd(key, 0, "Failed in test run");
  }
  function advance() {
    const ms = Number(advanceMs);
    advanceTime(Number.isFinite(ms) ? ms : 0);
  }

  const activeIds = snapshot?.activeElementIds ?? [];
  const incidentIds = snapshot?.incidentElementIds ?? [];
  const started = snapshot !== null;

  const effectiveTraceKey = traceKey ?? snapshot?.instances[0]?.key ?? null;
  const simProcessId =
    snapshot?.instances.find((i) => i.key === effectiveTraceKey)?.processId ??
    process;
  const simTrace = useMemo(
    () =>
      effectiveTraceKey
        ? foldSimTrace(events, effectiveTraceKey, simProcessId)
        : null,
    [events, effectiveTraceKey, simProcessId],
  );

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
      {/* Simulated diagram / trace with live token highlighting */}
      <div className="relative flex min-w-0 flex-1 flex-col bg-app">
        <div className="flex items-center gap-2 border-b border-edge px-3 py-1.5">
          <div className="flex gap-1">
            <button
              onClick={() => setLeftView("diagram")}
              className={`rounded px-2.5 py-1 text-xs ${
                leftView === "diagram"
                  ? "bg-accent/10 font-medium text-accent-strong"
                  : "bg-raised text-fg-muted hover:bg-hover"
              }`}
            >
              Diagram
            </button>
            <button
              onClick={() => setLeftView("trace")}
              className={`rounded px-2.5 py-1 text-xs ${
                leftView === "trace"
                  ? "bg-accent/10 font-medium text-accent-strong"
                  : "bg-raised text-fg-muted hover:bg-hover"
              }`}
            >
              Trace
            </button>
          </div>
          <Badge tone="accent" className="uppercase tracking-wide">
            Simulation
          </Badge>
          {leftView === "trace" &&
            started &&
            snapshot!.instances.length > 1 && (
              <select
                value={effectiveTraceKey ?? ""}
                onChange={(e) => setTraceKey(e.target.value)}
                className="ml-auto rounded border border-edge-strong bg-inset px-2 py-0.5 font-mono text-xs text-fg"
              >
                {snapshot!.instances.map((i) => (
                  <option key={i.key} value={i.key}>
                    #{i.key}
                  </option>
                ))}
              </select>
            )}
        </div>
        <div className="relative min-h-0 flex-1">
          {!started ? (
            <div className="flex h-full items-center justify-center text-sm text-fg-faint">
              {phase === "loading"
                ? "Loading the in-browser engine…"
                : "Start an instance to simulate this process."}
            </div>
          ) : leftView === "diagram" ? (
            <BpmnRuntimeView
              xml={xml}
              activeIds={activeIds}
              incidentIds={incidentIds}
              className="h-full w-full bg-white"
            />
          ) : simTrace ? (
            <div className="h-full overflow-auto p-6">
              <p className="mb-4 text-xs text-fg-faint">
                Folded from the in-browser engine's event log. The simulation's
                virtual clock only advances on “Advance time”, so the axis is
                the logical step (event) index, not wall-clock time. This trace
                is never journaled, exported, or sent to the gateway.
              </p>
              <TraceTimeline trace={simTrace} fmt={stepFmt} />
            </div>
          ) : (
            <div className="flex h-full items-center justify-center text-sm text-fg-faint">
              No trace yet.
            </div>
          )}
          {leftView === "diagram" && allDone && (
            <div className="pointer-events-none absolute left-1/2 top-4 -translate-x-1/2 rounded-full border border-ok/30 bg-panel/90 px-4 py-1 text-xs font-medium text-ok">
              ✓ All instances completed
            </div>
          )}
        </div>
      </div>

      {/* Control panel */}
      <div className="flex w-96 shrink-0 flex-col border-l border-edge bg-panel">
        <header className="flex items-center justify-between border-b border-edge px-4 py-2">
          <div>
            <h2 className="text-sm font-semibold text-fg">Test run</h2>
            <p className="text-xs text-fg-faint">
              In-browser simulation (not deployed)
            </p>
          </div>
          <Button size="sm" onClick={onClose}>
            Close
          </Button>
        </header>

        {error && (
          <div className="border-b border-danger/30 bg-danger/10 px-4 py-2 text-xs text-danger">
            {error}
          </div>
        )}

        <div className="min-h-0 flex-1 space-y-4 overflow-auto p-4">
          {phase === "error" && (
            <ErrorText>Could not load the simulation engine.</ErrorText>
          )}

          {/* Start form */}
          <section className="space-y-2">
            <div className="flex items-center justify-between">
              <h3 className="text-xs font-semibold uppercase tracking-wide text-fg-faint">
                Start
              </h3>
              {started && (
                <button
                  onClick={resetEngine}
                  className="text-xs text-fg-muted hover:text-fg"
                >
                  Reset
                </button>
              )}
            </div>
            <label className="block text-xs text-fg-faint">
              Process
              <select
                value={process}
                onChange={(e) => setProcess(e.target.value)}
                disabled={phase !== "ready" || processIds.length === 0}
                className="mt-1 w-full rounded border border-edge-strong bg-inset px-2 py-1 font-mono text-xs text-fg outline-none focus:border-accent"
              >
                {processIds.length === 0 && <option value="">— none —</option>}
                {processIds.map((id) => (
                  <option key={id} value={id}>
                    {id}
                  </option>
                ))}
              </select>
            </label>
            <label className="block text-xs text-fg-faint">
              Variables (JSON)
              <textarea
                value={startVars}
                onChange={(e) => setStartVars(e.target.value)}
                rows={3}
                spellCheck={false}
                className="mt-1 w-full rounded border border-edge-strong bg-inset px-2 py-1 font-mono text-xs text-fg outline-none focus:border-accent"
              />
            </label>
            <Button
              variant="primary"
              size="sm"
              className="w-full"
              onClick={start}
              disabled={phase !== "ready" || !process}
            >
              {started ? "Start another instance" : "Start instance"}
            </Button>
          </section>

          {started && (
            <>
              {/* Waiting jobs */}
              <section className="space-y-2">
                <SectionLabel>
                  Waiting jobs ({snapshot!.jobs.length})
                </SectionLabel>
                {snapshot!.jobs.length === 0 ? (
                  <p className="text-xs text-fg-faint">No jobs are waiting.</p>
                ) : (
                  snapshot!.jobs.map((job) => (
                    <div
                      key={job.key}
                      className="rounded border border-edge bg-raised p-2"
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-mono text-xs text-fg-muted">
                          {job.elementId}
                        </span>
                        <span className="rounded bg-hover px-1.5 py-0.5 font-mono text-[10px] text-fg-muted">
                          {job.jobType}
                        </span>
                      </div>
                      <textarea
                        value={jobVars[job.key] ?? "{}"}
                        onChange={(e) =>
                          setJobVars((m) => ({
                            ...m,
                            [job.key]: e.target.value,
                          }))
                        }
                        rows={2}
                        spellCheck={false}
                        placeholder="output variables (JSON)"
                        className="mt-1.5 w-full rounded border border-edge-strong bg-inset px-2 py-1 font-mono text-[11px] text-fg outline-none focus:border-accent"
                      />
                      <div className="mt-1.5 flex gap-1.5">
                        <button
                          onClick={() => completeJob(job.key)}
                          className="flex-1 rounded border border-ok/30 bg-ok/10 px-2 py-1 text-[11px] font-medium text-ok hover:bg-ok/20"
                        >
                          Complete
                        </button>
                        <button
                          onClick={() => failJob(job.key)}
                          className="flex-1 rounded border border-danger/30 bg-danger/10 px-2 py-1 text-[11px] font-medium text-danger hover:bg-danger/20"
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
                  <SectionLabel>
                    Timers ({snapshot!.timers.length})
                  </SectionLabel>
                  {snapshot!.timers.map((t) => (
                    <div
                      key={t.key}
                      className="flex items-center justify-between rounded border border-edge bg-raised px-2 py-1 text-xs"
                    >
                      <span className="font-mono text-fg-muted">
                        {t.elementId}
                      </span>
                      <span className="text-fg-faint">
                        due in {t.dueInMs} ms
                      </span>
                    </div>
                  ))}
                </section>
              )}

              {/* Incidents */}
              {snapshot!.incidents.length > 0 && (
                <section className="space-y-2">
                  <h3 className="text-xs font-semibold uppercase tracking-wide text-danger">
                    Incidents ({snapshot!.incidents.length})
                  </h3>
                  {snapshot!.incidents.map((i) => (
                    <div
                      key={i.key}
                      className="rounded border border-danger/30 bg-danger/10 p-2 text-xs"
                    >
                      <div className="font-mono text-danger">{i.elementId}</div>
                      <div className="text-fg-muted">
                        {i.kind}: {i.reason}
                      </div>
                    </div>
                  ))}
                </section>
              )}

              {/* Clock */}
              <section className="space-y-1.5">
                <SectionLabel>Virtual clock</SectionLabel>
                <div className="text-xs text-fg-faint">
                  now = {snapshot!.now} ms
                </div>
                <div className="flex gap-1.5">
                  <input
                    value={advanceMs}
                    onChange={(e) => setAdvanceMs(e.target.value)}
                    className="w-24 rounded border border-edge-strong bg-inset px-2 py-1 font-mono text-xs text-fg outline-none focus:border-accent"
                  />
                  <button
                    onClick={advance}
                    className="flex-1 rounded border border-edge-strong bg-raised px-2 py-1 text-xs text-fg hover:bg-hover"
                  >
                    Advance time
                  </button>
                </div>
              </section>

              {/* Variables */}
              <section className="space-y-2">
                <SectionLabel>
                  Instances ({snapshot!.completedInstances}/
                  {snapshot!.totalInstances} done)
                </SectionLabel>
                {snapshot!.instances.map((inst) => (
                  <div
                    key={inst.key}
                    className="rounded border border-edge bg-raised p-2 text-xs"
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono text-fg-muted">
                        #{inst.key}
                      </span>
                      <span
                        className={
                          inst.state === "Completed"
                            ? "text-ok"
                            : inst.state === "Terminated"
                              ? "text-danger"
                              : "text-warn"
                        }
                      >
                        {inst.state}
                      </span>
                    </div>
                    <pre className="mt-1 overflow-auto whitespace-pre-wrap break-words font-mono text-[11px] text-fg-muted">
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
