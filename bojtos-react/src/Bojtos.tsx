import { useCallback, useEffect, useRef, useState } from "react";
import type { JobHandler, WasmEvent } from "@nanobpm/bojtos-kit";
import { BpmnRuntimeView } from "./BpmnRuntimeView.js";
import { useBojtos } from "./useBojtos.js";

/** A single engine event handed to `onTrace` as the simulation runs. */
export type TraceEvent = WasmEvent;

export interface BojtosProps {
  /** The BPMN diagram XML to run. */
  bpmn: string;
  /**
   * The in-browser workers, keyed by the model's job type (task definition
   * type). Each handler receives the activated job (with the instance's live
   * variables) and returns the variables to merge on completion — or throws to
   * fail the job. This is the code a demo author edits to shape the run.
   */
  workers: Record<string, JobHandler>;
  /** Initial instance variables (the starting payload). Defaults to `{}`. */
  seed?: Record<string, unknown>;
  /** Start the instance and run the workers automatically once ready. */
  autoplay?: boolean;
  /**
   * Milliseconds between dispatch rounds while playing (default 700). The pause
   * is what makes the token visibly hop task-to-task instead of settling
   * instantly.
   */
  stepDelayMs?: number;
  /**
   * Which deployed process to start. Defaults to the first process in the
   * diagram — set this only for a multi-process `.bpmn`.
   */
  processId?: string;
  /**
   * Optional engine wasm URL for bundlers where the default `import.meta.url`
   * loader can't resolve the binary (ADR 0043 §3).
   */
  wasmUrl?: string;
  /** Called for every engine event as the simulation advances. */
  onTrace?: (event: TraceEvent) => void;
  /** Optional class for the outer container. */
  className?: string;
}

const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

const MARKER_CSS = `
.bojtos-diagram .nano-active .djs-visual > :nth-child(1) {
  stroke: #10b981 !important;
  stroke-width: 3px !important;
}
.bojtos-diagram .nano-incident .djs-visual > :nth-child(1) {
  stroke: #ef4444 !important;
  stroke-width: 3px !important;
  fill: #fee2e2 !important;
}
`;

/**
 * The turnkey Bojtos demo component (ADR 0043 §2): drop in a `bpmn` diagram and
 * a map of in-browser `workers`, and it renders the live token/incident diagram
 * beside the running variable payload, driving the "activate → handler →
 * complete/fail" loop so you watch the token advance and the payload mutate as
 * each worker runs.
 *
 * The consuming app must load bpmn-js's diagram CSS once
 * (`bpmn-js/dist/assets/diagram-js.css` and
 * `.../bpmn-font/css/bpmn-embedded.css`); the token/incident marker styles are
 * injected here.
 */
export function Bojtos({
  bpmn,
  workers,
  seed,
  autoplay,
  stepDelayMs = 700,
  processId,
  wasmUrl,
  onTrace,
  className,
}: BojtosProps) {
  const {
    phase,
    error,
    processIds,
    snapshot,
    events,
    createInstance,
    stepWorkers,
    reset,
  } = useBojtos({ bpmn, wasm: wasmUrl });

  const [playing, setPlaying] = useState(false);
  const playingRef = useRef(false);
  const startedRef = useRef(false);
  // Tracks whether the component is still mounted, so the async play loop (which
  // can outlive an unmount while awaiting `stepWorkers()` / `delay()`) neither
  // sets state on an unmounted component nor keeps driving a freed session.
  const mountedRef = useRef(true);
  useEffect(
    () => () => {
      mountedRef.current = false;
      playingRef.current = false;
    },
    [],
  );

  // Keep the object/function props in refs so the play loop and the autoplay
  // effect don't churn (or re-fire) when a parent re-renders with fresh
  // identities for `workers` / `seed` / `onTrace`.
  const workersRef = useRef(workers);
  workersRef.current = workers;
  const seedRef = useRef(seed);
  seedRef.current = seed;
  const onTraceRef = useRef(onTrace);
  onTraceRef.current = onTrace;

  // A fresh engine (bpmn change / reset drops back to `loading`) clears the
  // "instance created" latch and stops any in-flight play loop.
  useEffect(() => {
    if (phase === "loading") {
      startedRef.current = false;
      playingRef.current = false;
      setPlaying(false);
    }
  }, [phase]);

  // Forward every newly-appended engine event to `onTrace`.
  const emittedRef = useRef(0);
  useEffect(() => {
    const cb = onTraceRef.current;
    if (cb) {
      for (let i = emittedRef.current; i < events.length; i++) cb(events[i]);
    }
    emittedRef.current = events.length;
  }, [events]);

  const ensureStarted = useCallback((): boolean => {
    if (startedRef.current) return true;
    const target = processId ?? processIds[0];
    if (!target) return false;
    // Only latch once the instance actually started — a failed createInstance
    // (returns null, e.g. an engine error) must stay retryable rather than
    // wedging the demo in a non-started state.
    if (!createInstance(target, JSON.stringify(seedRef.current ?? {}))) {
      return false;
    }
    startedRef.current = true;
    return true;
  }, [createInstance, processId, processIds]);

  const step = useCallback(async () => {
    if (phase !== "ready") return;
    if (!ensureStarted()) return;
    await stepWorkers(workersRef.current);
  }, [phase, ensureStarted, stepWorkers]);

  const play = useCallback(async () => {
    if (phase !== "ready" || playingRef.current) return;
    if (!ensureStarted()) return;
    playingRef.current = true;
    setPlaying(true);
    try {
      while (playingRef.current) {
        const round = await stepWorkers(workersRef.current);
        if (!round || round.handled === 0) break;
        await delay(stepDelayMs);
      }
    } finally {
      playingRef.current = false;
      if (mountedRef.current) setPlaying(false);
    }
  }, [phase, ensureStarted, stepWorkers, stepDelayMs]);

  const pause = useCallback(() => {
    playingRef.current = false;
    setPlaying(false);
  }, []);

  const restart = useCallback(() => {
    playingRef.current = false;
    setPlaying(false);
    startedRef.current = false;
    reset();
  }, [reset]);

  // Autoplay once, when the engine first becomes ready.
  const autoplayedRef = useRef(false);
  useEffect(() => {
    if (autoplay && phase === "ready" && !autoplayedRef.current) {
      autoplayedRef.current = true;
      void play();
    }
    if (phase === "loading") autoplayedRef.current = false;
  }, [autoplay, phase, play]);

  const ready = phase === "ready";
  const instance = snapshot?.instances[0];
  const variables = instance?.variables ?? {};

  return (
    <div
      className={className}
      style={{ display: "flex", flexDirection: "column", gap: 8, minHeight: 320 }}
    >
      <style>{MARKER_CSS}</style>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <button type="button" onClick={play} disabled={!ready || playing}>
          ▶ Play
        </button>
        <button type="button" onClick={pause} disabled={!playing}>
          ⏸ Pause
        </button>
        <button type="button" onClick={step} disabled={!ready || playing}>
          ⏭ Step
        </button>
        <button type="button" onClick={restart} disabled={!ready}>
          ↺ Reset
        </button>
        <span style={{ marginLeft: "auto", fontSize: 12, opacity: 0.7 }}>
          {error
            ? `error: ${error}`
            : phase === "loading"
              ? "loading engine…"
              : instance
                ? instance.completed
                  ? "completed"
                  : "running"
                : "ready"}
        </span>
      </div>
      <div style={{ display: "flex", gap: 8, flex: 1, minHeight: 280 }}>
        <div
          className="bojtos-diagram"
          style={{ flex: 2, border: "1px solid #e5e7eb", borderRadius: 6 }}
        >
          <BpmnRuntimeView
            xml={bpmn}
            activeIds={snapshot?.activeElementIds ?? []}
            incidentIds={snapshot?.incidentElementIds ?? []}
          />
        </div>
        <div
          style={{
            flex: 1,
            minWidth: 200,
            border: "1px solid #e5e7eb",
            borderRadius: 6,
            padding: 8,
            overflow: "auto",
            font: "12px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace",
            background: "#f9fafb",
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Variables</div>
          <pre style={{ margin: 0, whiteSpace: "pre-wrap" }}>
            {JSON.stringify(variables, null, 2)}
          </pre>
        </div>
      </div>
    </div>
  );
}
