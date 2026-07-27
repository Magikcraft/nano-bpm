import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useCallback, useEffect, useRef, useState } from "react";
import { BpmnRuntimeView } from "./BpmnRuntimeView.js";
import { useBojtos } from "./useBojtos.js";
const delay = (ms) => new Promise((r) => setTimeout(r, ms));
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
export function Bojtos({ bpmn, workers, seed, autoplay, stepDelayMs = 700, processId, wasmUrl, onTrace, className, }) {
    const { phase, error, processIds, snapshot, events, createInstance, stepWorkers, reset, } = useBojtos({ bpmn, wasm: wasmUrl });
    const [playing, setPlaying] = useState(false);
    const playingRef = useRef(false);
    const startedRef = useRef(false);
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
            for (let i = emittedRef.current; i < events.length; i++)
                cb(events[i]);
        }
        emittedRef.current = events.length;
    }, [events]);
    const ensureStarted = useCallback(() => {
        if (startedRef.current)
            return true;
        const target = processId ?? processIds[0];
        if (!target)
            return false;
        createInstance(target, JSON.stringify(seedRef.current ?? {}));
        startedRef.current = true;
        return true;
    }, [createInstance, processId, processIds]);
    const step = useCallback(async () => {
        if (phase !== "ready")
            return;
        if (!ensureStarted())
            return;
        await stepWorkers(workersRef.current);
    }, [phase, ensureStarted, stepWorkers]);
    const play = useCallback(async () => {
        if (phase !== "ready" || playingRef.current)
            return;
        if (!ensureStarted())
            return;
        playingRef.current = true;
        setPlaying(true);
        try {
            while (playingRef.current) {
                const round = await stepWorkers(workersRef.current);
                if (!round || round.handled === 0)
                    break;
                await delay(stepDelayMs);
            }
        }
        finally {
            playingRef.current = false;
            setPlaying(false);
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
        if (phase === "loading")
            autoplayedRef.current = false;
    }, [autoplay, phase, play]);
    const ready = phase === "ready";
    const instance = snapshot?.instances[0];
    const variables = instance?.variables ?? {};
    return (_jsxs("div", { className: className, style: { display: "flex", flexDirection: "column", gap: 8, minHeight: 320 }, children: [_jsx("style", { children: MARKER_CSS }), _jsxs("div", { style: { display: "flex", alignItems: "center", gap: 8 }, children: [_jsx("button", { type: "button", onClick: play, disabled: !ready || playing, children: "\u25B6 Play" }), _jsx("button", { type: "button", onClick: pause, disabled: !playing, children: "\u23F8 Pause" }), _jsx("button", { type: "button", onClick: step, disabled: !ready || playing, children: "\u23ED Step" }), _jsx("button", { type: "button", onClick: restart, disabled: !ready, children: "\u21BA Reset" }), _jsxs("span", { style: { marginLeft: "auto", fontSize: 12, opacity: 0.7 }, children: [phase === "loading" && "loading engine…", phase === "error" && `error: ${error ?? "unknown"}`, ready &&
                                (instance
                                    ? instance.completed
                                        ? "completed"
                                        : "running"
                                    : "ready")] })] }), _jsxs("div", { style: { display: "flex", gap: 8, flex: 1, minHeight: 280 }, children: [_jsx("div", { className: "bojtos-diagram", style: { flex: 2, border: "1px solid #e5e7eb", borderRadius: 6 }, children: _jsx(BpmnRuntimeView, { xml: bpmn, activeIds: snapshot?.activeElementIds ?? [], incidentIds: snapshot?.incidentElementIds ?? [] }) }), _jsxs("div", { style: {
                            flex: 1,
                            minWidth: 200,
                            border: "1px solid #e5e7eb",
                            borderRadius: 6,
                            padding: 8,
                            overflow: "auto",
                            font: "12px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace",
                            background: "#f9fafb",
                        }, children: [_jsx("div", { style: { fontWeight: 600, marginBottom: 4 }, children: "Variables" }), _jsx("pre", { style: { margin: 0, whiteSpace: "pre-wrap" }, children: JSON.stringify(variables, null, 2) })] })] })] }));
}
