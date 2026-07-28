import { useCallback, useEffect, useRef, useState } from "react";
import { createBojtosSession, dispatchRound, dispatchWorkers, } from "@nanobpm/bojtos-kit";
/**
 * React binding over a headless {@link BojtosSession}: owns the engine's
 * lifecycle and the reactive `snapshot` / `events` / `processIds` state, and
 * exposes the engine commands. The consuming component owns its own form state
 * (selected process, seed vars, per-job output) and drives the visual contract
 * (`<BpmnRuntimeView>` + the variable payload) off `snapshot`.
 *
 * This is the reactive half of the Bojtos public API (ADR 0043 §2); the console
 * test-run panel is its first consumer (§8 step 2 — dogfooding is the acceptance
 * test).
 */
export function useBojtos({ bpmn, wasm }) {
    const sessionRef = useRef(null);
    const [phase, setPhase] = useState("loading");
    const [error, setError] = useState(null);
    const [processIds, setProcessIds] = useState([]);
    const [snapshot, setSnapshot] = useState(null);
    const [events, setEvents] = useState([]);
    // The wasm source is an init-time concern (the first `ensureWasm` wins), so
    // keep it in a ref rather than the mount effect's deps — a fresh URL/bytes
    // identity each render must not re-create the session.
    const wasmRef = useRef(wasm);
    wasmRef.current = wasm;
    const deployInto = useCallback((session) => {
        const res = session.deploy(bpmn);
        setProcessIds(res.processIds);
        setSnapshot(null);
        setEvents([]);
        setError(null);
    }, [bpmn]);
    useEffect(() => {
        let cancelled = false;
        // A new diagram means a fresh engine: drop back to `loading` and clear the
        // previous session's state — including `processIds` — so consumers never
        // see `ready` (or a stale process list) against a freed session while the
        // new one is still loading.
        setPhase("loading");
        setProcessIds([]);
        setSnapshot(null);
        setEvents([]);
        setError(null);
        createBojtosSession({ wasm: wasmRef.current })
            .then((session) => {
            if (cancelled) {
                session.free();
                return;
            }
            try {
                deployInto(session);
            }
            catch (e) {
                // A failed deploy (e.g. invalid BPMN) must free the just-created
                // engine rather than leak it until unmount, and must not be stored
                // as the active session.
                session.free();
                setError(String(e));
                setPhase("error");
                return;
            }
            sessionRef.current = session;
            setPhase("ready");
        })
            .catch((e) => {
            if (cancelled)
                return;
            setError(String(e));
            setPhase("error");
        });
        return () => {
            cancelled = true;
            sessionRef.current?.free();
            sessionRef.current = null;
        };
    }, [deployInto]);
    const run = useCallback((fn) => {
        const session = sessionRef.current;
        if (!session)
            return null;
        try {
            const snap = fn(session);
            setSnapshot(snap);
            setEvents(session.events());
            setError(null);
            return snap;
        }
        catch (e) {
            setError(String(e));
            return null;
        }
    }, []);
    const createInstance = useCallback((processId, variablesJson) => run((s) => s.createInstance(processId, variablesJson)), [run]);
    const completeJob = useCallback((jobKey, variablesJson) => run((s) => s.completeJob(jobKey, variablesJson)), [run]);
    const failJob = useCallback((jobKey, retries, message) => run((s) => s.failJob(jobKey, retries, message)), [run]);
    const advanceTime = useCallback((byMs) => run((s) => s.advanceTime(byMs)), [run]);
    const runWorkers = useCallback(async (workers, opts) => {
        const session = sessionRef.current;
        if (!session)
            return null;
        try {
            const { snapshot: settled } = await dispatchWorkers(session, workers, opts);
            // The session may have been torn down/replaced (bpmn change, unmount)
            // while we awaited — don't publish stale state or read a freed session.
            if (sessionRef.current !== session)
                return null;
            setSnapshot(settled);
            setEvents(session.events());
            setError(null);
            return settled;
        }
        catch (e) {
            if (sessionRef.current !== session)
                return null;
            // Reflect whatever state the engine reached before the drain aborted
            // (e.g. the maxRounds guard) so the view isn't left stale.
            setSnapshot(session.snapshot());
            setEvents(session.events());
            setError(String(e));
            return null;
        }
    }, []);
    const stepWorkers = useCallback(async (workers, opts) => {
        const session = sessionRef.current;
        if (!session)
            return null;
        try {
            const round = await dispatchRound(session, workers, opts);
            // Bail if the session was replaced/freed while we awaited the round.
            if (sessionRef.current !== session)
                return null;
            setSnapshot(round.snapshot);
            setEvents(session.events());
            setError(null);
            return round;
        }
        catch (e) {
            if (sessionRef.current !== session)
                return null;
            setSnapshot(session.snapshot());
            setEvents(session.events());
            setError(String(e));
            return null;
        }
    }, []);
    const reset = useCallback(() => {
        const session = sessionRef.current;
        if (!session)
            return;
        try {
            deployInto(session);
        }
        catch (e) {
            setError(String(e));
        }
    }, [deployInto]);
    return {
        phase,
        error,
        processIds,
        snapshot,
        events,
        createInstance,
        completeJob,
        failJob,
        advanceTime,
        runWorkers,
        stepWorkers,
        reset,
    };
}
