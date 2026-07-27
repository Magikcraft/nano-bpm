import { useCallback, useEffect, useRef, useState } from "react";
import { createBojtosSession, } from "@nanobpm/bojtos-kit";
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
export function useBojtos({ bpmn }) {
    const sessionRef = useRef(null);
    const [phase, setPhase] = useState("loading");
    const [error, setError] = useState(null);
    const [processIds, setProcessIds] = useState([]);
    const [snapshot, setSnapshot] = useState(null);
    const [events, setEvents] = useState([]);
    const deployInto = useCallback((session) => {
        const res = session.deploy(bpmn);
        setProcessIds(res.processIds);
        setSnapshot(null);
        setEvents([]);
        setError(null);
    }, [bpmn]);
    useEffect(() => {
        let cancelled = false;
        createBojtosSession()
            .then((session) => {
            if (cancelled) {
                session.free();
                return;
            }
            sessionRef.current = session;
            deployInto(session);
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
        reset,
    };
}
