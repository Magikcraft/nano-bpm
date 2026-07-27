import { useCallback, useEffect, useRef, useState } from "react";
import {
  createBojtosSession,
  type BojtosSession,
  type Snapshot,
  type WasmEvent,
} from "@nanobpm/bojtos-kit";

/** Lifecycle of the in-browser engine load. */
export type BojtosPhase = "loading" | "ready" | "error";

export interface UseBojtosOptions {
  /** The BPMN diagram XML to deploy. Re-deploys on a fresh engine when it changes. */
  bpmn: string;
}

export interface BojtosControls {
  phase: BojtosPhase;
  error: string | null;
  /** Deployable process ids from the current deployment. */
  processIds: string[];
  /** The latest snapshot, or `null` before the first command / after a reset. */
  snapshot: Snapshot | null;
  /** The engine's full event log after the latest command. */
  events: WasmEvent[];
  /** Start an instance; returns the post-run snapshot (with `created`) or null. */
  createInstance(processId: string, variablesJson: string): Snapshot | null;
  /** Complete a waiting job, merging output variables. */
  completeJob(jobKey: string, variablesJson: string): Snapshot | null;
  /** Fail a waiting job (raises an incident with no retries left). */
  failJob(jobKey: string, retries: number, message: string): Snapshot | null;
  /** Advance the virtual clock. */
  advanceTime(byMs: number): Snapshot | null;
  /** Re-deploy the diagram on the existing engine, clearing run state. */
  reset(): void;
}

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
export function useBojtos({ bpmn }: UseBojtosOptions): BojtosControls {
  const sessionRef = useRef<BojtosSession | null>(null);
  const [phase, setPhase] = useState<BojtosPhase>("loading");
  const [error, setError] = useState<string | null>(null);
  const [processIds, setProcessIds] = useState<string[]>([]);
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [events, setEvents] = useState<WasmEvent[]>([]);

  const deployInto = useCallback(
    (session: BojtosSession) => {
      const res = session.deploy(bpmn);
      setProcessIds(res.processIds);
      setSnapshot(null);
      setEvents([]);
      setError(null);
    },
    [bpmn],
  );

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
        if (cancelled) return;
        setError(String(e));
        setPhase("error");
      });
    return () => {
      cancelled = true;
      sessionRef.current?.free();
      sessionRef.current = null;
    };
  }, [deployInto]);

  const run = useCallback(
    (fn: (s: BojtosSession) => Snapshot): Snapshot | null => {
      const session = sessionRef.current;
      if (!session) return null;
      try {
        const snap = fn(session);
        setSnapshot(snap);
        setEvents(session.events());
        setError(null);
        return snap;
      } catch (e) {
        setError(String(e));
        return null;
      }
    },
    [],
  );

  const createInstance = useCallback(
    (processId: string, variablesJson: string) =>
      run((s) => s.createInstance(processId, variablesJson)),
    [run],
  );
  const completeJob = useCallback(
    (jobKey: string, variablesJson: string) =>
      run((s) => s.completeJob(jobKey, variablesJson)),
    [run],
  );
  const failJob = useCallback(
    (jobKey: string, retries: number, message: string) =>
      run((s) => s.failJob(jobKey, retries, message)),
    [run],
  );
  const advanceTime = useCallback(
    (byMs: number) => run((s) => s.advanceTime(byMs)),
    [run],
  );

  const reset = useCallback(() => {
    const session = sessionRef.current;
    if (!session) return;
    try {
      deployInto(session);
    } catch (e) {
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
