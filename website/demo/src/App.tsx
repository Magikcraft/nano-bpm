import { useEffect, useMemo, useRef, useState } from "react";
import { BpmnRuntimeView, useBojtos } from "@nanobpm/bojtos-react";
import convergenceLoopBpmn from "./convergence-loop.bpmn?raw";
import { ParticleField } from "./ParticleField";
import { makeWorkers, type ReviewStep } from "./workers";

const PROCESS_ID = "convergence-loop";
const PR_NUMBER = 4821;
const PR_KEY = `pr-${PR_NUMBER}`;
const MAX_ROUNDS = 3;
const SEED = JSON.stringify({
  prNumber: PR_NUMBER,
  prKey: PR_KEY,
  round: 0,
  maxRounds: MAX_ROUNDS,
});

/** Pace of the animation, ms per token beat. */
const BEAT = 1400;
const RESTART_DELAY = 4200;

type LogKind = "submit" | "task" | "review" | "msg" | "done";
interface LogEntry {
  id: number;
  kind: LogKind;
  text: string;
}

export function App() {
  const { phase, error, processIds, snapshot, createInstance, correlateMessage, stepWorkers, reset } =
    useBojtos({ bpmn: convergenceLoopBpmn });

  const [running, setRunning] = useState(true);
  const [log, setLog] = useState<LogEntry[]>([]);

  const runningRef = useRef(running);
  runningRef.current = running;
  const processIdRef = useRef<string>(PROCESS_ID);
  processIdRef.current = processIds[0] ?? PROCESS_ID;

  const logIdRef = useRef(0);
  const counterRef = useRef({ i: 0 });

  const pushLog = (kind: LogKind, text: string) =>
    setLog((prev) => {
      const next = [...prev, { id: logIdRef.current++, kind, text }];
      return next.slice(-9);
    });

  const workers = useMemo(
    () =>
      makeWorkers(counterRef.current, (step: ReviewStep, round: number) => {
        pushLog("review", `Round ${round + 1} — ${labelFor(step)}: ${step.summary}`);
      }),
    [],
  );

  // The auto-driver: runs the real convergence loop over the wasm engine,
  // auto-publishing `review-ready` / `escalation-answered` (keyed by prKey)
  // whenever the token parks at a message-catch, exactly as a real app would.
  useEffect(() => {
    if (phase !== "ready") return;
    let cancelled = false;
    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
    const waitWhilePaused = async () => {
      while (!runningRef.current && !cancelled) await sleep(120);
    };

    (async () => {
      while (!cancelled) {
        await waitWhilePaused();
        if (cancelled) break;

        counterRef.current.i = 0;
        setLog([]);
        // Wipe the engine before each run so `completedInstances` starts at 0 —
        // otherwise prior runs' completed instances stay resident and a re-run
        // reads `completedInstances >= 1` immediately and "converges" at once.
        reset();
        pushLog("submit", `PR #${PR_NUMBER} submitted for review`);
        let snap = createInstance(processIdRef.current, SEED);
        await sleep(BEAT);

        let guard = 0;
        while (!cancelled && snap && snap.completedInstances < 1 && guard++ < 40) {
          await waitWhilePaused();
          if (cancelled) break;

          const round = await stepWorkers(workers);
          snap = round?.snapshot ?? snap;
          if (!snap) break;
          await sleep(BEAT);

          if (snap.activeElementIds.includes("wait-review")) {
            pushLog("msg", "App published message → review-ready");
            snap = correlateMessage("review-ready", PR_KEY, "{}") ?? snap;
            await sleep(BEAT);
          } else if (snap.activeElementIds.includes("wait-answer")) {
            pushLog("msg", "Human answered escalation → escalation-answered");
            snap =
              correlateMessage(
                "escalation-answered",
                PR_KEY,
                JSON.stringify({ answer: "soft-delete" }),
              ) ?? snap;
            await sleep(BEAT);
          }
        }

        if (!cancelled && snap && snap.completedInstances >= 1) {
          pushLog("done", "Converged — pull request approved ✅");
        }
        await sleep(RESTART_DELAY);
      }
    })();

    return () => {
      cancelled = true;
    };
    // Intentionally keyed on phase only: the driver reads live values via refs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [phase]);

  const activeIds = snapshot?.activeElementIds ?? [];
  const incidentIds = snapshot?.incidentElementIds ?? [];
  const vars = snapshot?.instances?.[0]?.variables ?? {};

  return (
    <div className="page">
      <ParticleField />

      <header className="topbar">
        <div className="brand">
          <span className="logo" aria-hidden>
            ◆
          </span>
          nanobpm<span className="brand-io">.io</span>
        </div>
        <nav className="topnav">
          <a href="https://github.com/Magikcraft/nano-bpm">GitHub</a>
          <a href="/schemas/">Schemas</a>
        </nav>
      </header>

      <section className="hero">
        <h1>
          A BPMN engine that runs
          <br />
          <span className="grad">right here in your browser.</span>
        </h1>
        <p className="sub">
          nanobpm is a from-scratch process orchestration engine in a single Rust binary.
          The workflow below is the real <code>urban-pr-review</code> convergence loop,
          executing live on the WebAssembly build of the very same engine — no server, no
          mocks in the runtime.
        </p>
      </section>

      <section className="demo">
        <div className="demo-head">
          <div className="demo-title">
            <span className={`pulse ${running ? "on" : ""}`} aria-hidden />
            Live: PR review convergence loop
          </div>
          <div className="controls">
            <button className="btn ghost" onClick={() => setRunning((r) => !r)}>
              {running ? "Pause" : "Play"}
            </button>
          </div>
        </div>

        <div className="demo-grid">
          <div className="diagram-wrap">
            {phase === "error" ? (
              <div className="fallback">Failed to load the engine: {error}</div>
            ) : phase === "loading" ? (
              <div className="fallback">Booting the WebAssembly engine…</div>
            ) : (
              <BpmnRuntimeView
                xml={convergenceLoopBpmn}
                activeIds={activeIds}
                incidentIds={incidentIds}
                className="diagram"
              />
            )}
          </div>

          <aside className="side">
            <div className="panel">
              <div className="panel-h">Process variables</div>
              <dl className="vars">
                {Object.entries(vars).map(([k, v]) => (
                  <div className="var" key={k}>
                    <dt>{k}</dt>
                    <dd>{format(v)}</dd>
                  </div>
                ))}
                {Object.keys(vars).length === 0 && (
                  <div className="var muted">
                    <dt>—</dt>
                    <dd>waiting for the first instance…</dd>
                  </div>
                )}
              </dl>
            </div>

            <div className="panel grow">
              <div className="panel-h">Activity</div>
              <ul className="log">
                {log.map((e) => (
                  <li key={e.id} className={`log-${e.kind}`}>
                    {e.text}
                  </li>
                ))}
              </ul>
            </div>
          </aside>
        </div>
      </section>

      <footer className="foot">
        Running <code>@nanobpm/engine-wasm</code> via the Bojtos in-browser framework.
        The reviewer here is a scripted stand-in for the real <code>senior:pr-review</code>{" "}
        agent; every token move, gateway, message and variable update is the actual engine.
      </footer>
    </div>
  );
}

function labelFor(step: ReviewStep): string {
  if (step.status === "converged") return "approved";
  if (step.status === "needs_input") return "needs input";
  return "changes requested";
}

function format(v: unknown): string {
  if (typeof v === "string") return v;
  return JSON.stringify(v);
}
