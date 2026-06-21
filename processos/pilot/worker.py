#!/usr/bin/env python3
"""ProcessOS pilot worker — serves the `pilot-evolve` job of the pilot loop.

The pilot loop (pilot-self-optimize.bpmn) is the §7.9 optimization loop authored
as a Nano process: the engine owns the durable body (iteration budget, the human
review gate, routing, the journal) and delegates one round of "propose & prove"
to this worker via a `pilot-evolve` service task.

For each job this worker:
  1. reads the round's variables (processId, baselineModel, iteration, …),
  2. calls ProcessOS `/api/harness/evolve` — which distils the target's recorded
     production traces, asks the LLM for structural redesigns, and replay-ranks
     them against the real history (the droid proposes, the engine proves),
  3. completes the job with a compact scorecard (bestName, bestConservedRate,
     proposed, the ranking summary) and the incremented iteration counter.

The human then reviews at the `Review` user task and sets `decision`
(accept / iterate / …); the engine routes from there.

This is a self-contained reference worker over the Camunda v2 REST API; it has no
dependency on engine-core. Run it alongside a `--capture` Nano cluster and a
ProcessOS server pointed at that cluster and an LLM.

Env:
  NANO_BASE_URL       default http://localhost:8080   (the Nano gateway)
  PROCESSOS_BASE_URL  default http://localhost:8090   (the ProcessOS server)
  PILOT_WORKER_NAME   default pilot-worker
  PILOT_JOB_TIMEOUT_MS default 600000  (LLM rounds are slow; keep the lock long)
  PILOT_MAX_ROUNDS    default 0 (unlimited) — a client-side safety stop
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

NANO = os.environ.get("NANO_BASE_URL", "http://localhost:8080").rstrip("/")
PROCESSOS = os.environ.get("PROCESSOS_BASE_URL", "http://localhost:8090").rstrip("/")
WORKER = os.environ.get("PILOT_WORKER_NAME", "pilot-worker")
JOB_TIMEOUT_MS = int(os.environ.get("PILOT_JOB_TIMEOUT_MS", "600000"))
MAX_ROUNDS = int(os.environ.get("PILOT_MAX_ROUNDS", "0"))
JOB_TYPE = "pilot-evolve"


def _post(base, path, body, timeout):
    req = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        txt = r.read().decode()
        return json.loads(txt) if txt.strip() else {}


def activate():
    return _post(
        NANO,
        "/v2/jobs/activation",
        {
            "type": JOB_TYPE,
            "worker": WORKER,
            "timeout": JOB_TIMEOUT_MS,
            "maxJobsToActivate": 1,
            "fetchVariable": ["processId", "baselineModel", "iteration", "maxIterations", "promptId"],
        },
        timeout=30,
    ).get("jobs", [])


def complete(job_key, variables):
    _post(NANO, f"/v2/jobs/{job_key}/completion", {"variables": variables}, timeout=30)


def fail(job_key, retries, message):
    _post(
        NANO,
        f"/v2/jobs/{job_key}/failure",
        {"retries": retries, "errorMessage": message[:500]},
        timeout=30,
    )


def run_round(job):
    v = job.get("variables", {})
    process_id = v.get("processId")
    baseline = v.get("baselineModel")
    iteration = int(v.get("iteration", 0))
    prompt_id = v.get("promptId")
    if not process_id or not baseline:
        raise ValueError("job is missing processId/baselineModel variables")

    print(f"[round {iteration}] evolving '{process_id}'"
          + (f" with prompt '{prompt_id}'" if prompt_id else "") + " …", flush=True)
    payload = {"processId": process_id, "baselineModel": baseline}
    if prompt_id:
        payload["promptId"] = prompt_id
    res = _post(
        PROCESSOS,
        "/api/harness/evolve",
        payload,
        timeout=JOB_TIMEOUT_MS / 1000.0,
    )
    ranking = res.get("ranking", {})
    cands = ranking.get("candidates", [])
    best_name = ranking.get("best")
    best_rate = None
    summary = []
    for c in cands:
        rep = c.get("report", {})
        summary.append(
            {
                "name": c.get("name"),
                "feasible": c.get("feasible"),
                "fidelityTier": c.get("fidelityTier"),
                "requiresNewWorkers": c.get("requiresNewWorkers"),
                "confidence": c.get("confidence"),
                "conservedRate": rep.get("conservedRate"),
                "avgE2eLatencyMs": rep.get("avgE2eLatencyMs"),
                "uncoveredJobTypes": rep.get("uncoveredJobTypes"),
                "divergentKeys": rep.get("divergentKeys"),
                "rationale": c.get("rationale"),
            }
        )
        if c.get("name") == best_name:
            best_rate = rep.get("conservedRate")

    out = {
        "iteration": iteration + 1,
        "proposed": res.get("proposed", len(cands)),
        "bestName": best_name,
        "bestConservedRate": best_rate,
        "rankingSummary": summary,
        "signal": res.get("signal"),
    }
    print(
        f"[round {iteration}] best='{best_name}' conservedRate={best_rate} "
        f"proposed={out['proposed']}",
        flush=True,
    )
    return out


def main():
    print(
        f"pilot worker: serving '{JOB_TYPE}' on {NANO}, evolving via {PROCESSOS}",
        flush=True,
    )
    rounds = 0
    while True:
        try:
            jobs = activate()
        except urllib.error.URLError as e:
            print(f"activation error: {e}; retrying", flush=True)
            time.sleep(2)
            continue
        if not jobs:
            time.sleep(1)
            continue
        for job in jobs:
            key = job["jobKey"]
            try:
                variables = run_round(job)
                complete(key, variables)
            except Exception as e:  # noqa: BLE001 — surface as a job failure
                print(f"round failed: {e}", flush=True)
                fail(key, max(int(job.get("retries", 1)) - 1, 0), str(e))
            rounds += 1
            if MAX_ROUNDS and rounds >= MAX_ROUNDS:
                print(f"reached PILOT_MAX_ROUNDS={MAX_ROUNDS}; exiting", flush=True)
                return


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
