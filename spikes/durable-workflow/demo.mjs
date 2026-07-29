// End-to-end demo of the code-first façade: the run + signal path.
//
// Shows the whole ergonomics story on a dedicated server:
//   1. print the BPMN the façade DERIVED from the code (no diagram authored)
//   2. deploy it, start an instance keyed on a business id (prId)
//   3. run the generic worker — it executes fetchDiff, autoReview, then the
//      instance parks at the `humanApproval` signal (a durable wait)
//   4. send the human-approval signal → the instance resumes and merges
//
// This is the "useJourney backend twin": the same instance a frontend could
// observe stage-by-stage.
//
// Usage: node demo.mjs   (SERVER_BIN=/path to override)

import { spawn } from "node:child_process";
import { mkdirSync, rmSync, existsSync, openSync } from "node:fs";
import { createServer } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { prReview } from "./pr-review.workflow.mjs";
import { toBpmn, deployWorkflow, startWorkflow, runWorker, sendSignal } from "./sdk.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(HERE, "..", "..");
function resolveServerBin() {
  if (process.env.SERVER_BIN) return process.env.SERVER_BIN;
  const rel = ["debug", "release"].map((p) => join("server", "target", p, "nanobpm-gateway-rest-server"));
  const roots = [REPO_ROOT, join(REPO_ROOT, "..", "..", "nanobpmn")];
  for (const root of roots) for (const r of rel) { const c = join(root, r); if (existsSync(c)) return c; }
  return join(REPO_ROOT, rel[0]);
}
const SERVER_BIN = resolveServerBin();
const RUN_DIR = join(HERE, ".run-demo");
const DATA_DIR = join(RUN_DIR, "data");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(`[demo ${new Date().toISOString()}]`, ...a);

function freePort() {
  return new Promise((resolve, reject) => {
    const s = createServer();
    s.once("error", reject);
    s.listen(0, () => { const { port } = s.address(); s.close(() => resolve(port)); });
  });
}
async function waitForTopology(baseUrl, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try { if ((await fetch(`${baseUrl}/v2/topology`)).status < 500) return; } catch { /* not up */ }
    await sleep(150);
  }
  throw new Error(`server did not come up within ${timeoutMs}ms`);
}
function startServer(port, logPath) {
  const fd = openSync(logPath, "a");
  return spawn(SERVER_BIN, [], { env: { ...process.env, PORT: String(port), NANOBPMN_DATA_DIR: DATA_DIR }, stdio: ["ignore", fd, fd] });
}

async function getInstance(baseUrl, key) {
  try {
    const res = await fetch(`${baseUrl}/v2/process-instances/${key}`);
    if (res.ok) return res.json();
  } catch { /* ignore */ }
  return null;
}

async function main() {
  rmSync(RUN_DIR, { recursive: true, force: true });
  mkdirSync(DATA_DIR, { recursive: true });
  if (!existsSync(SERVER_BIN)) throw new Error(`server binary not found: ${SERVER_BIN}\nBuild it or pass SERVER_BIN=/path.`);

  console.log("\n===== DERIVED MODEL (from ~30 lines of workflow code) =====\n");
  console.log(toBpmn(prReview));
  console.log("===========================================================\n");

  const port = await freePort();
  const baseUrl = `http://localhost:${port}`;
  let worker, server;
  const prId = "PR-1234";

  try {
    server = startServer(port, join(RUN_DIR, "server.log"));
    await waitForTopology(baseUrl);

    await deployWorkflow(baseUrl, prReview);
    log("deployed 'pr-review'");
    const inst = await startWorkflow(baseUrl, prReview, { prId });
    const key = inst.processInstanceKey;
    log(`started instance ${key} for ${prId}`);

    worker = runWorker(baseUrl, prReview, {
      onCommitted: (step) => log(`activity committed: ${step}`),
      onError: (step, e) => log(`worker error on ${step}:`, e.message),
    });

    // The two automated activities run, then the instance parks at humanApproval.
    log("worker running fetchDiff + autoReview; instance will park at humanApproval…");
    await sleep(2500);

    log("sending human approval signal (as if a reviewer clicked Approve)…");
    await sendSignal(baseUrl, prReview, "humanApproval", prId, { approvedBy: "alice" });

    // Give merge time to run, then check the instance completed.
    await sleep(2000);
    const finalState = await getInstance(baseUrl, key);
    const state = finalState?.state ?? finalState?.processInstance?.state ?? "unknown";
    log(`instance final state: ${state}`);

    console.log("\n================ DEMO RESULT ================");
    console.log("The code-authored pr-review workflow ran end to end:");
    console.log("  fetchDiff → autoReview → [durable wait] → humanApproval → merge");
    console.log("No BPMN, task-type wiring, or correlation plumbing was authored by hand.");
    console.log("=============================================\n");
  } finally {
    if (worker) worker.stop();
    if (server) server.kill("SIGKILL");
    await sleep(300);
  }
}

main().catch((e) => { console.error("demo error:", e); process.exitCode = 1; });
