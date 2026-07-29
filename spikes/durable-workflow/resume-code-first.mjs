// Crash-resume PARITY proof for a CODE-AUTHORED workflow.
//
// run.mjs proved durability for a HAND-WRITTEN .bpmn. This proves a workflow
// authored purely in code via the façade (defineWorkflow → derived model +
// derived job types + generic worker) inherits the SAME durability:
//
//   engine SIGKILL'd after step B commits → restart from the journal →
//   A and B are NOT replayed, C runs exactly once.
//
// If the façade's derivation or the generic worker were wrong (e.g. a step
// re-ran, or completion didn't durably advance the instance), the side-effect
// ledger would show duplicates and this FAILS.
//
// Usage: node resume-code-first.mjs   (SERVER_BIN=/path to override)

import { spawn } from "node:child_process";
import { mkdirSync, rmSync, readFileSync, existsSync, writeFileSync, appendFileSync, openSync } from "node:fs";
import { createServer } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { defineWorkflow, deployWorkflow, startWorkflow, runWorker } from "./sdk.mjs";

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
const RUN_DIR = join(HERE, ".run-code");
const DATA_DIR = join(RUN_DIR, "data");
const LEDGER = join(RUN_DIR, "ledger.log");
const PROGRESS = join(RUN_DIR, "progress.log");

const STEPS = ["stepA", "stepB", "stepC"];
const CRASH_AFTER = "stepB";

// The workflow, authored in code. Handlers record a durable side effect BEFORE
// completion; the façade completes the job (engine commit) and then fires the
// post-commit hook. Exactly the side-effect→commit ordering run.mjs models.
const wf = defineWorkflow("durable-code", (w) => {
  for (const s of STEPS) {
    w.run(s, async () => {
      appendFileSync(LEDGER, `sideeffect ${s}\n`);
      return { [`${s}Done`]: true };
    });
  }
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(`[code-driver ${new Date().toISOString()}]`, ...a);

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
const progressHas = (line) => existsSync(PROGRESS) && readFileSync(PROGRESS, "utf8").includes(line);
async function waitFor(pred, what, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) { if (pred()) return; await sleep(100); }
  throw new Error(`timed out waiting for: ${what}`);
}
function ledgerCounts() {
  const counts = Object.fromEntries(STEPS.map((s) => [s, 0]));
  if (!existsSync(LEDGER)) return counts;
  for (const line of readFileSync(LEDGER, "utf8").split("\n"))
    for (const s of STEPS) if (line === `sideeffect ${s}`) counts[s] += 1;
  return counts;
}

async function main() {
  rmSync(RUN_DIR, { recursive: true, force: true });
  mkdirSync(DATA_DIR, { recursive: true });
  writeFileSync(LEDGER, "");
  writeFileSync(PROGRESS, "");

  if (!existsSync(SERVER_BIN)) throw new Error(`server binary not found: ${SERVER_BIN}\nBuild it or pass SERVER_BIN=/path.`);

  const port = await freePort();
  const baseUrl = `http://localhost:${port}`;
  let worker, server;

  // The generic worker runs in-process for the whole test; it reconnects on its
  // own across the engine restart (runWorker swallows transport errors). The
  // post-commit hook records progress and HOLDS after the crash-trigger step so
  // the driver has a clean window to kill the engine before the next step.
  const startWorker = () =>
    runWorker(baseUrl, wf, {
      onCommitted: async (step) => {
        appendFileSync(PROGRESS, `committed ${step}\n`);
        if (step === CRASH_AFTER) await sleep(6000);
      },
      onError: (step, e) => log(`worker error on ${step}:`, e.message),
    });

  try {
    log(`starting engine on ${baseUrl}`);
    server = startServer(port, join(RUN_DIR, "server-1.log"));
    await waitForTopology(baseUrl);

    await deployWorkflow(baseUrl, wf);
    log("deployed derived model for workflow 'durable-code'");
    const inst = await startWorkflow(baseUrl, wf, { biz: "code-first" });
    log(`started instance ${inst.processInstanceKey ?? JSON.stringify(inst)}`);

    worker = startWorker();

    await waitFor(() => progressHas("committed stepA"), "A committed", 15000);
    await waitFor(() => progressHas("committed stepB"), "B committed", 15000);
    log("A and B committed — SIGKILL the engine before C");

    server.kill("SIGKILL");
    await sleep(1000);

    if (process.env.NEGATIVE_CONTROL) {
      rmSync(DATA_DIR, { recursive: true, force: true });
      mkdirSync(DATA_DIR, { recursive: true });
      log("NEGATIVE_CONTROL: wiped journal (simulating a non-durable engine)");
    }

    log("restarting engine against the SAME data dir (journal replay)");
    server = startServer(port, join(RUN_DIR, "server-2.log"));
    await waitForTopology(baseUrl);

    const cTimeout = process.env.NEGATIVE_CONTROL ? 8000 : 30000;
    await waitFor(() => progressHas("committed stepC"), "C committed after restart", cTimeout);
    log("C committed after the restart");

    await sleep(500);
    const counts = ledgerCounts();
    log(`side-effect ledger counts: ${JSON.stringify(counts)}`);

    const problems = [];
    for (const s of STEPS) if (counts[s] !== 1) problems.push(`${s} ran ${counts[s]} time(s), expected exactly 1`);

    console.log("\n========== CODE-FIRST CRASH-RESUME RESULT ==========");
    if (problems.length === 0) {
      console.log("PASS ✅  A CODE-AUTHORED workflow (model + job types + worker");
      console.log("        all derived by the façade) survived an engine SIGKILL:");
      console.log("        A and B were NOT replayed, C ran exactly once.");
      console.log("====================================================\n");
      process.exitCode = 0;
    } else {
      console.log("FAIL ❌  Durable-resume property violated:");
      for (const p of problems) console.log("        - " + p);
      console.log("====================================================\n");
      process.exitCode = 1;
    }
  } finally {
    if (worker) worker.stop();
    if (server) server.kill("SIGKILL");
    await sleep(300);
  }
}

main().catch((e) => { console.error("driver error:", e); process.exitCode = 1; });
