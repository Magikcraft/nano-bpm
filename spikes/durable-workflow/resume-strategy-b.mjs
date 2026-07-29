// Crash-resume proof for STRATEGY B — the replayed imperative surface.
//
// Proves the "true Temporal model" durability on nanobpmn: an ordinary
// imperative async orchestration function (with real control flow) is driven by
// REPLAY against an engine-durable journal, and survives an engine SIGKILL —
// already-recorded steps are REPLAYED (returned from the journal, NOT re-run),
// only the frontier step executes, each side effect happens exactly once.
//
// Usage: node resume-strategy-b.mjs   (SERVER_BIN=/path; NEGATIVE_CONTROL=1 to
// prove the harness detects a durability failure)

import { spawn } from "node:child_process";
import { mkdirSync, rmSync, readFileSync, existsSync, writeFileSync, appendFileSync, openSync } from "node:fs";
import { createServer } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { defineWorkflowB, deployWorkflowB, startWorkflowB, runOrchestrator } from "./sdk-b.mjs";

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
const RUN_DIR = join(HERE, ".run-b");
const DATA_DIR = join(RUN_DIR, "data");
const LEDGER = join(RUN_DIR, "ledger.log");
const PROGRESS = join(RUN_DIR, "progress.log");

const STEPS = ["stepA", "stepB", "stepC"];
const CRASH_AFTER_KEY = "2:stepB";

// The workflow authored as ONE imperative function — note the real local
// variable + `if` (control flow Strategy A can't express). Each ctx.run handler
// records a durable side effect; on replay these handlers are NOT called.
const wf = defineWorkflowB("durable-imperative", async (ctx) => {
  const a = await ctx.run("stepA", async () => {
    appendFileSync(LEDGER, "sideeffect stepA\n");
    return { n: 1 };
  });
  const b = await ctx.run("stepB", async () => {
    appendFileSync(LEDGER, "sideeffect stepB\n");
    return { n: a.n + 1 };
  });
  if (b.n === 2) {
    await ctx.run("stepC", async () => {
      appendFileSync(LEDGER, "sideeffect stepC\n");
      return { n: b.n + 1 };
    });
  }
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(`[b-driver ${new Date().toISOString()}]`, ...a);

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

  const startWorker = () =>
    runOrchestrator(baseUrl, wf, {
      onStep: async (s) => {
        const label = s.done ? "done" : s.key;
        appendFileSync(PROGRESS, `committed ${label}\n`);
        // Hold after B's journal commit so the driver can SIGKILL before C's turn.
        if (s.key === CRASH_AFTER_KEY) await sleep(6000);
      },
      onError: (e) => log("orchestrator error:", e.message),
    });

  try {
    log(`starting engine on ${baseUrl}`);
    server = startServer(port, join(RUN_DIR, "server-1.log"));
    await waitForTopology(baseUrl);

    await deployWorkflowB(baseUrl, wf);
    log("deployed looped-orchestrator model for 'durable-imperative'");
    const inst = await startWorkflowB(baseUrl, wf, { requestedBy: "strategy-b" });
    log(`started instance ${inst.processInstanceKey ?? JSON.stringify(inst)}`);

    worker = startWorker();

    await waitFor(() => progressHas("committed 1:stepA"), "A journalled", 15000);
    await waitFor(() => progressHas("committed 2:stepB"), "B journalled", 15000);
    log("A and B journalled via replay — SIGKILL the engine before C");

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
    await waitFor(() => progressHas("committed done"), "workflow completed after restart", cTimeout);
    log("workflow completed after the restart");

    await sleep(500);
    const counts = ledgerCounts();
    log(`side-effect ledger counts: ${JSON.stringify(counts)}`);

    const problems = [];
    for (const s of STEPS) if (counts[s] !== 1) problems.push(`${s} ran ${counts[s]} time(s), expected exactly 1`);

    console.log("\n============ STRATEGY B CRASH-RESUME RESULT ============");
    if (problems.length === 0) {
      console.log("PASS ✅  An IMPERATIVE orchestration function (real control flow),");
      console.log("        driven by REPLAY against an engine-durable journal, survived");
      console.log("        an engine SIGKILL: A and B were replayed (not re-run), C ran");
      console.log("        exactly once. Each side effect happened exactly once.");
      console.log("=======================================================\n");
      process.exitCode = 0;
    } else {
      console.log("FAIL ❌  Replay/durability property violated:");
      for (const p of problems) console.log("        - " + p);
      console.log("=======================================================\n");
      process.exitCode = 1;
    }
  } finally {
    if (worker) worker.stop();
    if (server) server.kill("SIGKILL");
    await sleep(300);
  }
}

main().catch((e) => { console.error("driver error:", e); process.exitCode = 1; });
