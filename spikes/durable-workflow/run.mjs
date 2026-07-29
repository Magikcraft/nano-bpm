// Durable-workflow spike — the "driver" (self-contained proof).
//
// Proves the crown-jewel durable-execution property on nanobpmn:
//
//   An in-flight workflow survives the ENGINE being killed mid-execution and
//   resumes from its durable journal WITHOUT re-running already-completed steps.
//
// It does this end to end, on a DEDICATED server instance (its own temp data
// dir + free port) so it never touches any server you already have running:
//
//   1. start engine, deploy durable-abc (A -> B -> C), create one instance
//   2. start the worker; it runs A, then B, then HOLDS after B commits
//   3. SIGKILL the engine (crash between B's commit and C's activation)
//   4. restart the engine against the SAME data dir (journal replay)
//   5. the worker reconnects on its own and runs C exactly once; instance ends
//   6. assert the side-effect ledger has EXACTLY ONE entry per activity
//      (a lost/replayed engine would re-run A and B → duplicates → FAIL)
//
// Usage:
//   node run.mjs                 # uses the prebuilt debug server binary
//   SERVER_BIN=/path node run.mjs
//
// Artifacts (data dir, ledger, progress, logs) go in ./.run and are gitignored.

import { spawn } from "node:child_process";
import { mkdirSync, rmSync, readFileSync, existsSync, writeFileSync, openSync } from "node:fs";
import { createServer } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(HERE, "..", "..");
// The spike needs a built gateway binary. Prefer an explicit SERVER_BIN, then
// this worktree's debug/release build, then a sibling main checkout's build
// (worktrees often aren't built themselves). Set SERVER_BIN to override.
function resolveServerBin() {
  if (process.env.SERVER_BIN) return process.env.SERVER_BIN;
  const rel = ["debug", "release"].map((p) =>
    join("server", "target", p, "nanobpm-gateway-rest-server"),
  );
  const roots = [REPO_ROOT, join(REPO_ROOT, "..", "..", "nanobpmn")];
  for (const root of roots) {
    for (const r of rel) {
      const cand = join(root, r);
      if (existsSync(cand)) return cand;
    }
  }
  return join(REPO_ROOT, rel[0]); // reported in the not-found error below
}
const SERVER_BIN = resolveServerBin();
const BPMN = join(HERE, "durable-abc.bpmn");
const RUN_DIR = join(HERE, ".run");
const DATA_DIR = join(RUN_DIR, "data");
const LEDGER = join(RUN_DIR, "ledger.log");
const PROGRESS = join(RUN_DIR, "progress.log");
const PROCESS_ID = "durable-abc";
const ACTIVITIES = ["act-a", "act-b", "act-c"];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(`[driver ${new Date().toISOString()}]`, ...a);

function freePort() {
  return new Promise((resolve, reject) => {
    const s = createServer();
    s.once("error", reject);
    s.listen(0, () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });
}

async function waitForTopology(baseUrl, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${baseUrl}/v2/topology`);
      if (res.status < 500) return;
    } catch {
      /* not up yet */
    }
    await sleep(150);
  }
  throw new Error(`server did not come up within ${timeoutMs}ms`);
}

function startServer(port, logPath) {
  const fd = openSync(logPath, "a");
  const proc = spawn(SERVER_BIN, [], {
    env: { ...process.env, PORT: String(port), NANOBPMN_DATA_DIR: DATA_DIR },
    stdio: ["ignore", fd, fd],
  });
  return proc;
}

async function deploy(baseUrl) {
  const bytes = readFileSync(BPMN);
  const form = new FormData();
  form.append("resources", new Blob([bytes], { type: "text/xml" }), "durable-abc.bpmn");
  const res = await fetch(`${baseUrl}/v2/deployments`, { method: "POST", body: form });
  if (!res.ok) throw new Error(`deploy failed: ${res.status} ${await res.text()}`);
  return res.json();
}

async function createInstance(baseUrl) {
  const res = await fetch(`${baseUrl}/v2/process-instances`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ processDefinitionId: PROCESS_ID }),
  });
  if (!res.ok) throw new Error(`create failed: ${res.status} ${await res.text()}`);
  return res.json();
}

function progressHas(line) {
  if (!existsSync(PROGRESS)) return false;
  return readFileSync(PROGRESS, "utf8").includes(line);
}

async function waitFor(pred, what, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (pred()) return;
    await sleep(100);
  }
  throw new Error(`timed out waiting for: ${what}`);
}

function ledgerCounts() {
  const counts = Object.fromEntries(ACTIVITIES.map((a) => [a, 0]));
  if (!existsSync(LEDGER)) return counts;
  for (const line of readFileSync(LEDGER, "utf8").split("\n")) {
    for (const a of ACTIVITIES) if (line.startsWith(`sideeffect ${a} `)) counts[a] += 1;
  }
  return counts;
}

async function main() {
  // Fresh run dir every time so the proof is deterministic.
  rmSync(RUN_DIR, { recursive: true, force: true });
  mkdirSync(DATA_DIR, { recursive: true });
  writeFileSync(LEDGER, "");
  writeFileSync(PROGRESS, "");

  if (!existsSync(SERVER_BIN)) {
    throw new Error(
      `server binary not found: ${SERVER_BIN}\n` +
        `Build it (\`make debug\` or \`cargo build -p ...\`) or pass SERVER_BIN=/path.`,
    );
  }

  const port = await freePort();
  const baseUrl = `http://localhost:${port}`;
  let worker;
  let server;

  try {
    // 1. Engine up, deploy, one instance.
    log(`starting engine on ${baseUrl} (data=${DATA_DIR})`);
    server = startServer(port, join(RUN_DIR, "server-1.log"));
    await waitForTopology(baseUrl);
    await deploy(baseUrl);
    const inst = await createInstance(baseUrl);
    log(`created instance ${inst.processInstanceKey ?? JSON.stringify(inst)}`);

    // 2. Worker runs A, then B, then holds.
    worker = spawn(process.execPath, [join(HERE, "worker.mjs")], {
      env: { ...process.env, BASE_URL: baseUrl, LEDGER, PROGRESS, CRASH_TRIGGER: "act-b", HOLD_MS: "6000" },
      stdio: ["ignore", "inherit", "inherit"],
    });
    log("worker started");

    await waitFor(() => progressHas("committed act-a"), "A committed", 15000);
    await waitFor(() => progressHas("committed act-b"), "B committed", 15000);
    log("A and B committed — now crashing the engine (SIGKILL) before C");

    // 3. Kill the engine hard, mid-workflow.
    server.kill("SIGKILL");
    await sleep(1000);

    // Negative control: wiping the journal here simulates a NON-durable engine.
    // The instance state is lost, so the workflow cannot resume — proving this
    // harness actually detects a durability failure (a PASS isn't a tautology).
    if (process.env.NEGATIVE_CONTROL) {
      rmSync(DATA_DIR, { recursive: true, force: true });
      mkdirSync(DATA_DIR, { recursive: true });
      log("NEGATIVE_CONTROL: wiped journal (simulating a non-durable engine)");
    }

    log("engine killed; restarting against the SAME data dir (journal replay)");

    // 4. Restart the engine on the same port + data dir.
    server = startServer(port, join(RUN_DIR, "server-2.log"));
    await waitForTopology(baseUrl);
    log("engine back up");

    // 5. Worker reconnects on its own and runs C.
    const cTimeout = process.env.NEGATIVE_CONTROL ? 8000 : 30000;
    await waitFor(() => progressHas("committed act-c"), "C committed after restart", cTimeout);
    log("C committed after the restart");

    // 6. Assertions.
    await sleep(500);
    const counts = ledgerCounts();
    log(`side-effect ledger counts: ${JSON.stringify(counts)}`);

    const problems = [];
    for (const a of ACTIVITIES) {
      if (counts[a] !== 1) problems.push(`${a} ran ${counts[a]} time(s), expected exactly 1`);
    }

    console.log("\n================ SPIKE RESULT ================");
    if (problems.length === 0) {
      console.log("PASS ✅  Engine crashed after B and resumed from its durable");
      console.log("        journal: A and B were NOT replayed, C ran exactly once,");
      console.log("        each side effect happened exactly once.");
      console.log("=============================================\n");
      process.exitCode = 0;
    } else {
      console.log("FAIL ❌  Durable-resume property violated:");
      for (const p of problems) console.log("        - " + p);
      console.log("=============================================\n");
      process.exitCode = 1;
    }
  } finally {
    if (worker) worker.kill("SIGTERM");
    if (server) server.kill("SIGKILL");
    await sleep(300);
  }
}

main().catch((e) => {
  console.error("driver error:", e);
  process.exitCode = 1;
});
