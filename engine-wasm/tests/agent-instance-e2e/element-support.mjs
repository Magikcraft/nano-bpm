// Package-level end-to-end guard (#1168): the advertised `sendTask` and
// `inclusiveGateway` support must actually work through the COMMITTED
// `@nanobpm/engine-wasm` artifacts — the browser/console execution surface —
// not merely the native Rust `Engine`. The native `parsed_inclusive_sendtask_exec.rs`
// integration test can pass while a stale or un-regenerated committed `pkg`
// wasm still rejects these element kinds; this probe deploys the same canonical
// diagrams (shared verbatim from `engine-core/tests/fixtures/`, so there is no
// second copy to drift) and creates instances against BOTH shipped subpath
// variants (lean + read-model), failing if either wasm cannot handle them.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const sendTask = readFileSync(new URL(
  "../../../engine-core/tests/fixtures/send-task.bpmn", import.meta.url,
), "utf8");
const inclusive = readFileSync(new URL(
  "../../../engine-core/tests/fixtures/inclusive-gateway.bpmn", import.meta.url,
), "utf8");

for (const variant of ["lean", "readmodel"]) {
  const entry = variant === "lean" ? "@nanobpm/engine-wasm" : "@nanobpm/engine-wasm/readmodel";
  const { initSync, TestEngine } = await import(entry);
  initSync({ module: readFileSync(require.resolve(
    `@nanobpm/engine-wasm/${variant}/nanobpmn_engine_bg.wasm`,
  )) });

  // sendTask: deploy, start an instance, and confirm it activates a worker job
  // exactly like a service task (the advertised sendTask execution semantics).
  {
    const engine = new TestEngine();
    engine.deploy(sendTask);
    engine.createInstance("notify", "{}");
    const jobs = JSON.parse(engine.activateJobs("notifier", 1, 1000, "W", true));
    assert.equal(jobs.length, 1, `${variant}: sendTask must activate one 'notifier' job`);
    assert.equal(jobs[0].elementId, "send", `${variant}: sendTask job carries its element id`);
  }

  // inclusiveGateway split/join: deploy and start an instance with the branch
  // condition satisfied — the committed wasm must accept the gateway kind and
  // route it (the conditional branch activates its job).
  {
    const engine = new TestEngine();
    engine.deploy(inclusive);
    engine.createInstance("review", JSON.stringify({ go: true }));
    const jobs = JSON.parse(engine.activateJobs("ta", 1, 1000, "W", true));
    assert.equal(jobs.length, 1, `${variant}: inclusiveGateway must route the conditional branch`);
  }

  console.log(`[${variant}] sendTask + inclusiveGateway deploy/createInstance OK`);
}

console.log("element-support: all variants OK");
