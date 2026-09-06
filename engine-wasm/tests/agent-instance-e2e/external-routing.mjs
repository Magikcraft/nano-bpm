import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const model = readFileSync(
  new URL("../../../engine-core/tests/fixtures/external-agent-job-type.bpmn", import.meta.url),
  "utf8",
);

for (const variant of ["lean", "readmodel"]) {
  const entrypoint = variant === "lean" ? "@nanobpm/engine-wasm" : "@nanobpm/engine-wasm/readmodel";
  const { initSync, TestEngine } = await import(entrypoint);
  initSync({
    module: readFileSync(require.resolve(`@nanobpm/engine-wasm/${variant}/nanobpmn_engine_bg.wasm`)),
  });
  for (const type of ["senior:rebase", "= localRoute", null]) {
    const engine = new TestEngine();
    try {
      const xml = model.replace(
        '<zeebe:taskDefinition type="senior:rebase"/>',
        type === null ? "" : `<zeebe:taskDefinition type="${type}"/>`,
      );
      engine.deploy(xml);
      engine.createInstance("external-agent-routing", '{"route":"senior:rebase"}');
      const expected = type === null ? "agent" : "senior:rebase";
      if (type !== null) {
        assert.deepEqual(JSON.parse(engine.activateJobs("agent", 1, 1000, "W")), []);
      }
      const jobs = JSON.parse(engine.activateJobs(expected, 1, 1000, "W"));
      assert.equal(jobs.length, 1, `${variant}: ${type ?? "element-id fallback"}`);
      const job = jobs[0];
      assert.equal(job.type, expected);
      assert.equal(job.elementId, "agent");
      assert.equal(typeof job.jobLease, "string");
      const request = {
        elementInstanceKey: job.elementInstanceKey,
        jobKey: job.key,
        jobLease: job.jobLease,
      };
      assert.throws(() => engine.createAgentInstance(JSON.stringify({
        ...request,
        jobLease: String(BigInt(job.jobLease) + 1n),
      })));
      engine.createAgentInstance(JSON.stringify(request));
      if (variant === "readmodel") {
        const agents = JSON.parse(engine.searchAgentInstances("{}")).items;
        assert.equal(agents.length, 1);
        assert.equal(agents[0].elementInstanceKey, job.elementInstanceKey);
      }
      const completed = JSON.parse(engine.completeJob(job.key, "{}"));
      assert.equal(completed.instances[0].state, "Completed");
    } finally {
      engine.free();
    }
  }
}
console.log("External agent routing passed for both WASM entrypoints.");
