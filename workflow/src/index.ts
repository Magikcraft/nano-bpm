// @nanobpm/workflow — code-first durable orchestration for nanobpmn (ADR 0044).
//
// Two authoring surfaces over the same engine durability:
//
//   defineWorkflow(id, async (ctx) => { await ctx.run(...) })   // imperative (replay)
//   defineFlow(id, (w) => { w.run(...); w.signal(...); })       // declarative (+signals)
//
// The SDK derives the BPMN model, job types, and message/correlation wiring; the
// Worker runtime hosts them; the WorkflowClient deploys, starts, and signals.
//
// Quickstart:
//
//   import { defineWorkflow, WorkflowClient, Worker } from "@nanobpm/workflow";
//
//   const wf = defineWorkflow("pr-review", async (ctx) => {
//     const diff = await ctx.run("fetchDiff", () => gh.diff(ctx.input.prId));
//     await ctx.run("merge", () => gh.merge(ctx.input.prId));
//   });
//
//   const client = new WorkflowClient({ baseUrl: "http://localhost:8080" });
//   await client.deploy(wf);
//   const worker = new Worker({ baseUrl: "http://localhost:8080", workflows: [wf] });
//   worker.start();
//   await client.start(wf, { prId: "PR-1234" });

export { defineWorkflow, imperativeToBpmn, replayOnce } from "./imperative.js";
export type { Journal, ReplayStep } from "./imperative.js";
export { defineFlow, declarativeToBpmn } from "./declarative.js";
export type { FlowBuilder } from "./declarative.js";
export { WorkflowClient, WorkflowError, toBpmn } from "./client.js";
export type { WorkflowClientOptions, ActivateOptions } from "./client.js";
export { Worker } from "./worker.js";
export type { WorkerOptions, ActivityEvent } from "./worker.js";
export type {
  Json,
  JsonObject,
  Job,
  StepHandler,
  DeclarativeFlow,
  DeclarativeStep,
  ImperativeWorkflow,
  Orchestration,
  WorkflowContext,
  Workflow,
  DeployResult,
  StartResult,
} from "./types.js";
