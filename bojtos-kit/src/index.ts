// @nanobpm/bojtos-kit — the framework-agnostic core of the Bojtos demo
// framework (ADR 0043). Wraps the in-browser wasm engine as a single scenario
// runner and re-exports the engine's snapshot/event contract types.

export {
  ensureWasm,
  createBojtosSession,
  type BojtosSession,
  type WasmSource,
} from "./session.js";
export {
  dispatchWorkers,
  dispatchRound,
  JobFailure,
  type JobHandler,
  type JobResult,
  type AgentHandler,
  type DispatchOptions,
  type DispatchResult,
  type RoundResult,
} from "./worker.js";
export type {
  Snapshot,
  InstanceDto,
  JobDto,
  ActivatedJob,
  IncidentDto,
  TimerDto,
  ActiveEl,
  AgentActivation,
  AgentResult,
  WasmEvent,
} from "./types.js";
