// @nanobpm/bojtos-kit — the framework-agnostic core of the Bojtos demo
// framework (ADR 0043). Wraps the in-browser wasm engine as a single scenario
// runner and re-exports the engine's snapshot/event contract types.

export {
  ensureWasm,
  createBojtosSession,
  type BojtosSession,
} from "./session.js";
export type {
  Snapshot,
  InstanceDto,
  JobDto,
  IncidentDto,
  TimerDto,
  ActiveEl,
  WasmEvent,
} from "./types.js";
