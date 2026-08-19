// @nanobpm/engine-testkit — the engine-agnostic `assertThat*` fluent assertion
// DSL for the Nano BPMN engine read model.
//
// Lifted out of `@nanobpm/urban-testkit` (issue Magikcraft/nano-bpm#894) so the
// DSL is reusable beyond Urban apps. Every matcher is a pure function of the
// {@link EngineReadModel} port — a minimal structural read surface (a process
// `snapshot()` plus a user-task read channel) that `@nanobpm/engine-wasm`, and
// any adapter over it (urban-testkit's `WasmEngineClient`, a bojtos-kit session,
// web-demo-framework), satisfies. There are no runtime dependencies.

export {
  type EngineReadModel,
  type UserTaskQuery,
  type UserTaskRow,
  type UserTaskState,
} from "./port.ts";
export {
  type ProcessInstanceState,
  wasmStateToProcessInstanceState,
} from "./state.ts";
export {
  assertThatInstance,
  type IncidentSelector,
  type InstanceAssert,
} from "./instance.ts";
export {
  assertThatUserTask,
  type UserTaskAssert,
  type UserTaskSelector,
} from "./user-task.ts";
export {
  byKey,
  byProcessId,
  type ByKeySelector,
  type ByProcessIdSelector,
  type InstanceRow,
  type InstanceSelector,
  readInstances,
  resolveFromInstances,
  resolveInstanceKey,
} from "./selectors.ts";
export {
  deepEqual,
  deepSubset,
  failAssertion,
  type FailOptions,
  formatValue,
  renderDiff,
} from "./format.ts";
