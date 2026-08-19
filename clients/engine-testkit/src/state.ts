// The transport-agnostic process-instance lifecycle state and the mapping from
// the engine's raw snapshot `state` string onto it. Lifted verbatim from
// urban-testkit's `wasm-engine.ts` (the only two exports the DSL needs) so the
// matchers do not depend on the Urban `EngineClient` adapter — issue #894.

/** A process instance's externally-visible lifecycle state — the small set the
 *  instance matchers key on. Mirrors the engine's REST projection. */
export type ProcessInstanceState = "ACTIVE" | "COMPLETED" | "TERMINATED";

/** Map the engine's process-instance `state` string onto the transport-agnostic
 *  {@link ProcessInstanceState}, mirroring the engine's REST projection: the
 *  transient `Terminating` drain state has already discarded its tokens and is on
 *  its way to `Terminated`, so it projects as `TERMINATED` externally (parity
 *  with `process_instance_state_enum` in the engine server). Case-insensitive —
 *  the snapshot spells the state mixed-case (e.g. "Active"). Returns `undefined`
 *  for an unrecognized value, which callers skip. */
export function wasmStateToProcessInstanceState(
  raw: unknown,
): ProcessInstanceState | undefined {
  if (typeof raw !== "string") return undefined;
  switch (raw.toUpperCase()) {
    case "ACTIVE":
      return "ACTIVE";
    case "COMPLETED":
      return "COMPLETED";
    case "TERMINATED":
    case "TERMINATING":
      return "TERMINATED";
    default:
      return undefined;
  }
}
