// @nanobpm/bojtos-react — the React binding for the Bojtos in-browser BPMN demo
// framework (ADR 0043). `useBojtos` owns the engine session and reactive state;
// `<BpmnRuntimeView>` renders the live token/incident diagram. The engine's
// snapshot/event contract types are re-exported from @nanobpm/bojtos-kit for
// convenience.

export {
  useBojtos,
  type UseBojtosOptions,
  type BojtosControls,
  type BojtosPhase,
} from "./useBojtos.js";
export { Bojtos, type BojtosProps, type TraceEvent } from "./Bojtos.js";
export {
  OrderFulfillmentDemo,
  ORDER_FULFILLMENT_BPMN,
  orderFulfillmentWorkers,
} from "./examples/orderFulfillment.js";
export {
  BpmnRuntimeView,
  type BpmnRuntimeViewProps,
} from "./BpmnRuntimeView.js";
export {
  JobFailure,
  type JobHandler,
  type JobResult,
  type AgentHandler,
  type DispatchOptions,
  type DispatchResult,
  type RoundResult,
} from "@nanobpm/bojtos-kit";
export type {
  BojtosSession,
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
} from "@nanobpm/bojtos-kit";
