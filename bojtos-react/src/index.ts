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
export {
  BpmnRuntimeView,
  type BpmnRuntimeViewProps,
} from "./BpmnRuntimeView.js";
export type {
  BojtosSession,
  Snapshot,
  InstanceDto,
  JobDto,
  IncidentDto,
  TimerDto,
  ActiveEl,
  WasmEvent,
} from "@nanobpm/bojtos-kit";
