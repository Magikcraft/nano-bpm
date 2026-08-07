//! Custom (non-DAP-spec) event names emitted by the nanobpmn debug session, in a
//! dependency-free module so consumers (e.g. the VS Code webview extension, #652)
//! can import the constant without pulling in the wasm engine or `@vscode/*`.

/**
 * Custom DAP event carrying the BPMN elements the run is paused on. The webview
 * subscribes to it (via a debug-adapter tracker) to highlight the paused node(s)
 * on the diagram. `elements` is empty on termination so the webview clears.
 */
export const ACTIVE_ELEMENTS_EVENT = 'nanobpmn/activeElements';

/** Body shape of {@link ACTIVE_ELEMENTS_EVENT}. */
export interface ActiveElementsBody {
  elements: string[];
}
