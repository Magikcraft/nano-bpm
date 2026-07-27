// bpmn-js ships no types for the `NavigatedViewer` entry point; declare the
// minimal surface `BpmnRuntimeView` uses. This ambient declaration is a build-
// time input only — it is never emitted into `dist/`, so it cannot collide with
// a consumer's own bpmn-js typings.
declare module "bpmn-js/lib/NavigatedViewer" {
  export interface ImportResult {
    warnings: unknown[];
  }
  export default class NavigatedViewer {
    constructor(options: { container: HTMLElement });
    importXML(xml: string): Promise<ImportResult>;
    get<T = unknown>(service: string): T;
    destroy(): void;
  }
}

declare module "bpmn-js/dist/assets/diagram-js.css";
declare module "bpmn-js/dist/assets/bpmn-font/css/bpmn-embedded.css";
