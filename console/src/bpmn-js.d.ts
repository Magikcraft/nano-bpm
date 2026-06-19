// bpmn-js ships partial types; declare the entry points the console uses.
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
