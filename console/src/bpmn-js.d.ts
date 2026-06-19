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

declare module "bpmn-js/lib/Modeler" {
  export interface ImportResult {
    warnings: unknown[];
  }
  export interface SaveResult {
    xml: string;
  }
  export default class Modeler {
    constructor(options: { container: HTMLElement });
    importXML(xml: string): Promise<ImportResult>;
    createDiagram(): Promise<ImportResult>;
    saveXML(options?: { format?: boolean }): Promise<SaveResult>;
    get<T = unknown>(service: string): T;
    on(event: string, callback: (...args: unknown[]) => void): void;
    off(event: string, callback: (...args: unknown[]) => void): void;
    destroy(): void;
  }
}
