declare module "dmn-js/lib/Modeler" {
  export interface ImportResult {
    warnings: unknown[];
  }
  export interface SaveResult {
    xml: string;
  }
  export interface ModelerOptions {
    container: HTMLElement;
  }
  export default class Modeler {
    constructor(options: ModelerOptions);
    importXML(xml: string): Promise<ImportResult>;
    saveXML(options?: { format?: boolean }): Promise<SaveResult>;
    getActiveViewer(): unknown;
    on(event: string, callback: (...args: unknown[]) => void): void;
    off(event: string, callback: (...args: unknown[]) => void): void;
    destroy(): void;
  }
}
