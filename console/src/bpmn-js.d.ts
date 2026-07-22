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
  export interface ModelerOptions {
    container: HTMLElement;
    propertiesPanel?: { parent: HTMLElement };
    additionalModules?: unknown[];
    moddleExtensions?: Record<string, unknown>;
  }
  export default class Modeler {
    constructor(options: ModelerOptions);
    importXML(xml: string): Promise<ImportResult>;
    createDiagram(): Promise<ImportResult>;
    saveXML(options?: { format?: boolean }): Promise<SaveResult>;
    get<T = unknown>(service: string): T;
    on(event: string, callback: (...args: unknown[]) => void): void;
    off(event: string, callback: (...args: unknown[]) => void): void;
    destroy(): void;
  }
}

declare module "bpmn-js-properties-panel" {
  const BpmnPropertiesPanelModule: unknown;
  const BpmnPropertiesProviderModule: unknown;
  const ZeebePropertiesProviderModule: unknown;
  const CamundaPlatformPropertiesProviderModule: unknown;
  export {
    BpmnPropertiesPanelModule,
    BpmnPropertiesProviderModule,
    ZeebePropertiesProviderModule,
    CamundaPlatformPropertiesProviderModule,
  };
}

declare module "zeebe-bpmn-moddle/resources/zeebe.json" {
  const value: Record<string, unknown>;
  export default value;
}

declare module "bpmn-js-element-templates" {
  /** ElementTemplates service (didi id `elementTemplates`). */
  export interface ElementTemplatesService {
    set(templates: unknown[]): void;
    get(): unknown[];
    createElement(template: unknown, options?: Record<string, unknown>): unknown;
    applyTemplate(element: unknown, template: unknown, options?: Record<string, unknown>): unknown;
  }
  const CloudElementTemplatesCoreModule: unknown;
  const CloudElementTemplatesPropertiesProviderModule: unknown;
  export { CloudElementTemplatesCoreModule, CloudElementTemplatesPropertiesProviderModule };
}

declare module "camunda-bpmn-js-behaviors/lib/camunda-cloud" {
  const mod: unknown;
  export default mod;
}
