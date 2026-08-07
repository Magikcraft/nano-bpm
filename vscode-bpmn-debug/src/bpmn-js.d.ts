// bpmn-js ships partial types; declare the entry point the webview uses, plus the
// CSS assets esbuild inlines as text (see build.mjs `loader: { '.css': 'text' }`).
declare module 'bpmn-js/lib/NavigatedViewer' {
  export interface ImportResult {
    warnings: unknown[];
  }
  export default class NavigatedViewer {
    constructor(options: { container: string | HTMLElement });
    importXML(xml: string): Promise<ImportResult>;
    get<T = unknown>(service: string): T;
    destroy(): void;
  }
}

declare module 'bpmn-js/dist/assets/diagram-js.css' {
  const css: string;
  export default css;
}
declare module 'bpmn-js/dist/assets/bpmn-font/css/bpmn-embedded.css' {
  const css: string;
  export default css;
}
