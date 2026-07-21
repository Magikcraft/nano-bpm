// Ambient module declarations for the moddle parsers, which ship no types.
// We only use a thin slice of each: the constructor + async fromXML(). The
// parsed tree is walked as generic nodes (see symbol-index.ts).

declare module "bpmn-moddle" {
  export interface ParseResult {
    rootElement: any;
    references?: unknown[];
    warnings?: unknown[];
    elementsById?: Record<string, unknown>;
  }
  export default class BpmnModdle {
    constructor(packages?: Record<string, unknown>);
    fromXML(xml: string, typeName?: string): Promise<ParseResult>;
  }
}

declare module "dmn-moddle" {
  export interface ParseResult {
    rootElement: any;
  }
  export default class DmnModdle {
    constructor(packages?: Record<string, unknown>);
    fromXML(xml: string, typeName?: string): Promise<ParseResult>;
  }
}

declare module "zeebe-bpmn-moddle/resources/zeebe.json" {
  const descriptor: Record<string, unknown>;
  export default descriptor;
}
