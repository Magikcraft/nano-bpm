// Minimal ambient types for `bpmn-auto-layout` (1.3.0 ships none). `layoutProcess`
// adds diagram interchange (DI) to a BPMN XML string. The resolved shape changed
// across versions: 1.3.x resolves to the laid-out XML string; >= 1.4 / `main`
// resolves to `{ xml, warnings }`. We type the union and normalize at the call
// site (see `layout.ts`).
declare module "bpmn-auto-layout" {
  export function layoutProcess(
    xml: string,
  ): Promise<string | { xml: string; warnings?: unknown[] }>;
}
