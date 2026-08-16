import { layoutProcess } from 'bpmn-auto-layout';

type LayoutProcess = (xml: string) => Promise<string>;
type Warn = (message: string, error: unknown) => void;

export async function renderableXml(
  xml: string,
  layout: LayoutProcess = layoutProcess,
  warn: Warn = console.warn,
): Promise<string> {
  if (hasBpmnDiagram(xml)) return xml;
  try {
    return await layout(xml);
  } catch (err: unknown) {
    warn('nanobpmn: auto-layout failed; rendering the original BPMN XML.', err);
    return xml;
  }
}

export function hasBpmnDiagram(xml: string): boolean {
  return /<[^<\s:]+:BPMNDiagram\b|<BPMNDiagram\b/.test(xml);
}
