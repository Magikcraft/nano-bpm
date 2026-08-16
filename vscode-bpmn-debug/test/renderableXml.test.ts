import { describe, expect, it } from 'vitest';

import { hasBpmnDiagram, renderableXml } from '../src/webview/renderableXml.js';

describe('renderableXml', () => {
  it('uses the existing XML when BPMN DI is present', async () => {
    const xml = '<bpmn:definitions><bpmndi:BPMNDiagram id="Diagram_1" /></bpmn:definitions>';
    let layoutCalled = false;

    const result = await renderableXml(xml, async () => {
      layoutCalled = true;
      return '<unused />';
    });

    expect(result).toBe(xml);
    expect(layoutCalled).toBe(false);
  });

  it('auto-layouts DI-free BPMN before rendering', async () => {
    const xml = '<bpmn:definitions />';
    const laidOut = '<bpmn:definitions><bpmndi:BPMNDiagram id="Diagram_1" /></bpmn:definitions>';

    await expect(
      renderableXml(xml, async (input) => {
        expect(input).toBe(xml);
        return laidOut;
      }),
    ).resolves.toBe(laidOut);
  });

  it('falls back to the original XML when auto-layout fails', async () => {
    const xml = '<bpmn:definitions />';
    const err = new Error('layout failed');
    const warnings: Array<[string, unknown]> = [];

    await expect(
      renderableXml(
        xml,
        async () => {
          throw err;
        },
        (message, error) => warnings.push([message, error]),
      ),
    ).resolves.toBe(xml);
    expect(warnings).toEqual([
      ['nanobpmn: auto-layout failed; rendering the original BPMN XML.', err],
    ]);
  });
});

describe('hasBpmnDiagram', () => {
  it('detects namespaced and unprefixed BPMNDiagram elements', () => {
    expect(hasBpmnDiagram('<bpmndi:BPMNDiagram id="Diagram_1" />')).toBe(true);
    expect(hasBpmnDiagram('<BPMNDiagram id="Diagram_1" />')).toBe(true);
    expect(hasBpmnDiagram('<bpmn:definitions />')).toBe(false);
  });
});
