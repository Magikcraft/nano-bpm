import { describe, expect, it } from 'vitest';

import { BpmnSourceMap } from '../src/sourceMap.js';

const XML = [
  '<?xml version="1.0"?>', // 1
  '<bpmn:definitions>', // 2
  '  <bpmn:process id="p">', // 3
  '    <bpmn:startEvent id="s" />', // 4
  '    <bpmn:serviceTask id="first" />', // 5
  '    <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="first" />', // 6
  '    <bpmn:endEvent id="e" />', // 7
  '  </bpmn:process>', // 8
  '</bpmn:definitions>', // 9
].join('\n');

describe('BpmnSourceMap', () => {
  const map = new BpmnSourceMap(XML);

  it('maps a flow-node line to its element id', () => {
    expect(map.elementAt(4)).toBe('s');
    expect(map.elementAt(5)).toBe('first');
    expect(map.elementAt(7)).toBe('e');
  });

  it('maps an element id back to its line', () => {
    expect(map.lineOf('s')).toBe(4);
    expect(map.lineOf('first')).toBe(5);
    expect(map.lineOf('e')).toBe(7);
  });

  it('does not anchor breakpoints to sequence flows', () => {
    expect(map.elementAt(6)).toBeUndefined();
    expect(map.lineOf('a')).toBeUndefined();
  });

  it('returns undefined for lines with no flow node', () => {
    expect(map.elementAt(1)).toBeUndefined();
    expect(map.elementAt(3)).toBeUndefined();
  });

  it('lists resolvable element ids in declaration order', () => {
    expect(map.elementIds()).toEqual(['s', 'first', 'e']);
  });
});
