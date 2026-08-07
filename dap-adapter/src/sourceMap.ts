//! A minimal BPMN source map: resolve a `.bpmn` XML file's line numbers to the
//! flow-node element id declared on that line, and back. DAP anchors breakpoints
//! to `{source, line}`; BPMN is XML, so we map a clicked line to the element whose
//! opening tag carries the `id="…"` on (or nearest above) that line.
//!
//! This is deliberately regex-based rather than a full XML parse: it needs no
//! dependency, and a flow node's opening tag + `id` are on one line in every
//! practical exporter (bpmn-js, Camunda Modeler). Sequence flows are excluded —
//! you break on nodes, not edges. A proper element-anchored UX (click the diagram)
//! is the webview's job (#652); this is the text-editor fallback.

/** BPMN flow-node local tag names we anchor breakpoints to (not `sequenceFlow`). */
const FLOW_NODE_TAG =
  /<(?:\w+:)?((?:start|end|intermediateCatch|intermediateThrow|boundary)Event|(?:service|user|script|business ?Rule|send|receive|manual|)Task|(?:exclusive|parallel|inclusive|eventBased|complex)Gateway|callActivity|subProcess|adHocSubProcess|task)\b[^>]*?\bid="([^"]+)"/i;

export class BpmnSourceMap {
  /** 1-based line number → element id. */
  private readonly lineToId = new Map<number, string>();
  /** element id → 1-based line number. */
  private readonly idToLine = new Map<string, number>();

  constructor(xml: string) {
    const lines = xml.split(/\r?\n/);
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i] ?? '';
      const m = FLOW_NODE_TAG.exec(line);
      if (m) {
        const id = m[2];
        if (id !== undefined) {
          const lineNo = i + 1;
          this.lineToId.set(lineNo, id);
          if (!this.idToLine.has(id)) {
            this.idToLine.set(id, lineNo);
          }
        }
      }
    }
  }

  /** The element id declared on `line`, or `undefined` if that line has no node. */
  elementAt(line: number): string | undefined {
    return this.lineToId.get(line);
  }

  /** The 1-based line where `id` is declared, or `undefined`. */
  lineOf(id: string): number | undefined {
    return this.idToLine.get(id);
  }

  /** All resolvable element ids, in declaration order. */
  elementIds(): string[] {
    return [...this.idToLine.keys()];
  }
}
