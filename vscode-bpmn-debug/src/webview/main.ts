//! The webview client (runs inside the VS Code webview iframe). Renders the BPMN
//! with a read-only `bpmn-js` viewer, highlights the element(s) the debugger is
//! paused on, shows breakpoint markers, and turns an element click into a
//! toggle-breakpoint message back to the extension host.

import NavigatedViewer from 'bpmn-js/lib/NavigatedViewer';
// esbuild bundles these as text (see build.mjs) so we can inject them into the
// webview iframe, which cannot load diagram-js's assets by URL under the CSP.
import diagramCss from 'bpmn-js/dist/assets/diagram-js.css';
import bpmnFontCss from 'bpmn-js/dist/assets/bpmn-font/css/bpmn-embedded.css';

import { markerDelta } from '../protocol.js';
import type { HostToWebview, WebviewToHost } from '../protocol.js';
import { renderableXml } from './renderableXml.js';

const ACTIVE_MARKER = 'nano-active';
const BREAKPOINT_MARKER = 'nano-breakpoint';

interface Canvas {
  zoom(mode: string): void;
  addMarker(elementId: string, marker: string): void;
  removeMarker(elementId: string, marker: string): void;
}
interface EventBus {
  on(event: string, cb: (e: { element: { id: string; type: string } }) => void): void;
}
interface VsCodeApi {
  postMessage(msg: WebviewToHost): void;
}
declare function acquireVsCodeApi(): VsCodeApi;

const vscodeApi = acquireVsCodeApi();

function injectCss(): void {
  const style = document.createElement('style');
  style.textContent = `${diagramCss}\n${bpmnFontCss}\n` +
    `.${ACTIVE_MARKER} .djs-visual > :nth-child(1){stroke:#e08a00 !important;stroke-width:3px !important;fill:#ffe9c2 !important;}` +
    `.${BREAKPOINT_MARKER} .djs-visual > :nth-child(1){stroke:#e51400 !important;stroke-width:2px !important;}`;
  document.head.appendChild(style);
}

const viewer = new NavigatedViewer({ container: '#canvas' });
let activeHighlight: string[] = [];
let breakpointHighlight: string[] = [];

function applyMarkers(next: string[], marker: string, prevRef: { ids: string[] }): void {
  const canvas = viewer.get<Canvas>('canvas');
  const { add, remove } = markerDelta(prevRef.ids, next);
  for (const id of remove) safe(() => canvas.removeMarker(id, marker));
  for (const id of add) safe(() => canvas.addMarker(id, marker));
  prevRef.ids = next;
}

function safe(fn: () => void): void {
  try {
    fn();
  } catch {
    /* element may not exist in this diagram */
  }
}

async function load(xml: string): Promise<void> {
  await viewer.importXML(await renderableXml(xml));
  const canvas = viewer.get<Canvas>('canvas');
  canvas.zoom('fit-viewport');
  // Reset the highlight state for the freshly loaded diagram.
  activeHighlight = [];
  breakpointHighlight = [];
  activeRef.ids = activeHighlight;
  bpRef.ids = breakpointHighlight;
}

// Register the click handler once: the viewer (and its eventBus) persists across
// diagram loads, so registering inside load() would accumulate handlers and fire
// the toggle multiple times per click.
const bus = viewer.get<EventBus>('eventBus');
bus.on('element.click', (e) => {
  // Ignore the diagram root / connections; break on flow nodes only.
  if (e.element.id && e.element.type !== 'bpmn:Process' && !e.element.type.includes('SequenceFlow')) {
    vscodeApi.postMessage({ type: 'toggleBreakpoint', element: e.element.id });
  }
});

const activeRef = { ids: activeHighlight };
const bpRef = { ids: breakpointHighlight };

window.addEventListener('message', (event: MessageEvent<HostToWebview>) => {
  const msg = event.data;
  switch (msg.type) {
    case 'load':
      void load(msg.xml).then(() => vscodeApi.postMessage({ type: 'ready' }));
      break;
    case 'highlight':
      applyMarkers(msg.elements, ACTIVE_MARKER, activeRef);
      break;
    case 'breakpoints':
      applyMarkers(msg.elements, BREAKPOINT_MARKER, bpRef);
      break;
  }
});

injectCss();
