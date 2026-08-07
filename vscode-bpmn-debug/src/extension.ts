//! The VS Code extension host for the nanobpmn BPMN debug view (#652).
//!
//! Responsibilities:
//! - Auto-open a diagram webview when a `nanobpmn` debug session starts, and load
//!   its `.bpmn` into a `bpmn-js` viewer.
//! - Track the debug adapter and forward the custom `nanobpmn/activeElements`
//!   event (emitted by the DAP session on each pause) to the webview, which
//!   highlights the paused element(s).
//! - Click-to-breakpoint: when the user clicks an element in the diagram, toggle a
//!   VS Code `SourceBreakpoint` at that element's source line (resolved via the
//!   same BPMN source map the adapter uses), and reflect the current breakpoint
//!   set back onto the diagram.

import { readFileSync } from 'node:fs';

import { ACTIVE_ELEMENTS_EVENT, type ActiveElementsBody } from '@nanobpm/dap-adapter/events';
import { BpmnSourceMap } from '@nanobpm/dap-adapter/sourceMap';
import * as vscode from 'vscode';

import type { HostToWebview, WebviewToHost } from './protocol.js';

/** The webview + the BPMN it is showing, for the active debug session. */
interface DiagramView {
  panel: vscode.WebviewPanel;
  bpmnPath: string;
  sourceMap: BpmnSourceMap;
}

let current: DiagramView | undefined;

export function activate(context: vscode.ExtensionContext): void {
  context.subscriptions.push(
    vscode.commands.registerCommand('nanobpmn.showDiagram', () => {
      const session = vscode.debug.activeDebugSession;
      if (session?.type === 'nanobpmn') {
        openDiagram(context, session);
      } else {
        void vscode.window.showInformationMessage('No active nanobpmn debug session.');
      }
    }),
  );

  context.subscriptions.push(
    vscode.debug.onDidStartDebugSession((session) => {
      if (session.type === 'nanobpmn') {
        openDiagram(context, session);
      }
    }),
  );

  context.subscriptions.push(
    vscode.debug.onDidTerminateDebugSession((session) => {
      if (session.type === 'nanobpmn') {
        post({ type: 'highlight', elements: [] });
      }
    }),
  );

  // Forward the adapter's custom active-elements event to the webview.
  context.subscriptions.push(
    vscode.debug.registerDebugAdapterTrackerFactory('nanobpmn', {
      createDebugAdapterTracker() {
        return {
          onDidSendMessage(message: unknown) {
            if (isActiveElementsEvent(message)) {
              post({ type: 'highlight', elements: message.body.elements });
            }
          },
        };
      },
    }),
  );

  // Keep the diagram's breakpoint dots in sync with VS Code's breakpoints.
  context.subscriptions.push(
    vscode.debug.onDidChangeBreakpoints(() => pushBreakpoints()),
  );
}

export function deactivate(): void {
  current?.panel.dispose();
  current = undefined;
}

function openDiagram(context: vscode.ExtensionContext, session: vscode.DebugSession): void {
  const bpmnPath: unknown = session.configuration.bpmn;
  if (typeof bpmnPath !== 'string') {
    void vscode.window.showErrorMessage('nanobpmn: launch config has no "bpmn" path.');
    return;
  }
  let xml: string;
  try {
    xml = readFileSync(bpmnPath, 'utf8');
  } catch (err) {
    void vscode.window.showErrorMessage(`nanobpmn: cannot read ${bpmnPath}: ${String(err)}`);
    return;
  }

  if (current) {
    current.panel.reveal(vscode.ViewColumn.Beside);
  } else {
    const panel = vscode.window.createWebviewPanel(
      'nanobpmnDiagram',
      'BPMN Debug Diagram',
      vscode.ViewColumn.Beside,
      { enableScripts: true, retainContextWhenHidden: true },
    );
    panel.onDidDispose(() => {
      if (current?.panel === panel) current = undefined;
    });
    panel.webview.onDidReceiveMessage((msg: WebviewToHost) => handleWebviewMessage(msg));
    panel.webview.html = renderHtml(panel.webview, context);
    current = { panel, bpmnPath, sourceMap: new BpmnSourceMap(xml) };
  }

  current.bpmnPath = bpmnPath;
  current.sourceMap = new BpmnSourceMap(xml);
  // Load the diagram; the webview requests breakpoints via its `ready` handshake
  // once the XML is imported, so we don't push them here against a canvas that
  // isn't initialized yet.
  post({ type: 'load', xml });
}

function handleWebviewMessage(msg: WebviewToHost): void {
  if (msg.type === 'ready') {
    pushBreakpoints();
    return;
  }
  if (msg.type === 'toggleBreakpoint') {
    toggleBreakpoint(msg.element);
  }
}

/** Toggle a source breakpoint at the clicked element's line in the `.bpmn`. */
function toggleBreakpoint(element: string): void {
  const view = current;
  if (!view) return;
  const line = view.sourceMap.lineOf(element);
  if (line === undefined) return;

  const uri = vscode.Uri.file(view.bpmnPath);
  const existing = vscode.debug.breakpoints.find(
    (bp): bp is vscode.SourceBreakpoint =>
      bp instanceof vscode.SourceBreakpoint &&
      bp.location.uri.fsPath === uri.fsPath &&
      bp.location.range.start.line === line - 1,
  );
  if (existing) {
    vscode.debug.removeBreakpoints([existing]);
  } else {
    const position = new vscode.Position(line - 1, 0);
    vscode.debug.addBreakpoints([
      new vscode.SourceBreakpoint(new vscode.Location(uri, position)),
    ]);
  }
}

/** Send the element ids that currently carry a breakpoint to the webview. */
function pushBreakpoints(): void {
  const view = current;
  if (!view) return;
  const elements: string[] = [];
  for (const bp of vscode.debug.breakpoints) {
    if (
      bp instanceof vscode.SourceBreakpoint &&
      bp.location.uri.fsPath === vscode.Uri.file(view.bpmnPath).fsPath
    ) {
      const line = bp.location.range.start.line + 1;
      const element = view.sourceMap.elementAt(line);
      if (element !== undefined) elements.push(element);
    }
  }
  post({ type: 'breakpoints', elements });
}

function post(message: HostToWebview): void {
  void current?.panel.webview.postMessage(message);
}

function isActiveElementsEvent(
  message: unknown,
): message is { type: 'event'; event: string; body: ActiveElementsBody } {
  if (typeof message !== 'object' || message === null) return false;
  const m = message as Record<string, unknown>;
  if (m.type !== 'event' || m.event !== ACTIVE_ELEMENTS_EVENT) return false;
  const body = m.body;
  return (
    typeof body === 'object' &&
    body !== null &&
    Array.isArray((body as { elements?: unknown }).elements)
  );
}

function renderHtml(webview: vscode.Webview, context: vscode.ExtensionContext): string {
  const scriptUri = webview.asWebviewUri(
    vscode.Uri.joinPath(context.extensionUri, 'dist', 'webview.js'),
  );
  const csp = [
    "default-src 'none'",
    `img-src ${webview.cspSource} data:`,
    `style-src ${webview.cspSource} 'unsafe-inline'`,
    `script-src ${webview.cspSource}`,
    `font-src ${webview.cspSource}`,
  ].join('; ');
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="Content-Security-Policy" content="${csp}" />
  <style>html,body,#canvas{height:100%;margin:0;padding:0} #canvas{background:var(--vscode-editor-background)}</style>
</head>
<body>
  <div id="canvas"></div>
  <script src="${scriptUri.toString()}"></script>
</body>
</html>`;
}
