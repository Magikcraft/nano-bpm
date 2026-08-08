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
import { NanobpmnDebugSession } from '@nanobpm/dap-adapter/session';
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
    vscode.debug.registerDebugAdapterDescriptorFactory('nanobpmn', {
      createDebugAdapterDescriptor() {
        return new vscode.DebugAdapterInlineImplementation(new NanobpmnDebugSession());
      },
    }),
  );

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
    vscode.commands.registerCommand('nanobpmn.createDebugSample', () => {
      void createDebugSample().catch((err: unknown) => {
        void vscode.window.showErrorMessage(
          `nanobpmn: could not create debug sample: ${formatError(err)}`,
        );
      });
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

async function createDebugSample(): Promise<void> {
  const folder = vscode.workspace.workspaceFolders?.[0];
  if (folder === undefined) {
    void vscode.window.showErrorMessage('nanobpmn: open a workspace folder before creating a sample.');
    return;
  }

  const bpmnUri = vscode.Uri.joinPath(folder.uri, 'nanobpmn-debug-sample.bpmn');
  const vscodeDir = vscode.Uri.joinPath(folder.uri, '.vscode');
  const launchUri = vscode.Uri.joinPath(vscodeDir, 'launch.json');
  const encoder = new TextEncoder();
  const created: string[] = [];
  const skipped: string[] = [];

  if (await exists(bpmnUri)) {
    skipped.push(vscode.workspace.asRelativePath(bpmnUri));
  } else {
    await vscode.workspace.fs.writeFile(bpmnUri, encoder.encode(SAMPLE_BPMN));
    created.push(vscode.workspace.asRelativePath(bpmnUri));
  }

  await vscode.workspace.fs.createDirectory(vscodeDir);
  if (await exists(launchUri)) {
    skipped.push(vscode.workspace.asRelativePath(launchUri));
  } else {
    await vscode.workspace.fs.writeFile(launchUri, encoder.encode(SAMPLE_LAUNCH_JSON));
    created.push(vscode.workspace.asRelativePath(launchUri));
  }

  if (created.length > 0) {
    void vscode.window.showInformationMessage(`nanobpmn: created ${created.join(', ')}.`);
  }
  if (skipped.length > 0) {
    void vscode.window.showInformationMessage(
      `nanobpmn: left existing file(s) unchanged: ${skipped.join(', ')}.`,
    );
  }

  const doc = await vscode.workspace.openTextDocument(bpmnUri);
  await vscode.window.showTextDocument(doc);
}

async function exists(uri: vscode.Uri): Promise<boolean> {
  try {
    await vscode.workspace.fs.stat(uri);
    return true;
  } catch {
    return false;
  }
}

function formatError(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function isActiveElementsEvent(
  message: unknown,
): message is { type: 'event'; event: string; body: ActiveElementsBody } {
  if (!isRecord(message)) return false;
  if (message.type !== 'event' || message.event !== ACTIVE_ELEMENTS_EVENT) return false;
  const body = message.body;
  if (!isRecord(body) || !Array.isArray(body.elements)) return false;
  return body.elements.every((element) => typeof element === 'string');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
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

const SAMPLE_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  id="Definitions_sample" targetNamespace="https://nanobpm.dev/debug-sample">
  <bpmn:process id="debugSample" isExecutable="true">
    <bpmn:startEvent id="start" name="Start">
      <bpmn:outgoing>flow_start_task</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="serviceTask" name="Do work">
      <bpmn:incoming>flow_start_task</bpmn:incoming>
      <bpmn:outgoing>flow_task_end</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end" name="Done">
      <bpmn:incoming>flow_task_end</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="flow_start_task" sourceRef="start" targetRef="serviceTask" />
    <bpmn:sequenceFlow id="flow_task_end" sourceRef="serviceTask" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>
`;

const SAMPLE_LAUNCH_JSON = `{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "nanobpmn",
      "request": "launch",
      "name": "Debug sample BPMN process",
      "bpmn": "\${workspaceFolder}/nanobpmn-debug-sample.bpmn",
      "processId": "debugSample",
      "variables": {}
    }
  ]
}
`;
