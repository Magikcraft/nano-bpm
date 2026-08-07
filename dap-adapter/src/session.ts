//! The Debug Adapter Protocol session: translates DAP requests from a client
//! (VS Code, or a headless test client) into calls on the wasm engine's debug
//! surface (#650), and engine pauses back into DAP `stopped` / `terminated`
//! events. One BPMN process instance = one DAP "thread".
//!
//! Lifecycle: `initialize` → `launch` (deploy, emit `initialized`) →
//! `setBreakpoints` (resolve lines → element ids) → `configurationDone` (start the
//! run, stop at the first breakpoint) → `continue` / `next` (resume / step) →
//! `terminated` when the run drains.

import { readFileSync } from 'node:fs';

import {
  Event,
  InitializedEvent,
  LoggingDebugSession,
  Scope,
  Source,
  StackFrame,
  StoppedEvent,
  TerminatedEvent,
  Thread,
} from '@vscode/debugadapter';
import type { DebugProtocol } from '@vscode/debugprotocol';

import { type BreakCondition, DebugEngine, type DebugState } from './engine.js';
import { ACTIVE_ELEMENTS_EVENT } from './events.js';
import { BpmnSourceMap } from './sourceMap.js';

/** DAP `launch` arguments for a nanobpmn process debug session. */
interface LaunchArgs extends DebugProtocol.LaunchRequestArguments {
  /** Absolute path to the `.bpmn` file to debug. */
  bpmn: string;
  /** The process id to start an instance of. */
  processId: string;
  /** Initial process variables. */
  variables?: Record<string, unknown>;
}

const THREAD_ID = 1;
const VARIABLES_REF = 1;

export class NanobpmnDebugSession extends LoggingDebugSession {
  private readonly engine = new DebugEngine();
  private sourceMap?: BpmnSourceMap;
  private bpmnPath = '';
  private launchArgs?: LaunchArgs;
  /** Element ids the client has set breakpoints on. */
  private breakElementIds: string[] = [];
  /** Element ids reported active at the last pause (the "call stack"). */
  private activeElements: string[] = [];

  constructor() {
    super('nanobpmn-dap.log');
    this.setDebuggerLinesStartAt1(true);
    this.setDebuggerColumnsStartAt1(true);
  }

  protected override initializeRequest(
    response: DebugProtocol.InitializeResponse,
    _args: DebugProtocol.InitializeRequestArguments,
  ): void {
    response.body = response.body ?? {};
    response.body.supportsConfigurationDoneRequest = true;
    response.body.supportsTerminateRequest = true;
    this.sendResponse(response);
  }

  protected override launchRequest(
    response: DebugProtocol.LaunchResponse,
    args: LaunchArgs,
  ): void {
    try {
      const xml = readFileSync(args.bpmn, 'utf8');
      this.bpmnPath = args.bpmn;
      this.sourceMap = new BpmnSourceMap(xml);
      this.engine.deploy(xml);
      this.launchArgs = args;
    } catch (err) {
      this.sendErrorResponse(response, 1001, `launch failed: ${String(err)}`);
      return;
    }
    this.sendResponse(response);
    // Signal we are ready to receive breakpoints; the run starts in
    // configurationDone once they have arrived.
    this.sendEvent(new InitializedEvent());
  }

  protected override setBreakPointsRequest(
    response: DebugProtocol.SetBreakpointsResponse,
    args: DebugProtocol.SetBreakpointsArguments,
  ): void {
    const requested = args.breakpoints ?? [];
    const verified: DebugProtocol.Breakpoint[] = [];
    const ids: string[] = [];
    for (const bp of requested) {
      const id = this.sourceMap?.elementAt(bp.line);
      if (id !== undefined) {
        ids.push(id);
        verified.push({ verified: true, line: bp.line });
      } else {
        // No flow node on that line — report unverified so the client greys it.
        verified.push({ verified: false, line: bp.line, message: 'no BPMN element on this line' });
      }
    }
    this.breakElementIds = ids;
    response.body = { breakpoints: verified };
    this.sendResponse(response);
  }

  protected override configurationDoneRequest(
    response: DebugProtocol.ConfigurationDoneResponse,
    args: DebugProtocol.ConfigurationDoneArguments,
  ): void {
    super.configurationDoneRequest(response, args);
    const launch = this.launchArgs;
    if (!launch) {
      this.sendEvent(new TerminatedEvent());
      return;
    }
    const conditions: BreakCondition[] = this.breakElementIds.map((id) => ({
      kind: 'elementActivated',
      id,
    }));
    const state = this.engine.start(launch.processId, launch.variables ?? {}, conditions);
    this.reportStopOrTerminate(state, 'breakpoint');
  }

  protected override threadsRequest(response: DebugProtocol.ThreadsResponse): void {
    response.body = { threads: [new Thread(THREAD_ID, 'process instance')] };
    this.sendResponse(response);
  }

  protected override stackTraceRequest(
    response: DebugProtocol.StackTraceResponse,
    _args: DebugProtocol.StackTraceArguments,
  ): void {
    const source = new Source(this.bpmnPath.split('/').pop() ?? 'process.bpmn', this.bpmnPath);
    const frames: StackFrame[] =
      this.activeElements.length > 0
        ? this.activeElements.map((id, i) => {
            const line = this.sourceMap?.lineOf(id) ?? 1;
            return new StackFrame(i, id, source, line);
          })
        : [new StackFrame(0, '(no active element)', source, 1)];
    response.body = { stackFrames: frames, totalFrames: frames.length };
    this.sendResponse(response);
  }

  protected override scopesRequest(
    response: DebugProtocol.ScopesResponse,
    _args: DebugProtocol.ScopesArguments,
  ): void {
    response.body = { scopes: [new Scope('Variables', VARIABLES_REF, false)] };
    this.sendResponse(response);
  }

  protected override variablesRequest(
    response: DebugProtocol.VariablesResponse,
    args: DebugProtocol.VariablesArguments,
  ): void {
    const out: DebugProtocol.Variable[] = [];
    if (args.variablesReference === VARIABLES_REF) {
      for (const [name, value] of Object.entries(this.engine.variables())) {
        out.push({ name, value: JSON.stringify(value), variablesReference: 0 });
      }
    }
    response.body = { variables: out };
    this.sendResponse(response);
  }

  protected override continueRequest(
    response: DebugProtocol.ContinueResponse,
    _args: DebugProtocol.ContinueArguments,
  ): void {
    this.sendResponse(response);
    this.reportStopOrTerminate(this.engine.resume(), 'breakpoint');
  }

  protected override nextRequest(
    response: DebugProtocol.NextResponse,
    _args: DebugProtocol.NextArguments,
  ): void {
    this.sendResponse(response);
    this.reportStopOrTerminate(this.engine.step(), 'step');
  }

  protected override stepInRequest(
    response: DebugProtocol.StepInResponse,
    _args: DebugProtocol.StepInArguments,
  ): void {
    this.sendResponse(response);
    this.reportStopOrTerminate(this.engine.step(), 'step');
  }

  protected override disconnectRequest(
    response: DebugProtocol.DisconnectResponse,
    _args: DebugProtocol.DisconnectArguments,
  ): void {
    this.engine.clear();
    this.sendResponse(response);
  }

  /** Emit a `stopped` event if the run paused, else `terminated`. */
  private reportStopOrTerminate(state: DebugState, reason: 'breakpoint' | 'step'): void {
    if (state.paused) {
      this.activeElements = state.activeElements;
      this.sendEvent(new Event(ACTIVE_ELEMENTS_EVENT, { elements: state.activeElements }));
      this.sendEvent(new StoppedEvent(reason, THREAD_ID));
    } else {
      this.activeElements = [];
      this.sendEvent(new Event(ACTIVE_ELEMENTS_EVENT, { elements: [] }));
      this.sendEvent(new TerminatedEvent());
    }
  }
}
