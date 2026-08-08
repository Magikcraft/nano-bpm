#!/usr/bin/env node
//! stdio entry point: a nanobpmn DAP adapter a client (VS Code) launches and
//! talks to over stdin/stdout. `DebugSession.run` wires the session's request
//! handlers to the stdio transport.
import { DebugSession } from '@vscode/debugadapter';

import { NanobpmnDebugSession } from './session.js';

DebugSession.run(NanobpmnDebugSession);
