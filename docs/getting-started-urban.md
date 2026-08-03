# Getting Started with Urban

Urban builds agentic apps on your workstation. This guide explains Urban. Then it shows two journeys and three pathways.

## What is Urban

Urban is a RAAD environment. RAAD means Rapid Agentic Application Development. Urban runs on the developer workstation.

Urban composes coding agents, tools, and human approvals into durable workflows. A durable workflow survives a crash. After a restart, the workflow resumes at the exact step.

Urban supports two styles. You write the workflow as code, or you draw it as a model. The same engine runs both.

Urban is provider-agnostic. Urban runs on a Raspberry Pi.

## Prerequisites

Complete these steps once.

- Install Node.js. Nano needs Node.js version 22.6 or later.
- Install the CLI. Run `npm i -g @camunda8/cli`.
- Load the Nano plugin. Run `c8ctl load plugin c8ctl-plugin-nano`.
- Start a node. Run `c8ctl nano start`.

A node is one Nano process. The node serves the console, the engine, and the agent endpoint.

## Two journeys

Urban has two journeys. You consume an app that another author built. You author a new app.

Each journey has three pathways. A pathway is a surface. The three surfaces are Nano Studio, your own IDE or CLI, and the agent endpoint. Pick the pathway that fits your tools. Every pathway ends at the same place: a deployed app, observed in Studio.

## Journey 1 — Consume an existing app

Goal: run an app that another author built.

### Pathway A — Nano Studio

1. Open the console. Open Studio.
2. Import the app by reference, or open the shared project.
3. Deploy the app.
4. Observe the instances in the Explorer.

### Pathway B — Your own IDE or CLI

1. Clone the app repository, or install the app package.
2. Change into the app directory.
3. Install the dependencies. Run `npm install`.
4. Run the app. Run `npm run dev`.
5. Open the app link from the command output.

### Pathway C — The agent endpoint

1. Give the agent the address `http://<node>/llms.txt`.
2. The agent reads the index. The index links the app surfaces.
3. The agent links the app in.

## Journey 2 — Author a new app

Goal: build a new app.

### Pathway A — Nano Studio

Nano Studio provides a first-class **New Urban App** affordance. Studio delivers the Urban toolkit through the extensions marketplace. On first use, Studio installs the toolkit. The first install needs network access once.

1. Open the console. Open Studio.
2. Select **New Urban App**.
3. Select the **model-first** card, or the **code-first** card.
4. Select the runtime. Choose **Node**, or choose **Deno**.
5. Create the app. Studio scaffolds the app from the toolkit.
6. Deploy the app.
7. Observe the instances in the Explorer.

The Deno option needs Deno on the machine. If Deno is absent, the Deno option stays disabled. To enable the Deno option, install Deno from `deno.com`.

### Pathway B — Your own IDE or CLI

Nano derives the executable model, the job types, the message correlation, and a generic worker. You write the steps and the handlers.

1. Scaffold the app. Run `npm create urban-app@latest my-app`.
2. To author code-first, add `--style code`. The default style is model-first.
3. To target Deno, add `--deno`. The default runtime is Node.
4. Change into the app directory. Run `cd my-app`.
5. Install the dependencies. Run `npm install`.
6. Write the flow with `@nanobpm/workflow`.
7. Run the app. Run `npm run dev`.
8. Deploy the app to the node. Run `npm run deploy`.
9. Observe the app in Studio.

### Pathway C — The agent endpoint

The agent endpoint teaches an external coding agent to author an app.

1. Give the agent the address `http://<node>/agent`.
2. The agent reads the brief. The brief describes the App schema and the run steps.
3. The agent authors `nano.app.json`. The agent links the app in.
4. Deploy the app.
5. Observe the app in Studio.

## Next steps

- Read the published schemas at [nanobpm.io/schemas](/schemas/).
- Open the browser demo at [nanobpm.io/demo](/demo/).
