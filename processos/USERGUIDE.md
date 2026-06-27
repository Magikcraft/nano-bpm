# Nano ProcessOS — User Guide

**ProcessOS is your process-optimization workbench.** You point it at a business
process, hand it the history of how that process actually ran, and a built-in AI
investigator — *the droid* — helps you find bottlenecks, form hypotheses, and
test changes by replaying them against the real data. Nothing is changed in your
live system: ProcessOS is **suggest-only**. You stay in control and decide what,
if anything, to act on.

This guide is task-focused. It walks you through running ProcessOS, exploring the
bundled demo processes, talking to the droid, choosing and configuring the AI
models it uses, and packaging up a session so someone else can replay your
investigation. You do **not** need source code or a developer setup to follow it.

> **New here?** Start with [Getting started](#getting-started), then
> [Load a demo dataset](#load-a-demo-dataset) and
> [Investigate a process](#investigate-a-process). Everything else you can reach
> for when you need it.

## Getting started

ProcessOS ships as a single binary with its web console built in. There is
nothing to install beyond the binary itself.

1. **Start ProcessOS.** Run the binary you were given (for example
   `./processos`, or launch it with `c8ctl` if you manage Nano that way). It
   starts a local web server and prints the address it is listening on.
2. **Open the console.** In your browser, go to **http://localhost:8090** (this
   is the default; if you set a different port, use that instead). You'll land on
   the ProcessOS home page.
3. **Pick where to go.** From the home page you can jump to **Workspaces** (where
   your processes and data live), the **Cockpit** (where you investigate), the
   **User guide** (this document), and **What it does** (a feature tour).

That's all you need to explore the demos. A couple of optional capabilities have
extra requirements, and ProcessOS tells you about them when they matter:

- **Local AI models** need [llama.cpp](https://github.com/ggml-org/llama.cpp)
  installed. You only need this if you want ProcessOS to run a model on your own
  machine instead of calling a hosted API.
- **Workers** (the code that runs process tasks) need
  [Deno](https://deno.com/) installed.

If either tool is missing when you try to use it, a banner appears at the top of
the console explaining what's missing and linking to install instructions for
your operating system. You can dismiss the banner and keep working on everything
that doesn't need that tool.

### Changing the port and data location

ProcessOS reads a couple of environment variables when it starts:

| Setting | Default | What it does |
|---|---|---|
| `PROCESSOS_PORT` | `8090` | The port the console is served on. |
| `PROCESSOS_DATA_DIR` | `./.processos-data` | Where your workspaces, datasets, and saved investigations are stored. Point this at a stable folder so your work persists between runs. |

Set them before launching, for example `PROCESSOS_PORT=9000 ./processos`.

## The console at a glance

ProcessOS is organized into a few surfaces. You'll spend most of your time in
Workspaces and the Cockpit.

| Surface | Where | What it's for |
|---|---|---|
| **Home** | `/` | The landing page — your jumping-off point to everything below. |
| **Workspaces** | `/workspace` | Browse and create workspaces and processes, and **load the demo datasets**. A *workspace* is a tenant or business area; a *process* is one BPMN process inside it, together with the trace data recorded for it. |
| **Cockpit** | `/cockpit` | The investigation surface: chat with the droid, run replays, inspect the model, and export your session. This is where the real work happens. |
| **Live instance** | `/console` | A dashboard of insights from a connected Nano engine, for when you're analysing a live production system. |
| **Features** | `/features` | A short tour of what ProcessOS can do. |
| **User guide** | `/guide` | This guide. |

Throughout the console, a **Settings cog** in the lower-left opens the settings
panel, where you manage AI models and a few system options. See
[Configure AI models](#configure-ai-models).

## Load a demo dataset

The fastest way to learn ProcessOS is to explore one of the bundled demo
processes. Each demo generates a realistic, synthetic history (a *trace corpus*)
so the droid has real data to investigate — no live system required.

1. Go to **Workspaces** (`/workspace`).
2. Click **Load demo dataset**.
3. In the picker — *"Choose a demo scenario to generate and explore"* — pick a
   scenario and click **Load**. ProcessOS shows *"Generating demo corpus…"* while
   it builds the data, then drops you into the seeded process.

Three demos ship with ProcessOS:

| Demo | Workspace · Process | The story |
|---|---|---|
| **Loan Approval** | Northwind Bank · Loan Approval | Loan origination with a credit-check bottleneck that bites under weekday-morning peaks. *"Keep loan approvals fast and reliable as volume grows."* |
| **CDD Refresh** | Meridian Trust · CDD Refresh | A multi-phase KYC/CDD refresh orchestrator with an agentic AI investigation stage. *"Complete periodic refreshes within SLA without overloading analysts."* |
| **Delivery Exception Resolution** | Amazing Retail · Delivery Exception Resolution | Resolving delivery exceptions quickly while keeping AI-agent cost under control. |

Loading a demo creates a workspace, a process, the generated trace dataset, and
the BPMN model — exactly the shape your own data will take later. Once it's
loaded, open the process and start investigating.

> **Bringing your own data later?** The demos mirror how real datasets are
> structured. When you import a customer's process history, it shows up as a
> workspace and process in the same place, and everything in this guide applies
> unchanged.

## Investigate a process

The **Cockpit** is where you and the droid work a process together. Open a
process from Workspaces (or click through after loading a demo) to get there.

1. **Pick a persona.** The **Persona** selector sets the droid's standing
   instructions — its job description for this conversation (for example a
   general investigator, or the *Experiment Designer* who specializes in testing
   changes). The persona is fixed once the conversation starts, so choose it
   first.
2. **Ask a question.** Type into the compose box and click
   **Investigate with the droid →**. The droid reads the process model and the
   recorded traces, runs its own analysis, and answers — showing its reasoning
   and the tools it used along the way.
3. **Test a change by replaying it.** In the Experiment Designer persona, the
   droid can **fork the process model**, apply a change, and **replay it against
   the dataset**. Each run appears in the **Simulations** view with the variant
   it tried and a scorecard, so you can compare options on real history rather
   than guesswork.
4. **See exactly what was sent.** The **Debug** view captures the precise request
   sent to the model for the last turn — useful when you want to understand or
   share what the droid actually saw.

Because ProcessOS is suggest-only, replays and forks never touch your live
process. They're experiments you can keep, discard, or hand to someone else.

## Choose an AI model

Every investigation turn is answered by an **AI model** (an *LLM profile* in
ProcessOS terms). You can switch which model answers without leaving the cockpit.

- The send button is labelled with the **active profile** — e.g.
  *"Investigate with Local (llama.cpp) →"* — so you always know which model is
  about to answer.
- Click the **▾** caret next to the send button to pick a **different profile**
  for the next message. This is handy for comparing how two models reason about
  the same question.
- To change the default for new conversations, open the **Settings cog** and
  click **Set active** on the profile you want.

A *persona* and an *LLM profile* are different things: the persona is *what the
droid is told to do*; the profile is *which model does the thinking*. You can mix
and match them freely.

## Configure AI models

Open the **Settings cog** (lower-left in the cockpit or console) to manage your
AI models. Each model you set up is saved as a **profile**. Use **+ Add** to
create one, **Set active** to make it the default, and **Delete** to remove one.

There are two kinds of profile.

### External model (hosted API)

Point ProcessOS at a model served over the network — your own gateway, a hosted
provider, or any OpenAI-compatible endpoint.

| Field | What to enter |
|---|---|
| **Name** | A label you'll recognise, e.g. *"OpenAI GPT-4o"* or *"Team gateway"*. |
| **Provider** | The provider/protocol (e.g. an OpenAI-compatible API). |
| **Base URL** | The API endpoint, e.g. `https://api.openai.com/v1`. |
| **Model** | The model id the endpoint expects. |
| **API key** | Your key. It's stored locally; use **Clear API key** to remove it. |
| **Max tokens** / **Temperature** | Optional generation limits and randomness. |

Click **Save profile** when you're done. Use **Set active** to make it the model
the droid uses by default.

### Local model (managed llama.cpp sidecar)

ProcessOS can run a model **on your own machine** by launching a local
`llama-server` for you — this is a *sidecar*. This needs
[llama.cpp](https://github.com/ggml-org/llama.cpp) installed (ProcessOS will tell
you, with a link, if it isn't).

1. Create a profile and choose **Managed sidecar**.
2. In **Model file / HF spec**, enter either a Hugging Face GGUF reference such
   as `unsloth/Qwen3-4B-GGUF:UD-Q4_K_XL`, or a path to a `.gguf` file you already
   have.
3. Optionally set **Startup args** (advanced llama.cpp flags, e.g.
   `-ngl 99 -c 32768 --jinja`).
4. **Save profile**, then click **Start sidecar**. ProcessOS downloads the model
   if needed and starts it locally. **Download** can pre-fetch a model ahead of
   time, **Stop** shuts the sidecar down, and **Logs** shows the server output if
   you need to troubleshoot.

Once the sidecar is running, select its profile (via the **▾** caret or
**Set active**) and the droid will use your local model — no external API
required.

> **System settings.** The settings panel also lets you set the **llama-server
> binary** location and a **Models directory** (where downloaded models are
> cached), and a **Python interpreter** used by some analysis tools. Defaults
> work for most setups; change these only if ProcessOS can't find a tool or you
> want models stored elsewhere.

## Export a psychological trace for debugging

A **psychological trace** is a shareable snapshot of an entire investigation, so
someone else can replay and debug your session exactly as you experienced it —
without needing your machine. This is the way to get help: package up what
happened and send it on.

To export one:

1. In the cockpit, open the **Export ▾** menu above the conversation.
2. Under **Psychological trace**, click **Download trace (.zip)**.

Your browser downloads a `.zip` you can share (for example, drop it into Slack
for the person helping you). The bundle contains:

- The full **conversation transcript** and the droid's debug reasoning.
- The **AI models used**, with their configuration — **API keys are redacted**,
  so it's safe to share.
- The **dataset and model names** the session ran against.

The same **Export ▾** menu can also save just the chat transcript or the debug
log as Markdown or plain text, if that's all you need.

### Importing a shared trace

If someone sends *you* a psychological trace, use **Import** in the cockpit to
load it. It appears in your ProcessOS as an **Investigation** marked with a
special icon, so you can pick up and replay their session locally.

## Troubleshooting

**A banner says Deno or llama.cpp is missing.** You tried to use a capability
that needs an external tool. The banner links to install instructions for your
operating system — install the tool, then reload the console. Everything that
doesn't need that tool keeps working in the meantime.

**My work disappeared after restarting.** ProcessOS stores everything under the
folder named by `PROCESSOS_DATA_DIR` (default `./.processos-data`). If you start
it from a different directory, it looks in a different place. Set
`PROCESSOS_DATA_DIR` to a fixed path so your workspaces and investigations
persist.

**The console won't open at localhost:8090.** Check the address ProcessOS printed
when it started — you may have set `PROCESSOS_PORT` to something else, or another
program may be using the port. Restart with a free port, e.g.
`PROCESSOS_PORT=9000 ./processos`.

**A local model won't start.** Make sure llama.cpp is installed and, if needed,
set the **llama-server binary** path in Settings. Use **Logs** on the sidecar to
see why `llama-server` exited.
