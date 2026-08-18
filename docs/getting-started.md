# Getting Started

**Nano Workforce** turns a durable BPMN process into an agent graph that drives
software delivery: it plans an issue, fans a fleet of coding agents out to
implement it, drives each pull request to review convergence, and merges —
escalating to a human when an agent is stuck or a round cap is hit.

This guide takes you from nothing to a running Workforce on your own machine,
then shows how to spread the worker fleet across a LAN, how to stay up to date,
and how to make the app your own.

## Prerequisites

- **Node.js 22.18+** on the machine that runs Nano.
- The **GitHub Copilot CLI** available on your `PATH` as `copilot` — this is the
  default agent each worker runs.
- **A GitHub identity for the agents.** Either `gh` authenticated on the machine
  (`gh auth login`) **or** a `GITHUB_TOKEN` exported in the environment. The
  agents use it to read issues and open, review, and merge pull requests.

## Run it locally

The fastest path: engine, console, and worker fleet all on one box.

### 1. Install Nano

Install the Camunda 8 CLI and load the Nano plugin. The plugin fetches the
matching prebuilt Nano engine binary for your OS and architecture.

```bash
npm install @camunda8/cli -g
c8ctl load plugin c8ctl-plugin-nano
```

### 2. Start Nano

```bash
c8ctl nano start
```

This starts a single-node cluster on port `8080`. Open the web console at
<http://localhost:8080/console>.

### 3. Install the Nano Workforce extension

In the console, open the **Extensions** tab (the marketplace), find **Nano
Workforce**, and select **Install**. The console fetches the pack and adds its
element template, configuration metadata, and worker code to your workspace.

### 4. Create a project from the Nano Workforce template

Open the **New Project** picker and choose the **Nano Workforce** template.
Because Nano Workforce ships as an *example* extension, selecting it **copies the
whole app into a new project you own** — BPMN models, worker code, prompts, and
`nano.app.json` — ready to run and to modify.

### 5. Start Nano Workforce and open its UI

From the console, **start** the Nano Workforce project. The console runs its
toolchain and the app comes up on its own port; open its UI to see the Workforce
dashboard (epics, features, and the live agent cockpit). It will be empty until
you enrol workers and submit work.

### 6. Enrol a worker

A *hire* is a persisted agent profile — a name, a rank, and the CLI command each
job runs. Hire one Copilot worker:

```bash
c8ctl nano hire --name copilot --rank senior --command copilot
```

Here `--command copilot` is the GitHub Copilot CLI you installed in the
prerequisites; `--name` is how you refer to the profile from now on.

### 7. Start the supervisor

The supervisor is a detached manager that runs and restarts your worker fleet
from one terminal:

```bash
c8ctl nano supervisor start
```

### 8. Run three Copilot instances

Add three instances of the `copilot` profile in **auto** mode — `--auto` is
zero-config job detection: each worker reads the deployed agent job types
straight from the engine and serves them, with no wiring.

```bash
c8ctl nano supervisor add copilot --instances 3 --auto
```

Check the fleet at any time:

```bash
c8ctl nano supervisor status
```

Your three workers should appear live on the Workforce UI's agent cockpit
(presence, and — for roles hired with `--terminal pty` — streamed terminals).

### 9. Submit a PR for convergence

From the Nano Workforce UI, start a **convergence run** against a pull request
(or seed an epic and let the graph open the PRs for you). The supervised workers
pick up the jobs, drive the PR to review convergence round by round, and merge on
success — escalating to you if an agent stalls or hits the round cap.

That is the full local loop: **install → start → extend → hire → supervise →
converge.**

## Run the workers on another box

For real throughput you run the workers on separate machines on the same trusted
LAN, all pointed at one Nano box. Nothing here needs a shared secret — the engine,
the capability hooks, and the agentic visibility channel are all open on the
trusted LAN by default.

### On the Nano box

1. In the project's configuration (the Studio project settings for the Nano
   Workforce app), set **`NANO_WORKFORCE_BASE_URL`** to the box's LAN address —
   for example `http://192.168.1.10:3000`. This is the base URL baked into each
   agent's capability-hook URLs, so it must resolve from the *worker* boxes, not
   `localhost`.
2. Make sure the app binds to all interfaces so off-box workers can reach it —
   `nano.app.json` ships `"network": { "bind": "all" }` by default.
3. **Start** Nano Workforce.

> The `NANO_WORKFORCE_BASE_URL` is captured when an instance is seeded and baked
> into each agent's prompt, so changing it later does not heal already-running
> instances — re-seed them to pick up a new base. See the Nano Workforce README's
> *Fleet networking* section for the console-reverse-proxy alternative.

### On each worker box

1. **Install Nano** exactly as in step 1 above (`@camunda8/cli` + the plugin).
2. **Create a c8ctl profile for the Nano server and select it**, so the workers
   talk to the remote engine instead of a local one:

   ```bash
   c8ctl add profile nano --baseUrl=http://192.168.1.10:8080
   c8ctl use profile nano
   ```

3. **Enrol workers** and **start the supervisor** exactly as in steps 6–8:

   ```bash
   c8ctl nano hire --name copilot --rank senior --command copilot
   c8ctl nano supervisor start
   c8ctl nano supervisor add copilot --instances 3 --auto
   ```

The workers pull jobs from the remote engine, `curl` their capability hooks at
`NANO_WORKFORCE_BASE_URL`, and appear live on the Nano box's Workforce UI. Repeat
on as many boxes as you like.

## Updating Nano Workforce

Nano Workforce is published as a versioned package, so updates arrive through the
same marketplace you installed it from.

1. **Update the extension.** In the console's **Extensions** tab, a newer version
   shows an **Update** action — select it to replace the pack cleanly.
2. **Update your project.** Because your project is a *copy* taken at create time,
   updating the extension does not touch it. Create a fresh project from the
   updated template to get the new app, then port your customisations across (or,
   if you never modified it, just switch to the new project).
3. **Restart Nano Workforce** so the running app and its workers pick up the new
   code.

## Making Nano Workforce your own

When you create a project from the Nano Workforce template, the console **copies
the whole app directory** into a new project under your workspace. From that
moment the project is *yours*: a normal directory of files — BPMN models, the
worker code under `workers/`, prompts and forms under `resources/`, and
`nano.app.json` — that you can edit freely and keep under version control.

A copy is a snapshot, not a live link, so a good pattern is to keep **two
projects**:

- **A vanilla project** you leave untouched. It always matches the published
  template, so you can re-create it cleanly on every update and diff it against
  your customised one to see what changed upstream.
- **A customised project** where you make it your own — tune the agent prompts,
  swap models or ranks, adjust the BPMN convergence graph, or add your own
  worker capabilities.

If you'd rather manage the app in your own repository, **fork it** and point Nano
at your fork directly: because an example is just an app directory, you can work
against your fork on the filesystem as the project's app directory, or publish it
as your own community extension (any pack carrying the `nano-ide-ext` keyword is
installable). See the [Extensions guide](/docs/extensions) for the pack layout,
the `nano-ide.ext.json` manifest, and publishing.
