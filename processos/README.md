# ProcessOS

The runtime **process-optimization plane** for Nano BPM — a separate component, on
the principle that **Nano handles production, ProcessOS handles optimization.**

It runs as its own process with its own webserver and talks to a Nano cluster *only*
over Nano's public HTTP contracts. The dependency is one-way: ProcessOS reads Nano;
**Nano never depends on ProcessOS** and runs unaffected when this binary is absent.

See [`../docs/processos-index.md`](../docs/processos-index.md) for the design-series
reading guide, [`../docs/processos-design.md`](../docs/processos-design.md) for where
the optimizer lives and its contracts, and
[`../docs/process-optimization-design.md`](../docs/process-optimization-design.md) for
the optimization loop it implements.

## Status

The optimization rig and the self-hosting **cockpit** are built and green
(`make processos-test`). The loop is **measure → calibrate → hypothesize → rank →
replay**, and it is **suggest-only**: nothing is actuated automatically — the engine
stays untouched and a human/verifier decides.

Milestones (design §7.4): **M0** (`SimRunner`), **M1** (baked candidate ranking) and
**M2** (LLM hypothesis) are done; **M3** (real-cluster realism + the live production
path) has its measurement/queueing/calibration substrate built. The §10 "cockpit"
build adds the human↔droid framing: an editable, **forkable pilot process**, a
persisted cockpit conversation, a prompt library, and ProcessOS's **own supervised
Nano engine** for the pilot loop.

Deliberately deferred (not gaps — see §7.6): a full discrete-event simulator, the
**actuating** scaling/fleet control verb, portfolio (cross-process) scope.

## Architecture — two engines

ProcessOS distinguishes the two Nano engines it talks to (design §1.4):

- **target** — the client's **production** engine. **Read-only**: ProcessOS folds its
  exported traces + metrics into Insights and a measured baseline. It never writes here.
- **own** — an engine ProcessOS **supervises** for its *pilot loop*: it can spawn the
  gateway as a child process, learn its port, deploy the pilot BPMN on boot, and reap
  it on shutdown. The self-optimizing loop and its meta-workers run here, never on the
  client's engine.

With no env set both roles resolve to the same local gateway, so a single-machine dev
setup "just works".

## The optimization harness (design §7)

The harness embeds the **real `engine-core`** (a one-way `path` dependency) and drives
it with a virtual clock to evaluate process variants. A **scenario** supplies a test
model, seeded **mock workers** (cost / latency / failure models), **latent
worker-swap options** (the MVP transform space), and **inputs carrying expected
outputs**. The `SimRunner` runs every candidate over every input and ranks them across
cost / latency / incident-rate / correctness, reporting whether exploration recovered
the optional **golden** variant. It is deterministic: same scenario + seed ⇒ same
ranking.

Beyond the synthetic baseline, the harness grounds itself on **real** data:

- **ClusterRunner / cluster read** (`harness/cluster.rs`) — cluster-aware
  measurement: discovers every node via `GET /v2/topology` and **unions** their
  partition-local traces, reporting throughput, the e2e tail (p50/p95/p99), the
  per-job-type **queue-vs-service split**, and per-node backlog (`byNode`).
- **Queueing recommender** (`harness/queueing.rs`) — an `M/M/c` (Erlang-C) worker-pool
  model fitted from the measured λ/S, answering *how many workers to hold p99
  queue-wait < X?* Advisory only.
- **Calibration** (`harness/calibrate.rs`) — overrides the SimRunner's hand-authored
  per-task service time / failure rate with **measured** distributions, so candidates
  are scored against the bar production actually set.
- **Recorded-input replay** (`harness/replay.rs`, `replay_rank.rs`, `evolve.rs`) — the
  Level-2 verifier: replay a historical instance's **real** creation inputs + ordered
  stimuli (captured opt-in on Nano's trace, see below) against a candidate model and
  rank by fidelity gradient.

### LLM hypothesis (M2)

Instead of (or alongside) the baked worker-swap grid, an LLM can *propose* candidates.
The model only generates hypotheses — the harness still measures and ranks them with
the SimRunner, so an improvement that doesn't actually help is exposed by the numbers.
The model is reached through a **pluggable client**, so the same path serves a local
model on your network and a hosted API:

- `openai` — the OpenAI **chat-completions** shape, spoken by local `llama.cpp`
  (`--api`), Ollama (`/v1`), vLLM, LM Studio, and OpenAI. **Default.**
- `anthropic` — the Anthropic **messages** API.

The **system prompt** is itself an experimental variable: an authorable, selectable
`PromptLibrary` (`harness/prompts.rs`), seeded with the built-in default and optionally
imported from `PROCESSOS_PROMPTS_DIR`. `POST /api/harness/hypothesize` selects by
precedence **inline `prompt` > `promptId` > built-in default**.

| Env | Default | Meaning |
|-----|---------|---------|
| `PROCESSOS_LLM_PROVIDER` | `openai` | `openai` (local/llama.cpp/ollama/vllm) or `anthropic` |
| `PROCESSOS_LLM_BASE_URL` | `http://127.0.0.1:8080/v1` (openai) / `https://api.anthropic.com` (anthropic) | Endpoint base |
| `PROCESSOS_LLM_MODEL` | _(required)_ | Model name |
| `PROCESSOS_LLM_API_KEY` | _(none)_ | Bearer / `x-api-key`; local models usually need none |
| `PROCESSOS_LLM_MAX_TOKENS` | `2048` | Completion budget |
| `PROCESSOS_LLM_TEMPERATURE` | `0.2` | Sampling temperature |

These are *defaults*; the console **Settings** panel (the gear in the lower-left of the
console and cockpit) manages one or more named **LLM profiles** — switch the *active*
profile, add/delete profiles, and **Fetch** an endpoint's model list to pick a model id
(the fetch also reads each model's **context window** — llama.cpp's `meta.n_ctx`,
`n_ctx_train`, or a `context_length`-style field — and fills the Max-tokens budget from it).
It ships with a `Local (llama.cpp)` profile pointing at `http://localhost:8888/v1`.
Profiles + the active selection + the global Python interpreter persist to
`${XDG_CONFIG_HOME:-~/.config}/processos/settings.json` (overridable with
`PROCESSOS_CONFIG_DIR`; written `0600` as it may hold an API key). The effective config
layers **built-in default → `PROCESSOS_LLM_*` env → active profile → per-request
`llm` override**, each winning when present — so the console is authoritative over the
environment without a relaunch. The panel also configures `PROCESSOS_PYTHON`
(the `run_python` interpreter). See the [settings endpoints](#endpoints).

## Workspaces — bounded contexts & processes (multi-tenant + datasets)

A consultant manages many engagements, so ProcessOS roots a **workspace tree** of
`workspaces / processes` on disk (`src/workspace.rs`). A **workspace** is a bounded
context for one customer/deployment. Each process **binds** to a trace source —
either a **live** Nano instance (`targetUrl`) or a **loaded dataset** (a folder of
instance-trace JSON, or a sibling `traces/` folder) — and the Insights report is
folded over whichever source is bound (`src/dataset.rs`, `report::build_over`).
Folders are scanned from disk, so a consultant can curate a workspace folder
structure by hand and it shows up; the CRUD API also creates them. The front door
is `/workspace`: browse workspaces → open one → open a process to see its **rendered
BPMN model** (vendored bpmn-js viewer, served from `/assets/bpmn/`) and Insights,
then **Reason in cockpit →** to investigate the bound trace with the droid.

```
<PROCESSOS_WORKSPACES_DIR>/
  <workspace-slug>/
    workspace.json
    <process-slug>/
      process.json          # { displayName, targetUrl? | dataset?, objective?, notes? }
      model.bpmn            # optional process model (rendered in the browser)
      traces/               # optional dataset folder (else `dataset` points elsewhere)
```

A process is `live` when `targetUrl` is set, `dataset` when a `dataset` path or a
`traces/` folder resolves, else `unbound`. Slugs are single, lowercased
`[a-z0-9-_]` path segments (no traversal). Browse it at `/workspace`.

**Load the demo dataset in one click:** `POST /api/workspaces/seed-demo` (or the
"Load demo dataset" button) creates the **Northwind Bank** workspace + **Loan
Approval** process, generates a labelled trace corpus into its `traces/` folder, and
writes `model.bpmn` — ready to view and investigate.

A dataset folder holds per-instance `*.json` files (each a
`GET /console/api/traces/{key}` shape) at top level or under `instances/`/`traces/`,
and/or a single `traces.json` array, plus an optional `metrics.json`.

## Synthetic trace corpus — labelled benchmark for domain inference (`src/corpus.rs`)

To exercise the distributed-sensing goal *without a running server*, ProcessOS can
**synthesise a labelled trace corpus** from a "before" archetype process plus a thin
infra sidecar. It runs the **real** `engine-core` per instance for the logical path
(branching, errors) and overlays timing analytically: lognormal service times + an
M/M/c queue-wait sampled against a **time-varying arrival schedule**. The result is a
`traces.json` array — byte-compatible with a real Nano capture, so it loads straight
into a workspace `DatasetSource` — carrying a **planted pathology** (e.g. an
under-provisioned job whose queue tail spikes weekday 09–12) and a ground-truth label.

A `pack.json` describes the scenario; the bundled
`corpus-packs/loan-approval/` plants a 2-worker `credit-check` bureau pool overloaded
by an 8× weekday-morning arrival spike:

```bash
# generate ~10k instances of traffic over a 14-day horizon
processos gen corpus-packs/loan-approval/pack.json /tmp/loan-corpus
#  -> traces.json (load via a workspace dataset) + metrics.json + expected.json

# run the inference + score it against the planted label
processos infer /tmp/loan-corpus 500   # 500ms p99-wait staffing target
#  -> bottleneckJob credit-check, window weekday-morning, recommendedWorkers 44
#  -> SCORE {"domainOk":true,"jobOk":true,"windowOk":true,"points":3,"maxPoints":3}
```

The inference time-buckets per-`(job, window)` queue means, localises the tail to the
job + temporal window with the largest peak-vs-off-peak inflation, fits the peak arrival
rate to an Erlang-C staffing recommendation, and classifies the domain — then `score`
grades that against `expected.json` (domain + job + window = 3 points). The generator is
the symmetric write-side of `replay.rs`, so a corpus also feeds `replay-rank`/`evolve`.

## LLM-driven investigation — open-ended sensing over the data (`src/analysis.rs`, `agent.rs`, `investigate.rs`)

The pre-built analyzers above (`corpus::infer`, `queueing`, `cluster`) encode *our*
priors. The research goal is the opposite: can the cockpit droid **reason over the trace
data and form its own hypotheses**? To open the hypothesis space, ProcessOS flattens a
bound trace source into in-memory **DuckDB** tables and gives the model a single
**read-only** `query_traces(sql)` tool — so it writes SQL, sees the result, and reasons,
instead of picking from a fixed menu.

- `analysis.rs` — flattens traces into `instances` / `jobs` / `incidents` tables with the
  cheap temporal primitives pre-derived (`hour`, `dow`, `is_weekend`, a real
  `started_ts`) but **no** pre-bucketed "windows" (that labelling is the inference we want
  the droid to perform). `query()` admits a single `SELECT`/`WITH` only — DDL/DML are
  rejected — over an ephemeral in-memory connection.
- `agent.rs` — a provider-agnostic tool-calling loop (`run_agent`) over an `AgentStep`
  transport (OpenAI/llama.cpp `chat/completions` tool-calls today), unit-testable with a
  deterministic mock. Every query+result is recorded as a reproducible **lab notebook**.
- `investigate.rs` — wires the two: a system prompt that imposes analytical discipline
  (state a hypothesis before querying, report effect size + sample size, replicate on a
  held-out slice before concluding) and emits a gradeable JSON conclusion.
- `pyrunner.rs` — the optional **Python escape hatch** (`run_python`): for hypotheses SQL
  can't express (distribution fitting, changepoint/seasonal decomposition, clustering).
  Off by default; pass `"allowPython": true`. The tables are exported as CSV to a private
  temp workdir and a **dependency-tolerant preamble** loads them as `pandas` DataFrames +
  a `duckdb` connection when those libs are present (`HAVE_PANDAS` / `HAVE_DUCKDB` flags),
  degrading to stdlib `csv` row-dicts otherwise — so it works on a bare interpreter too.
  It runs **arbitrary operator-trusted code** (a trusted-operator hatch, *not* a security
  sandbox) with pragmatic rails: a private workdir, a wall-clock timeout (child killed on
  overrun), and an output cap. Env: `PROCESSOS_PYTHON` (default `python3`),
  `PROCESSOS_PYTHON_TIMEOUT_SECS` (default 20), `PROCESSOS_PYTHON_MAX_OUTPUT` (default
  8000). For the rich path, point `PROCESSOS_PYTHON` at a venv:
  `python3 -m venv .venv && .venv/bin/pip install duckdb pandas numpy scipy` then
  `PROCESSOS_PYTHON=$PWD/.venv/bin/python`.

```bash
# Point the configured LLM at a workspace process bound to a dataset:
curl -XPOST .../api/workspaces/{workspace}/processes/{process}/investigate \
  -d '{"llm":{"model":"local-model"},"maxRounds":12,"allowPython":false}'
# -> { dataset:{instances,jobs,incidents},
#      run:{ answer:"{\"bottleneckJob\":\"credit-check\",\"window\":\"weekday-morning\",…}",
#            steps:[{tool:"query_traces", arguments:{sql}, result}], rounds } }
```

The DuckDB connection is `!Send`, so the endpoint runs the whole loop on a dedicated
current-thread runtime via `spawn_blocking`. The labelled corpus is the **eval substrate**:
`investigate.rs` tests drive the loop over a generated corpus and assert it recovers the
planted `credit-check` / `weekday-morning` fault through the full model→tool→DuckDB→model
path (and via `run_python` over the exported CSV with stdlib-only Python).

### Interactive cockpit chat (`src/chat.rs`, `chat_prompts.rs`)

The one-shot `investigate` call is also available as a **multi-turn, scrollable chat** at
`/cockpit?workspace=&process=`. The operator converses with the droid over the bound
dataset; each turn resumes from the persisted transcript, so the model remembers its own
earlier answers (ask *"where is the bottleneck?"* then *"when does **that** happen?"*).

- `chat.rs` — a file-backed `ChatStore` under `PROCESSOS_DATA_DIR` (one JSON transcript per
  `(workspace, process)` session) plus `render_view`, which projects the raw model
  transcript into operator-facing `user`/`droid` turns (each droid turn carries the SQL/
  Python tool steps it ran as a collapsible **lab notebook**).
- `agent::run_agent_resumable` drives the loop over a `&mut Vec<Msg>`, appending the
  assistant/tool turns **and** the final answer so the next turn keeps full context. A turn
  can also be **wrapped up early** (`POST .../chat/wrapup`): the loop checks a per-session
  cancel flag between rounds and, when set (or when the round budget is exhausted), tells the
  model to stop investigating and report its findings so far instead of erroring.
- `chat_prompts.rs` — a **prompt library** of reusable compose-box message templates,
  persisted to the user **config dir** (`chat-prompts.json`, alongside `settings.json`).
  Ships four built-ins led by an **open investigation** (the default, pre-loaded into a
  fresh compose box so the unbiased default action is to let the droid profile the data and
  find the problem itself) plus a specific *worker-swap hypothesis*, a temporal angle, and a
  failures angle. Operators author new ones from the compose box ("Save prompt") or the
  console **Prompts** view (which lists both libraries), and load any into the box from a
  dropdown. The CHAT_SYSTEM framing is deliberately open — it asks the analyst to profile
  broadly and form its own hypotheses rather than confirm a preconceived answer — and the
  process's operator-set **objective** (editable on the cockpit surface) is woven in as
  *background context, not a conclusion*, so the operator can optionally steer without
  pre-revealing the planted issue.

```bash
# Send one turn (resumes the persisted transcript); GET to reload it, /reset to forget.
curl -XPOST .../api/workspaces/{workspace}/processes/{process}/chat \
  -d '{"message":"Which job type has the worst queue tail?","allowPython":false}'
# -> { answer, rounds, dataset:{instances,jobs,incidents}, turns:[{role,text,steps}] }
curl       .../api/workspaces/{workspace}/processes/{process}/chat        # load transcript
curl -XPOST .../api/workspaces/{workspace}/processes/{process}/chat/reset # forget it
curl       .../api/chat-prompts                                           # list templates
curl -XPOST .../api/chat-prompts -d '{"id":"my-probe","name":"My probe","text":"…"}'
curl -XPOST .../api/workspaces/{workspace}/processes/{process}/chat/wrapup # stop & report now
curl       .../api/python/status   # { configured, interpreter, interpreterRuns, dataScience, missing }
```

The chat surface adds a few operator conveniences: the **Send** button is a split control
labelled with the active LLM profile (*"Investigate with &lt;profile&gt; →"*) whose **caret
opens a profile picker** so the operator can switch the model that answers the next message
without opening Settings; the button relabels live when the active profile changes. Droid
bubbles are titled with the **profile name** and the model id in a smaller, dimmer
parenthetical (*"Remote Qwen 3.6 (unsloth/qwen3.6-…)"*); they render the answer as
**Markdown** (headings, lists, bold/italic, inline + fenced code, links — escaped first, only
http(s) links emitted) and any reasoning as a **collapsed** "Thinking" disclosure. The droid's
chain-of-thought is captured whether the backend emits inline `<think>…</think>` tags or a
separate `reasoning_content` field (as llama.cpp does for Gemma/Qwen — `parse_openai_turn`
folds it into a `<think>` block).
**A−/A+** controls size the chat font (persisted in `localStorage`); a **Wrap it up →** button
appears while a turn is in flight (`POST .../chat/wrapup`); and the Python toggle
self-describes from `GET /api/python/status` — *"Enable Python Data Science tools"* when the
interpreter has pandas/duckdb, *"Enable Python (Optional: Install Data Science tools)"* with an
install popup when it runs but lacks them, or *"Configure Python"* with a setup popup when no
interpreter is usable.


## The cockpit & pilot loop (§10)

The optimization loop is itself authored as **editable BPMN** (the *pilot process*,
`pilot/pilot-self-optimize.bpmn`) deployed on the own engine — its **user tasks** are
the human's turns, **LLM tasks** the droid's, **engine tasks** the craft responding.
The pilot is **forkable** and file-backed under the data dir (`src/pilot.rs`), so the
delegation boundary an operator encodes in it is durable and inspectable. The
**cockpit** (`/cockpit`, `src/cockpit.rs` + `conversation.rs`) wraps the experiment
stepper in a persisted droid-conversation surface.

## Run

### Prerequisites (machine setup)

- **Rust** (stable) — `cargo` builds/tests; the `duckdb` crate vendors its engine (bundled),
  so no system DuckDB is needed. First build compiles the bundled amalgamation (~95s, one-time).
- **Python 3** — only required for the optional `run_python` investigation tool. A bare
  `python3` works (stdlib fallback). For the **rich** analysis path (pandas/numpy/scipy/duckdb)
  create a venv once and point `PROCESSOS_PYTHON` at it:

  ```sh
  # from the processos/ directory
  python3 -m venv .venv
  .venv/bin/pip install --upgrade pip
  .venv/bin/pip install duckdb pandas numpy scipy
  export PROCESSOS_PYTHON="$PWD/.venv/bin/python"   # else run_python uses stdlib-only python3
  ```

  The `.venv/` is git-ignored and machine-local. Without it, `run_python` still runs but the
  preamble degrades to stdlib `csv` row-dicts (`HAVE_PANDAS`/`HAVE_DUCKDB` are `False`).

```sh
# Build + test
make processos-build
make processos-test

# Run against a Nano gateway (defaults shown)
PROCESSOS_PORT=8090 NANO_BASE_URL=http://localhost:8080 cargo run

# …or let ProcessOS spawn + supervise its own engine for the pilot loop
PROCESSOS_SPAWN_NANO=1 cargo run
```

| Env | Default | Meaning |
|-----|---------|---------|
| `PROCESSOS_PORT` | `8090` | Port ProcessOS listens on |
| `NANO_BASE_URL` | `http://localhost:8080` | Back-compat alias defaulting **both** the target and own URLs |
| `NANO_TARGET_URL` | _(= `NANO_BASE_URL`)_ | The **read-only production** engine to analyse |
| `PROCESSOS_NANO_URL` | _(= `NANO_BASE_URL`)_ | The **own** engine the pilot loop runs on |
| `PROCESSOS_DATA_DIR` | `./.processos-data` | Persisted forks, conversations, prefs |
| `PROCESSOS_WORKSPACES_DIR` | _(`<data_dir>/workspaces`)_ | Root of the workspaces/processes tree |
| `PROCESSOS_PROMPTS_DIR` | _(none)_ | Directory of prompt files imported into the library on boot |
| `PROCESSOS_SPAWN_NANO` | `false` | Spawn + supervise an own Nano engine as a child process |
| `PROCESSOS_NANO_BIN` | _(built gateway)_ | Path to the own-engine gateway binary |
| `PROCESSOS_NANO_PORT` | `0` (auto) | Port for the spawned own engine |
| `PROCESSOS_NANO_DATA_DIR` | `.processos-nano-data` | Data dir for the spawned own engine |
| `PROCESSOS_NANO_CAPTURE` | `true` | Enable trace/stimulus capture on the own engine |
| `PROCESSOS_PYTHON` | `python3` | Interpreter for the `run_python` tool — point at a venv for the rich (pandas/duckdb) path |
| `PROCESSOS_PYTHON_TIMEOUT_SECS` | `20` | Wall-clock budget for a `run_python` call (child killed past it) |
| `PROCESSOS_PYTHON_MAX_OUTPUT` | `8000` | Max chars of combined stdout+stderr returned to the model |

### Capturing replay inputs on Nano (opt-in, on the engine being read)

Recorded-input replay needs the real inputs on the trace. These are **off by default**
(footprint + PII) and enabled on the *Nano* side:

- `NANOBPMN_TRACE_VARIABLES` — retain creation inputs + per-incident variable snapshots
  (`NANOBPMN_TRACE_VARIABLES_MAX_BYTES`, default 16384, caps each snapshot).
- `NANOBPMN_TRACE_STIMULI` — also retain the ordered external-stimulus log per instance
  (implies variable capture; `NANOBPMN_TRACE_STIMULI_MAX`, default 1024, caps the log).

When ProcessOS spawns its own engine with capture on, it sets `NANOBPMN_TRACE_STIMULI`
for it automatically.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/` | Landing page |
| `GET` | `/features` | Features page |
| `GET` | `/console` | Insights dashboard (fetches `/api/insights`) |
| `GET` | `/cockpit` | The cockpit — Console → Process → Experiment, with the droid conversation; `?workspace=&process=` opens the workspace-dataset investigation surface |
| `GET` | `/harness` | Optimization-harness dashboard (runs the example scenario) |
| `GET` | `/workspace` | Workspace browser — workspaces → processes → BPMN model + Insights + "Reason in cockpit" |
| `GET` | `/health` | Liveness (`ok`) |
| `GET` | `/api/insights?limit=&sample=` | Folded performance report from the **target** traces/metrics |
| `GET` | `/api/cockpit/overview` | Cockpit state: process(es), pilot, recent experiments |
| `GET`/`POST` | `/api/cockpit/experiments` | List / create experiments |
| `GET` | `/api/cockpit/experiments/{key}` | One experiment |
| `POST` | `/api/cockpit/experiments/{key}/decision` | The pilot's turn at `Review` (`accept`\|`iterate`\|`stop`) |
| `GET`/`POST` | `/api/cockpit/experiments/{key}/conversation` | Read / append to the persisted droid conversation |
| `GET` | `/api/harness/example` | The bundled example scenario JSON (a template to copy) |
| `GET` | `/api/harness/example/run` | Run the example scenario, return the ranked report |
| `POST` | `/api/harness/run` | Run a caller-supplied scenario, return the ranked report |
| `POST` | `/api/harness/calibrate` | Recalibrate a scenario from supplied measured distributions, then rank |
| `POST` | `/api/harness/hypothesize` | LLM proposes candidates; harness evaluates + ranks (`{scenario, llm?, includeBaked?, measured?, prompt?/promptId?}`) |
| `POST` | `/api/harness/replay` | Replay one historical instance against a candidate (Level-2 verifier) |
| `POST` | `/api/harness/replay-batch` | Batch replay over many instances |
| `POST` | `/api/harness/replay-rank` | Replay-rank a candidate population by fidelity gradient |
| `POST` | `/api/harness/evolve` | LLM-propose structural candidates, replay-rank them on real traces |
| `GET` | `/api/harness/production` | Live-source baseline a candidate search must beat |
| `GET` | `/api/harness/cluster?...&targetP99Ms=` | Cluster-aware measurement + optional Erlang-C staffing recommendation |
| `GET`/`POST` | `/api/prompts` | List / author (import) hypothesis prompts |
| `GET`/`DELETE` | `/api/prompts/{id}` | Get / delete a prompt (refuses built-ins) |
| `GET`/`PUT` | `/api/pilot` | Read / fork the pilot process BPMN |
| `POST` | `/api/pilot/reset` | Restore the built-in default pilot |
| `GET`/`POST` | `/api/workspaces` | List / create workspaces |
| `POST` | `/api/workspaces/seed-demo` | One-click demo: Northwind Bank + Loan Approval + generated traces + model |
| `GET` | `/api/workspaces/{workspace}` | A workspace + its processes |
| `POST` | `/api/workspaces/{workspace}/processes` | Create a process (bind `targetUrl` or `dataset`) |
| `GET`/`PUT` | `/api/workspaces/{workspace}/processes/{process}` | Read / update a process config |
| `GET`/`PUT` | `/api/workspaces/{workspace}/processes/{process}/model` | Read / set the process's BPMN model |
| `GET` | `/api/workspaces/{workspace}/processes/{process}/insights` | Insights folded over the process's bound source |
| `POST` | `/api/workspaces/{workspace}/processes/{process}/investigate` | One-shot LLM-driven investigation over the bound source via the `query_traces` SQL tool (+ optional `run_python` when `allowPython:true`) |
| `GET`/`POST` | `/api/workspaces/{workspace}/processes/{process}/chat` | Multi-turn cockpit chat: `GET` loads the persisted transcript; `POST {message,allowPython}` sends one turn and resumes from it |
| `POST` | `/api/workspaces/{workspace}/processes/{process}/chat/reset` | Forget this dataset's conversation |
| `GET`/`POST` | `/api/chat-prompts` | List / author reusable compose-box prompt templates (persisted to the config dir) |
| `DELETE` | `/api/chat-prompts/{id}` | Delete a non-built-in chat prompt |
| `GET` | `/assets/bpmn/{file}` | Vendored bpmn-js viewer assets (model rendering) |
| `GET`/`PUT` | `/api/settings` | Read settings / update the **globals** (`activeProfile`, `pythonBin`). `GET` lists LLM profiles (each redacting its key as `apiKeySet`) + the active profile + the Python interpreter |
| `POST` | `/api/settings/profiles` | Create a new LLM profile (optional `name`); returns the new `id` + the updated settings |
| `PUT`/`DELETE` | `/api/settings/profiles/{id}` | Partial-update / delete one profile. On `PUT`, absent fields are unchanged, an empty string (or `0`/negative number) clears a field back to the env default; the API key is set only when a non-empty `apiKey` is sent |
| `POST` | `/api/settings/models` | Query an endpoint for its model list (body: `profileId` + optional `provider`/`baseUrl`/`apiKey` overrides) so the console can pick a model id; each entry includes its `contextWindow` when the endpoint reports one |

## Layout

```
src/
  main.rs         webserver bootstrap, config (target vs own engine), routes, pages
  contracts.rs    typed mirror of Nano's read-contract DTOs + the HTTP client
  report.rs       pure aggregation: traces -> Insights (with unit tests)
  supervisor.rs   spawn + supervise the own Nano engine (learn port, deploy pilot, reap)
  pilot.rs        the forkable pilot process — plastic surface (a) of §10
  workspace.rs    workspaces/processes tree + persistence + CRUD + source binding
  dataset.rs      DatasetSource (loaded trace folder) + TraceSource enum (live | dataset)
  corpus.rs       synthetic labelled trace corpus: generator + inference + scorer
  analysis.rs     flatten traces into in-memory DuckDB; read-only query_traces surface
  agent.rs        provider-agnostic tool-calling loop (OpenAI transport) + lab notebook
  investigate.rs  LLM-driven investigation: analysis ToolBox + disciplined system prompt
  pyrunner.rs     optional run_python escape hatch: CSV export + timeout subprocess runner
  settings.rs     operator-editable LLM + Python settings, persisted to ~/.config/processos
  cockpit.rs      the cockpit: Console -> Process -> Experiment surface
  chat.rs         interactive cockpit chat: file-backed transcript store + view projection
  chat_prompts.rs reusable compose-box prompt library, persisted to the config dir
  conversation.rs persisted cockpit conversations + data-dir resolution
  harness/
    mod.rs         scenario / worker / variant types + seeded PRNG
    sim.rs         SimRunner: drives engine-core on a virtual clock (M0)
    rank.rs        candidate enumeration + evaluation + ranking (M1)
    llm.rs         pluggable LLM client: openai-compatible + anthropic (M2)
    prompts.rs     authorable, selectable prompt library (M2)
    hypothesize.rs LLM proposes candidates, validate + evaluate + rank (M2)
    calibrate.rs   recalibrate SimRunner worker models from measured production
    cluster.rs     cluster-aware measurement (M3): topology union, tail, queue/service split
    queueing.rs    Erlang-C worker-pool staffing recommendation (advisory)
    production.rs  live-source baseline (generation-skipped path)
    replay.rs      recorded-input replay evaluator (Level-2 verifier)
    replay_rank.rs replay-rank a candidate population by fidelity gradient
    evolve.rs      LLM-propose structural candidates, replay-rank on real traces
    example.rs     the bundled Classify->Summarize worker-swap demo
```

`engine-core` is consumed read-only over a `path` dependency and is never modified —
the harness embeds it for exact native replay without ever adding a back-edge from Nano
to ProcessOS.
