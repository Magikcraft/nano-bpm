# Nano repositories — how the pieces fit

Nano is developed across **three repositories**. This is the map: what each one
is, where each piece lives, and how they depend on one another.

| Repo | Product name | Role |
| --- | --- | --- |
| [`Magikcraft/nano-bpm`](https://github.com/Magikcraft/nano-bpm) | **Nano BPM** | The core product: engine, gateway server, web console, embeddable engine libraries, and schemas. The source of truth. |
| [`jwulf/nano-ide`](https://github.com/jwulf/nano-ide) | **Nano IDE** | Extension packs (languages, app templates, examples, themes, triggers) for the console's RAD IDE. Published to npm. |
| [`jwulf/c8ctl-plugin-nano`](https://github.com/jwulf/c8ctl-plugin-nano) | **c8ctl Nano plugin** | A `c8ctl` CLI plugin that installs, runs, and manages a local Nano BPM cluster; ships the prebuilt gateway binary; and turns CLI agents into job workers. |

At a glance, the dependency direction is one-way into Nano BPM:

```
   nano-ide packs ──(npm, "nano-ide-ext" keyword)──▶ Nano BPM console
                                                          │
                          Nano BPM  ◀───────────── source of truth
              (engine + server + console + libs + schemas)
                          │  gateway binary (release)     ▲
                          ▼                               │
   c8ctl-plugin-nano ──(prebuilt binary + `c8ctl nano`)──┘  runs a local cluster
```

---

## 1. Nano BPM — the core (`Magikcraft/nano-bpm`)

A Rust monorepo that produces the `nanobpmn` gateway binary and the embeddable
engine. It owns the REST contract, the console, the extension-manifest schema,
and the release pipeline. **Everything else depends on it; it depends on nothing
in the other two repos.**

Where each piece lives:

| Path | What |
| --- | --- |
| `engine-core/` | The deterministic, event-sourced BPMN engine (pure Rust crate, no I/O). |
| `server/` | The `nanobpmn` gateway binary: Camunda 8-compatible **v2 REST API**, append-only journal, SQLite read model, optional multi-node Raft replication, and the built-in **web console** host. |
| `server/src/console/extensions.rs` | The **authoritative extension-manifest schema** that Nano IDE packs target (see repo 2). |
| `console/` | The built-in web console SPA — Modeler, RAD IDE, Explorer, Workers, Topology/Metrics. |
| `engine-wasm/` + `clients/nano-bernd/` | The engine compiled to WASM and wrapped as an **embeddable library** with two hosts: `@nanobpm/nano-bernd` (npm) and `io.github.jwulf:nano-bernd` (JVM / Maven Central). |
| `clients/` | Client transports (e.g. the `node-stream` command-stream client). |
| `spec/`, `spec-app/`, `spec-console/` | JSON Schemas / OpenAPI specs, published to `nanobpm.io`. |

**Release trains** (see [`RELEASE.md`](../RELEASE.md)) — three independent trains
cut from the same `main` commit:

1. **Gateway binary** (`v*`) → published to the **`c8ctl-plugin-nano` repo
   releases** + an S3 mirror. This is the binary the CLI plugin ships (repo 3).
2. **`@nanobpm/nano-bernd`** (embedded engine, npm).
3. **`io.github.jwulf:nano-bernd`** (embedded engine, JVM / Maven Central).

---

## 2. Nano IDE — console extension packs (`jwulf/nano-ide`)

A monorepo of npm packages under `packages/*`. Each package is a self-contained
**extension pack** for the Nano BPM console's RAD IDE. It is *content for* the
console; it is not a running service.

- **Contract.** Every pack ships a `nano-ide.ext.json` manifest. That manifest is
  a **mirror of the authoritative schema in Nano BPM**
  (`server/src/console/extensions.rs`) — Nano BPM is the single source of truth;
  Nano IDE targets it.
- **Discovery.** The console finds packs by the `nano-ide-ext` npm keyword,
  installs the tarball into the workspace, reads the manifest, and wires the pack
  in. No `preinstall`/`postinstall` scripts run.
- **Baseline.** The `deno` runtime and the `deno-gui` app template ship built
  into the server so the console works offline with zero installs; every other
  pack (including `rust`) is installed from here.
- **Pack kinds:** `lang` (file types + toolchain + templates), `app` (a runnable
  project template), `example` (a complete app copied into a new project),
  `theme` (console colour themes as pure data), and `trigger` (an event source
  that starts processes).

**Direction of dependency:** `nano-ide` → `nano-bpm` (targets its manifest
contract). Nano BPM's console consumes these packs **at runtime, from npm**.

---

## 3. c8ctl Nano plugin — run + manage + agentic workers (`jwulf/c8ctl-plugin-nano`)

A plugin for [`c8ctl`](https://github.com/camunda/c8ctl) (the Camunda 8 CLI). It
adds the `c8ctl nano` command and is the easiest way to run and operate Nano BPM
locally.

- **Ships the prebuilt gateway binary.** Per-platform npm `optionalDependencies`
  (under the `@nanobpm/` scope) carry the compiled `nanobpmn` binary that Nano
  BPM's release workflow produced — so there is nothing to compile.
- **Cluster ops:** `start`, `stop`, `status`, `restart`, `logs`, `pause`,
  `resume`, `clean`, `set`, `config`, `update`.
- **Agentic job workers:** `hire` / `work` turn an interactive CLI agent harness
  (Copilot CLI, Claude CLI, …) into a Nano BPM job worker.

**Direction of dependency:** `c8ctl-plugin-nano` → `nano-bpm` (consumes its
compiled binary). Nano BPM's README points users here for the quick start.

---

## How they fit together

**Nano BPM is the single source of truth.** It compiles the gateway binary and
the embedded engine, and owns the REST contract, the console, and the
extension-manifest schema. The other two repos consume it — neither is a
dependency of Nano BPM.

Two publish/consume flows connect them:

- **Binary distribution.** Nano BPM CI builds the gateway binary → publishes it
  to `c8ctl-plugin-nano` releases (+ S3) → the plugin packages it per platform on
  npm → end users get it via `c8ctl nano`.
- **Extensions.** Nano IDE publishes packs → npm → Nano BPM's console discovers
  and installs them at runtime by the `nano-ide-ext` keyword.

**End-user paths:**

- **Run a cluster:** install `c8ctl` → `c8ctl load plugin c8ctl-plugin-nano` →
  `c8ctl nano start`.
- **Extend the IDE:** from the console UI, install a pack by its `nano-ide-ext`
  keyword.
- **Embed the engine:** depend on `@nanobpm/nano-bernd` (npm) or
  `io.github.jwulf:nano-bernd` (JVM).
