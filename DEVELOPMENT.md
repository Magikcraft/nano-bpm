# Developing Nano BPM

This document covers **building Nano BPM from source** and the internals of the
code-generation pipeline. **End users do not need any of this** — see the
[README](README.md) and install the prebuilt binary through the
[`c8ctl-plugin-nano`](https://github.com/jwulf/c8ctl-plugin-nano) plugin, which
ships a platform binary per OS/arch as an npm package.

Under the hood, Nano BPM is a self-contained Rust code-generation project for the
Camunda 8 Orchestration Cluster REST API. It bundles a copy of the OpenAPI
specification and generates a Rust REST layer (models + `axum` router + service
traits) from it, plus a runnable server wired to the embedded `engine-core` BPMN
engine.

## Two crates

nanobpmn is deliberately split so the execution engine stays embeddable
(including on mobile via FFI and in the browser via wasm) while the REST layer
remains a server-only concern:

| Crate | What it is | Runs where |
| --- | --- | --- |
| **`engine-core/`** | The BPMN engine: a deterministic single-writer `command → event → applier` state machine. **Zero dependencies, `std`-only.** | Server, iOS/Android (FFI, e.g. UniFFI), `wasm32` |
| **`server/`** + `generated/` | The Camunda 8 v2 REST API generated from `spec/`, with a stub server. | Server only |

You would **not** run the HTTP server on a phone; there you embed `engine-core`
directly and call it through generated bindings. See
[`engine-core/README.md`](engine-core/README.md) for the architecture and the
rationale for following the Camunda 8 (Zeebe) model rather than the Camunda 7 PVM.

## Requirements

- A Java runtime (JRE/JDK 11+) — runs the pinned `openapi-generator-cli` JAR,
  which is downloaded once into `build/tools/` (no Docker, no global install)
- A Rust toolchain (`cargo`, `rustfmt`)
- Python 3 with PyYAML (for the spec pre-processing step). The repo uses
  [`uv`](https://docs.astral.sh/uv/); `uv sync` provisions it.
- `curl` or `wget` (to fetch the generator JAR on first run)
- Node.js (only to build the web console frontend under `console/`)
- **Linux only:** the [`mold`](https://github.com/rui314/mold) linker. The
  workspace `.cargo/config.toml` wires `mold` as the linker for the
  `x86_64-unknown-linux-gnu` target (it's much faster, and the deploy/soak
  build leans on it), so a Linux `cargo build` fails with `cannot find 'ld'`
  until it's installed: `sudo apt-get install -y mold`. macOS builds are
  unaffected (the config is scoped to the Linux target triple only).

> Java is **build-time only**: it runs the OpenAPI generator. The resulting
> binary has no Java (or other) runtime dependency.

## Building

```bash
# Generate the Rust REST layer + server stubs from spec/
make generate

# Generate if needed, then compile both crates
make build

# Build the optimized production server binary (includes the web console)
make release
# -> server/target/release/nanobpm-gateway-rest-server

# Build the API-only gateway (no console / landing page / Swagger UI)
make release-gateway

# Run the server (defaults to port 8080; override with PORT)
make run
PORT=18080 make run

# Lint / format
make clippy
make fmt

# Remove all generated artifacts
make clean
```

`make release` produces a single self-contained binary at
`server/target/release/nanobpm-gateway-rest-server`. See the
[README](README.md#running-the-binary) for how end users run and configure it.

> The generated REST layer under `generated/` is a build dependency, so
> `make release` runs `make generate` first if needed (which downloads and runs
> the `openapi-generator-cli` JAR with local Java — no Docker).
> Once generated, the binary itself has no run-time external dependencies. (At
> build time the vendored jemalloc allocator is compiled from source, so a C
> compiler — `cc`/`clang`, already present on macOS and most Linux toolchains —
> is required; the resulting binary is still self-contained.)

## Building the web console

The web console lives behind the **`console`** Cargo feature (see
[Web console](README.md#web-console) in the README for what it does). The gateway
embeds the built frontend from `../console/dist`, so the frontend must be built
first.

```bash
# 1. Build the frontend bundle (also vendors Swagger UI + bundles the spec and
#    renders the docs site). Required before any console build.
cd console && npm install && npm run build && cd ..

# 2a. Debug: rust-embed reads ../console/dist from disk at runtime, so frontend
#     rebuilds are picked up live without recompiling the gateway.
cargo build --features console --bin nanobpm-gateway-rest-server
NANOBPMN_DATA_DIR=./nanobpm.data PORT=8080 \
  ./server/target/debug/nanobpm-gateway-rest-server

# 2b. Release: `make release` builds the frontend and the console-enabled,
#     optimized single-file binary in the right order (it also forces a
#     re-embed so a rebuilt frontend is baked in). Equivalent to `make console`.
make release
```

> **Note:** the `console` feature is required for the console, landing page and
> Swagger UI. `make release` includes it; a plain `cargo build --release` (or
> `make release-gateway`) produces the default API-only gateway, where `/console`
> returns 404. The binaries shipped via the c8ctl plugin are built **with** the
> console feature.

## Repository layout

```
nanobpmn/
├── Makefile                       # generate / build / run / fmt / clippy / clean
├── openapi-generator-config.yaml  # generator configuration
├── spec/                          # bundled OpenAPI spec (upstream, source of truth)
│   ├── rest-api.yaml              # entrypoint ($refs the sibling files)
│   └── *.yaml
├── spec-patches/                  # local overlays applied to the build copy
│   └── patches.yaml               # project-specific spec additions (spec/ stays pristine)
├── scripts/
│   ├── generate.sh                # end-to-end generation pipeline
│   ├── preprocess-spec.py         # sanitizes + overlays a temp copy of the spec
│   ├── postprocess-generated.py   # patches known rust-axum generator bugs
│   └── gen-stub-server.py         # generates the server's stub trait impls
├── server/                        # runnable server (binary crate)
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs                # ServerImpl, auth/error glue, bootstrap
│       └── stub_impls.rs          # generated trait impls (git-ignored)
├── console/                       # web console frontend (Vite/React + wasm engine)
├── build/                         # temp sanitized spec + cached generator JAR (git-ignored)
└── generated/                     # generated library crate (git-ignored)
```

The engine-core crate sits alongside these:

```
nanobpmn/
└── engine-core/                   # embeddable BPMN engine (zero-dep, std-only)
    ├── Cargo.toml
    ├── src/
    │   ├── lib.rs                 # crate docs + public API
    │   ├── model.rs               # ProcessDefinition / Element + ProcessBuilder
    │   ├── command.rs             # Command enum (engine inputs)
    │   ├── event.rs               # Event enum (engine facts)
    │   ├── state.rs               # State + apply() — the sole mutator
    │   ├── engine.rs              # single-writer loop + processor
    │   └── ffi.rs                 # coarse C-ABI surface (feature "ffi")
    ├── scripts/verify-wasm-ffi.mjs # asserts the wasm FFI exports + a round-trip
    └── tests/public_api.rs
```

The `generated/` crate, `build/`, and `server/src/stub_impls.rs` are **build
artifacts** and are git-ignored. Regenerate them on demand with `make generate`.
The `spec/` tree is the committed source of truth.

## How the REST layer is generated

The REST layer is generated with [OpenAPI Generator](https://openapi-generator.tech)
using its [`rust-axum`](https://openapi-generator.tech/docs/generators/rust-axum)
server generator (run from the version-pinned `openapi-generator-cli` JAR via
the local Java runtime — no Docker required). It produces a self-contained
library crate (`nanobpm-gateway-rest`) with:

- **`src/models.rs`** — serde structs for every schema in the spec.
- **`src/apis/`** — one trait per API tag, with one async method per operation.
- **`src/server/mod.rs`** — an `axum` router (`server::new(api_impl)`) that
  extracts requests, dispatches to the trait implementation, and serializes
  responses.

### Generation pipeline

`scripts/generate.sh` runs four stages:

1. **Preprocess** (`preprocess-spec.py`) — the bundled spec in `spec/` is never
   edited. It is copied into `build/spec/`, where a few constructs that the beta
   `rust-axum` generator cannot handle are sanitized (currently: schema-less
   request bodies such as `content: { application/json: {} }`), and any local
   overlays from `spec-patches/patches.yaml` are applied (see
   [Local spec overlays](#local-spec-overlays)). Files that need no change are
   copied verbatim to keep the transform minimal.
2. **Generate** — the version-pinned `openapi-generator-cli` JAR (downloaded once
   into `build/tools/` and run with local Java — no Docker) emits the crate into
   `generated/`.
3. **Post-process** (`postprocess-generated.py`) — deterministically patches
   known `rust-axum` code-generation bugs so the crate compiles and behaves
   correctly (an invalid `oneOf` date-time enum variant, discriminator helpers
   for optional `type` fields, and `#[serde(deny_unknown_fields)]` on the four
   pagination structs so the untagged `SearchQueryPageRequest` can disambiguate
   limit/offset/forward-cursor/backward-cursor requests instead of always
   collapsing to limit pagination).
4. **Stub impls** (`gen-stub-server.py`) — parses the generated trait
   definitions and emits `server/src/stub_impls.rs`.

### Updating the spec

`spec/` is a copy of the Camunda v2 OpenAPI spec
(`zeebe/gateway-protocol/src/main/proto/v2` in `camunda/camunda`). To refresh it,
replace the files under `spec/` and run `make generate`.

### Local spec overlays

`spec/` is kept byte-for-byte identical to the upstream Camunda release so it can
be refreshed by simply replacing files. Project-specific additions to the API
contract live separately in `spec-patches/patches.yaml` and are applied to the
build copy (`build/spec/`) during preprocessing — `spec/` is never mutated.

Each overlay entry names a `file` (relative to `spec/`) and a dotted `target`
path inside it, then either deep-`merge`s a mapping or `append`s items to a list:

```yaml
- file: process-instances.yaml
  target: components.schemas.CreateProcessInstanceResult.properties
  merge:
    processCompleted: { type: boolean, description: "…" }
- file: process-instances.yaml
  target: components.schemas.CreateProcessInstanceResult.required
  append: [processCompleted]
```

This is how nanobpmn adds the `processCompleted` flag to
`CreateProcessInstanceResult` (it reports whether the returned variables are the
authoritative final result) without forking the upstream spec.

## Engine (`engine-core`)

The embeddable BPMN engine builds and tests with plain `cargo` — no Docker, no
code generation:

```bash
make engine-test    # unit + integration + doc tests
make engine-build   # debug build
make engine-wasm    # prove it compiles for wasm32 (needs the wasm32 target)
make engine-wasm-ffi # build the FFI cdylib for wasm32 + verify exports & a round-trip (needs node)
```

See [`engine-core/README.md`](engine-core/README.md) for the architecture. The
engine also exposes a coarse C-ABI (`src/ffi.rs`, behind the `ffi` feature) for
embedding via UniFFI on mobile or as wasm exports in a browser; `make
engine-wasm-ffi` proves that wasm/FFI build end to end.

## Server internals

The `server/` crate wires the generated REST layer into a runnable `axum` server.
Most operations return `Err(())`, which the server's `ErrorHandler` maps to a
`501 Not Implemented` response, so the whole API surface is routable end to end.
A few operations are backed by the embedded `engine-core` engine:

```console
$ PORT=18080 make run
... listening on http://0.0.0.0:18080/v2

# Engine-backed: deploy a BPMN file -> process is parsed and versioned
$ curl -s -X POST localhost:18080/v2/deployments \
    -H 'Authorization: Bearer x' -F 'resources=@order.bpmn'
{"deploymentKey":"3","tenantId":"<default>","deployments":[{"processDefinition":
  {"processDefinitionId":"shipping","processDefinitionVersion":1,
   "resourceName":"order.bpmn","processDefinitionKey":"4",...},...}]}

# Engine-backed: start an instance of the just-deployed process
$ curl -s -X POST localhost:18080/v2/process-instances \
    -H 'Authorization: Bearer x' -H 'Content-Type: application/json' \
    -d '{"processDefinitionId":"shipping"}'
{"processDefinitionId":"shipping",...,"processInstanceKey":"5",...}

# Engine-backed: activate the job parked on the service task -> locked to "w1"
$ curl -s -X POST localhost:18080/v2/jobs/activation \
    -H 'Authorization: Bearer x' -H 'Content-Type: application/json' \
    -d '{"type":"ship","worker":"w1","timeout":60000,"maxJobsToActivate":10}'
{"jobs":[{"type":"ship",...,"jobKey":"8","deadline":...,...}]}

# Engine-backed: complete the activated job -> 204, token resumes, instance ends
$ curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:18080/v2/jobs/8/completion \
    -H 'Authorization: Bearer x' -H 'Content-Type: application/json' -d '{}'
204

# Still a stub:
$ curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:18080/v2/process-instances/search \
    -H 'Authorization: Bearer x' -H 'Content-Type: application/json' -d '{}'
501
```

The `ServerImpl` type (which owns the embedded engine), authentication, error
glue, and the engine-backed handlers live in `server/src/main.rs` (committed).
The per-tag trait impls are generated into `server/src/stub_impls.rs` by
`gen-stub-server.py`, which routes the wired operations to the handlers via its
`OVERRIDES` table and stubs everything else.
