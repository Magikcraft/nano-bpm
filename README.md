# nanobpmn

A self-contained Rust code-generation project for the Camunda 8 Orchestration
Cluster REST API. It bundles a copy of the OpenAPI specification and generates a
Rust REST layer (models + `axum` router + service traits) from it, plus a
runnable stub server.

No backend services are wired into the REST layer: every operation responds with
`501 Not Implemented`. The project also contains **`engine-core/`**, an
embeddable, dependency-free BPMN execution engine that the REST layer will
eventually call.

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

## Approach

The REST layer is generated with [OpenAPI Generator](https://openapi-generator.tech)
using its [`rust-axum`](https://openapi-generator.tech/docs/generators/rust-axum)
server generator (run via Docker, version-pinned). It produces a self-contained
library crate (`camunda-gateway-rest`) with:

- **`src/models.rs`** — serde structs for every schema in the spec.
- **`src/apis/`** — one trait per API tag, with one async method per operation.
- **`src/server/mod.rs`** — an `axum` router (`server::new(api_impl)`) that
  extracts requests, dispatches to the trait implementation, and serializes
  responses.

## Layout

```
nanobpmn/
├── Makefile                       # generate / build / run / fmt / clippy / clean
├── openapi-generator-config.yaml  # generator configuration
├── spec/                          # bundled OpenAPI spec (source of truth)
│   ├── rest-api.yaml              # entrypoint ($refs the sibling files)
│   └── *.yaml
├── scripts/
│   ├── generate.sh                # end-to-end generation pipeline
│   ├── preprocess-spec.py         # sanitizes a temp copy of the spec
│   ├── postprocess-generated.py   # patches known rust-axum generator bugs
│   └── gen-stub-server.py         # generates the server's stub trait impls
├── server/                        # runnable stub server (binary crate)
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs                # ServerImpl, auth/error glue, bootstrap
│       └── stub_impls.rs          # generated trait impls (git-ignored)
├── build/                         # temp sanitized spec (git-ignored)
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
    │   └── engine.rs              # single-writer loop + processor
    └── tests/public_api.rs
```

The `generated/` crate, `build/`, and `server/src/stub_impls.rs` are **build
artifacts** and are git-ignored. Regenerate them on demand with `make generate`.
The `spec/` tree is the committed source of truth.

## Requirements

- Docker (to run the pinned `openapi-generator-cli` image)
- A Rust toolchain (`cargo`, `rustfmt`)
- Python 3 with PyYAML (for the spec pre-processing step)

## Usage

```bash
# Generate the Rust REST layer + server stubs from spec/
make generate

# Generate if needed, then compile both crates
make build

# Run the stub server (defaults to port 8080; override with PORT)
make run
PORT=18080 make run

# Lint / format
make clippy
make fmt

# Remove all generated artifacts
make clean
```

## Engine (`engine-core`)

The embeddable BPMN engine builds and tests with plain `cargo` — no Docker, no
code generation:

```bash
make engine-test    # unit + integration + doc tests
make engine-build   # debug build
make engine-wasm    # prove it compiles for wasm32 (needs the wasm32 target)
```

See [`engine-core/README.md`](engine-core/README.md) for the architecture.

## Stub server

The `server/` crate wires the generated REST layer into a runnable `axum` server
with **no backends connected**. It implements every generated `apis::*` trait on a
single `ServerImpl` type, where each operation returns `Err(())`. That error is
mapped by the server's `ErrorHandler` to a `501 Not Implemented` response, so the
whole API surface is routable end to end:

```console
$ PORT=18080 make run
... listening on http://0.0.0.0:18080/v2

$ curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:18080/v2/process-instances/search \
    -H 'Authorization: Bearer x' -H 'Content-Type: application/json' -d '{}'
501

$ curl -s localhost:18080/v2/license -H 'Authorization: Bearer x'
Not implemented: backend services are not wired yet.
```

The `ServerImpl` type, authentication, and error glue live in `server/src/main.rs`
(committed). The per-tag trait impls are generated into `server/src/stub_impls.rs`
by `gen-stub-server.py` and regenerated whenever the spec changes.

## Generation pipeline

`scripts/generate.sh` runs four stages:

1. **Preprocess** (`preprocess-spec.py`) — the bundled spec in `spec/` is never
   edited. It is copied into `build/spec/`, where a few constructs that the beta
   `rust-axum` generator cannot handle are sanitized (currently: schema-less
   request bodies such as `content: { application/json: {} }`). Files that need
   no change are copied verbatim to keep the transform minimal.
2. **Generate** — `openapi-generator-cli` (Docker, version-pinned) emits the
   crate into `generated/`.
3. **Post-process** (`postprocess-generated.py`) — deterministically patches
   known `rust-axum` code-generation bugs so the crate compiles (an invalid
   `oneOf` date-time enum variant, and discriminator helpers for optional
   `type` fields).
4. **Stub impls** (`gen-stub-server.py`) — parses the generated trait
   definitions and emits `server/src/stub_impls.rs`.

## Updating the spec

`spec/` is a copy of the Camunda v2 OpenAPI spec
(`zeebe/gateway-protocol/src/main/proto/v2` in `camunda/camunda`). To refresh it,
replace the files under `spec/` and run `make generate`.
