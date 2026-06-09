# nanobpmn

A self-contained Rust code-generation project for the Camunda 8 Orchestration
Cluster REST API. It bundles a copy of the OpenAPI specification and generates a
Rust REST layer (models + `axum` router + service traits) from it, plus a
runnable stub server.

No backend services are wired into *most* of the REST layer: operations respond
with `501 Not Implemented`. A few operations are now backed by the embedded
**`engine-core`** BPMN engine as a proof of the REST → engine path:

- `POST /v2/deployments` (`createDeployment`) parses the uploaded BPMN 2.0 XML
  resources and deploys them, assigning each process a key and a per-id version.
- `POST /v2/process-instances` (`createProcessInstance`, by `processDefinitionId`)
  starts a real instance and returns its engine-assigned key.
- `POST /v2/jobs/{jobKey}/completion` (`completeJob`) completes the job and
  resumes the token.

A demo process (`processDefinitionId: "demo"`, a single service task) is
pre-deployed at server startup, but you can also deploy your own `.bpmn` files
through the deployment endpoint. Engine and parse errors map to real status
codes (`400`/`404`/`409`); everything else is still `501`.

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

The `server/` crate wires the generated REST layer into a runnable `axum` server.
Most operations return `Err(())`, which the server's `ErrorHandler` maps to a
`501 Not Implemented` response, so the whole API surface is routable end to end.
A few operations are backed by the embedded `engine-core` engine (see above):

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

# Engine-backed: complete the resulting job -> 204, token resumes, instance ends
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
