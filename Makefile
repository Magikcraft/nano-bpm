# nanobpmn — Orchestration Cluster REST layer code generation (Rust / rust-axum).
#
# Quick start:
#   make setup       — check the toolchain (rust, uv, java), provision the Python
#                      build env via `uv sync`, then generate the REST stubs.
#   make all         — build nano top-to-bottom: generate the REST stubs, then
#                      compile the gateway (server) and ProcessOS (debug).
#   make all-release — same, but build optimized release binaries of the gateway
#                      (nano) and ProcessOS.
#   make release     — the optimized self-contained distribution (gateway with the
#                      embedded web console + Swagger).
#   make debug       — the same full self-contained stack as `make release`, but an
#                      unoptimized debug build — far faster to compile, for a tight
#                      local iteration loop.
#
# `make generate` produces the Rust REST layer + server stubs from spec/; the
# output under generated/ and server/src/stub_impls.rs is git-ignored.

SHELL := /usr/bin/env bash
PROJECT_ROOT := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
GENERATED_DIR := $(PROJECT_ROOT)/generated
CONSOLE_GENERATED_DIR := $(PROJECT_ROOT)/generated-console
STUB_IMPLS := $(PROJECT_ROOT)/server/src/stub_impls.rs
ENGINE_DIR := $(PROJECT_ROOT)/engine-core
CONSOLE_DIR := $(PROJECT_ROOT)/console
PROCESSOS_DIR := $(PROJECT_ROOT)/processos
WASM_DIR := $(PROJECT_ROOT)/engine-wasm
UV := uv

# Spec inputs for the generated crates. Listing these as prerequisites of the
# codegen targets makes `make` regenerate when the spec (or its generator
# config/patches/script) changes — otherwise, once generated/ exists, make
# treats it as up to date forever and a spec change silently leaves the crate
# stale (e.g. new endpoints added to spec-console/console-api.yaml fail the
# release build with "cannot find type ...PathParams").
REST_SPEC_SRCS := $(wildcard $(PROJECT_ROOT)/spec/*.yaml) \
	$(wildcard $(PROJECT_ROOT)/spec-patches/*.yaml) \
	$(PROJECT_ROOT)/openapi-generator-config.yaml \
	$(PROJECT_ROOT)/scripts/generate.sh
CONSOLE_SPEC_SRCS := $(PROJECT_ROOT)/spec-console/console-api.yaml \
	$(PROJECT_ROOT)/openapi-generator-config-console.yaml \
	$(PROJECT_ROOT)/scripts/generate-console.sh

# Canonical formatter: pinned nightly rustfmt. The repo's rustfmt.toml uses
# unstable options (group_imports), which stable `cargo fmt` silently ignores —
# producing import-ordering drift. Always format via this toolchain so the tree
# matches the `make fmt-check` CI gate. Bump deliberately (single style commit).
FMT_TOOLCHAIN := nightly-2026-06-26
FMT_CRATES := $(ENGINE_DIR) $(PROJECT_ROOT)/server $(PROCESSOS_DIR) $(WASM_DIR)

.DEFAULT_GOAL := build

.PHONY: setup
setup: check-deps ## Verify the toolchain, provision the Python build env (uv sync), then generate the REST stubs — run this first
	$(UV) sync
	$(MAKE) generate
	@echo
	@echo "Setup complete. Build everything with: make all"

.PHONY: check-deps
check-deps: ## Check that the required toolchain is installed (rust, uv, java) and report optional tools
	@missing=0; \
	chk() { \
	  if command -v "$$1" >/dev/null 2>&1; then \
	    printf '  \033[32m✓\033[0m %-10s %s\n' "$$1" "$$($$2 2>&1 | head -n1)"; \
	  else \
	    printf '  \033[31m✗\033[0m %-10s MISSING — %s\n' "$$1" "$$3"; \
	    [ "$$4" = required ] && missing=1 || true; \
	  fi; \
	}; \
	echo "Required:"; \
	chk cargo "cargo --version" "install Rust via https://rustup.rs" required; \
	chk uv "uv --version" "install uv: https://docs.astral.sh/uv/getting-started/installation/" required; \
	chk java "java -version" "install a JRE/JDK 11+ for the OpenAPI generator" required; \
	echo "Optional (web console / wasm):"; \
	chk node "node --version" "install Node.js to build the console SPA" optional; \
	chk npm "npm --version" "bundled with Node.js" optional; \
	chk wasm-pack "wasm-pack --version" "cargo install wasm-pack (else committed wasm artifacts are used)" optional; \
	if [ "$$missing" -ne 0 ]; then \
	  echo; echo "error: required tools are missing (see ✗ above). Install them and re-run 'make setup'."; \
	  exit 1; \
	fi

.PHONY: all
all: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) ## Build nano top-to-bottom: generate REST stubs, then compile the gateway (nano) and ProcessOS
	cd $(GENERATED_DIR) && cargo build
	cd $(PROJECT_ROOT)/server && cargo build
	cd $(PROCESSOS_DIR) && cargo build
	@echo
	@echo "Built nano top-to-bottom: generated REST crate + gateway (server) + ProcessOS."

.PHONY: all-release
all-release: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) ## Build optimized release binaries of nano (gateway) and ProcessOS top-to-bottom
	cd $(GENERATED_DIR) && cargo build --release
	cd $(PROJECT_ROOT)/server && cargo build --release
	cd $(PROCESSOS_DIR) && cargo build --release
	@echo
	@echo "Built release binaries:"
	@echo "  gateway (nano): $(PROJECT_ROOT)/server/target/release/nanobpm-gateway-rest-server"
	@echo "  ProcessOS:      $(PROCESSOS_DIR)/target/release/processos"

.PHONY: generate
generate: ## Generate the Rust REST layer + server stub impls from spec/ (needs local Java)
	./scripts/generate.sh
	./scripts/generate-console.sh

.PHONY: generate-app-manifest
generate-app-manifest: ## Generate the Urban App manifest TypeScript types from spec-app/nano-app.schema.json (ADR 0027; needs Node)
	./scripts/generate-app-manifest.sh

$(GENERATED_DIR)/Cargo.toml: $(REST_SPEC_SRCS)
	$(MAKE) generate

$(CONSOLE_GENERATED_DIR)/Cargo.toml: $(CONSOLE_SPEC_SRCS)
	./scripts/generate-console.sh

# stub_impls.rs is generated (git-ignored) and wires each REST trait method to
# its hand-written `*_impl` on ServerImpl via the delegation map in
# scripts/gen-stub-server.py. Depend on that script so a changed delegation map
# regenerates the stub: otherwise a stale local copy leaves newly wired `*_impl`
# methods reachable only from tests, and since `make release` builds the bin
# alone (no --all-targets), `warnings = "deny"` rejects them as dead code — a
# failure CI never sees because it regenerates the stub and compiles tests.
# When the generated apis already exist this is a fast, Java-free re-run of just
# the Python stub step; otherwise fall back to a full `make generate`.
$(STUB_IMPLS): scripts/gen-stub-server.py $(GENERATED_DIR)/Cargo.toml
	@if [ -d "$(GENERATED_DIR)/src/apis" ]; then \
		echo "Regenerating $(STUB_IMPLS) from $(GENERATED_DIR)/src/apis"; \
		python3 scripts/gen-stub-server.py "$(GENERATED_DIR)/src/apis" "$(STUB_IMPLS)"; \
		command -v rustfmt >/dev/null 2>&1 && rustfmt --edition 2024 "$(STUB_IMPLS)" || true; \
	else \
		$(MAKE) generate; \
	fi

.PHONY: build
build: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) ## Compile the generated crate and the stub server
	cd $(GENERATED_DIR) && cargo build
	cd $(PROJECT_ROOT)/server && cargo build

.PHONY: release
release: $(GENERATED_DIR)/Cargo.toml $(CONSOLE_GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) console-frontend ## Build the optimized self-contained distribution (gateway + embedded console + Swagger)
	@# Force the RustEmbed derive to re-run so the just-built console/dist (which
	@# release builds bake in at compile time) is embedded, even if the gateway
	@# sources are otherwise unchanged.
	touch $(PROJECT_ROOT)/server/src/console/mod.rs
	cd $(PROJECT_ROOT)/server && cargo build --release --features console
	@echo "Built self-contained distribution: $(PROJECT_ROOT)/server/target/release/nanobpm-gateway-rest-server"
	@echo "  landing /  ·  console /console  ·  API docs /swagger  ·  REST /v2"

.PHONY: debug
debug: $(GENERATED_DIR)/Cargo.toml $(CONSOLE_GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) console-frontend ## Full self-contained stack (gateway + embedded console + Swagger) as an UNOPTIMIZED debug build — same as `release` but far faster to compile, for a tight local loop
	@# Force the RustEmbed derive to re-run so the just-built console/dist is
	@# embedded, even if the gateway sources are otherwise unchanged (mirrors
	@# `release`; the embed is a compile-time bake regardless of profile).
	touch $(PROJECT_ROOT)/server/src/console/mod.rs
	cd $(PROJECT_ROOT)/server && cargo build --features console
	@echo "Built self-contained debug distribution: $(PROJECT_ROOT)/server/target/debug/nanobpm-gateway-rest-server"
	@echo "  landing /  ·  console /console  ·  API docs /swagger  ·  REST /v2"

.PHONY: release-gateway
release-gateway: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) ## Build the optimized API-only gateway (no web console)
	cd $(PROJECT_ROOT)/server && cargo build --release
	@echo "Built API-only gateway: $(PROJECT_ROOT)/server/target/release/nanobpm-gateway-rest-server"

.PHONY: console-frontend
console-frontend: console-wasm bojtos ## Build the web console SPA (console/ -> console/dist)
	cd $(CONSOLE_DIR) && npm install && npm run build

.PHONY: bojtos
bojtos: console-wasm ## Rebuild the Bojtos packages (@nanobpm/bojtos-kit + @nanobpm/bojtos-react) from source into their committed dist/ (ADR 0043 §8 step 2). The kit is built first; bojtos-react depends on its dist via a file: link.
	cd $(PROJECT_ROOT)/bojtos-kit && npm install && npm run build
	cd $(PROJECT_ROOT)/bojtos-react && npm install && npm run build

.PHONY: console-wasm
console-wasm: ## Regenerate the in-browser engine package (engine-wasm -> engine-wasm/pkg, the @nanobpm/engine-wasm package). Needs wasm-pack; falls back to the committed artifacts if absent.
	@if command -v wasm-pack >/dev/null 2>&1; then \
		echo "Regenerating engine-wasm/pkg (@nanobpm/engine-wasm) via wasm-pack..."; \
		cd $(PROJECT_ROOT)/engine-wasm && wasm-pack build --target web --release --no-pack --out-dir pkg --out-name nanobpmn_engine \
		&& rm -f $(PROJECT_ROOT)/engine-wasm/pkg/.gitignore \
		&& cp $(PROJECT_ROOT)/engine-wasm/pkg.package.json $(PROJECT_ROOT)/engine-wasm/pkg/package.json; \
	else \
		echo "wasm-pack not found; using the committed engine-wasm/pkg artifacts (run 'cargo install wasm-pack' to regenerate)."; \
	fi

.PHONY: console
console: release ## Alias for `release` (the self-contained single-node distribution)

.PHONY: console-dev
console-dev: ## Run the console frontend dev server (Vite); proxies /console/api to a gateway on :8080
	cd $(CONSOLE_DIR) && npm run dev

.PHONY: run
run: $(STUB_IMPLS) ## Run the stub server (PORT overrides the default 8080)
	cd $(PROJECT_ROOT)/server && cargo run

.PHONY: server-test
server-test: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) ## Test the stub server (incl. journal-replay e2e tests)
	cd $(PROJECT_ROOT)/server && cargo test

.PHONY: server-test-release
server-test-release: $(GENERATED_DIR)/Cargo.toml $(CONSOLE_GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) ## Test the stub server with release optimizations (uses the release-test profile to avoid the panic=abort double-compile)
	cd $(PROJECT_ROOT)/server && cargo test --profile release-test --features console

.PHONY: engine-build
engine-build: ## Build the embeddable BPMN engine-core crate (no Docker, no codegen)
	cd $(ENGINE_DIR) && cargo build

.PHONY: engine-test
engine-test: ## Test the engine-core crate (unit + integration + doctests)
	cd $(ENGINE_DIR) && cargo test

.PHONY: engine-wasm
engine-wasm: ## Build engine-core for wasm32 (portability check; needs the wasm32 target)
	cd $(ENGINE_DIR) && cargo build --target wasm32-unknown-unknown

.PHONY: engine-wasm-ffi
engine-wasm-ffi: ## Build the FFI cdylib for wasm32 and verify its exports (needs the wasm32 target + node)
	cd $(ENGINE_DIR) && cargo build --release --features ffi --target wasm32-unknown-unknown
	node $(ENGINE_DIR)/scripts/verify-wasm-ffi.mjs

.PHONY: engine-wasm-ffi-dist
engine-wasm-ffi-dist: engine-wasm-ffi ## Emit the release FFI wasm + manifest into dist/engine-wasm-ffi/ (needs wasm-opt/binaryen)
	node $(ENGINE_DIR)/scripts/emit-dist.mjs

.PHONY: engine-wasm-check
engine-wasm-check: ## Type-check the console wasm-bindgen crate for wasm32 (guards the `make release` console-wasm build; needs the wasm32 target)
	cd $(WASM_DIR) && cargo check --target wasm32-unknown-unknown

.PHONY: processos-build
processos-build: ## Build ProcessOS, the separate optimization-plane server (Stage T1: Insights)
	cd processos && cargo build

.PHONY: processos-build-release
processos-build-release: ## Build ProcessOS in release (opt-level=s, lto) -> processos/target/release/processos
	cd processos && cargo build --release

.PHONY: processos-test
processos-test: ## Test the ProcessOS crate
	cd processos && cargo test

.PHONY: processos-run
processos-run: ## Run ProcessOS (PROCESSOS_PORT=8090, NANO_BASE_URL=http://localhost:8080)
	cd processos && cargo run

.PHONY: fmt
fmt: ## Format every hand-written crate with the pinned nightly rustfmt (see FMT_TOOLCHAIN)
	@for d in $(FMT_CRATES); do \
		echo "fmt $$d"; \
		(cd $$d && rustup run $(FMT_TOOLCHAIN) cargo fmt) || exit 1; \
	done

.PHONY: fmt-check
fmt-check: ## Verify formatting with the pinned nightly rustfmt (CI gate; fails on drift)
	@for d in $(FMT_CRATES); do \
		echo "fmt-check $$d"; \
		(cd $$d && rustup run $(FMT_TOOLCHAIN) cargo fmt -- --check) || exit 1; \
	done

.PHONY: console-fmt
console-fmt: ## Format the web console TypeScript/CSS with Prettier (console/)
	cd $(CONSOLE_DIR) && npm run format

.PHONY: console-fmt-check
console-fmt-check: ## Verify console Prettier formatting (CI gate; fails on drift)
	cd $(CONSOLE_DIR) && npm run format:check

.PHONY: install-hooks
install-hooks: ## Activate the tracked git hooks (.githooks) — adds a pre-push rustfmt gate
	git config core.hooksPath .githooks
	@chmod +x .githooks/* 2>/dev/null || true
	@echo "git hooks installed: core.hooksPath -> .githooks (pre-push runs 'make fmt-check')"

.PHONY: clippy
clippy: $(GENERATED_DIR)/Cargo.toml ## Lint the generated crate, the stub server and engine-core
	cd $(GENERATED_DIR) && cargo clippy
	cd $(PROJECT_ROOT)/server && cargo clippy
	cd $(ENGINE_DIR) && cargo clippy --all-targets -- -D warnings

.PHONY: clean
clean: ## Remove all generated artifacts
	rm -rf $(PROJECT_ROOT)/build $(GENERATED_DIR) $(STUB_IMPLS) $(PROJECT_ROOT)/server/target $(ENGINE_DIR)/target

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
