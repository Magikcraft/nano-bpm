# nanobpmn — Orchestration Cluster REST layer code generation (Rust / rust-axum).
#
# `make generate` produces the Rust REST layer + server stubs from spec/; the
# output under generated/ and server/src/stub_impls.rs is git-ignored.

SHELL := /usr/bin/env bash
PROJECT_ROOT := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
GENERATED_DIR := $(PROJECT_ROOT)/generated
STUB_IMPLS := $(PROJECT_ROOT)/server/src/stub_impls.rs
ENGINE_DIR := $(PROJECT_ROOT)/engine-core
CONSOLE_DIR := $(PROJECT_ROOT)/console

.DEFAULT_GOAL := build

.PHONY: generate
generate: ## Generate the Rust REST layer + server stub impls from spec/ (needs local Java)
	./scripts/generate.sh

$(GENERATED_DIR)/Cargo.toml:
	$(MAKE) generate

$(STUB_IMPLS):
	$(MAKE) generate

.PHONY: build
build: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) ## Compile the generated crate and the stub server
	cd $(GENERATED_DIR) && cargo build
	cd $(PROJECT_ROOT)/server && cargo build

.PHONY: release
release: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) console-frontend ## Build the optimized self-contained distribution (gateway + embedded console + Swagger)
	@# Force the RustEmbed derive to re-run so the just-built console/dist (which
	@# release builds bake in at compile time) is embedded, even if the gateway
	@# sources are otherwise unchanged.
	touch $(PROJECT_ROOT)/server/src/console/mod.rs
	cd $(PROJECT_ROOT)/server && cargo build --release --features console
	@echo "Built self-contained distribution: $(PROJECT_ROOT)/server/target/release/nanobpm-gateway-rest-server"
	@echo "  landing /  ·  console /console  ·  API docs /swagger  ·  REST /v2"

.PHONY: release-gateway
release-gateway: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) ## Build the optimized API-only gateway (no web console)
	cd $(PROJECT_ROOT)/server && cargo build --release
	@echo "Built API-only gateway: $(PROJECT_ROOT)/server/target/release/nanobpm-gateway-rest-server"

.PHONY: console-frontend
console-frontend: console-wasm ## Build the web console SPA (console/ -> console/dist)
	cd $(CONSOLE_DIR) && npm install && npm run build

.PHONY: console-wasm
console-wasm: ## Regenerate the in-browser test-run engine (engine-wasm -> console/src/wasm). Needs wasm-pack; falls back to the committed artifacts if absent.
	@if command -v wasm-pack >/dev/null 2>&1; then \
		echo "Regenerating console/src/wasm via wasm-pack..."; \
		cd $(PROJECT_ROOT)/engine-wasm && wasm-pack build --target web --release --out-dir ../console/src/wasm --out-name nanobpmn_engine; \
		rm -f $(CONSOLE_DIR)/src/wasm/.gitignore; \
	else \
		echo "wasm-pack not found; using the committed console/src/wasm artifacts (run 'cargo install wasm-pack' to regenerate)."; \
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

.PHONY: processos-build
processos-build: ## Build ProcessOS, the separate optimization-plane server (Stage T1: Insights)
	cd processos && cargo build

.PHONY: processos-test
processos-test: ## Test the ProcessOS crate
	cd processos && cargo test

.PHONY: processos-run
processos-run: ## Run ProcessOS (PROCESSOS_PORT=8090, NANO_BASE_URL=http://localhost:8080)
	cd processos && cargo run

.PHONY: fmt
fmt: $(GENERATED_DIR)/Cargo.toml ## Format the generated crate, the stub server and engine-core
	cd $(GENERATED_DIR) && cargo fmt
	cd $(PROJECT_ROOT)/server && cargo fmt
	cd $(ENGINE_DIR) && cargo fmt

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
