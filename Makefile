# nanobpmn — Orchestration Cluster REST layer code generation (Rust / rust-axum).
#
# `make generate` produces the Rust REST layer + server stubs from spec/; the
# output under generated/ and server/src/stub_impls.rs is git-ignored.

SHELL := /usr/bin/env bash
PROJECT_ROOT := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
GENERATED_DIR := $(PROJECT_ROOT)/generated
STUB_IMPLS := $(PROJECT_ROOT)/server/src/stub_impls.rs
ENGINE_DIR := $(PROJECT_ROOT)/engine-core

.DEFAULT_GOAL := build

.PHONY: generate
generate: ## Generate the Rust REST layer + server stub impls from spec/ (requires Docker)
	./scripts/generate.sh

$(GENERATED_DIR)/Cargo.toml:
	$(MAKE) generate

$(STUB_IMPLS):
	$(MAKE) generate

.PHONY: build
build: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) ## Compile the generated crate and the stub server
	cd $(GENERATED_DIR) && cargo build
	cd $(PROJECT_ROOT)/server && cargo build

.PHONY: run
run: $(STUB_IMPLS) ## Run the stub server (PORT overrides the default 8080)
	cd $(PROJECT_ROOT)/server && cargo run

.PHONY: engine-build
engine-build: ## Build the embeddable BPMN engine-core crate (no Docker, no codegen)
	cd $(ENGINE_DIR) && cargo build

.PHONY: engine-test
engine-test: ## Test the engine-core crate (unit + integration + doctests)
	cd $(ENGINE_DIR) && cargo test

.PHONY: engine-wasm
engine-wasm: ## Build engine-core for wasm32 (portability check; needs the wasm32 target)
	cd $(ENGINE_DIR) && cargo build --target wasm32-unknown-unknown

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
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
