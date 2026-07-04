/*
 * Copyright 2026 Josh Wulf
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.jwulf.nano.bernd;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metadata for the {@code nano_engine.wasm} artefact packaged with this jar.
 *
 * <p>Deserialised from {@code /nano-bernd/manifest.json} on the classpath.
 * The manifest is emitted by {@code engine-core/scripts/emit-dist.mjs} and
 * uses snake_case field names — mapped here to Java camelCase via
 * {@link JsonProperty} annotations.
 *
 * @param abiVersion    the C-ABI version the wasm was built against; must match
 *                      {@link EmbeddedEngine#EXPECTED_ABI_VERSION}.
 * @param engineVersion the engine-core crate version emitted in the wasm.
 * @param sizeBytes     size of the wasm blob, for diagnostics.
 * @param sha256        content hash, for supply-chain checks.
 */
public record WasmManifest(
    @JsonProperty("abi_version") int abiVersion,
    @JsonProperty("engine_version") String engineVersion,
    @JsonProperty("size_bytes") long sizeBytes,
    @JsonProperty("sha256") String sha256) {}
