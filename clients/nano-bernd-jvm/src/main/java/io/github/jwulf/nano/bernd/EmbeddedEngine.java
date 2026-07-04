/*
 * Copyright 2026 Josh Wulf
 * SPDX-License-Identifier: Apache-2.0
 *
 *       _   _                       ____                     _
 *      | \ | | __ _ _ __   ___     | __ )  ___ _ __ _ __  __| |
 *      |  \| |/ _` | '_ \ / _ \    |  _ \ / _ \ '__| '_ \/ _` |
 *      | |\  | (_| | | | | (_) |   | |_) |  __/ |  | | | | (_| |
 *      |_| \_|\__,_|_| |_|\___/    |____/ \___|_|  |_| |_|\__,_|
 *
 *      Named for Bernd Ruecker, whose talks on decoupled workers,
 *      Sagas and the compensation pattern are the intellectual
 *      source of the embedded-engine design realised here.
 *      Artists sign their work. See ADR 0015.
 */
package io.github.jwulf.nano.bernd;

import com.dylibso.chicory.runtime.ExportFunction;
import com.dylibso.chicory.runtime.Instance;
import com.dylibso.chicory.runtime.Memory;
import com.dylibso.chicory.wasm.Parser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Embedded Nano BPMN engine, hosted in-process on the JVM.
 *
 * <p>Loads {@code nano_engine.wasm} (ABI v2) through the pure-Java
 * <a href="https://chicory.dev">Chicory</a> runtime — no JNI, no native
 * binary, no external process. The class name is intentionally technical
 * ({@code EmbeddedEngine}) so IDE search finds it; the codename Bernd is
 * exposed as the constant {@link #CODENAME} for banners, telemetry and
 * {@code /v2/topology} advertisements (see ADR 0005).
 *
 * <p>Lifecycle:
 *
 * <pre>{@code
 * try (var engine = EmbeddedEngine.create()) {
 *   engine.deploy(bpmnXml);
 *   var pi = engine.createInstance("orders");
 *
 *   for (ActivatedJob job : engine.activateJobs("charge-card", "worker-1", 10, 30_000)) {
 *     doWork(job);
 *     engine.completeJob(job.key());
 *   }
 * }
 * }</pre>
 */
public final class EmbeddedEngine implements AutoCloseable {

  /** The C-ABI version this host is written against. */
  public static final int EXPECTED_ABI_VERSION = 2;

  /**
   * Codename of the embedded engine (see ADR 0005, "Sign the embedded engine").
   *
   * <p>Kept as a public constant so downstream code (banners, /v2/topology handlers,
   * telemetry, log prefixes) can surface the codename without hard-coding.
   */
  public static final String CODENAME = "Bernd";

  private static final ObjectMapper MAPPER =
      new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private final Instance instance;
  private final Memory memory;
  private final int engine;
  private final WasmManifest manifest;

  private final ExportFunction alloc;
  private final ExportFunction free;
  private final ExportFunction deploy;
  private final ExportFunction createInstance;
  private final ExportFunction correlateMessage;
  private final ExportFunction triggerTimers;
  private final ExportFunction expireJobs;
  private final ExportFunction isCompleted;
  private final ExportFunction instanceCount;
  private final ExportFunction activateJobs;
  private final ExportFunction completeJob;
  private final ExportFunction failJob;
  private final ExportFunction engineFree;

  private boolean closed;

  private EmbeddedEngine(Instance instance, WasmManifest manifest, int engineHandle) {
    this.instance = instance;
    this.memory = instance.memory();
    this.manifest = manifest;
    this.engine = engineHandle;
    this.alloc = instance.export("nbpmn_alloc");
    this.free = instance.export("nbpmn_free");
    this.deploy = instance.export("nbpmn_deploy_bpmn");
    this.createInstance = instance.export("nbpmn_create_instance");
    this.correlateMessage = instance.export("nbpmn_correlate_message");
    this.triggerTimers = instance.export("nbpmn_trigger_timers");
    this.expireJobs = instance.export("nbpmn_expire_jobs");
    this.isCompleted = instance.export("nbpmn_is_completed");
    this.instanceCount = instance.export("nbpmn_instance_count");
    this.activateJobs = instance.export("nbpmn_activate_jobs");
    this.completeJob = instance.export("nbpmn_complete_job");
    this.failJob = instance.export("nbpmn_fail_job");
    this.engineFree = instance.export("nbpmn_engine_free");
  }

  /** Load the packaged {@code nano_engine.wasm} from the classpath. */
  public static EmbeddedEngine create() {
    try (InputStream wasm = resource("nano-bernd/nano_engine.wasm");
        InputStream mfJson = resource("nano-bernd/manifest.json")) {
      WasmManifest manifest = MAPPER.readValue(mfJson, WasmManifest.class);
      return create(wasm.readAllBytes(), manifest);
    } catch (IOException e) {
      throw new RuntimeException("failed to load packaged nano_engine.wasm", e);
    }
  }

  /** Load a caller-provided wasm blob + manifest. Used in tests and by hosts embedding their own build. */
  public static EmbeddedEngine create(byte[] wasmBytes, WasmManifest manifest) {
    Objects.requireNonNull(wasmBytes, "wasmBytes");
    Objects.requireNonNull(manifest, "manifest");
    if (manifest.abiVersion() != EXPECTED_ABI_VERSION) {
      throw new IllegalStateException(
          "nano_engine.wasm ABI mismatch: manifest reports v"
              + manifest.abiVersion()
              + ", this host requires v"
              + EXPECTED_ABI_VERSION
              + ". Upgrade nano-bernd.");
    }
    var module = Parser.parse(wasmBytes);
    var instance = Instance.builder(module).build();
    long[] engineHandle = instance.export("nbpmn_engine_new").apply();
    if (engineHandle.length == 0 || engineHandle[0] == 0) {
      throw new IllegalStateException("nbpmn_engine_new returned null");
    }
    return new EmbeddedEngine(instance, manifest, (int) engineHandle[0]);
  }

  private static InputStream resource(String path) {
    InputStream in = EmbeddedEngine.class.getClassLoader().getResourceAsStream(path);
    if (in == null) throw new IllegalStateException("missing classpath resource: " + path);
    return in;
  }

  public WasmManifest manifest() {
    return manifest;
  }

  /** Deploy a BPMN document. Returns the number of process definitions parsed. */
  public int deploy(String bpmnXml) {
    ensureOpen();
    Alloc buf = writeUtf8(bpmnXml);
    try {
      long rc = deploy.apply(engine, buf.ptr, buf.len)[0];
      if (rc < 0) throw new IllegalStateException("deploy failed (" + rc + ")");
      return (int) rc;
    } finally {
      buf.free();
    }
  }

  /** Start a process instance. Returns the process instance key (u64 as decimal string). */
  public String createInstance(String processId) {
    return createInstance(processId, System.currentTimeMillis());
  }

  /** Overload for deterministic tests. */
  public String createInstance(String processId, long nowEpochMs) {
    ensureOpen();
    Alloc buf = writeUtf8(processId);
    try {
      long key = createInstance.apply(engine, buf.ptr, buf.len, nowEpochMs)[0];
      if (key == 0L)
        throw new IllegalStateException("create_instance failed (no such process id: " + processId + ")");
      return Long.toUnsignedString(key);
    } finally {
      buf.free();
    }
  }

  /**
   * Correlate a message and dispatch it into the running engine.
   *
   * <p>Returns the number of engine events produced by the correlation (which
   * therefore also indicates whether a subscription matched: {@code 0} means
   * no subscription matched, positive means at least one instance advanced).
   * Returns {@code -1} only on invalid input (null engine / non-UTF-8 bytes).
   */
  public long correlateMessage(String name, String correlationKey) {
    return correlateMessage(name, correlationKey, System.currentTimeMillis());
  }

  public long correlateMessage(String name, String correlationKey, long nowEpochMs) {
    ensureOpen();
    Alloc nameBuf = writeUtf8(name);
    Alloc keyBuf = writeUtf8(correlationKey);
    try {
      return correlateMessage
          .apply(engine, nameBuf.ptr, nameBuf.len, keyBuf.ptr, keyBuf.len, nowEpochMs)[0];
    } finally {
      nameBuf.free();
      keyBuf.free();
    }
  }

  /** Fire due timers at {@code nowEpochMs}. Returns the number of timers fired. */
  public long triggerTimers(long nowEpochMs) {
    ensureOpen();
    return triggerTimers.apply(engine, nowEpochMs)[0];
  }

  /** Release job activation locks whose deadline &le; {@code nowEpochMs}. */
  public long expireJobs(long nowEpochMs) {
    ensureOpen();
    return expireJobs.apply(engine, nowEpochMs)[0];
  }

  /** Convenience: activate at most {@code maxJobs} of {@code type} for {@code worker}, now = {@code System.currentTimeMillis()}. */
  public List<ActivatedJob> activateJobs(String type, String worker, int maxJobs, long timeoutMs) {
    return activateJobs(type, worker, maxJobs, timeoutMs, System.currentTimeMillis());
  }

  public List<ActivatedJob> activateJobs(
      String type, String worker, int maxJobs, long timeoutMs, long nowEpochMs) {
    ensureOpen();
    Alloc typeBuf = writeUtf8(type);
    Alloc workerBuf = writeUtf8(worker);
    // Scratch region for the two out-params (ptr: u32, len: u32) = 8 bytes.
    int outPtrPtr = (int) alloc.apply(8L)[0];
    if (outPtrPtr == 0)
      throw new IllegalStateException("nbpmn_alloc(8) failed for activate out-params");
    int outLenPtr = outPtrPtr + 4;
    try {
      long rc =
          activateJobs.apply(
              engine,
              typeBuf.ptr,
              typeBuf.len,
              workerBuf.ptr,
              workerBuf.len,
              maxJobs,
              timeoutMs,
              nowEpochMs,
              outPtrPtr,
              outLenPtr)[0];
      if (rc < 0) throw new IllegalStateException("activate_jobs failed (" + rc + ")");
      int jsonPtr = memory.readInt(outPtrPtr);
      int jsonLen = memory.readInt(outLenPtr);
      if (jsonLen == 0) return List.of();
      byte[] jsonBytes = memory.readBytes(jsonPtr, jsonLen);
      try {
        CollectionType listType =
            MAPPER.getTypeFactory().constructCollectionType(List.class, ActivatedJob.class);
        return MAPPER.readValue(new String(jsonBytes, StandardCharsets.UTF_8), listType);
      } catch (IOException e) {
        throw new IllegalStateException("failed to decode activate_jobs JSON", e);
      } finally {
        free.apply((long) jsonPtr, (long) jsonLen);
      }
    } finally {
      free.apply((long) outPtrPtr, 8L);
      typeBuf.free();
      workerBuf.free();
    }
  }

  /** Complete an activated job. Throws if unknown or already resolved. */
  public void completeJob(String jobKey) {
    ensureOpen();
    long rc = completeJob.apply(engine, Long.parseUnsignedLong(jobKey))[0];
    if (rc != 0) throw new IllegalStateException("complete_job(" + jobKey + ") failed (" + rc + ")");
  }

  /** Fail a job with {@code retries} attempts remaining (0 raises an incident on next failure). */
  public void failJob(String jobKey, int retries, String message) {
    ensureOpen();
    Alloc msgBuf = (message == null || message.isEmpty()) ? null : writeUtf8(message);
    try {
      int ptr = msgBuf == null ? 0 : msgBuf.ptr;
      int len = msgBuf == null ? 0 : msgBuf.len;
      long rc = failJob.apply(engine, Long.parseUnsignedLong(jobKey), retries, ptr, len)[0];
      if (rc != 0) throw new IllegalStateException("fail_job(" + jobKey + ") failed (" + rc + ")");
    } finally {
      if (msgBuf != null) msgBuf.free();
    }
  }

  public boolean isCompleted(String processInstanceKey) {
    ensureOpen();
    return isCompleted.apply(engine, Long.parseUnsignedLong(processInstanceKey))[0] == 1L;
  }

  public long instanceCount() {
    ensureOpen();
    return instanceCount.apply(engine)[0];
  }

  @Override
  public void close() {
    if (closed) return;
    engineFree.apply(engine);
    closed = true;
  }

  private void ensureOpen() {
    if (closed) throw new IllegalStateException("EmbeddedEngine has been closed");
  }

  private Alloc writeUtf8(String s) {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    int ptr = (int) alloc.apply((long) bytes.length)[0];
    if (ptr == 0 && bytes.length > 0)
      throw new IllegalStateException("nbpmn_alloc(" + bytes.length + ") failed");
    memory.write(ptr, bytes);
    return new Alloc(ptr, bytes.length);
  }

  /** Tracks a wasm allocation so it can be freed exactly once. */
  private final class Alloc {
    final int ptr;
    final int len;

    Alloc(int ptr, int len) {
      this.ptr = ptr;
      this.len = len;
    }

    void free() {
      free.apply((long) ptr, (long) len);
    }
  }
}
