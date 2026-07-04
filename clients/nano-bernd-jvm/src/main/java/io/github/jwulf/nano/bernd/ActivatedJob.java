/*
 * Copyright 2026 Josh Wulf
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.jwulf.nano.bernd;

import java.util.Map;

/**
 * A job the embedded engine has activated on behalf of a worker.
 *
 * <p>Keys are stringified u64 values (the wasm engine's native representation)
 * so that callers can round-trip them without worrying about JVM {@code long}
 * signedness or JSON precision loss.
 *
 * @param key                job key (u64 as decimal string)
 * @param type               job type from {@code zeebe:taskDefinition/@type}
 * @param instanceKey        owning process instance key
 * @param elementInstanceKey token id parked on this job
 * @param elementId          BPMN element id of the service task
 * @param worker             worker id the activation is locked to
 * @param deadline           epoch-ms at which the activation lock expires
 * @param retries            remaining retries; 0 = next failure raises an incident
 * @param variables          snapshot of instance variables at activation time
 */
public record ActivatedJob(
    String key,
    String type,
    String instanceKey,
    String elementInstanceKey,
    String elementId,
    String worker,
    long deadline,
    int retries,
    Map<String, Object> variables) {}
