/*
 * Copyright 2026 Josh Wulf
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.jwulf.nano.bernd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class EmbeddedEngineTest {

  private static final String TRIVIAL_BPMN =
      """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
        <bpmn:process id="p" isExecutable="true">
          <bpmn:startEvent id="s"><bpmn:outgoing>f</bpmn:outgoing></bpmn:startEvent>
          <bpmn:endEvent id="e"><bpmn:incoming>f</bpmn:incoming></bpmn:endEvent>
          <bpmn:sequenceFlow id="f" sourceRef="s" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>
      """;

  private static final String SERVICE_BPMN =
      """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="svc" isExecutable="true">
          <bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
          <bpmn:serviceTask id="t" name="Do work">
            <bpmn:extensionElements><zeebe:taskDefinition type="do-work" /></bpmn:extensionElements>
            <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
          <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>
      """;

  @Test
  void loads_wasm_and_completes_a_straight_through_process() {
    try (var engine = EmbeddedEngine.create()) {
      assertEquals(EmbeddedEngine.EXPECTED_ABI_VERSION, engine.manifest().abiVersion());
      assertEquals("Bernd", EmbeddedEngine.CODENAME);
      assertEquals(0L, engine.instanceCount());

      int deployed = engine.deploy(TRIVIAL_BPMN);
      assertTrue(deployed >= 1);

      String pi = engine.createInstance("p", 1_000L);
      assertNotNull(pi);
      assertTrue(engine.isCompleted(pi));
      assertEquals(1L, engine.instanceCount());
    }
  }

  @Test
  void rejects_create_for_unknown_process_id() {
    try (var engine = EmbeddedEngine.create()) {
      engine.deploy(TRIVIAL_BPMN);
      assertThrows(IllegalStateException.class, () -> engine.createInstance("does-not-exist"));
    }
  }

  @Test
  void trigger_timers_and_expire_jobs_are_safe_noops_on_a_trivial_process() {
    try (var engine = EmbeddedEngine.create()) {
      engine.deploy(TRIVIAL_BPMN);
      assertEquals(0L, engine.triggerTimers(System.currentTimeMillis()));
      assertEquals(0L, engine.expireJobs(System.currentTimeMillis()));
    }
  }

  @Test
  void refuses_use_after_close() {
    var engine = EmbeddedEngine.create();
    engine.close();
    assertThrows(IllegalStateException.class, () -> engine.deploy(TRIVIAL_BPMN));
  }

  @Test
  void full_job_lifecycle_activate_then_complete() {
    try (var engine = EmbeddedEngine.create()) {
      engine.deploy(SERVICE_BPMN);
      String pi = engine.createInstance("svc", 1_000L);
      assertFalse(engine.isCompleted(pi));

      List<ActivatedJob> activated = engine.activateJobs("do-work", "w1", 10, 30_000L, 1_000L);
      assertEquals(1, activated.size());
      var job = activated.get(0);
      assertEquals("do-work", job.type());
      assertEquals("w1", job.worker());
      assertEquals("t", job.elementId());
      assertEquals(31_000L, job.deadline());
      assertTrue(job.retries() > 0);

      engine.completeJob(job.key());
      assertTrue(engine.isCompleted(pi));
    }
  }

  @Test
  void activate_returns_empty_list_when_no_matching_jobs() {
    try (var engine = EmbeddedEngine.create()) {
      engine.deploy(TRIVIAL_BPMN);
      assertEquals(List.of(), engine.activateJobs("nothing", "w", 5, 30_000L, 1_000L));
    }
  }

  @Test
  void fail_with_retries_then_expire_allows_reactivation() {
    String flakyBpmn =
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
          <bpmn:process id="svc" isExecutable="true">
            <bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
            <bpmn:serviceTask id="t">
              <bpmn:extensionElements><zeebe:taskDefinition type="flaky" retries="3" /></bpmn:extensionElements>
              <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
            </bpmn:serviceTask>
            <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
            <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
          </bpmn:process>
        </bpmn:definitions>
        """;
    try (var engine = EmbeddedEngine.create()) {
      engine.deploy(flakyBpmn);
      engine.createInstance("svc", 1_000L);

      var first = engine.activateJobs("flaky", "w", 1, 30_000L, 1_000L);
      assertEquals(1, first.size());
      engine.failJob(first.get(0).key(), 2, "transient upstream error");

      engine.expireJobs(2_000_000L);
      var second = engine.activateJobs("flaky", "w", 1, 30_000L, 2_000_000L);
      assertEquals(1, second.size());
      assertEquals(2, second.get(0).retries());
    }
  }

  @Test
  void correlate_message_returns_event_count_and_advances_a_waiting_instance() {
    // Process: start -> intermediate message catch (name="ping", correlationKey=orderId) -> end.
    // Correlating with a matching key must return > 0 (events produced) and complete the instance.
    // Correlating with a non-matching key must return 0 (no subscription matched).
    String msgBpmn =
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
          <bpmn:message id="pingMsg" name="ping">
            <bpmn:extensionElements><zeebe:subscription correlationKey="=orderId" /></bpmn:extensionElements>
          </bpmn:message>
          <bpmn:process id="p" isExecutable="true">
            <bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
            <bpmn:intermediateCatchEvent id="c">
              <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
              <bpmn:messageEventDefinition messageRef="pingMsg" />
            </bpmn:intermediateCatchEvent>
            <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="c" />
            <bpmn:sequenceFlow id="f2" sourceRef="c" targetRef="e" />
          </bpmn:process>
        </bpmn:definitions>
        """;
    try (var engine = EmbeddedEngine.create()) {
      engine.deploy(msgBpmn);
      // Note: this instance has no `orderId` variable, so the subscription's
      // correlation key resolves to empty — matched below by an empty key.
      String pi = engine.createInstance("p", 1_000L);
      assertFalse(engine.isCompleted(pi));

      // Non-matching message name: instance stays parked (no advancement).
      engine.correlateMessage("does-not-exist", "", 1_100L);
      assertFalse(engine.isCompleted(pi));

      // Matching name: return is the number of engine events produced (>=1),
      // NOT a correlation-record id, and the waiting instance advances to end.
      long produced = engine.correlateMessage("ping", "", 1_200L);
      assertTrue(produced > 0, "expected correlate to produce events, got " + produced);
      assertTrue(engine.isCompleted(pi));
    }
  }
}
