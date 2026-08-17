// End-to-end coverage for the Studio test view's message-publish /
// signal-broadcast wiring (#787), driving the REAL published WASM engine.
//
// The Studio test view (`TestRunPanel.tsx`) renders `snapshot.messageSubscriptions`
// / `snapshot.signalSubscriptions` and drives them with `correlateMessage` /
// `broadcastSignal`. The pure form-mapping is unit-tested in
// `../lib/testScaffold.test.ts`; this test closes the loop by proving those two
// commands actually advance a token past a message-catch and a signal-catch, and
// that the prefilled form the UI hands to each command is the one the engine
// correlates on.
//
// This is deliberately a `node --test` integration test rather than a Playwright
// smoke: the console's e2e suite is intentionally engine-free and its own
// guidance (`e2e/journeys.spec.ts`) treats `/projects/:name` workspace flows as
// flaky-prone (they need a scaffolded, running project). Driving the same
// `@nanobpm/bojtos-kit` session the panel uses — headless, with the real wasm —
// gives a deterministic, first-run-green assertion of the exact behaviour the
// smoke was meant to catch, with no browser, modeler, or vite server in the loop.
//
// Run: `node --experimental-strip-types --test src/components/testRunMessaging.engine.test.ts`
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { createBojtosSession } from "@nanobpm/bojtos-kit";
import {
  prefillMessagePublish,
  prefillSignalBroadcast,
} from "../lib/testScaffold.ts";

const require = createRequire(import.meta.url);
// The published engine-wasm is wasm-pack `--target web` output; its `init` fetches
// the binary from a URL, which Node can't do — so we hand it the bytes directly
// (the same `WasmSource` escape hatch the react binding exposes for non-Vite
// bundlers). In the browser panel Vite serves the `.wasm` as a hashed asset.
const WASM_BYTES = readFileSync(
  require.resolve("@nanobpm/engine-wasm/lean/nanobpmn_engine_bg.wasm"),
);

// A model that parks on BOTH a message intermediate-catch and a signal catch at
// once (parallel fork), so a single "Start instance" opens exactly one message
// and one signal subscription — the two affordances the panel renders. DI is
// omitted: the headless engine executes the semantic model, and this test never
// renders a diagram (a human-facing model would carry DI).
const XML = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
  id="d1" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:signal id="Sig" name="cancel-sig" />
  <bpmn:message id="Msg" name="order-msg">
    <bpmn:extensionElements>
      <zeebe:subscription correlationKey="=orderId" />
    </bpmn:extensionElements>
  </bpmn:message>
  <bpmn:process id="proc" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f0</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f0" sourceRef="Start" targetRef="Fork" />
    <bpmn:parallelGateway id="Fork">
      <bpmn:incoming>f0</bpmn:incoming>
      <bpmn:outgoing>fa</bpmn:outgoing><bpmn:outgoing>fb</bpmn:outgoing>
    </bpmn:parallelGateway>
    <bpmn:sequenceFlow id="fa" sourceRef="Fork" targetRef="MsgCatch" />
    <bpmn:sequenceFlow id="fb" sourceRef="Fork" targetRef="SigCatch" />
    <bpmn:intermediateCatchEvent id="MsgCatch">
      <bpmn:incoming>fa</bpmn:incoming><bpmn:outgoing>fa2</bpmn:outgoing>
      <bpmn:messageEventDefinition messageRef="Msg" />
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="SigCatch">
      <bpmn:incoming>fb</bpmn:incoming><bpmn:outgoing>fb2</bpmn:outgoing>
      <bpmn:signalEventDefinition signalRef="Sig" />
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="fa2" sourceRef="MsgCatch" targetRef="Join" />
    <bpmn:sequenceFlow id="fb2" sourceRef="SigCatch" targetRef="Join" />
    <bpmn:parallelGateway id="Join">
      <bpmn:incoming>fa2</bpmn:incoming><bpmn:incoming>fb2</bpmn:incoming>
      <bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:parallelGateway>
    <bpmn:sequenceFlow id="f3" sourceRef="Join" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>`;

test("publishing a message and broadcasting a signal drives the token to completion", async () => {
  const session = await createBojtosSession({ wasm: WASM_BYTES });
  assert.deepEqual(session.deploy(XML).processIds, ["proc"]);

  // Start parks on both catches: one open message + one open signal subscription.
  const started = session.createInstance("proc", '{"orderId":"order-42"}');
  assert.equal(started.messageSubscriptions.length, 1);
  assert.equal(started.signalSubscriptions.length, 1);

  const msgSub = started.messageSubscriptions[0];
  const sigSub = started.signalSubscriptions[0];
  assert.equal(msgSub.elementId, "MsgCatch");
  assert.equal(sigSub.elementId, "SigCatch");

  // The panel publishes/broadcasts with exactly the prefilled form — so assert
  // the engine correlates on what the helper hands it.
  const msgForm = prefillMessagePublish(msgSub);
  assert.deepEqual(msgForm, {
    messageName: "order-msg",
    correlationKey: "order-42",
    variables: "{}",
  });
  const sigForm = prefillSignalBroadcast(sigSub);
  assert.deepEqual(sigForm, { signalName: "cancel-sig", variables: "{}" });

  // Publish the message → its subscription is consumed and the token advances
  // (Join now waits, the signal branch still parks). The trace/event log grows.
  const afterMessage = session.correlateMessage(
    msgForm.messageName,
    msgForm.correlationKey,
    msgForm.variables,
  );
  assert.equal(afterMessage.messageSubscriptions.length, 0);
  assert.equal(afterMessage.signalSubscriptions.length, 1);
  assert.ok(afterMessage.activeElementIds.includes("Join"));
  assert.ok(afterMessage.eventCount > started.eventCount);

  // Broadcast the signal (name only) → the last branch advances, the join
  // completes, and the instance finishes.
  const afterSignal = session.broadcastSignal(
    sigForm.signalName,
    sigForm.variables,
  );
  assert.equal(afterSignal.signalSubscriptions.length, 0);
  assert.equal(afterSignal.completedInstances, 1);
  assert.equal(afterSignal.totalInstances, 1);
  assert.deepEqual(
    afterSignal.instances.map((i) => i.state),
    ["Completed"],
  );
  assert.ok(afterSignal.eventCount > afterMessage.eventCount);
});

test("a signal broadcast with no matching subscription is a no-op (not buffered)", async () => {
  const session = await createBojtosSession({ wasm: WASM_BYTES });
  session.deploy(XML);
  session.createInstance("proc", '{"orderId":"order-99"}');

  // Signals correlate by name only and are NOT buffered: an unmatched name does
  // nothing and leaves the open subscription intact (the copy the panel shows).
  const after = session.broadcastSignal("no-such-signal", "{}");
  assert.equal(after.signalSubscriptions.length, 1);
  assert.equal(after.completedInstances, 0);
});
