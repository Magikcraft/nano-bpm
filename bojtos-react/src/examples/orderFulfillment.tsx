import type { JobHandler } from "@nanobpm/bojtos-kit";
import { Bojtos } from "../Bojtos.js";

/**
 * A laid-out order-fulfillment diagram: start → reserve stock (inventory) →
 * charge card (payment) → ship (shipping) → done. Unlike the headless engine
 * fixtures, this carries `bpmndi` diagram-interchange so bpmn-js actually
 * renders the shapes for the token to walk across.
 */
export const ORDER_FULFILLMENT_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="order-fulfillment-defs" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="order-fulfillment" isExecutable="true">
    <bpmn:startEvent id="start" name="Order placed">
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="reserve" name="Reserve stock">
      <bpmn:extensionElements><zeebe:taskDefinition type="inventory" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="charge" name="Charge card">
      <bpmn:extensionElements><zeebe:taskDefinition type="payment" /></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming>
      <bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="ship" name="Ship">
      <bpmn:extensionElements><zeebe:taskDefinition type="shipping" /></bpmn:extensionElements>
      <bpmn:incoming>f3</bpmn:incoming>
      <bpmn:outgoing>f4</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="done" name="Fulfilled">
      <bpmn:incoming>f4</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="reserve" />
    <bpmn:sequenceFlow id="f2" sourceRef="reserve" targetRef="charge" />
    <bpmn:sequenceFlow id="f3" sourceRef="charge" targetRef="ship" />
    <bpmn:sequenceFlow id="f4" sourceRef="ship" targetRef="done" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="diagram">
    <bpmndi:BPMNPlane id="plane" bpmnElement="order-fulfillment">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start">
        <dc:Bounds x="150" y="102" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="reserve_di" bpmnElement="reserve">
        <dc:Bounds x="240" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="charge_di" bpmnElement="charge">
        <dc:Bounds x="400" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="ship_di" bpmnElement="ship">
        <dc:Bounds x="560" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="done_di" bpmnElement="done">
        <dc:Bounds x="720" y="102" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1">
        <di:waypoint x="186" y="120" />
        <di:waypoint x="240" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2">
        <di:waypoint x="340" y="120" />
        <di:waypoint x="400" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f3_di" bpmnElement="f3">
        <di:waypoint x="500" y="120" />
        <di:waypoint x="560" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f4_di" bpmnElement="f4">
        <di:waypoint x="660" y="120" />
        <di:waypoint x="720" y="120" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

/**
 * The in-browser workers for {@link ORDER_FULFILLMENT_BPMN}. Each reads the
 * instance's live payload and returns the variables it adds — edit a handler and
 * re-run to watch the payload change (ADR 0043 §8 step 4 makes these boxes
 * editable in the browser).
 */
export const orderFulfillmentWorkers: Record<string, JobHandler> = {
  inventory: (job) => ({ reserved: true, sku: job.variables.sku }),
  payment: (job) => ({ charged: job.variables.amount ?? 0 }),
  shipping: () => ({ tracking: `1Z${Math.floor(Math.random() * 1e9)}` }),
};

/** A ready-to-drop-in Bojtos demo — the canonical first example (ADR 0043 §8). */
export function OrderFulfillmentDemo() {
  return (
    <Bojtos
      bpmn={ORDER_FULFILLMENT_BPMN}
      workers={orderFulfillmentWorkers}
      seed={{ sku: "WIDGET-1", amount: 4200 }}
      autoplay
    />
  );
}
