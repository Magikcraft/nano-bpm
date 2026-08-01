// Unit tests for the workspace main-pane state machine. Node-native: run with
// `node --experimental-strip-types --test src/lib/workspacePanel.test.ts`.
//
// Regression guard for issue #485: clicking a file in the explorer must return
// to the file-content editor from any of the Model / Data / Triggers /
// Connectors panels. Because the pane is a single nullable value, the old
// "selection and panel visibility contradict" defect class is unrepresentable.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  type WorkspacePanel,
  togglePanel,
  selectFile,
  mainPane,
} from "./workspacePanel.ts";

const PANELS: WorkspacePanel[] = ["data", "triggers", "connectors", "model"];

test("togglePanel opens a panel from the editor", () => {
  for (const p of PANELS) {
    assert.equal(togglePanel(null, p), p);
  }
});

test("togglePanel closes the panel when its own button is clicked again", () => {
  for (const p of PANELS) {
    assert.equal(togglePanel(p, p), null);
  }
});

test("togglePanel switches directly between panels", () => {
  assert.equal(togglePanel("data", "triggers"), "triggers");
  assert.equal(togglePanel("model", "connectors"), "connectors");
  assert.equal(togglePanel("triggers", "data"), "data");
});

test("selectFile returns to the editor from every panel (#485)", () => {
  assert.equal(selectFile(null), null);
  for (const p of PANELS) {
    assert.equal(selectFile(p), null);
  }
});

test("mainPane derives the editor when no panel is active", () => {
  assert.equal(mainPane(null), "editor");
});

test("mainPane derives the active panel when one is open", () => {
  for (const p of PANELS) {
    assert.equal(mainPane(p), p);
  }
});
