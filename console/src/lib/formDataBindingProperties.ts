// Visual binding inspector for the form editor (ADR 0024 §5). Contributes a
// "Data source (Urban)" group to the form-js properties panel for choice fields
// (select/checklist/radio/taglist), so an author binds a data-aware control —
// pick a datasource alias, write the query, name the value/label columns —
// without hand-editing the `.form` JSON. The binding is stored on the field as
// `dataSource: { source, query, value?, label? }`, exactly the shape the
// spec-app resolver + validator understand.
//
// form-js-editor bundles (inlines) its own copy of `@bpmn-io/properties-panel`
// with its own preact contexts, so we deliberately DON'T reuse that package's
// entry components — their context hooks would read providers form-js never
// mounts. Instead this group is a fully self-contained preact component
// (rendered by the shared preact singleton form-js imports) styled with the
// `bio-properties-panel-*` classes that ship in form-js-editor.css, so it looks
// native without depending on properties-panel internals.
import { createElement as h } from "preact";
import { useState } from "preact/hooks";
import { CHOICE_FIELD_TYPES } from "@nanobpm/nano-app-schema";

interface DataSourceBinding {
  source?: string;
  query?: string;
  value?: string;
  label?: string;
}

interface FormField {
  type?: string;
  dataSource?: DataSourceBinding;
  [key: string]: unknown;
}

// Sets a single top-level property on the field (form-js `modeling.editFormField`).
type EditField = (field: FormField, key: string, value: unknown) => unknown;

interface SelectOption {
  value: string;
  label: string;
}

// Empty value = the "not bound" option, which clears the binding.
const UNBOUND = "";

function description(text?: string) {
  return text ? h("div", { class: "bio-properties-panel-description" }, text) : null;
}

function selectEntry(
  id: string,
  label: string,
  desc: string,
  value: string,
  options: SelectOption[],
  onChange: (value: string) => void,
) {
  return h(
    "div",
    { class: "bio-properties-panel-entry", "data-entry-id": id },
    h(
      "div",
      { class: "bio-properties-panel-select" },
      h("label", { class: "bio-properties-panel-label", for: `${id}-input` }, label),
      h(
        "select",
        {
          id: `${id}-input`,
          class: "bio-properties-panel-input",
          value,
          onInput: (e: Event) => onChange((e.currentTarget as HTMLSelectElement).value),
        },
        options.map((o) => h("option", { value: o.value, key: o.value }, o.label)),
      ),
    ),
    description(desc),
  );
}

function textAreaEntry(
  id: string,
  label: string,
  desc: string,
  value: string,
  onChange: (value: string) => void,
) {
  return h(
    "div",
    { class: "bio-properties-panel-entry", "data-entry-id": id },
    h(
      "div",
      { class: "bio-properties-panel-textarea" },
      h("label", { class: "bio-properties-panel-label", for: `${id}-input` }, label),
      h("textarea", {
        id: `${id}-input`,
        class: "bio-properties-panel-input bio-properties-panel-input-monospace",
        spellcheck: false,
        rows: 4,
        value,
        "data-gramm": "false",
        onInput: (e: Event) => onChange((e.currentTarget as HTMLTextAreaElement).value),
      }),
    ),
    description(desc),
  );
}

function textFieldEntry(
  id: string,
  label: string,
  desc: string,
  value: string,
  onChange: (value: string) => void,
) {
  return h(
    "div",
    { class: "bio-properties-panel-entry", "data-entry-id": id },
    h(
      "div",
      { class: "bio-properties-panel-textfield" },
      h("label", { class: "bio-properties-panel-label", for: `${id}-input` }, label),
      h("input", {
        id: `${id}-input`,
        type: "text",
        class: "bio-properties-panel-input",
        spellcheck: false,
        value,
        onInput: (e: Event) => onChange((e.currentTarget as HTMLInputElement).value),
      }),
    ),
    description(desc),
  );
}

const ARROW_PATH =
  "m11.657 8-4.95 4.95a1 1 0 0 1-1.414-1.414L8.828 8 5.293 4.464A1 1 0 1 1 6.707 3.05L11.657 8Z";

interface GroupProps {
  id: string;
  label: string;
  field: FormField;
  editField: EditField;
  getSources: () => string[];
}

function DataSourceGroup(props: GroupProps) {
  const { id, label, field, editField, getSources } = props;
  const [open, setOpen] = useState(true);

  const source = field.dataSource?.source ?? UNBOUND;

  const setBinding = (patch: Partial<DataSourceBinding> | undefined) => {
    if (patch === undefined) {
      // Clearing the source drops the whole binding — an unbound choice field
      // falls back to its native (static/input/expression) options.
      editField(field, "dataSource", undefined);
      return;
    }
    editField(field, "dataSource", { ...(field.dataSource ?? {}), ...patch });
  };

  const setColumn = (key: "value" | "label", value: string) => {
    const next: DataSourceBinding = { ...(field.dataSource ?? {}) };
    if (value) next[key] = value;
    else delete next[key];
    editField(field, "dataSource", next);
  };

  const sourceOptions: SelectOption[] = [
    { value: UNBOUND, label: "\u2014 none \u2014" },
    ...getSources().map((s) => ({ value: s, label: s })),
  ];

  const entries = [
    selectEntry(
      `${id}-source`,
      "Datasource",
      "Populate this field's options live from a datasource alias.",
      source,
      sourceOptions,
      (v) => (v ? setBinding({ source: v }) : setBinding(undefined)),
    ),
  ];
  // Only surface the query/column entries once a source is chosen — an unbound
  // field shows just the selector.
  if (source) {
    entries.push(
      textAreaEntry(
        `${id}-query`,
        "Query (SQL)",
        "SELECT returning one row per option.",
        field.dataSource?.query ?? "",
        (v) => setBinding({ query: v }),
      ),
      textFieldEntry(
        `${id}-value`,
        "Value column",
        'Column mapped to each option value (default "value").',
        field.dataSource?.value ?? "",
        (v) => setColumn("value", v),
      ),
      textFieldEntry(
        `${id}-label`,
        "Label column",
        'Column mapped to each option label (default "label").',
        field.dataSource?.label ?? "",
        (v) => setColumn("label", v),
      ),
    );
  }

  return h(
    "div",
    { class: "bio-properties-panel-group", "data-group-id": `group-${id}` },
    h(
      "div",
      {
        class: `bio-properties-panel-group-header ${open ? "open" : ""}`,
        onClick: () => setOpen(!open),
      },
      h("div", { class: "bio-properties-panel-group-header-title" }, label),
      h(
        "div",
        { class: "bio-properties-panel-group-header-buttons" },
        h(
          "button",
          {
            type: "button",
            title: "Toggle section",
            class: "bio-properties-panel-group-header-button bio-properties-panel-arrow",
          },
          h(
            "svg",
            {
              width: 16,
              height: 16,
              xmlns: "http://www.w3.org/2000/svg",
              class: open ? "bio-properties-panel-arrow-down" : "bio-properties-panel-arrow-right",
            },
            h("path", { "fill-rule": "evenodd", d: ARROW_PATH }),
          ),
        ),
      ),
    ),
    h("div", { class: `bio-properties-panel-group-entries ${open ? "open" : ""}` }, entries),
  );
}

/**
 * Builds the didi module that registers the Urban data-source properties
 * provider. `getSources` returns the datasource aliases declared in the current
 * project's `nano.app.json` (`data.sources`); it is read lazily on every panel
 * render so switching projects/files reflects the right list.
 */
export function createFormDataBindingModule(getSources: () => string[]) {
  class DataBindingPropertiesProvider {
    static $inject = ["propertiesPanel"];

    constructor(propertiesPanel: {
      registerProvider: (provider: unknown, priority?: number) => void;
    }) {
      // Default priority appends our group after the built-in ones.
      propertiesPanel.registerProvider(this);
    }

    getGroups(field: FormField, editField: EditField) {
      return (groups: Array<unknown>) => {
        if (field && typeof field.type === "string" && CHOICE_FIELD_TYPES.has(field.type)) {
          groups.push({
            id: "nanoDataSource",
            label: "Data source (Urban)",
            component: DataSourceGroup,
            field,
            editField,
            getSources,
          });
        }
        return groups;
      };
    }
  }

  return {
    __init__: ["nanoDataBindingPropertiesProvider"],
    nanoDataBindingPropertiesProvider: ["type", DataBindingPropertiesProvider],
  };
}
