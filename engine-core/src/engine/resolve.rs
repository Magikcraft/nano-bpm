//! `impl Engine` methods: resolve concern (extracted from the monolithic engine module).

use super::*;

impl Engine {
    /// Resolves the correlation value a subscription captures at open time by
    /// evaluating the stored `correlation_key` FEEL expression against the
    /// instance variables (a bare name like `orderId` is just a variable
    /// reference; `order.id` reads a context member). A missing variable, an
    /// empty key, or an evaluation error yields the empty string (matching the
    /// REST default `correlationKey` of `""`).
    pub(crate) fn resolve_correlation_value(
        &self,
        instance_key: Key,
        correlation_key: &str,
    ) -> String {
        if correlation_key.is_empty() {
            return String::new();
        }
        let vars = self.variables(instance_key);
        crate::feel::eval_string(correlation_key, &vars).unwrap_or_default()
    }

    /// Resolves a message or signal event `name` at subscription-open time.
    ///
    /// A static value (`"order canceled"`) is returned verbatim. A FEEL
    /// expression (a leading `=`, e.g. `="order " + awaitingAction`) is
    /// evaluated to a string against the instance variables, falling back to the
    /// literal text when it cannot be evaluated (parse error, unresolved
    /// variable, non-string result) — matching the error-tolerant behaviour of
    /// the other `resolve_*` helpers, which have no incident path.
    ///
    /// `instance_key` is `None` for a message-start-event name, which Zeebe
    /// evaluates at deploy time against an empty context; the catch/boundary
    /// cases pass `Some(instance_key)` so the name is evaluated on activation.
    pub(crate) fn resolve_event_name(&self, instance_key: Option<Key>, raw: &str) -> String {
        let trimmed = raw.trim();
        if !trimmed.starts_with('=') {
            return raw.to_string();
        }
        let empty = HashMap::new();
        let vars = instance_key.map(|k| self.variables(k));
        let ctx: &HashMap<String, Value> = vars.as_deref().unwrap_or(&empty);
        crate::feel::eval_string(trimmed, ctx).unwrap_or_else(|_| raw.to_string())
    }

    /// Resolves a job's initial retry count from the `zeebe:taskDefinition`
    /// `retries` expression declared on the element. A literal integer (`"5"`)
    /// is used directly; a FEEL expression (leading `=`, e.g. `=maxRetries`) is
    /// evaluated to a number against the instance variables. `None` (no
    /// declaration) or an unresolvable expression defaults to
    /// [`crate::state::DEFAULT_JOB_RETRIES`]. The result is floored at 0.
    pub(crate) fn resolve_retries(&self, instance_key: Key, raw: Option<&str>) -> i32 {
        let default = crate::state::DEFAULT_JOB_RETRIES;
        let Some(raw) = raw else {
            return default;
        };
        let trimmed = raw.trim();
        let value = if let Some(expr) = trimmed.strip_prefix('=') {
            let vars = self.variables(instance_key);
            match crate::feel::eval(expr, &vars) {
                Ok(Value::Int(i)) => i as i32,
                Ok(Value::Double(d)) => d as i32,
                _ => return default,
            }
        } else {
            match trimmed.parse::<i32>() {
                Ok(i) => i,
                Err(_) => return default,
            }
        };
        value.max(0)
    }

    /// The raw `retries` expression declared on `element_id` (if any).
    pub(crate) fn retries_of(&self, instance_key: Key, element_id: &str) -> Option<String> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .and_then(|e| e.retries.clone())
    }

    /// Evaluates the given io mappings against an explicit variable context
    /// `vars` (rather than the instance's persisted variables), returning the
    /// merged projection. Used when an as-yet-unapplied result (e.g. a script
    /// task's `resultVariable`) must be visible to output mappings within the
    /// same step.
    pub(crate) fn eval_io_mappings_in(
        &self,
        vars: &HashMap<String, Value>,
        mappings: &[crate::model::Mapping],
    ) -> HashMap<String, Value> {
        let mut result: HashMap<String, Value> = HashMap::new();
        for m in mappings {
            match crate::feel::eval(m.source.trim(), vars) {
                Ok(value) => Self::assign_io_target(&mut result, vars, &m.target, value),
                Err(_) => continue,
            }
        }
        result
    }

    pub(crate) fn outgoing(&self, instance_key: Key, element_id: &str) -> Vec<SequenceFlow> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .map(|e| e.outgoing.clone())
            .unwrap_or_default()
    }

    pub(crate) fn incoming_count(&self, instance_key: Key, element_id: &str) -> usize {
        self.process_of_instance(instance_key)
            .map(|p| p.incoming_count(element_id))
            .unwrap_or(0)
    }

    pub(crate) fn variables(&self, instance_key: Key) -> Arc<HashMap<String, Value>> {
        self.state
            .instances
            .get(&instance_key)
            .map(|i| Arc::clone(&i.variables))
            .unwrap_or_default()
    }

    /// The variables visible to a specific element instance: the instance
    /// variables with any per-element local overlay (a multi-instance child's
    /// `inputElement`/`loopCounter` bindings) merged on top. For the common case
    /// of an element with no locals this returns the shared instance-variables
    /// `Arc` directly (a cheap refcount bump); only a multi-instance child pays
    /// the clone-and-overlay cost.
    pub(crate) fn variables_for_element(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> Arc<HashMap<String, Value>> {
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Arc::default();
        };
        match instance.element_locals.get(&element_instance_key) {
            Some(locals) if !locals.is_empty() => {
                let mut merged = (*instance.variables).clone();
                for (k, v) in locals {
                    merged.insert(k.clone(), v.clone());
                }
                Arc::new(merged)
            }
            _ => Arc::clone(&instance.variables),
        }
    }

    /// Resolves a service task's job type against the instance's variables.
    ///
    /// A static type (`"payment"`) is returned verbatim. A FEEL expression
    /// (`"=jobType"`, as emitted by the Camunda modeler) is treated as a simple
    /// variable reference: the leading `=` is stripped and the named variable's
    /// value supplies the type, so the job is created with the runtime type a
    /// worker actually subscribes to. An unresolvable expression (no such
    /// variable) falls back to the literal text — nano has no FEEL evaluator to
    /// raise an incident, and the literal at least surfaces the misconfiguration.
    /// Resolves a service task's job type at job-creation time. A static type is
    /// returned verbatim. A FEEL expression (a leading `=`, e.g. `=jobType` or
    /// `="worker-" + region`) is evaluated against the instance variables via
    /// [`crate::feel`], expecting a string-like result. An expression that fails
    /// to evaluate (parse error, unresolved variable, non-string result) falls
    /// back to the literal text — nano does not raise an incident here.
    pub(crate) fn resolve_job_type(&self, instance_key: Key, job_type: &str) -> String {
        let trimmed = job_type.trim();
        if !trimmed.starts_with('=') {
            return job_type.to_string();
        }
        let vars = self.variables(instance_key);
        crate::feel::eval_string(trimmed, &vars).unwrap_or_else(|_| job_type.to_string())
    }

    /// Resolves a user-task string attribute (assignee, due/follow-up date)
    /// declared on the BPMN element. A literal is returned verbatim; a FEEL
    /// expression (leading `=`) is evaluated against the instance variables,
    /// falling back to the literal text when it cannot be evaluated. `None` (the
    /// attribute was not declared) resolves to `None`.
    pub(crate) fn resolve_user_task_string(
        &self,
        instance_key: Key,
        raw: Option<&str>,
    ) -> Option<String> {
        let raw = raw?;
        let trimmed = raw.trim();
        if !trimmed.starts_with('=') {
            return Some(raw.to_string());
        }
        let vars = self.variables(instance_key);
        Some(crate::feel::eval_string(trimmed, &vars).unwrap_or_else(|_| raw.to_string()))
    }

    /// Resolves a user-task candidate list (groups or users). A literal is a
    /// comma-separated list. A FEEL expression (leading `=`) is evaluated against
    /// the instance variables; a list result yields its string items, a string
    /// result is split on commas, anything else (or a failure) yields an empty
    /// list. `None` resolves to an empty list.
    pub(crate) fn resolve_user_task_list(
        &self,
        instance_key: Key,
        raw: Option<&str>,
    ) -> Vec<String> {
        let Some(raw) = raw else {
            return Vec::new();
        };
        let trimmed = raw.trim();
        let split = |s: &str| -> Vec<String> {
            s.split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect()
        };
        if !trimmed.starts_with('=') {
            return split(raw);
        }
        let vars = self.variables(instance_key);
        match crate::feel::eval(trimmed, &vars) {
            Ok(Value::List(items)) => items
                .into_iter()
                .filter_map(|v| match v {
                    Value::Str(s) => Some(s),
                    Value::Int(i) => Some(i.to_string()),
                    _ => None,
                })
                .filter(|s| !s.is_empty())
                .collect(),
            Ok(Value::Str(s)) => split(&s),
            _ => Vec::new(),
        }
    }

    /// Resolves a user-task priority expression. A literal integer or a FEEL
    /// expression yielding a number is clamped to `0..=100`; anything
    /// unresolvable (or absent) defaults to `50`.
    /// Resolves a raw priority expression (literal or `=FEEL`) against the
    /// instance variables to a `0..=100` value, defaulting to 50 when absent or
    /// unresolvable. Shared by user-task scheduling priority and service-task job
    /// (activation) priority.
    pub(crate) fn resolve_priority(&self, instance_key: Key, raw: Option<&str>) -> i32 {
        const DEFAULT_PRIORITY: i32 = 50;
        let Some(raw) = raw else {
            return DEFAULT_PRIORITY;
        };
        let trimmed = raw.trim();
        let value = if let Some(expr) = trimmed.strip_prefix('=') {
            let vars = self.variables(instance_key);
            match crate::feel::eval(expr, &vars) {
                Ok(Value::Int(i)) => i as i32,
                Ok(Value::Double(d)) => d as i32,
                _ => return DEFAULT_PRIORITY,
            }
        } else {
            match trimmed.parse::<i32>() {
                Ok(i) => i,
                Err(_) => return DEFAULT_PRIORITY,
            }
        };
        value.clamp(0, 100)
    }

    /// Resolves a timer's due time (and, for a cycle, its re-arm interval) at
    /// timer-creation time, honouring a FEEL timer expression
    /// ([`crate::model::TimerDef`]) when one is declared on the element, and
    /// falling back to the statically-parsed `fallback_millis` otherwise.
    ///
    /// Returns `(due_at, interval_millis)` where `due_at` is an absolute epoch
    /// time in the engine's millisecond clock and `interval_millis` is the delay
    /// used to re-arm a repeating (cycle) timer (0 for an absolute date).
    /// `instance_key` is `None` for a process-level start timer evaluated at
    /// deploy against an empty context.
    pub(crate) fn resolve_timer(
        &self,
        instance_key: Option<Key>,
        def: Option<&crate::model::TimerDef>,
        base_now: u64,
        fallback_millis: u64,
    ) -> (u64, u64) {
        let default = (base_now.saturating_add(fallback_millis), fallback_millis);
        let Some(def) = def else {
            return default;
        };

        // A `=`-prefixed expression is FEEL, evaluated against the instance
        // variables (or an empty context at deploy); a bare value (only for a
        // literal timeDate) is used directly.
        let evaluated: Option<String> = if def.expr.trim_start().starts_with('=') {
            let empty = HashMap::new();
            let vars = instance_key.map(|k| self.variables(k));
            let ctx: &HashMap<String, Value> = vars.as_deref().unwrap_or(&empty);
            crate::feel::eval_string(&def.expr, ctx).ok()
        } else {
            Some(def.expr.clone())
        };
        let Some(text) = evaluated else {
            return default;
        };
        let text = text.trim();

        match def.kind {
            crate::model::TimerDefKind::Duration => {
                match crate::bpmn::parse_iso8601_duration(text) {
                    Some(ms) => (base_now.saturating_add(ms), ms),
                    None => default,
                }
            }
            crate::model::TimerDefKind::Cycle => match crate::bpmn::parse_iso8601_cycle(text) {
                Some(ms) => (base_now.saturating_add(ms), ms),
                None => default,
            },
            crate::model::TimerDefKind::Date => {
                match crate::feel::temporal::DateTime::parse(text) {
                    Some(dt) => {
                        let millis = (dt.epoch_nanos() / 1_000_000).max(0) as u64;
                        (millis, 0)
                    }
                    None => default,
                }
            }
        }
    }

    /// Looks up the FEEL timer expression declared on `element_id` in the process
    /// definition that owns `instance_key` (if any).
    pub(crate) fn timer_def_of(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Option<crate::model::TimerDef> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .and_then(|e| e.timer.clone())
    }

    /// Resolves a variable scope key to the process instance that owns it. The
    /// key may be the process instance itself or any of its active element
    /// instances; nano keeps a single instance-level variable scope, so both map
    /// to the same instance.
    pub(crate) fn resolve_scope(&self, scope_key: Key) -> Option<Key> {
        if self.state.instances.contains_key(&scope_key) {
            return Some(scope_key);
        }
        self.state
            .instances
            .values()
            .find(|i| i.active.contains_key(&scope_key))
            .map(|i| i.key)
    }

    pub(crate) fn join_eik(&self, instance_key: Key, element_id: &str) -> Option<Key> {
        self.state
            .instances
            .get(&instance_key)?
            .join_instances
            .get(element_id)
            .copied()
    }

    pub(crate) fn join_count(&self, instance_key: Key, element_id: &str) -> usize {
        self.state
            .instances
            .get(&instance_key)
            .and_then(|i| i.join_counts.get(element_id).copied())
            .unwrap_or(0)
    }

    /// Evaluates a list of `zeebe:input`/`zeebe:output` [`Mapping`]s against the
    /// instance variables and returns the top-level variable updates to merge.
    ///
    /// Each mapping's `source` FEEL expression is evaluated against the instance
    /// variables; the result is written to its `target` path (a plain name or a
    /// dotted path naming a nested context entry). Nano keeps a single flat
    /// instance-level variable scope, so the merge map is applied via a normal
    /// [`Event::VariablesUpdated`]. A mapping whose source fails to evaluate
    /// (parse error, unresolved variable) is skipped — matching the
    /// error-tolerant behaviour of the other `resolve_*` helpers, which have no
    /// incident path for expression failures.
    pub(crate) fn eval_io_mappings(
        &self,
        instance_key: Key,
        mappings: &[crate::model::Mapping],
    ) -> HashMap<String, Value> {
        let vars = self.variables(instance_key);
        self.eval_io_mappings_in(&vars, mappings)
    }

    /// Writes `value` to `target` (a plain name or a dotted path) inside the
    /// accumulating merge map, seeding nested context from the existing instance
    /// variables so a partial-path mapping (`order.total`) preserves the other
    /// members of `order`.
    fn assign_io_target(
        result: &mut HashMap<String, Value>,
        vars: &HashMap<String, Value>,
        target: &str,
        value: Value,
    ) {
        let parts: Vec<&str> = target.split('.').filter(|p| !p.is_empty()).collect();
        match parts.as_slice() {
            [] => {}
            [single] => {
                result.insert((*single).to_string(), value);
            }
            [top, rest @ ..] => {
                let mut root = result
                    .get(*top)
                    .or_else(|| vars.get(*top))
                    .cloned()
                    .unwrap_or_else(|| Value::Map(std::collections::BTreeMap::new()));
                Self::set_io_path(&mut root, rest, value);
                result.insert((*top).to_string(), root);
            }
        }
    }

    /// Sets a nested path inside a [`Value`], coercing intermediate nodes to
    /// maps as needed.
    fn set_io_path(node: &mut Value, parts: &[&str], value: Value) {
        let Some((head, tail)) = parts.split_first() else {
            *node = value;
            return;
        };
        if !matches!(node, Value::Map(_)) {
            *node = Value::Map(std::collections::BTreeMap::new());
        }
        if let Value::Map(map) = node {
            let entry = map
                .entry((*head).to_string())
                .or_insert_with(|| Value::Map(std::collections::BTreeMap::new()));
            Self::set_io_path(entry, tail, value);
        }
    }

    /// Cloned input mappings declared on an element (empty if none).
    pub(crate) fn io_inputs(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Vec<crate::model::Mapping> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .map(|e| e.io.inputs.clone())
            .unwrap_or_default()
    }

    /// Cloned output mappings declared on an element (empty if none).
    pub(crate) fn io_outputs(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Vec<crate::model::Mapping> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .map(|e| e.io.outputs.clone())
            .unwrap_or_default()
    }
}
