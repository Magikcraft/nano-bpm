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

    /// The variables visible to a specific element instance: its scope's local
    /// variables layered over its ancestors up to the root. For an element in the
    /// root scope with no sub-scopes this returns the shared instance-variables
    /// `Arc` directly (a cheap refcount bump); only an element that lives in (or
    /// under) a sub-scope — a sub-process, a multi-instance body or child — pays
    /// the clone-and-merge cost.
    pub(crate) fn variables_for_element(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> Arc<HashMap<String, Value>> {
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Arc::default();
        };
        // The hierarchical scoped view visible to this element (root scope
        // fast-path returns the shared `variables` Arc unchanged). Multi-instance
        // child bindings (`inputElement`/`loopCounter`) and sub-process locals all
        // live in the child/sub-process scope, resolved by this walk.
        let scope = self.variable_scope_of(instance, element_instance_key);
        self.visible_variables(instance, scope)
    }

    /// The variable scope that directly owns `key`'s variables: `key` itself when
    /// it is a registered scope-owner (sub-process / multi-instance body or
    /// child), otherwise the nearest enclosing scope recorded when the element
    /// activated, defaulting to the root (process-instance) scope. Root-only
    /// instances always resolve to the instance key.
    pub(crate) fn variable_scope_of(
        &self,
        instance: &crate::state::ProcessInstance,
        key: Key,
    ) -> Key {
        if key == 0 || key == instance.key {
            return instance.key;
        }
        if instance.scope_parents.contains_key(&key) {
            return key;
        }
        // `scopes` maps an active element instance to its enclosing sub-process
        // scope-owner; walk to a registered scope, else fall back to root.
        match instance.scopes.get(&key) {
            Some(&enclosing) if instance.scope_parents.contains_key(&enclosing) => enclosing,
            _ => instance.key,
        }
    }

    /// The variables visible in `scope_key`, resolving each name from the scope
    /// upward to the root (a local binding shadows an ancestor's). The root scope
    /// with no child scopes is the hot path: it returns the shared `variables`
    /// `Arc` directly (a refcount bump, no clone), preserving the flat engine's
    /// job-activation cost. Only instances that actually opened a sub-scope pay
    /// the merge.
    pub(crate) fn visible_variables(
        &self,
        instance: &crate::state::ProcessInstance,
        scope_key: Key,
    ) -> Arc<HashMap<String, Value>> {
        let is_root = scope_key == 0 || scope_key == instance.key;
        if is_root || instance.scope_variables.is_empty() {
            return Arc::clone(&instance.variables);
        }
        // Collect the scope chain leaf -> ... -> root, then merge root-first so
        // nearer scopes overwrite (shadow) farther ones.
        let mut chain: Vec<Key> = Vec::new();
        let mut cur = scope_key;
        loop {
            chain.push(cur);
            match instance.scope_parents.get(&cur) {
                Some(&parent) if parent != cur => cur = parent,
                _ => break,
            }
        }
        let mut merged = (*instance.variables).clone();
        for scope in chain.iter().rev() {
            if let Some(local) = instance.scope_variables.get(scope) {
                for (k, v) in local {
                    merged.insert(k.clone(), v.clone());
                }
            }
        }
        Arc::new(merged)
    }

    /// Builds the variable-write **events** for merging `variables` into `scope`
    /// with Zeebe propagation semantics (see [`Self::propagate_variables`]).
    ///
    /// A write that resolves to the root (process-instance) scope is emitted as
    /// the legacy flat [`crate::Event::VariablesUpdated`] so the log, read model
    /// and conditional-event re-evaluation stay byte-identical to the pre-scoping
    /// engine; a write that lands in any other scope is emitted as
    /// [`crate::Event::ScopedVariablesUpdated`]. The result is sorted by scope key
    /// for deterministic replay.
    pub(crate) fn propagated_updates(
        &self,
        instance_key: Key,
        scope: Key,
        variables: HashMap<String, Value>,
        local: bool,
    ) -> Vec<crate::Event> {
        let mut writes = self.propagate_variables(instance_key, scope, variables, local);
        writes.sort_by_key(|(dest, _)| *dest);
        writes
            .into_iter()
            .filter(|(_, vars)| !vars.is_empty())
            .map(|(dest, vars)| {
                if dest == 0 || dest == instance_key {
                    crate::Event::VariablesUpdated {
                        instance_key,
                        variables: vars,
                    }
                } else {
                    crate::Event::ScopedVariablesUpdated {
                        instance_key,
                        scope_key: dest,
                        variables: vars,
                    }
                }
            })
            .collect()
    }

    /// Resolves Zeebe variable **propagation** for a merge of `variables` into
    /// `target_scope`, returning the per-scope writes to emit (as
    /// [`crate::Event::ScopedVariablesUpdated`]). With `local` true every value
    /// lands in `target_scope`. Otherwise each name is written to the nearest
    /// ancestor scope (from the target upward) that already defines it; a name
    /// defined nowhere is created in the root scope. Root-only instances collapse
    /// to a single root write, matching the flat engine.
    pub(crate) fn propagate_variables(
        &self,
        instance_key: Key,
        target_scope: Key,
        variables: HashMap<String, Value>,
        local: bool,
    ) -> Vec<(Key, HashMap<String, Value>)> {
        if variables.is_empty() {
            return Vec::new();
        }
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return vec![(instance_key, variables)];
        };
        let root = instance.key;
        let target = self.variable_scope_of(instance, target_scope);
        // Fast path: local write, or a root-only instance — one scope, no walk.
        if local {
            return vec![(target, variables)];
        }
        if instance.scope_variables.is_empty() && instance.scope_parents.is_empty() {
            return vec![(root, variables)];
        }
        let mut by_scope: HashMap<Key, HashMap<String, Value>> = HashMap::new();
        for (name, value) in variables {
            let dest = self.scope_defining(instance, target, &name).unwrap_or(root);
            by_scope.entry(dest).or_default().insert(name, value);
        }
        by_scope.into_iter().collect()
    }

    /// The nearest scope (from `scope` upward to the root) that already defines
    /// `name`, or `None` if no scope in the chain holds it.
    fn scope_defining(
        &self,
        instance: &crate::state::ProcessInstance,
        scope: Key,
        name: &str,
    ) -> Option<Key> {
        let root = instance.key;
        let mut cur = scope;
        loop {
            let holds = if cur == root {
                instance.variables.contains_key(name)
            } else {
                instance
                    .scope_variables
                    .get(&cur)
                    .is_some_and(|m| m.contains_key(name))
            };
            if holds {
                return Some(cur);
            }
            if cur == root {
                return None;
            }
            match instance.scope_parents.get(&cur) {
                Some(&parent) if parent != cur => cur = parent,
                _ => return None,
            }
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
