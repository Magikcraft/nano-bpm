//! `impl Engine` methods: resolve concern (extracted from the monolithic engine module).

use super::*;

impl Engine {

    /// Resolves the correlation value a subscription captures at open time by
    /// evaluating the stored `correlation_key` FEEL expression against the
    /// instance variables (a bare name like `orderId` is just a variable
    /// reference; `order.id` reads a context member). A missing variable, an
    /// empty key, or an evaluation error yields the empty string (matching the
    /// REST default `correlationKey` of `""`).
    pub(crate) fn resolve_correlation_value(&self, instance_key: Key, correlation_key: &str) -> String {
        if correlation_key.is_empty() {
            return String::new();
        }
        let vars = self.variables(instance_key);
        crate::feel::eval_string(correlation_key, &vars).unwrap_or_default()
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
    pub(crate) fn resolve_user_task_list(&self, instance_key: Key, raw: Option<&str>) -> Vec<String> {
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
}
