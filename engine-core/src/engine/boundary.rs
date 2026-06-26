//! `impl Engine` methods: boundary concern (extracted from the monolithic engine module).

use super::*;

impl Engine {

    /// Finds the error boundary event attached to `task_element_id` that catches
    /// `error_code`, if any. When several match (a malformed model), the one with
    /// the smallest id is chosen so selection stays deterministic.
    pub(crate) fn find_error_boundary(
        &self,
        instance_key: Key,
        task_element_id: &str,
        error_code: &str,
    ) -> Option<ElementId> {
        let process = self.process_of_instance(instance_key)?;
        process
            .elements
            .values()
            .filter(|e| match &e.kind {
                ElementKind::ErrorBoundaryEvent {
                    attached_to,
                    error_code: ec,
                } => attached_to == task_element_id && ec == error_code,
                _ => false,
            })
            .map(|e| e.id.clone())
            .min()
    }

    /// The element instance of the embedded sub-process that encloses
    /// `element_instance_key`, or `0` if it lives in the process-level scope.
    pub(crate) fn scope_of(&self, instance_key: Key, element_instance_key: Key) -> Key {
        self.state
            .instances
            .get(&instance_key)
            .and_then(|i| i.scopes.get(&element_instance_key).copied())
            .unwrap_or(0)
    }

    /// The element id of an active element instance, if it is still active.
    pub(crate) fn element_id_of_instance(&self, instance_key: Key, element_instance_key: Key) -> Option<ElementId> {
        self.state
            .instances
            .get(&instance_key)?
            .active
            .get(&element_instance_key)
            .cloned()
    }

    /// Finds the error boundary that catches `error_code` thrown from the
    /// activity `from_element_id` (element instance `from_eik`), propagating up
    /// enclosing sub-process scopes until one is found. Returns
    /// `(boundary_id, caught_element_instance_key, caught_element_id)` — the
    /// boundary event and the activity it is attached to (the throwing task
    /// itself or an enclosing sub-process).
    pub(crate) fn find_catching_error_boundary(
        &self,
        instance_key: Key,
        from_element_id: &str,
        from_eik: Key,
        error_code: &str,
    ) -> Option<(ElementId, Key, ElementId)> {
        let mut element_id = from_element_id.to_string();
        let mut eik = from_eik;
        loop {
            if let Some(boundary_id) =
                self.find_error_boundary(instance_key, &element_id, error_code)
            {
                return Some((boundary_id, eik, element_id));
            }
            // Propagate to the enclosing sub-process, if any.
            let parent = self.scope_of(instance_key, eik);
            if parent == 0 {
                return None;
            }
            element_id = self.element_id_of_instance(instance_key, parent)?;
            eik = parent;
        }
    }

    /// Every element instance transitively contained in the sub-process scope
    /// `scope_eik`, sorted by key for deterministic processing.
    pub(crate) fn scope_descendants(&self, instance_key: Key, scope_eik: Key) -> Vec<Key> {
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        let mut stack = vec![scope_eik];
        while let Some(parent) = stack.pop() {
            for (child, p) in &instance.scopes {
                if *p == parent {
                    result.push(*child);
                    stack.push(*child);
                }
            }
        }
        result.sort_unstable();
        result
    }

    /// Terminates a sub-process scope: cancels every in-play job, armed timer and
    /// open subscription on each element instance inside `scope_eik` (and nested
    /// scopes) and completes those element instances. The sub-process element
    /// instance itself is left for the caller to complete. Used when an
    /// interrupting error boundary catches an error inside the sub-process.
    pub(crate) fn terminate_subprocess_scope(
        &mut self,
        log: &mut Vec<Event>,
        instance_key: Key,
        scope_eik: Key,
    ) {
        for eik in self.scope_descendants(instance_key, scope_eik) {
            let element_id = self
                .element_id_of_instance(instance_key, eik)
                .unwrap_or_default();
            if let Some(job_key) = self.active_job_on(eik) {
                self.emit(
                    log,
                    Event::JobCanceled {
                        job_key,
                        instance_key,
                    },
                );
            }
            for event in self.cancel_all_timers_on(eik) {
                self.emit(log, event);
            }
            for event in self.cancel_all_subscriptions_on(eik) {
                self.emit(log, event);
            }
            self.emit(
                log,
                Event::ElementCompleting {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                },
            );
            self.emit(
                log,
                Event::ElementCompleted {
                    instance_key,
                    element_instance_key: eik,
                    element_id,
                },
            );
        }
    }

    /// Cancels every armed (`Created`) timer resting on `element_instance_key`,
    /// regardless of kind, returning the `TimerCanceled` events (sorted by key).
    /// Used to tear down a sub-process scope on interruption.
    pub(crate) fn cancel_all_timers_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut timers: Vec<&state::Timer> = self
            .state
            .timers
            .values()
            .filter(|t| {
                t.element_instance_key == element_instance_key
                    && t.state == state::TimerState::Created
            })
            .collect();
        timers.sort_by_key(|t| t.key);
        timers
            .into_iter()
            .map(|t| Event::TimerCanceled {
                timer_key: t.key,
                instance_key: t.instance_key,
                element_instance_key: t.element_instance_key,
                element_id: t.element_id.clone(),
            })
            .collect()
    }

    /// Cancels every open subscription resting on `element_instance_key`,
    /// regardless of kind, returning the `MessageSubscriptionCanceled` events
    /// (sorted by key). Used to tear down a sub-process scope on interruption.
    pub(crate) fn cancel_all_subscriptions_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut subs: Vec<&state::MessageSubscription> = self
            .state
            .message_subscriptions
            .values()
            .filter(|s| {
                s.element_instance_key == element_instance_key
                    && matches!(
                        s.state,
                        state::MessageSubscriptionState::Open
                            | state::MessageSubscriptionState::Opening
                    )
            })
            .collect();
        subs.sort_by_key(|s| s.key);
        subs.into_iter()
            .map(Self::disarm_subscription_event)
            .collect()
    }

    /// The disarm event for a subscription being torn down. A cross-partition
    /// parked placeholder (state `Opening`, its canonical record lives on the
    /// `hash(correlation_key)` partition) emits a routable
    /// `MessageSubscriptionClosing` so the host disarms the canonical record;
    /// any other (local, `Open`) subscription emits a plain
    /// `MessageSubscriptionCanceled`. Single-partition runs only ever hold
    /// `Open` subs, so they always take the latter branch and stay
    /// byte-identical.
    pub(crate) fn disarm_subscription_event(s: &state::MessageSubscription) -> Event {
        if s.state == state::MessageSubscriptionState::Opening {
            Event::MessageSubscriptionClosing {
                subscription_key: s.key,
                instance_key: s.instance_key,
                element_instance_key: s.element_instance_key,
                element_id: s.element_id.clone(),
                message_name: s.message_name.clone(),
                correlation_key: s.correlation_key.clone(),
            }
        } else {
            Event::MessageSubscriptionCanceled {
                subscription_key: s.key,
                instance_key: s.instance_key,
                element_instance_key: s.element_instance_key,
                element_id: s.element_id.clone(),
            }
        }
    }

    /// The key of a still-in-play job parked on `element_instance_key`, if any.
    /// A job is "in play" until it is completed, errored or cancelled; this is
    /// the job an interrupting boundary event cancels. At most one such job
    /// exists per element instance.
    pub(crate) fn active_job_on(&self, element_instance_key: Key) -> Option<Key> {
        self.state
            .jobs
            .values()
            .find(|j| {
                j.element_instance_key == element_instance_key
                    && !matches!(
                        j.state,
                        state::JobState::Completed
                            | state::JobState::Errored
                            | state::JobState::Canceled
                    )
            })
            .map(|j| j.key)
    }

    /// All interrupting timer boundary events attached to `activity_id`, as
    /// `(boundary_id, duration_millis)` sorted by boundary id (so arming is
    /// deterministic). Empty when the activity has no timer boundaries.
    /// Arms a timer and/or opens a subscription for every boundary event
    /// attached to `element_id` (a service task or sub-process), returning the
    /// `TimerCreated`/`MessageSubscriptionCreated` events. Firing one later
    /// interrupts the activity (interrupting) or spawns a parallel token beside
    /// it (non-interrupting).
    pub(crate) fn arm_boundary_events(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        for (boundary_id, duration_millis, interrupting) in
            self.attached_timer_boundaries(instance_key, element_id)
        {
            let timer_key = self.mint_key();
            let kind = if interrupting {
                state::TimerKind::InterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            } else {
                state::TimerKind::NonInterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            };
            events.push(Event::TimerCreated {
                timer_key,
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
                due_at: self.now.saturating_add(duration_millis),
                kind,
            });
        }
        for (boundary_id, message_name, correlation_key, interrupting) in
            self.attached_message_boundaries(instance_key, element_id)
        {
            let subscription_key = self.mint_key();
            let correlation_value = self.resolve_correlation_value(instance_key, &correlation_key);
            let kind = if interrupting {
                state::MessageSubscriptionKind::InterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            } else {
                state::MessageSubscriptionKind::NonInterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            };
            // Placement matches the intermediate catch: a boundary subscription
            // whose correlation key hashes off-partition is opened on the message
            // partition; locally we keep only an `Opening` record (the host routes
            // the open). Single-partition always opens locally.
            if self.subscription_partition(&correlation_value) == self.partition_id {
                events.push(Event::MessageSubscriptionCreated {
                    subscription_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.to_string(),
                    message_name,
                    correlation_key: correlation_value,
                    kind,
                });
            } else {
                events.push(Event::MessageSubscriptionOpening {
                    subscription_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.to_string(),
                    message_name,
                    correlation_key: correlation_value,
                    kind,
                });
            }
        }
        events
    }

    /// Whether `element_id` is an embedded sub-process (interrupting it must tear
    /// down its whole inner token scope, not just cancel a job).
    pub(crate) fn is_subprocess(&self, instance_key: Key, element_id: &str) -> bool {
        matches!(
            self.element_kind(instance_key, element_id),
            Some(ElementKind::SubProcess { .. })
        )
    }

    /// Interrupts the activity `element_instance_key`/`element_id` because an
    /// interrupting timer or message boundary fired on it: tears down the work it
    /// owns, completes its element instance, and disarms any sibling boundaries.
    /// A service task's job is cancelled; a sub-process's whole inner token scope
    /// is terminated. The caller then routes the boundary's outgoing flow.
    pub(crate) fn interrupt_activity_via_boundary(
        &mut self,
        log: &mut Vec<Event>,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
    ) {
        if self.is_subprocess(instance_key, element_id) {
            // Cancel every job/timer/subscription inside the sub-process and
            // complete its inner element instances first.
            self.terminate_subprocess_scope(log, instance_key, element_instance_key);
        } else if let Some(job_key) = self.active_job_on(element_instance_key) {
            self.emit(
                log,
                Event::JobCanceled {
                    job_key,
                    instance_key,
                },
            );
        }
        self.emit(
            log,
            Event::ElementCompleting {
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
            },
        );
        self.emit(
            log,
            Event::ElementCompleted {
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
            },
        );
        // Disarm any sibling boundary timers and message subscriptions on it.
        for event in self.cancel_boundary_timers_on(element_instance_key) {
            self.emit(log, event);
        }
        for event in self.cancel_boundary_message_subscriptions_on(element_instance_key) {
            self.emit(log, event);
        }
    }

    pub(crate) fn attached_timer_boundaries(
        &self,
        instance_key: Key,
        activity_id: &str,
    ) -> Vec<(ElementId, u64, bool)> {
        let Some(process) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        let mut found: Vec<(ElementId, u64, bool)> = process
            .elements
            .values()
            .filter_map(|e| match &e.kind {
                ElementKind::TimerBoundaryEvent {
                    attached_to,
                    duration_millis,
                    interrupting,
                    repeating: _,
                } if attached_to == activity_id => {
                    Some((e.id.clone(), *duration_millis, *interrupting))
                }
                _ => None,
            })
            .collect();
        found.sort();
        found
    }

    /// Cancels every armed (`Created`) boundary timer resting on
    /// `element_instance_key` (interrupting or non-interrupting), returning the
    /// `TimerCanceled` events. Called when the guarded activity leaves the flow
    /// another way (it completed normally or a different boundary interrupted
    /// it), so a stale timer never fires later.
    pub(crate) fn cancel_boundary_timers_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut timers: Vec<&state::Timer> = self
            .state
            .timers
            .values()
            .filter(|t| {
                t.element_instance_key == element_instance_key
                    && t.state == state::TimerState::Created
                    && matches!(
                        t.kind,
                        state::TimerKind::InterruptingBoundary { .. }
                            | state::TimerKind::NonInterruptingBoundary { .. }
                    )
            })
            .collect();
        timers.sort_by_key(|t| t.key);
        timers
            .into_iter()
            .map(|t| Event::TimerCanceled {
                timer_key: t.key,
                instance_key: t.instance_key,
                element_instance_key: t.element_instance_key,
                element_id: t.element_id.clone(),
            })
            .collect()
    }

    /// All interrupting message boundary events attached to `activity_id`, as
    /// `(boundary_id, message_name, correlation_key)` sorted by boundary id (so
    /// arming is deterministic). Empty when the activity has no message
    /// boundaries.
    pub(crate) fn attached_message_boundaries(
        &self,
        instance_key: Key,
        activity_id: &str,
    ) -> Vec<(ElementId, String, String, bool)> {
        let Some(process) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        let mut found: Vec<(ElementId, String, String, bool)> = process
            .elements
            .values()
            .filter_map(|e| match &e.kind {
                ElementKind::MessageBoundaryEvent {
                    attached_to,
                    message_name,
                    correlation_key,
                    interrupting,
                } if attached_to == activity_id => Some((
                    e.id.clone(),
                    message_name.clone(),
                    correlation_key.clone(),
                    *interrupting,
                )),
                _ => None,
            })
            .collect();
        found.sort();
        found
    }

    /// Cancels every open boundary message subscription resting on
    /// `element_instance_key` (interrupting or non-interrupting), returning the
    /// `MessageSubscriptionCanceled` events. Called when the guarded activity
    /// leaves the flow another way (it completed normally or a different boundary
    /// interrupted it), so a stale subscription never correlates later.
    pub(crate) fn cancel_boundary_message_subscriptions_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut subs: Vec<&state::MessageSubscription> = self
            .state
            .message_subscriptions
            .values()
            .filter(|s| {
                s.element_instance_key == element_instance_key
                    && matches!(
                        s.state,
                        state::MessageSubscriptionState::Open
                            | state::MessageSubscriptionState::Opening
                    )
                    && matches!(
                        s.kind,
                        state::MessageSubscriptionKind::InterruptingBoundary { .. }
                            | state::MessageSubscriptionKind::NonInterruptingBoundary { .. }
                    )
            })
            .collect();
        subs.sort_by_key(|s| s.key);
        subs.into_iter()
            .map(Self::disarm_subscription_event)
            .collect()
    }
}
