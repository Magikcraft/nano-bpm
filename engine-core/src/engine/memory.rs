//! `impl Engine` methods: memory concern (extracted from the monolithic engine module).

use super::*;

impl Engine {

    /// Captures a complete, compact snapshot of this engine: the materialized
    /// [`State`] (already pruned of terminal instances) plus the scalar
    /// generator/clock metadata needed to resume minting keys and placing
    /// subscriptions without collision. Unlike an event log, its size tracks the
    /// *live* working set rather than growing with every command ever applied —
    /// the basis for bounded Raft state-machine snapshots.
    #[cfg(feature = "serde")]
    pub fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            state: self.state.clone(),
            partition_id: self.partition_id,
            next_local: self.next_local,
            num_partitions: self.num_partitions,
            now: self.now,
            start_dispatch_rr: self.start_dispatch_rr,
        }
    }

    /// Rebuilds an engine from an [`Engine::snapshot`] — the state-based
    /// counterpart to [`Engine::replay_partition`], restoring the exact
    /// materialized state and generator position in one step (no replay).
    #[cfg(feature = "serde")]
    pub fn from_snapshot(snapshot: EngineSnapshot) -> Self {
        assert!(
            snapshot.partition_id <= state::MAX_PARTITION_ID,
            "partition id {} exceeds MAX_PARTITION_ID {}",
            snapshot.partition_id,
            state::MAX_PARTITION_ID
        );
        Self {
            state: snapshot.state,
            partition_id: snapshot.partition_id,
            next_local: snapshot.next_local,
            num_partitions: snapshot.num_partitions.max(1),
            now: snapshot.now,
            start_dispatch_rr: snapshot.start_dispatch_rr,
            lenient_completion: false,
        }
    }

    /// Evicts a *completed* process instance and every entity it owns (jobs,
    /// timers, message subscriptions, incidents) from hot state, returning
    /// `true` if it was evicted. Process-level message-start subscriptions and
    /// timer-start events, and deployed definitions, are retained (they are not
    /// instance-scoped). No-op for an unknown or still-active instance.
    ///
    /// The engine keeps completed instances by default (so `is_completed`,
    /// `instance`, and the audit trail keep working). A host that has durably
    /// projected the instance's history into a separate read model can call
    /// this to keep hot state bounded to only in-flight work — the engine then
    /// never needs a completed instance again, because no command can target
    /// one (its jobs are settled, its timers fired, its subscriptions closed).
    pub fn evict_instance(&mut self, key: Key) -> bool {
        let terminal = matches!(
            self.state.instances.get(&key).map(|i| i.state),
            Some(ProcessInstanceState::Completed | ProcessInstanceState::Terminated)
        );
        if !terminal {
            return false;
        }
        self.state.instances.remove(&key);
        // Drop the instance's jobs via the reverse index (O(this instance's
        // jobs)), deindexing each from the activatable/activated indices. A
        // terminal instance's jobs are normally already settled and thus not in
        // the activatable index, but `deindex_job` is unconditional and safe.
        if let Some(job_keys) = self.state.jobs_by_instance.remove(&key) {
            for job_key in job_keys {
                if let Some(job) = self.state.jobs.remove(&job_key) {
                    self.state.deindex_job(&job.job_type, job_key, job.priority);
                }
            }
        }
        self.state.timers.retain(|_, t| t.instance_key != key);
        self.state
            .message_subscriptions
            .retain(|_, s| s.instance_key != key);
        self.state.incidents.retain(|_, i| i.instance_key != key);
        true
    }

    /// Evicts a batch of completed instances in a single pass. Each instance's
    /// jobs are dropped via the `jobs_by_instance` reverse index, so the cost is
    /// `O(evicted jobs)` rather than `O(total jobs)` — this is the steady-state
    /// exporter path where completions stream in continuously, and a backlog of
    /// in-flight instances must not make every eviction scan the whole job map.
    /// Timers, subscriptions and incidents are not reverse-indexed (they stay
    /// small or empty for job-only processes), so they are pruned with a `retain`
    /// pass. Non-terminal or unknown keys are ignored. Returns the number of
    /// instances evicted. Does **not** shrink the maps — capacity is reused by
    /// the next instances, which is exactly what is wanted under sustained load.
    pub fn evict_instances(&mut self, keys: &[Key]) -> usize {
        let terminal: HashSet<Key> = keys
            .iter()
            .copied()
            .filter(|k| {
                matches!(
                    self.state.instances.get(k).map(|i| i.state),
                    Some(ProcessInstanceState::Completed | ProcessInstanceState::Terminated)
                )
            })
            .collect();
        if terminal.is_empty() {
            return 0;
        }
        for key in &terminal {
            self.state.instances.remove(key);
            // Drop this instance's jobs via the reverse index (O(its jobs)),
            // deindexing each from the activatable/activated indices.
            if let Some(job_keys) = self.state.jobs_by_instance.remove(key) {
                for job_key in job_keys {
                    if let Some(job) = self.state.jobs.remove(&job_key) {
                        self.state.deindex_job(&job.job_type, job_key, job.priority);
                    }
                }
            }
        }
        self.state
            .timers
            .retain(|_, t| !terminal.contains(&t.instance_key));
        self.state
            .message_subscriptions
            .retain(|_, s| !terminal.contains(&s.instance_key));
        self.state
            .incidents
            .retain(|_, i| !terminal.contains(&i.instance_key));
        terminal.len()
    }

    /// Evicts every completed instance (see [`Engine::evict_instance`]) and
    /// shrinks the backing maps so freed capacity is returned. Returns the
    /// number of instances evicted. Intended to run once after a boot replay,
    /// when the read model is already caught up, so recovered hot state holds
    /// only in-flight instances rather than the whole history.
    pub fn evict_completed(&mut self) -> usize {
        let done: Vec<Key> = self
            .state
            .instances
            .iter()
            .filter(|(_, i)| {
                matches!(
                    i.state,
                    ProcessInstanceState::Completed | ProcessInstanceState::Terminated
                )
            })
            .map(|(k, _)| *k)
            .collect();
        for key in &done {
            self.evict_instance(*key);
        }
        if !done.is_empty() {
            self.shrink();
        }
        done.len()
    }

    /// Shrinks the capacity of the hot-state maps to fit their live contents,
    /// returning memory freed by eviction back to the allocator. (Rust maps
    /// never shrink on their own, so removal alone does not lower the resident
    /// footprint until this is called.)
    pub fn shrink(&mut self) {
        self.state.instances.shrink_to_fit();
        self.state.jobs.shrink_to_fit();
        self.state.timers.shrink_to_fit();
        self.state.message_subscriptions.shrink_to_fit();
        self.state.signal_subscriptions.shrink_to_fit();
        self.state.incidents.shrink_to_fit();
        self.state.activatable_jobs.shrink_to_fit();
        self.state.activated_jobs.shrink_to_fit();
        self.state.jobs_by_instance.shrink_to_fit();
    }

    // --- Variable spill: host-managed hot-state memory reclamation. ---
    //
    // The 50 KB-class `variables` payload dominates the per-instance footprint of
    // a large *active* backlog (instances created and parked on a job, waiting for
    // a worker). Such instances are quiescent: the only way their token resumes is
    // job activation followed by completion. So the host can move their variables
    // to a disk-backed store and rehydrate them at activation time, bounding hot
    // RAM the way Zeebe's RocksDB-backed state does — but keeping the in-memory
    // speed for the working set. These methods are pure (no I/O): the host owns the
    // store and decides the policy; the engine only swaps the `Arc` in and out.

    /// Returns `true` when `key`'s variables have been spilled and not yet
    /// rehydrated (so `instance(key).variables` is an empty placeholder).
    pub fn is_variables_spilled(&self, key: Key) -> bool {
        self.state
            .instances
            .get(&key)
            .is_some_and(|i| i.variables_spilled)
    }

    /// Spills `key`'s variables out of hot state: replaces them with an empty
    /// placeholder, marks the instance spilled, and returns the payload for the
    /// host to persist. Returns `None` (a no-op) if the instance does not exist,
    /// is already spilled, or carries no variables — nothing worth spilling.
    ///
    /// Pure: it only moves an `Arc` out of the map. The host must persist the
    /// returned payload and rehydrate it (via [`Engine::rehydrate_variables`])
    /// before any command that reads this instance's variables.
    pub fn spill_variables(&mut self, key: Key) -> Option<Arc<HashMap<String, Value>>> {
        let instance = self.state.instances.get_mut(&key)?;
        if instance.variables_spilled || instance.variables.is_empty() {
            return None;
        }
        instance.variables_spilled = true;
        Some(std::mem::take(&mut instance.variables))
    }

    /// Restores previously [spilled](Engine::spill_variables) variables into hot
    /// state. A no-op if the instance is gone. Idempotent with respect to the
    /// spilled flag (clears it regardless).
    pub fn rehydrate_variables(&mut self, key: Key, variables: Arc<HashMap<String, Value>>) {
        if let Some(instance) = self.state.instances.get_mut(&key) {
            instance.variables = variables;
            instance.variables_spilled = false;
        }
    }

    /// Picks up to `limit` *resident* spill candidates: `Active` instances that
    /// are parked solely on a job (hold at least one job and have **no** armed
    /// timer or open message subscription), still carry their variables, and are
    /// not already spilled. Returned oldest-key first (keys are monotonic, so the
    /// oldest backlog — least likely to be activated next — is shed first). Used
    /// by the host to choose which instances to spill when hot RAM crosses its
    /// budget.
    ///
    /// Excluding instances with an armed timer or open subscription is a
    /// correctness guard: those tokens can resume the flow **without** going
    /// through job activation (a boundary timer firing, a message correlating),
    /// and the host only rehydrates on activation. Spilling such an instance
    /// would let an async resumption read the empty placeholder variables, so
    /// they are kept resident.
    pub fn spillable_instances(&self, limit: usize) -> Vec<Key> {
        if limit == 0 {
            return Vec::new();
        }
        let guarded = self.instances_with_async_token();
        let mut candidates: Vec<Key> = self
            .state
            .instances
            .iter()
            .filter(|(key, i)| self.is_spillable(key, i, &guarded))
            .map(|(key, _)| *key)
            .collect();
        candidates.sort_unstable();
        candidates.truncate(limit);
        candidates
    }

    /// How many resident instances are spill candidates right now (see
    /// [`Engine::spillable_instances`]). Lets the host size its spill budget
    /// without materialising the key list.
    pub fn resident_spillable_count(&self) -> usize {
        let guarded = self.instances_with_async_token();
        self.state
            .instances
            .iter()
            .filter(|(key, i)| self.is_spillable(key, i, &guarded))
            .count()
    }

    /// Whether `instance` (keyed `key`) may have its variables spilled, given the
    /// set of instances that hold an async-resumable token (`guarded`). Shared by
    /// [`spillable_instances`](Engine::spillable_instances) and
    /// [`resident_spillable_count`](Engine::resident_spillable_count) so the
    /// budget accounting and the spill selection never diverge.
    pub(crate) fn is_spillable(
        &self,
        key: &Key,
        instance: &state::ProcessInstance,
        guarded: &std::collections::HashSet<Key>,
    ) -> bool {
        instance.state == ProcessInstanceState::Active
            && !instance.variables_spilled
            && !instance.variables.is_empty()
            && !guarded.contains(key)
            && self
                .state
                .jobs_by_instance
                .get(key)
                .is_some_and(|jobs| !jobs.is_empty())
    }

    /// The set of instance keys holding a token that can resume the flow without
    /// job activation: an armed (`Created`) timer or an open message
    /// subscription. Empty (and allocation-free on the fast path) when no timers
    /// or subscriptions exist — the common case under a create-heavy backlog.
    pub(crate) fn instances_with_async_token(&self) -> std::collections::HashSet<Key> {
        let mut guarded = std::collections::HashSet::new();
        if self.state.timers.is_empty() && self.state.message_subscriptions.is_empty() {
            return guarded;
        }
        for timer in self.state.timers.values() {
            if timer.state == state::TimerState::Created {
                guarded.insert(timer.instance_key);
            }
        }
        for sub in self.state.message_subscriptions.values() {
            if matches!(
                sub.state,
                state::MessageSubscriptionState::Open | state::MessageSubscriptionState::Opening
            ) {
                guarded.insert(sub.instance_key);
            }
        }
        guarded
    }

    // --- Cold spill: host-managed eviction of whole idle instances. ---
    //
    // Variable spill (above) sheds only the `variables` of a job-parked instance,
    // rehydrated at activation. It deliberately keeps instances parked on a timer
    // or message resident, because those resume *without* job activation. But the
    // long-lived, low-throughput workload — tens of thousands of instances each
    // waiting hours or days on a timer or an incoming message — is exactly those
    // instances, and their resident control state (the `active`/`scopes`/`join_*`
    // maps and the owned job/timer/subscription records) is what grows hot RAM
    // with the parked backlog even while nothing runs.
    //
    // Cold spill moves such an instance out of hot state *in full*: the host takes
    // a [`state::InstanceSnapshot`], persists it, and keeps only a slim routing
    // index (the instance's job keys, message name/correlation keys and timer
    // due-times) resident, so an event that targets the instance can rehydrate it
    // on demand. As with variable spill these methods are pure — no I/O: the host
    // owns the store, the index and the policy; the engine only lifts the instance
    // in and out of its maps and keeps its derived indices consistent.

    /// Picks up to `limit` *idle* cold-spill candidates: `Active` instances that
    /// hold at least one parked token and have **no activated (locked) job** — so
    /// no worker is mid-task on them and they are genuinely dormant. Returned
    /// oldest-key first (keys are monotonic, so the coldest backlog is shed
    /// first), the LRU order the host evicts in under memory pressure.
    ///
    /// Unlike [`spillable_instances`](Engine::spillable_instances) (variable
    /// spill) this **includes** instances parked on a timer or message — the
    /// whole point, since those are the long-lived waits — because the host
    /// rehydrates a cold instance on *any* targeting event (timer fire, message
    /// correlation, job activation or a direct command), not only activation.
    pub fn cold_spillable_instances(&self, limit: usize) -> Vec<Key> {
        if limit == 0 {
            return Vec::new();
        }
        let mut candidates: Vec<Key> = self
            .state
            .instances
            .iter()
            .filter(|(key, i)| self.is_cold_spillable(key, i))
            .map(|(key, _)| *key)
            .collect();
        candidates.sort_unstable();
        candidates.truncate(limit);
        candidates
    }

    /// How many resident instances are cold-spill candidates right now (see
    /// [`Engine::cold_spillable_instances`]).
    pub fn cold_spillable_count(&self) -> usize {
        self.state
            .instances
            .iter()
            .filter(|(key, i)| self.is_cold_spillable(key, i))
            .count()
    }

    /// Whether `instance` (keyed `key`) is an idle cold-spill candidate: `Active`,
    /// holding at least one parked token, and owning no job that currently holds
    /// an activation lock (a locked job means a worker is mid-task — keep it hot).
    pub(crate) fn is_cold_spillable(&self, key: &Key, instance: &state::ProcessInstance) -> bool {
        if instance.state != ProcessInstanceState::Active
            || instance.active.is_empty()
            || instance.variables_spilled
        {
            return false;
        }
        match self.state.jobs_by_instance.get(key) {
            Some(jobs) => !jobs.iter().any(|j| self.state.activated_jobs.contains(j)),
            None => true,
        }
    }

    /// Lifts an idle instance and every entity it owns (jobs, timers, message
    /// subscriptions, user tasks, incidents) out of hot state, returning a
    /// self-contained [`state::InstanceSnapshot`] for the host to persist. The
    /// engine's derived job indices are kept consistent (each job is deindexed as
    /// it leaves). Returns `None` — a no-op — for an unknown or terminal instance.
    ///
    /// If the instance's variables were previously variable-spilled the snapshot
    /// would capture an empty placeholder, so this refuses (returns `None`) while
    /// `variables_spilled` is set: the host must rehydrate the variables first so
    /// the cold snapshot is authoritative.
    pub fn snapshot_instance(&mut self, key: Key) -> Option<state::InstanceSnapshot> {
        let instance = self.state.instances.get(&key)?;
        if instance.state != ProcessInstanceState::Active || instance.variables_spilled {
            return None;
        }

        let mut jobs = Vec::new();
        if let Some(job_keys) = self.state.jobs_by_instance.remove(&key) {
            for job_key in job_keys {
                if let Some(job) = self.state.jobs.remove(&job_key) {
                    self.state.deindex_job(&job.job_type, job_key, job.priority);
                    jobs.push(job);
                }
            }
        }

        let timers = drain_owned(&mut self.state.timers, key);
        let message_subscriptions = drain_owned(&mut self.state.message_subscriptions, key);
        let signal_subscriptions = drain_owned(&mut self.state.signal_subscriptions, key);
        let user_tasks = drain_owned(&mut self.state.user_tasks, key);
        let incidents = drain_owned(&mut self.state.incidents, key);

        let instance = self.state.instances.remove(&key)?;
        Some(state::InstanceSnapshot {
            instance,
            jobs,
            timers,
            message_subscriptions,
            signal_subscriptions,
            user_tasks,
            incidents,
        })
    }

    /// Restores a previously [snapshotted](Engine::snapshot_instance) instance
    /// into hot state, re-inserting every owned entity and rebuilding the derived
    /// job indices, so command processing sees exactly the state that was lifted
    /// out. Idempotent-ish: re-inserting keys that already exist overwrites them.
    pub fn rehydrate_instance(&mut self, snapshot: state::InstanceSnapshot) {
        let state::InstanceSnapshot {
            instance,
            jobs,
            timers,
            message_subscriptions,
            signal_subscriptions,
            user_tasks,
            incidents,
        } = snapshot;
        let key = instance.key;
        self.state.instances.insert(key, instance);
        for job in jobs {
            let job_key = job.key;
            self.state
                .jobs_by_instance
                .entry(key)
                .or_default()
                .insert(job_key);
            self.state.jobs.insert(job_key, job);
            state::resync_job_index(&mut self.state, job_key);
        }
        for timer in timers {
            self.state.timers.insert(timer.key, timer);
        }
        for sub in message_subscriptions {
            self.state.message_subscriptions.insert(sub.key, sub);
        }
        for sub in signal_subscriptions {
            self.state.signal_subscriptions.insert(sub.key, sub);
        }
        for task in user_tasks {
            self.state.user_tasks.insert(task.key, task);
        }
        for incident in incidents {
            self.state.incidents.insert(incident.key, incident);
        }
    }
}
