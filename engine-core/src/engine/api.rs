//! `impl Engine` methods: api concern (extracted from the monolithic engine module).

use super::*;

impl Engine {

    /// Looks up a process instance.
    pub fn instance(&self, key: Key) -> Option<&state::ProcessInstance> {
        self.state.instances.get(&key)
    }

    /// Returns `true` if the instance exists and has completed.
    pub fn is_completed(&self, key: Key) -> bool {
        matches!(
            self.state.instances.get(&key).map(|i| i.state),
            Some(ProcessInstanceState::Completed)
        )
    }

    /// Looks up a job.
    pub fn job(&self, key: Key) -> Option<&state::Job> {
        self.state.jobs.get(&key)
    }

    /// Looks up an incident by key, whether active or resolved (resolved records
    /// are retained for audit).
    pub fn incident(&self, key: Key) -> Option<&state::Incident> {
        self.state.incidents.get(&key)
    }

    /// All incidents ever raised, active and resolved (resolved records are
    /// retained as an audit trail). Filter by [`state::Incident::state`] for a
    /// specific lifecycle state.
    pub fn incidents(&self) -> Vec<&state::Incident> {
        self.state.incidents.values().collect()
    }

    /// Only the currently-active (open) incidents.
    pub fn active_incidents(&self) -> Vec<&state::Incident> {
        self.state
            .incidents
            .values()
            .filter(|i| i.state == state::IncidentState::Active)
            .collect()
    }

    /// All jobs currently awaiting activation (created, or with an expired lock).
    pub fn pending_jobs(&self) -> Vec<&state::Job> {
        self.state
            .jobs
            .values()
            .filter(|j| j.state == state::JobState::Created)
            .collect()
    }

    /// The currently-held activation leases as `(job_key, deadline)` pairs — every
    /// job in [`state::JobState::Activated`] with a deadline. Used by the
    /// best-effort **lease digest** (leader-local activation, `digest` mode): a
    /// partition leader periodically broadcasts these so a future leader can
    /// recover them on takeover ([`Engine::recover_lease`]) and honour the
    /// deadline before redelivering, instead of redelivering immediately. Pure
    /// read; the digest is soft state and is never journaled or replicated.
    pub fn activated_leases(&self) -> Vec<(Key, u64)> {
        self.state
            .jobs
            .values()
            .filter(|j| j.state == state::JobState::Activated)
            .filter_map(|j| j.deadline.map(|d| (j.key, d)))
            .collect()
    }

    /// Recovers a soft activation lease from a digest: if `job_key` is currently
    /// [`state::JobState::Created`] and `deadline` is still in the future, marks
    /// it [`state::JobState::Activated`] until `deadline` under a synthetic worker.
    /// Returns `true` if a lease was set.
    ///
    /// This is a pure leader-local soft-state mutation — it emits and journals
    /// **nothing**, because the lease it restores was never replicated (that is
    /// the whole point of leader-local activation). A newly-promoted leader calls
    /// this for each lease in the last digest it received from the previous
    /// leader, so it holds redelivery of in-flight jobs until their original
    /// deadline (the normal leader-local `expire_jobs` tick then reclaims them)
    /// rather than redelivering the instant it takes over. Idempotent: a job that
    /// is already activated (e.g. re-leased by this leader) is left untouched.
    pub fn recover_lease(&mut self, job_key: Key, deadline: u64, now: u64) -> bool {
        if let Some(job) = self.state.jobs.get_mut(&job_key) {
            if job.state == state::JobState::Created && deadline > now {
                job.state = state::JobState::Activated;
                job.worker = Some(LEASE_DIGEST_WORKER.to_string());
                job.deadline = Some(deadline);
                job.activated = true;
                return true;
            }
        }
        false
    }

    /// Activates up to `max_jobs` activatable jobs of `job_type` for `worker`,
    /// locking each until `now + timeout`, and returns them — the **pull** worker
    /// API for embedded use (no polling, no network). `now` is a caller-supplied
    /// logical instant. Equivalent to applying a [`Command::ActivateJobs`] and
    /// reading back the activated jobs.
    pub fn activate_jobs(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
    ) -> Vec<ActivatedJob> {
        let events = self
            .apply_command_at(
                Command::activate_jobs(job_type, worker, max_jobs, timeout, now),
                now,
            )
            .expect("ActivateJobs never fails");
        events
            .iter()
            .filter_map(|e| match e {
                Event::JobActivated {
                    job_key,
                    worker,
                    deadline,
                    ..
                } => self.job(*job_key).map(|job| ActivatedJob {
                    key: job.key,
                    job_type: job.job_type.clone(),
                    instance_key: job.instance_key,
                    element_instance_key: job.element_instance_key,
                    element_id: job.element_id.clone(),
                    worker: worker.clone(),
                    deadline: *deadline,
                    retries: job.retries,
                    variables: self.variables(job.instance_key),
                }),
                _ => None,
            })
            .collect()
    }

    /// Projects an already-activated job into an [`ActivatedJob`] snapshot (job
    /// detail + a shared copy of its instance variables) WITHOUT mutating state.
    /// Returns `None` if the key is unknown. Used by the Raft write path, where
    /// activation is applied through the replicated log (so the lock replicates
    /// to followers) and the projection for the worker happens in a separate read
    /// on the leader. Mirrors the projection inside [`Engine::activate_jobs`].
    pub fn activated_job(&self, job_key: Key) -> Option<ActivatedJob> {
        self.job(job_key).map(|job| ActivatedJob {
            key: job.key,
            job_type: job.job_type.clone(),
            instance_key: job.instance_key,
            element_instance_key: job.element_instance_key,
            element_id: job.element_id.clone(),
            worker: job.worker.clone().unwrap_or_default(),
            deadline: job.deadline.unwrap_or(0),
            retries: job.retries,
            variables: self.variables(job.instance_key),
        })
    }
    /// making it activatable again. Like [`Engine::trigger_timers`], the host
    /// drives this periodically; the engine never reads a clock. Returns the
    /// [`Event::JobLockExpired`] events produced (empty when nothing was due), so
    /// the host can wake job dispatch immediately rather than waiting for a tick.
    pub fn expire_jobs(&mut self, now: u64) -> Vec<Event> {
        self.apply_command_at(Command::ExpireJobs { now }, now)
            .expect("ExpireJobs never fails")
    }

    /// Fires every armed timer whose due instant is at or before `now`, resuming
    /// the token parked on each. Like [`Engine::expire_jobs`], the host drives
    /// this periodically; the engine never reads a clock. Returns the events the
    /// tick produced (empty when nothing was due).
    pub fn trigger_timers(&mut self, now: u64) -> Vec<Event> {
        self.apply_command_at(Command::TriggerTimers { now }, now)
            .expect("TriggerTimers never fails")
    }

    /// All armed and fired timers (fired ones are retained so they never
    /// re-fire). Filter by [`state::Timer::state`] for only-pending timers.
    pub fn timers(&self) -> Vec<&state::Timer> {
        self.state.timers.values().collect()
    }

    /// Publishes a message and correlates it to every open subscription whose
    /// name and correlation key match, merging the message `variables` into each
    /// correlated instance. Like [`Engine::trigger_timers`], the host drives
    /// this; the engine never reads a clock. Returns the events produced — the
    /// heading [`Event::MessagePublished`] (always) plus an
    /// [`Event::MessageCorrelated`] per correlated subscription. Mirrors applying
    /// a [`Command::CorrelateMessage`].
    pub fn correlate_message(
        &mut self,
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
        variables: HashMap<String, Value>,
        now: u64,
    ) -> Vec<Event> {
        self.apply_command_at(
            Command::correlate_message_with(message_name, correlation_key, variables),
            now,
        )
        .expect("CorrelateMessage never fails")
    }

    /// Broadcasts a signal, correlating it to **every** open subscription whose
    /// signal name matches (name-only correlation). The host drives this; the
    /// engine never reads a clock. Returns the events produced — the heading
    /// [`Event::SignalBroadcast`] (always) plus an [`Event::SignalCorrelated`]
    /// per correlated subscription. Mirrors applying a
    /// [`Command::BroadcastSignal`].
    pub fn broadcast_signal(
        &mut self,
        signal_name: impl Into<String>,
        variables: HashMap<String, Value>,
        now: u64,
    ) -> Vec<Event> {
        self.apply_command_at(
            Command::BroadcastSignal {
                signal_name: signal_name.into(),
                variables,
            },
            now,
        )
        .expect("BroadcastSignal never fails")
    }

    /// All open and settled message subscriptions (correlated/cancelled ones are
    /// retained). Filter by [`state::MessageSubscription::state`] for only-open
    /// subscriptions.
    pub fn message_subscriptions(&self) -> Vec<&state::MessageSubscription> {
        self.state.message_subscriptions.values().collect()
    }

    /// Looks up a message subscription by key, whether open or settled.
    pub fn message_subscription(&self, key: Key) -> Option<&state::MessageSubscription> {
        self.state.message_subscriptions.get(&key)
    }

    /// All open and settled signal subscriptions (correlated/cancelled ones are
    /// retained). Filter by [`state::SignalSubscription::state`] for only-open
    /// subscriptions.
    pub fn signal_subscriptions(&self) -> Vec<&state::SignalSubscription> {
        self.state.signal_subscriptions.values().collect()
    }

    /// Activates jobs of `job_type` and dispatches each to `handler` — the
    /// **callback** worker API for embedded use. Whatever variables the handler
    /// returns complete the job (by key); returning `None` leaves the job locked.
    /// Returns the number of jobs the handler completed. Runs entirely on the
    /// single-writer thread, so there is no concurrency to reason about.
    pub fn poll_jobs<F>(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
        mut handler: F,
    ) -> usize
    where
        F: FnMut(&ActivatedJob) -> Option<HashMap<String, Value>>,
    {
        let jobs = self.activate_jobs(job_type, worker, max_jobs, timeout, now);
        let mut completed = 0;
        for job in jobs {
            if let Some(variables) = handler(&job) {
                self.apply_command(Command::complete_job_with(job.key, variables))
                    .expect("activated job can be completed");
                completed += 1;
            }
        }
        completed
    }
}
