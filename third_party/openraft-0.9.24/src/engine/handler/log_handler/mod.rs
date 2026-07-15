use crate::engine::Command;
use crate::engine::EngineConfig;
use crate::engine::EngineOutput;
use crate::raft_state::LogStateReader;
use crate::summary::MessageSummary;
use crate::AsyncRuntime;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::RaftState;
use crate::RaftTypeConfig;

#[cfg(test)]
mod calc_purge_upto_test;
#[cfg(test)]
mod purge_log_test;

/// Handle raft-log related operations
pub(crate) struct LogHandler<'x, C>
where C: RaftTypeConfig
{
    pub(crate) config: &'x mut EngineConfig<C::NodeId>,
    pub(crate) state: &'x mut RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>,
    pub(crate) output: &'x mut EngineOutput<C>,
}

impl<C> LogHandler<'_, C>
where C: RaftTypeConfig
{
    /// Purge log entries upto `RaftState.purge_upto()`, inclusive.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn purge_log(&mut self) {
        let st = &mut self.state;
        let purge_upto = st.purge_upto();

        tracing::info!(
            last_purged_log_id = display(st.last_purged_log_id().summary()),
            purge_upto = display(purge_upto.summary()),
            "purge_log"
        );

        if purge_upto <= st.last_purged_log_id() {
            return;
        }

        let upto = purge_upto.unwrap().clone();

        st.purge_log(&upto);
        self.output.push_command(Command::PurgeLog { upto });
    }

    /// Update the next log id to purge upto, if more logs can be purged, according to configured
    /// policy.
    ///
    /// This method is called after building a snapshot, because openraft only purge logs that are
    /// already included in snapshot.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn schedule_policy_based_purge(&mut self, retain_from_index: Option<u64>) {
        if let Some(purge_upto) = self.calc_purge_upto(retain_from_index) {
            self.update_purge_upto(purge_upto);
        }
    }

    /// Update the log id it expect to purge up to. It won't trigger purge immediately.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn update_purge_upto(&mut self, purge_upto: LogId<C::NodeId>) {
        debug_assert!(self.state.purge_upto() <= Some(&purge_upto));
        self.state.purge_upto = Some(purge_upto);
    }

    /// Calculate the log id up to which to purge, inclusive.
    ///
    /// Only log included in snapshot will be purged.
    /// It may return None if there is no log to purge.
    ///
    /// `max_keep` specifies the number of applied logs to keep.
    /// `max_keep==0` means every applied log can be purged.
    ///
    /// `retain_from_index`, when `Some`, is the first log index that must NOT be
    /// purged (a lagging replication target still needs it); the purge point is
    /// clamped below it. The caller is responsible for bounding it (see
    /// [`crate::Config::max_extra_log_to_keep_for_lagging`]) so a stuck target cannot
    /// pin the log without bound.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn calc_purge_upto(&self, retain_from_index: Option<u64>) -> Option<LogId<C::NodeId>> {
        let st = &self.state;
        let max_keep = self.config.max_in_snapshot_log_to_keep;
        let batch_size = self.config.purge_batch_size;

        let mut purge_end = self.state.snapshot_meta.last_log_id.next_index().saturating_sub(max_keep);

        // Retain logs a lagging replication target still needs so it can stream the
        // tail instead of installing a snapshot. Never purge at/above the retain
        // floor; the caller bounds how far below the policy point this can reach, so
        // a stuck target cannot pin the log without bound.
        if let Some(retain_from) = retain_from_index {
            purge_end = purge_end.min(retain_from);
        }

        tracing::debug!(
            snapshot_last_log_id = debug(self.state.snapshot_meta.last_log_id.clone()),
            max_keep,
            "try purge: (-oo, {})",
            purge_end
        );

        if st.last_purged_log_id().next_index() + batch_size > purge_end {
            tracing::debug!(
                snapshot_last_log_id = debug(self.state.snapshot_meta.last_log_id.clone()),
                max_keep,
                last_purged_log_id = display(st.last_purged_log_id().summary()),
                batch_size,
                purge_end,
                "no need to purge",
            );
            return None;
        }

        let log_id = self.state.log_ids.get(purge_end - 1);
        debug_assert!(
            log_id.is_some(),
            "log id not found at {}, engine.state:{:?}",
            purge_end - 1,
            st
        );

        log_id
    }
}
