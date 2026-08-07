//! Compile-time parity gate between `engine-core`'s command surface and the
//! in-browser [`crate::TestEngine`] wrapper.
//!
//! # Why this exists
//!
//! `TestEngine` is a hand-written wasm-bindgen facade: each modeler operation
//! (`deploy`, `completeJob`, `activateJobs`, …) is a bespoke method that applies
//! one [`Command`] to the embedded engine. Because the mapping is manual, a new
//! engine capability — a new [`Command`] variant — can land in `engine-core`
//! and the wasm build will keep compiling and publishing *without* the modeler
//! ever gaining the feature. That is a silent surface gap, and a package version
//! bump can't catch it (the code compiles either way).
//!
//! [`classify`] closes that gap with the one lever the compiler can't ignore: an
//! **exhaustive `match` with no wildcard arm** over every [`Command`] variant.
//! When someone adds a variant to `engine-core`, this match stops being
//! exhaustive and the crate fails to compile (`E0004`) on the `wasm32` type-check
//! (`make engine-wasm-check`, run in CI). The author is then forced to make a
//! *conscious* decision at the exact PR that adds the capability: surface it in
//! `TestEngine` and mark it [`Surface::Surfaced`], or record *why* it is left out
//! with [`Surface::NotSurfaced`].
//!
//! This is deliberately a build-time sentinel: `classify` is never called at
//! runtime (hence `#![allow(dead_code)]`). Its whole value is that the match body
//! is *type-checked*, so the exhaustiveness error fires. `NotSurfaced` is not a
//! verdict of "never" — it means a human decided the test engine doesn't need it
//! *yet*; revisit when the modeler does.
//!
//! Note on *field*-level drift (a new field on an already-surfaced command, e.g.
//! `customHeaders` on an activated job): variant exhaustiveness does not catch
//! that. Where the wrapper serialises engine types straight to JSON
//! (snapshots/events) new fields flow through serde automatically; where it maps
//! fields by hand in `lib.rs`, destructure without `..` so a new field also
//! breaks the build.
#![allow(dead_code)]

use nanobpmn_engine_core::Command;

/// How a given [`Command`] variant relates to the `TestEngine` wasm surface.
pub(crate) enum Surface {
    /// Exposed to the modeler via the named `TestEngine` method(s) — the JS
    /// name(s). Usually a single method; when one command is surfaced through
    /// more than one JS entry point this is their names joined with `/`
    /// (e.g. `advanceTime/tickNow`, which both fan out to the same command).
    Surfaced { js_method: &'static str },
    /// Deliberately not exposed, with the rationale. Revisit if the modeler ever
    /// needs it — this is a "not yet / not here", never a hard "never".
    NotSurfaced { reason: &'static str },
}

/// Exhaustive, wildcard-free classification of every `engine-core` command.
///
/// Adding a `Command` variant to `engine-core` without extending this match is a
/// hard compile error (`E0004: non-exhaustive patterns`) — that is the guard.
pub(crate) fn classify(cmd: &Command) -> Surface {
    match cmd {
        // ---- Surfaced: each has a #[wasm_bindgen] method on TestEngine ----
        Command::DeployResources(..) => Surface::Surfaced {
            js_method: "deploy",
        },
        Command::CreateInstance { .. } => Surface::Surfaced {
            js_method: "createInstance",
        },
        Command::CompleteJob { .. } => Surface::Surfaced {
            // Also the substrate for completeAgentJob (agentic JobResult).
            js_method: "completeJob",
        },
        Command::FailJob { .. } => Surface::Surfaced {
            js_method: "failJob",
        },
        Command::ThrowJobError { .. } => Surface::Surfaced {
            js_method: "throwError",
        },
        Command::UpdateJobRetries { .. } => Surface::Surfaced {
            js_method: "updateRetries",
        },
        Command::ResolveIncident { .. } => Surface::Surfaced {
            js_method: "resolveIncident",
        },
        Command::ActivateJobs { .. } => Surface::Surfaced {
            js_method: "activateJobs",
        },
        Command::CorrelateMessage { .. } => Surface::Surfaced {
            js_method: "correlateMessage",
        },
        Command::BroadcastSignal { .. } => Surface::Surfaced {
            js_method: "broadcastSignal",
        },
        Command::SetVariables { .. } => Surface::Surfaced {
            js_method: "setVariables",
        },
        Command::CancelInstance { .. } => Surface::Surfaced {
            js_method: "cancelInstance",
        },
        Command::ModifyInstance { .. } => Surface::Surfaced {
            js_method: "modify",
        },
        Command::AssignUserTask { .. } => Surface::Surfaced {
            js_method: "assignUserTask",
        },
        Command::UnassignUserTask { .. } => Surface::Surfaced {
            js_method: "unassignUserTask",
        },
        Command::UpdateUserTask { .. } => Surface::Surfaced {
            js_method: "updateUserTask",
        },
        Command::CompleteUserTask { .. } => Surface::Surfaced {
            js_method: "completeUserTask",
        },
        // The virtual clock drives both timer + job-deadline expiry; advanceTime
        // and tickNow fan out to TriggerTimers + ExpireJobs.
        Command::TriggerTimers { .. } => Surface::Surfaced {
            js_method: "advanceTime/tickNow",
        },
        Command::ExpireJobs { .. } => Surface::Surfaced {
            js_method: "advanceTime/tickNow",
        },

        // ---- Not surfaced: conscious exclusions from the modeler test engine ----
        Command::DeployProcess(..) => Surface::NotSurfaced {
            reason: "single-process deploy; the wrapper deploys via the DeployResources superset",
        },
        Command::DeployDecisionRequirements(..) => Surface::NotSurfaced {
            reason: "DMN deployment; the in-browser test engine exercises BPMN execution only",
        },
        Command::DeleteDecisionInstance { .. } => Surface::NotSurfaced {
            reason: "audit-only read-model deletion; no core engine state, irrelevant in-browser",
        },
        Command::UpdateJobTimeout { .. } => Surface::NotSurfaced {
            reason: "extends a job-activation lock deadline; the modeler test engine exposes \
                     no long-poll job-lease API to extend (jobs are activated and completed \
                     synchronously), so there is no held lock to prolong even though the \
                     virtual clock's ExpireJobs can retire deadlines. Surface if the modeler \
                     grows long-poll activation semantics.",
        },
        Command::OpenMessageSubscription { .. } => Surface::NotSurfaced {
            reason: "internal subscription lifecycle the engine drives itself; not a user op",
        },
        Command::CorrelateMessageSubscription { .. } => Surface::NotSurfaced {
            reason: "internal subscription correlation; user-facing entry is correlateMessage",
        },
        Command::CloseMessageSubscription { .. } => Surface::NotSurfaced {
            reason: "internal subscription lifecycle the engine drives itself; not a user op",
        },
        Command::DispatchStartInstance { .. } => Surface::NotSurfaced {
            reason:
                "engine-internal message/timer start dispatch; instances start via createInstance",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A representative surfaced + not-surfaced pair, to exercise `classify` and
    // pin the intent. The real guard is the exhaustiveness of the match above,
    // enforced by the wasm32 type-check in CI — this test just validates that the
    // classification wiring is sound.
    #[test]
    fn classifies_known_commands() {
        assert!(matches!(
            classify(&Command::ExpireJobs { now: 0 }),
            Surface::Surfaced { .. }
        ));
        assert!(matches!(
            classify(&Command::UpdateJobTimeout {
                job_key: 0,
                timeout: 0,
                operation_reference: None,
            }),
            Surface::NotSurfaced { .. }
        ));
    }
}
