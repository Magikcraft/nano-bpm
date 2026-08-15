//! Compile-time parity gate between the gateway's C8-style **REST read surface**
//! and the in-browser [`crate::TestEngine`] wrapper.
//!
//! # Why this exists
//!
//! The sibling [`crate::surface_parity`] gate guards the engine's *write* surface:
//! an exhaustive, wildcard-free `match` over `engine-core`'s `Command` enum forces
//! a conscious decision whenever a new command lands — surface it in `TestEngine`
//! or record why it is left out. That gate is structurally blind to **reads**.
//!
//! The gateway serves a rich C8-style REST *read* surface (`getFormByKey`,
//! `searchUserTasks({state})`, `searchProcessInstances`, `getResourceByKey`,
//! `searchVariables`, …) from **secondary storage** — the SQLite read model in
//! `server/src/readstore.rs`, fed by the read-model exporter — *not* from engine
//! state. Those reads are plain REST handlers with **no `Query`/`Command`-style
//! enum to match on**, so no exhaustiveness check can ever catch them: a new REST
//! read can be added to the gateway and the wasm build keeps compiling and
//! publishing without the test engine ever gaining read parity. That is a silent
//! surface gap — the exact drift that pushed `@nanobpm/urban-testkit`'s
//! `WasmEngineClient` into hand-reimplementing fragments of the read model in JS
//! (shadow stores, snapshot-scraping, a hardcoded `state == "Created"` filter).
//!
//! [`classify`] closes that gap the same way the command gate does: it enumerates
//! the gateway REST read surface as [`RestRead`] and pins each variant to a
//! [`ReadSurface`] verdict in an **exhaustive `match` with no wildcard arm**.
//! Adding a variant to [`RestRead`] (i.e. recording a newly added gateway read)
//! stops the match being exhaustive and the crate fails to compile (`E0004`) on
//! the `wasm32` type-check (`make engine-wasm-check`, run in CI) — forcing a
//! conscious *surface it / record why not* decision, exactly like a new command.
//!
//! This is deliberately a build-time sentinel: [`classify`] is never called at
//! runtime (hence `#![allow(dead_code)]`); its whole value is that the match body
//! is type-checked so the exhaustiveness error fires.
//!
//! ## Current verdict
//!
//! The wasm test engine has **no read channel yet** — it exposes the engine's
//! *primary* state (`snapshot()`, `events()`) but none of the gateway's
//! secondary-storage reads. So every [`RestRead`] is currently
//! [`ReadSurface::NotSurfaced`]. Unlike the command gate — where `NotSurfaced`
//! means "the *modeler* doesn't need it" — here the rationale is single: the
//! read-model channel that would serve these has not been built. Building it (the
//! shared read-model crate + wasm SQLite backend behind a `read-model` feature,
//! run inside `TestEngine`) is tracked by nano-bpm#796. As each read is wired
//! through the wasm read channel, flip its verdict to
//! [`ReadSurface::Surfaced`] with the JS method name.
#![allow(dead_code)]

/// The gateway's C8-style REST **read** surface, one variant per read operation
/// served from the read model (`server/src/readstore.rs`) via the REST handlers
/// (`*_impl` in `server/src/main.rs`).
///
/// This is the read analogue of `engine-core`'s `Command` enum. It is the single
/// enumerated list [`classify`] must exhaustively account for; adding a gateway
/// read means adding a variant here, which forces a wasm surface decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestRead {
    // ---- Process instances ----
    GetProcessInstance,
    SearchProcessInstances,
    // ---- Element instances (flow nodes) ----
    GetElementInstance,
    SearchElementInstances,
    SearchElementInstanceWaitStates,
    // ---- User tasks ----
    GetUserTask,
    SearchUserTasks,
    // ---- Variables ----
    GetVariable,
    SearchVariables,
    // ---- Jobs ----
    SearchJobs,
    // ---- Incidents ----
    GetIncident,
    SearchIncidents,
    SearchElementInstanceIncidents,
    // ---- Forms ----
    GetFormByKey,
    // ---- Generic resources ----
    GetResource,
    GetResourceContent,
    GetResourceContentBinary,
    SearchResources,
    // ---- Decision definitions / requirements / instances (DMN) ----
    GetDecisionInstance,
    SearchDecisionInstances,
    GetDecisionDefinition,
    GetDecisionDefinitionXml,
    SearchDecisionDefinitions,
    GetDecisionRequirements,
    GetDecisionRequirementsXml,
    SearchDecisionRequirements,
}

/// How a given [`RestRead`] relates to the `TestEngine` wasm read surface.
pub(crate) enum ReadSurface {
    /// Exposed to consumers via the named `TestEngine` method — the JS name.
    /// Set this once the read is wired through the wasm read channel.
    Surfaced { js_method: &'static str },
    /// Not yet served by the wasm read channel, with the rationale. This is a
    /// "not yet", never a hard "never" — revisit as the read channel grows.
    NotSurfaced { reason: &'static str },
}

/// The read-model channel that would serve the gateway reads on wasm does not
/// exist yet; see nano-bpm#796. Shared rationale for every currently-unsurfaced
/// REST read.
const READ_CHANNEL_PENDING: &str =
    "the wasm test engine has no read channel yet — the gateway serves this from \
     the SQLite read model (server/src/readstore.rs), which is not compiled into \
     engine-wasm. Surface once the shared read-model crate + wasm SQLite backend \
     (feature `read-model`) run inside TestEngine (nano-bpm#796).";

/// Exhaustive, wildcard-free classification of every gateway REST read.
///
/// Adding a [`RestRead`] variant without extending this match is a hard compile
/// error (`E0004: non-exhaustive patterns`) on the `wasm32` type-check — that is
/// the guard.
pub(crate) fn classify(read: &RestRead) -> ReadSurface {
    match read {
        RestRead::GetProcessInstance => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchProcessInstances => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetElementInstance => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchElementInstances => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchElementInstanceWaitStates => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetUserTask => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchUserTasks => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetVariable => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchVariables => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchJobs => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetIncident => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchIncidents => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchElementInstanceIncidents => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetFormByKey => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetResource => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetResourceContent => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetResourceContentBinary => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchResources => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetDecisionInstance => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchDecisionInstances => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetDecisionDefinition => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetDecisionDefinitionXml => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchDecisionDefinitions => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetDecisionRequirements => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::GetDecisionRequirementsXml => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
        RestRead::SearchDecisionRequirements => ReadSurface::NotSurfaced {
            reason: READ_CHANNEL_PENDING,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Every currently-enumerated read is unsurfaced pending the wasm read channel
    // (nano-bpm#796). This pins today's intent; the real guard is the
    // exhaustiveness of the `classify` match, enforced by the wasm32 type-check in
    // CI. As reads are wired through the read channel, move them out of this list
    // and assert `Surfaced` instead.
    #[test]
    fn all_reads_pending_read_channel() {
        let reads = [
            RestRead::GetProcessInstance,
            RestRead::SearchProcessInstances,
            RestRead::GetElementInstance,
            RestRead::SearchElementInstances,
            RestRead::SearchElementInstanceWaitStates,
            RestRead::GetUserTask,
            RestRead::SearchUserTasks,
            RestRead::GetVariable,
            RestRead::SearchVariables,
            RestRead::SearchJobs,
            RestRead::GetIncident,
            RestRead::SearchIncidents,
            RestRead::SearchElementInstanceIncidents,
            RestRead::GetFormByKey,
            RestRead::GetResource,
            RestRead::GetResourceContent,
            RestRead::GetResourceContentBinary,
            RestRead::SearchResources,
            RestRead::GetDecisionInstance,
            RestRead::SearchDecisionInstances,
            RestRead::GetDecisionDefinition,
            RestRead::GetDecisionDefinitionXml,
            RestRead::SearchDecisionDefinitions,
            RestRead::GetDecisionRequirements,
            RestRead::GetDecisionRequirementsXml,
            RestRead::SearchDecisionRequirements,
        ];
        for read in reads {
            assert!(
                matches!(classify(&read), ReadSurface::NotSurfaced { .. }),
                "{read:?} should be NotSurfaced until the wasm read channel lands (nano-bpm#796)"
            );
        }
    }
}
