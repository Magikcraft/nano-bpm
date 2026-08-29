//! Runtime layer of the nanobpm gateway server (ADR 0064 Phase 2).
//!
//! The engine-glue and cluster-topology layer between the storage stack
//! (`nano-server-storage`) and the raft/consensus layer (`nano-server-raft`):
//! backpressure/admission control, the graceful-drain guard, recovery
//! throttling, the runtime config file, partition placement weighting, cluster
//! topology, the single-writer engine actor (`deepthi`), the partition router,
//! and per-command allocation/latency profiling (`cmd_profile`).
//!
//! Extracted from the gateway binary crate under ADR 0064 so this layer — and
//! its dependents' recompiles — sit behind a stable crate boundary. The modules
//! were moved verbatim (visibility keywords and `use`-path fixes aside); the
//! gateway binary re-exports each of them at its own crate root
//! (`pub(crate) use nano_server_runtime::deepthi;` etc.) so existing
//! `crate::deepthi::…` paths there keep resolving.
//!
//! Dependency direction inside the crate is acyclic: `recovery_throttle`,
//! `cluster`, `placement`, `runtime_config` and `cmd_profile` are leaves;
//! `backpressure` builds on `recovery_throttle`; `deepthi` on `backpressure`
//! (and the storage `journal`); `drain_guard` on `backpressure`/`deepthi`; and
//! `partition` on `cluster`/`deepthi` (and the storage `journal`). Storage
//! items reached via `crate::journal`/`crate::metrics` are re-exported below so
//! the moved files' unqualified `crate::…` paths keep resolving.

// The storage modules the runtime layer reaches into (`deepthi`/`partition`
// journal the engine; `cmd_profile` records into `metrics`). Re-exported at the
// crate root so the moved files' existing `crate::journal::…`/`crate::metrics::…`
// paths keep resolving without per-line edits — the same seam the gateway binary
// uses for the storage crate.
pub(crate) use nano_server_storage::{journal, metrics};

pub mod backpressure;
pub mod cluster;
pub mod cmd_profile;
pub mod deepthi;
pub mod drain_guard;
pub mod partition;
pub mod placement;
pub mod recovery_throttle;
pub mod runtime_config;
