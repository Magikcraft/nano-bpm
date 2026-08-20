//! Host-injected **cluster variables** for FEEL runtime resolution.
//!
//! Cluster variables are cluster-wide configuration values (a set of *global*
//! variables plus per-*tenant* overlays) that BPMN FEEL expressions can read at
//! runtime. Unlike process/instance variables they are not part of a process
//! instance's journaled state: they live in a host-owned, mutable snapshot that
//! the gateway's REST layer maintains (create / update / delete) and the engine
//! reads while assembling a FEEL evaluation context.
//!
//! Because they are external configuration (like the wall clock the host feeds
//! in per command) they are **not** journaled and do not participate in
//! snapshot/replay determinism; a host re-installs the shared handle on every
//! engine rebuild so the engine keeps observing the live set.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::model::Value;

/// The tenant id every single-tenant process instance runs under. Cluster
/// variables scoped to this tenant (plus all `global` ones) are visible to the
/// engine's FEEL evaluation; a variable scoped to any other tenant is not.
pub const DEFAULT_TENANT: &str = "<default>";

/// A snapshot of the cluster-wide variables visible to FEEL evaluation: a set of
/// `global` variables plus per-tenant overlays. Global variables are visible to
/// every process instance; a tenant's variables are visible only to instances
/// running under that tenant. Instance/local variables always shadow cluster
/// variables when both define the same name.
#[derive(Debug, Default)]
pub struct ClusterVariableSnapshot {
    /// Global-scoped cluster variables, keyed by name.
    pub global: HashMap<String, Value>,
    /// Tenant-scoped cluster variables, keyed by tenant id then name.
    pub tenants: HashMap<String, HashMap<String, Value>>,
}

impl ClusterVariableSnapshot {
    /// `true` when no cluster variable is defined in any scope — the common case,
    /// which lets the engine keep its zero-copy variable-resolution fast path.
    pub fn is_empty(&self) -> bool {
        self.global.is_empty() && self.tenants.values().all(|m| m.is_empty())
    }

    /// The cluster variables an instance running under `tenant` resolves, as
    /// `(name, value)` pairs: every global variable first, then the tenant's own
    /// variables (which shadow a same-named global). The caller layers the
    /// instance's own variables on top so they shadow both.
    pub fn resolved_for<'a>(
        &'a self,
        tenant: &str,
    ) -> impl Iterator<Item = (&'a String, &'a Value)> {
        self.global
            .iter()
            .chain(self.tenants.get(tenant).into_iter().flatten())
    }
}

/// A shared, host-mutable handle to the cluster-variable snapshot. The gateway
/// owns the write side (REST create / update / delete); the engine holds a clone
/// and reads it while building a FEEL evaluation context. Empty by default, in
/// which case the engine's variable-resolution fast path is unaffected.
pub type ClusterVariables = Arc<RwLock<ClusterVariableSnapshot>>;
