//! The `wasm` backend: an in-memory SQLite for `wasm32-unknown-unknown` via the
//! optional `sqlite-wasm-rs` dependency (MemoryVFS).
//!
//! The read model only ever speaks the `rusqlite`/`libsqlite3-sys` C API. On
//! `wasm32-unknown-unknown` that C API has NO implementation of its own —
//! `libsqlite3-sys` is built without `bundled`, so the `sqlite3_*` symbols it
//! declares are supplied by `sqlite-wasm-rs` instead (its compiled
//! `libwsqlite3.a`), which also registers an in-memory `MemoryVFS` as the default
//! VFS at load time. `build.rs` drops an empty `libsqlite3.a` stub on the link
//! path so `libsqlite3-sys`'s bare `-lsqlite3` request resolves without shadowing
//! those real symbols.
//!
//! The build is verified two ways:
//! * `cargo build -p nanobpmn-read-model --no-default-features --features wasm`
//!   (host) — a fast type-check; `sqlite-wasm-rs` is a wasm32-only dependency, so
//!   the link-forcing `use` below is compiled only for the real target.
//! * `cargo build -p nanobpmn-read-model --no-default-features --features wasm
//!   --target wasm32-unknown-unknown` — the real thing (needs an LLVM clang with
//!   a wasm backend; Apple clang has none — see the epic toolchain note).

use std::path::Path;

use rusqlite::Connection;

// Force `sqlite-wasm-rs` into the link on the real wasm target: nothing in this
// crate names it (the read model speaks only the `libsqlite3-sys` C API), so
// without a reference it would be dropped as unused and every `sqlite3_*` symbol
// would go unresolved at the final `wasm-ld` link. It is a wasm32-only
// dependency, so this `use` is gated to that target; on a host type-check of
// `--features wasm` the dependency is absent and unneeded.
#[cfg(target_arch = "wasm32")]
use sqlite_wasm_rs as _;

/// Opens the in-memory (MemoryVFS) read-model connection for wasm32.
///
/// `sqlite-wasm-rs` registers its `MemoryVFS` as the default VFS, so a plain
/// `:memory:` connection is entirely RAM-backed — exactly right for the ephemeral
/// in-browser test engine (no OPFS/persistence). `path` is ignored: the wasm read
/// model has no on-disk file and is always opened with `None` by
/// [`crate::store::ReadStore::open`]; the parameter exists only to keep the seam
/// identical to [`super::native::open_connection`], which then applies the shared
/// [`crate::store::SCHEMA`].
pub(crate) fn open_connection(_path: Option<&Path>) -> rusqlite::Result<Connection> {
    Connection::open_in_memory()
}
