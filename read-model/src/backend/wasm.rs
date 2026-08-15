//! The `wasm` backend: an in-memory SQLite for `wasm32-unknown-unknown` via the
//! optional `sqlite-wasm-rs` dependency (MemoryVFS).
//!
//! **This is a compiling scaffold stub.** Wave-0 declares the module, the backend
//! seam ([`open_connection`], identical in shape to [`super::native`]), the
//! `wasm` Cargo feature and the optional `sqlite-wasm-rs` dependency so the whole
//! layout is in place; a sibling task (`wasm-sqlite-backend`) fills in the body,
//! editing ONLY this file. It must:
//!
//! * register `sqlite-wasm-rs`'s in-memory `MemoryVFS` (no OPFS/persistence — the
//!   in-browser test engine is ephemeral),
//! * open a `:memory:` connection through it, and
//! * return it as a `rusqlite::Connection` so the shared projection ([`crate::store`])
//!   and read queries run byte-for-byte the same SQL as on `native`.
//!
//! The shared [`crate::store::ReadStore::open`] applies the schema
//! ([`crate::store::SCHEMA`]) after this returns, so this only has to hand back an
//! opened, MemoryVFS-backed connection.
//!
//! Build the real thing with:
//! `cargo build -p nanobpmn-read-model --no-default-features --features wasm --target wasm32-unknown-unknown`
//! (needs an LLVM clang with a wasm backend — Apple clang has none — see the
//! epic's toolchain note).

use std::path::Path;

use rusqlite::Connection;

/// Opens the in-memory (MemoryVFS) read-model connection for wasm32. The read
/// model has no on-disk file on wasm, so `path` is expected to be `None`; it is
/// accepted only to keep the seam identical to [`super::native::open_connection`].
///
/// STUB: implemented by the `wasm-sqlite-backend` sibling task. See the module
/// docs for exactly what it must do.
pub(crate) fn open_connection(_path: Option<&Path>) -> rusqlite::Result<Connection> {
    unimplemented!(
        "wasm SQLite backend (sqlite-wasm-rs MemoryVFS) — implemented by the \
         `wasm-sqlite-backend` sibling task; wave-0 scaffold ships this as a stub"
    )
}
