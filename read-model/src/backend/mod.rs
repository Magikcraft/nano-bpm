//! The backend seam: how a `rusqlite::Connection` for the read model is opened
//! and configured. Everything above this module (the schema, projection and read
//! queries in [`crate::store`]) is backend-agnostic and speaks `rusqlite`; the
//! only thing that differs between a real C SQLite and the in-memory wasm SQLite
//! is *how the connection is created and tuned*, which is exactly what this
//! module abstracts.
//!
//! Feature selection:
//! * `native` (default) — [`native::open_connection`], a real (bundled) C SQLite
//!   with the read model's file/WAL tuning. Wins if both features are enabled
//!   (the server never enables `wasm`).
//! * `wasm` (only, no `native`) — [`wasm::open_connection`], an in-memory SQLite
//!   for `wasm32` via `sqlite-wasm-rs` (MemoryVFS). A compiling stub today.
//!
//! Both expose the identical seam:
//!
//! ```ignore
//! pub fn open_connection(path: Option<&std::path::Path>) -> rusqlite::Result<rusqlite::Connection>;
//! ```
//!
//! so [`crate::store::ReadStore::open`] is written once against it.

#[cfg(feature = "native")]
pub(crate) mod native;

// Only compile the wasm backend when `wasm` is enabled *and* `native` is not: the
// two are mutually usable independently, and when both happen to be on (not a
// configuration the server or engine-wasm use) `native` is the one selected
// below, so the wasm module would otherwise be dead code that still has to
// type-check its (stubbed) body.
#[cfg(all(feature = "wasm", not(feature = "native")))]
pub(crate) mod wasm;

#[cfg(feature = "native")]
pub(crate) use native::open_connection;

#[cfg(all(feature = "wasm", not(feature = "native")))]
pub(crate) use wasm::open_connection;
