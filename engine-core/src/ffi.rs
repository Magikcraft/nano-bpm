//! A coarse C-ABI surface over the engine, for embedding via FFI.
//!
//! This is the boundary a non-Rust host (a Swift/Kotlin app on mobile, or
//! JavaScript driving the `wasm32-unknown-unknown` build in a browser) calls
//! through. It is deliberately **coarse** — submit one command, read back a
//! scalar summary of what happened — rather than chatty, because every call
//! across an FFI boundary has a cost and the engine is a single owned state
//! machine.
//!
//! It stays **dependency-free and `std`-only**, like the rest of the crate, so
//! the same source compiles for `aarch64-apple-ios`, `aarch64-linux-android`
//! and `wasm32-unknown-unknown`. The crate must be built as a `cdylib` for the
//! exports below to appear in the artifact (`crate-type = ["lib", "cdylib"]`),
//! and this module is gated behind the off-by-default `ffi` feature so a pure
//! library embedder pays nothing for it.
//!
//! # Memory ownership
//!
//! The host allocates an input buffer with [`nbpmn_alloc`], writes its bytes
//! (BPMN XML, a process id, a message name…) into wasm linear memory, passes
//! the `(ptr, len)` to a call, and frees it with [`nbpmn_free`]. The engine
//! handle from [`nbpmn_engine_new`] must be released with [`nbpmn_engine_free`].
//! All pointers are borrowed for the duration of a call only.
//!
//! # Safety
//!
//! Every function is `unsafe` to call: the host must pass either null or a
//! pointer/length pair it actually owns, and a valid engine handle. Calls null-
//! check and validate UTF-8, returning a sentinel (`0` keys, negative counts)
//! rather than unwinding across the boundary.

use core::slice;

use crate::bpmn::parse_bpmn;
use crate::{Command, Engine};

/// Allocates `len` bytes inside the module and returns a pointer the host can
/// write to. Returns null for a zero-length request. Pair every call with
/// [`nbpmn_free`] using the **same** `len`.
///
/// # Safety
/// The returned pointer is valid for `len` bytes until passed to [`nbpmn_free`].
#[no_mangle]
pub unsafe extern "C" fn nbpmn_alloc(len: usize) -> *mut u8 {
    if len == 0 {
        return core::ptr::null_mut();
    }
    let mut buf = Vec::<u8>::with_capacity(len);
    let ptr = buf.as_mut_ptr();
    core::mem::forget(buf);
    ptr
}

/// Frees a buffer previously returned by [`nbpmn_alloc`]. `len` must match the
/// original allocation. A null pointer or zero length is a no-op.
///
/// # Safety
/// `ptr`/`len` must come from a prior [`nbpmn_alloc`] and not be used after.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    drop(Vec::from_raw_parts(ptr, 0, len));
}

/// Creates a fresh engine and returns an opaque handle. Release it with
/// [`nbpmn_engine_free`].
#[no_mangle]
pub extern "C" fn nbpmn_engine_new() -> *mut Engine {
    Box::into_raw(Box::new(Engine::new()))
}

/// Releases an engine handle from [`nbpmn_engine_new`]. Null is a no-op.
///
/// # Safety
/// `engine` must be a handle from [`nbpmn_engine_new`], not yet freed.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_engine_free(engine: *mut Engine) {
    if !engine.is_null() {
        drop(Box::from_raw(engine));
    }
}

/// Borrows the engine and the `(ptr, len)` bytes as a `&str`, or returns `None`
/// if anything is invalid (null engine/ptr, or non-UTF-8 bytes).
unsafe fn engine_and_str<'a>(
    engine: *mut Engine,
    ptr: *const u8,
    len: usize,
) -> Option<(&'a mut Engine, &'a str)> {
    if engine.is_null() || ptr.is_null() {
        return None;
    }
    let bytes = slice::from_raw_parts(ptr, len);
    let text = core::str::from_utf8(bytes).ok()?;
    Some((&mut *engine, text))
}

/// Parses BPMN 2.0 XML and deploys every process in it as one deployment.
/// Returns the number of processes deployed, or a negative error code:
/// `-1` invalid arguments (null/handle/UTF-8), `-2` the XML failed to parse,
/// `-3` the deployment was rejected (e.g. a process with no start event).
///
/// # Safety
/// `engine` must be a valid handle and `xml`/`xml_len` a UTF-8 buffer the
/// caller owns (or null/0).
#[no_mangle]
pub unsafe extern "C" fn nbpmn_deploy_bpmn(
    engine: *mut Engine,
    xml: *const u8,
    xml_len: usize,
) -> i64 {
    let Some((engine, xml)) = engine_and_str(engine, xml, xml_len) else {
        return -1;
    };
    let processes = match parse_bpmn(xml) {
        Ok(p) => p,
        Err(_) => return -2,
    };
    let count = processes.len() as i64;
    match engine.apply_command(Command::DeployResources(processes)) {
        Ok(_) => count,
        Err(_) => -3,
    }
}

/// Creates an instance of the process with id `(id, id_len)` at clock `now`.
/// Returns the new instance key, or `0` on any error (null/handle/UTF-8, or no
/// such process).
///
/// # Safety
/// `engine` must be a valid handle and `id`/`id_len` a UTF-8 buffer (or null/0).
#[no_mangle]
pub unsafe extern "C" fn nbpmn_create_instance(
    engine: *mut Engine,
    id: *const u8,
    id_len: usize,
    now: u64,
) -> u64 {
    let Some((engine, id)) = engine_and_str(engine, id, id_len) else {
        return 0;
    };
    match engine.apply_command_at(Command::create_instance(id), now) {
        Ok(events) => events.iter().find_map(|e| e.instance_key()).unwrap_or(0),
        Err(_) => 0,
    }
}

/// Correlates a message named `(name, name_len)` with correlation key
/// `(key, key_len)` at clock `now`. Returns the number of events produced (a
/// published message always yields at least one), or a negative code for
/// invalid arguments (`-1`).
///
/// # Safety
/// `engine` must be a valid handle and the name/key buffers UTF-8 (or null/0).
#[no_mangle]
pub unsafe extern "C" fn nbpmn_correlate_message(
    engine: *mut Engine,
    name: *const u8,
    name_len: usize,
    key: *const u8,
    key_len: usize,
    now: u64,
) -> i64 {
    if engine.is_null() || name.is_null() {
        return -1;
    }
    let Ok(name) = core::str::from_utf8(slice::from_raw_parts(name, name_len)) else {
        return -1;
    };
    let correlation_key = if key.is_null() {
        ""
    } else {
        match core::str::from_utf8(slice::from_raw_parts(key, key_len)) {
            Ok(k) => k,
            Err(_) => return -1,
        }
    };
    let engine = &mut *engine;
    engine
        .correlate_message(name, correlation_key, Default::default(), now)
        .len() as i64
}

/// Fires every timer due at clock `now` (timer catch events, boundary timers and
/// timer start events). Returns the number of events produced.
///
/// # Safety
/// `engine` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_trigger_timers(engine: *mut Engine, now: u64) -> i64 {
    if engine.is_null() {
        return -1;
    }
    (*engine).trigger_timers(now).len() as i64
}

/// Returns `1` if the instance with `instance_key` has completed, `0` if it is
/// still active or unknown, `-1` for a null handle.
///
/// # Safety
/// `engine` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_is_completed(engine: *mut Engine, instance_key: u64) -> i32 {
    if engine.is_null() {
        return -1;
    }
    i32::from((*engine).is_completed(instance_key))
}

/// Returns the number of process instances the engine is tracking (active and
/// completed), or `-1` for a null handle.
///
/// # Safety
/// `engine` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_instance_count(engine: *mut Engine) -> i64 {
    if engine.is_null() {
        return -1;
    }
    (*engine).state().instances.len() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drives a full deploy → create → complete cycle entirely through the
    /// C-ABI surface, mirroring how a host would call it.
    #[test]
    fn should_run_a_process_through_the_ffi() {
        const XML: &[u8] = br#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        unsafe {
            let engine = nbpmn_engine_new();
            assert!(!engine.is_null());

            // Deploy one process through the boundary.
            let deployed = nbpmn_deploy_bpmn(engine, XML.as_ptr(), XML.len());
            assert_eq!(deployed, 1);

            // Create and run an instance; it completes immediately (start->end).
            let id = b"p";
            let instance_key = nbpmn_create_instance(engine, id.as_ptr(), id.len(), 0);
            assert_ne!(instance_key, 0);
            assert_eq!(nbpmn_is_completed(engine, instance_key), 1);
            assert_eq!(nbpmn_instance_count(engine), 1);

            // An unknown process id is rejected with the 0 sentinel.
            let bad = b"nope";
            assert_eq!(nbpmn_create_instance(engine, bad.as_ptr(), bad.len(), 0), 0);

            nbpmn_engine_free(engine);
        }
    }

    #[test]
    fn should_round_trip_an_alloc() {
        unsafe {
            let ptr = nbpmn_alloc(8);
            assert!(!ptr.is_null());
            nbpmn_free(ptr, 8);
            // A zero-length alloc is null and freeing null is a no-op.
            assert!(nbpmn_alloc(0).is_null());
            nbpmn_free(core::ptr::null_mut(), 0);
        }
    }

    #[test]
    fn should_reject_bad_arguments_without_unwinding() {
        unsafe {
            assert_eq!(
                nbpmn_deploy_bpmn(core::ptr::null_mut(), core::ptr::null(), 0),
                -1
            );
            assert_eq!(nbpmn_is_completed(core::ptr::null_mut(), 1), -1);
            assert_eq!(nbpmn_instance_count(core::ptr::null_mut()), -1);

            let engine = nbpmn_engine_new();
            // Invalid UTF-8 in the XML buffer is a parse-stage rejection.
            let bytes = [0xff, 0xfe];
            assert_eq!(nbpmn_deploy_bpmn(engine, bytes.as_ptr(), bytes.len()), -1);
            // Well-formed bytes that aren't BPMN fail to parse (-2).
            let not_bpmn = b"<x/>";
            assert_eq!(
                nbpmn_deploy_bpmn(engine, not_bpmn.as_ptr(), not_bpmn.len()),
                -2
            );
            nbpmn_engine_free(engine);
        }
    }
}
