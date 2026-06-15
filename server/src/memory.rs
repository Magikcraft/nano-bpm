//! Global allocator and idle memory reclamation.
//!
//! The server allocates heavily in bursts: a flood of `createProcessInstance`s
//! grows the hot-state maps and 50 KB-class variable payloads, which are then
//! freed as instances complete and are evicted. The default system allocator
//! (macOS libmalloc, Linux glibc) keeps that freed memory in its own free lists
//! rather than returning it to the OS, so an *idle* server pins its peak resident
//! footprint long after the work is gone (the symptom: hundreds of MB held with
//! nothing happening).
//!
//! jemalloc fixes this two ways: it returns unused pages to the OS on a **decay**
//! schedule (driven by a background thread on Linux), and it exposes a portable
//! **purge** knob via `mallctl` that forces the return immediately — which the
//! idle-purge tick calls after compacting hot state on macOS, where jemalloc has
//! no background thread. jemalloc is vendored and built from source, so the
//! binary stays self-contained.

#[cfg(not(target_env = "msvc"))]
mod imp {
    use tikv_jemallocator::Jemalloc;

    #[global_allocator]
    static ALLOC: Jemalloc = Jemalloc;

    // Return dirty/muzzy pages to the OS ~5 s after they fall idle. On Linux the
    // background thread (enabled at startup) applies this automatically; on macOS,
    // which has no jemalloc background thread, the idle-purge tick forces it.
    #[allow(non_upper_case_globals)]
    #[unsafe(export_name = "_rjem_malloc_conf")]
    pub static malloc_conf: &[u8] = b"dirty_decay_ms:5000,muzzy_decay_ms:5000\0";

    /// Best-effort: enable jemalloc's background purge thread. Supported on Linux;
    /// a no-op (returns an error we ignore) where unavailable, e.g. macOS.
    pub fn enable_background_thread() {
        let enable: bool = true;
        // SAFETY: `background_thread` is a `bool` mallctl; we pass a valid pointer
        // and the matching size, and ignore the return code on platforms that do
        // not support it.
        unsafe {
            tikv_jemalloc_sys::mallctl(
                c"background_thread".as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &enable as *const bool as *mut std::ffi::c_void,
                std::mem::size_of::<bool>(),
            );
        }
    }

    /// Forces every arena to purge its dirty pages, returning freed memory to the
    /// OS immediately. Returns `true` on success.
    pub fn purge() -> bool {
        // `arena.<i>.purge` with i = MALLCTL_ARENAS_ALL (4096) purges all arenas.
        // SAFETY: a well-known mallctl name with no in/out parameters.
        let rc = unsafe {
            tikv_jemalloc_sys::mallctl(
                c"arena.4096.purge".as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            )
        };
        rc == 0
    }

    /// Bytes of physical memory jemalloc currently holds (mapped and backed by
    /// real pages), for before/after logging. Advances the stats epoch first so
    /// the read is fresh.
    pub fn resident_bytes() -> Option<usize> {
        use tikv_jemalloc_ctl::{epoch, stats};
        epoch::advance().ok()?;
        stats::resident::read().ok()
    }
}

#[cfg(target_env = "msvc")]
mod imp {
    pub fn enable_background_thread() {}
    pub fn purge() -> bool {
        false
    }
    pub fn resident_bytes() -> Option<usize> {
        None
    }
}

pub use imp::{enable_background_thread, purge, resident_bytes};
