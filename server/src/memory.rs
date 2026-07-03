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

    /// A snapshot of jemalloc's memory accounting, for decomposing RSS into
    /// **genuinely live** bytes vs allocator-retained slack. Reads all fields
    /// after a single epoch advance so they are mutually consistent.
    pub fn stats() -> Option<super::MemStats> {
        use tikv_jemalloc_ctl::{epoch, stats};
        epoch::advance().ok()?;
        Some(super::MemStats {
            // Bytes requested by the app and not yet freed (true live heap).
            allocated: stats::allocated::read().ok()? as u64,
            // Bytes in active pages (allocated rounded up to page/bin size).
            active: stats::active::read().ok()? as u64,
            // Physical pages jemalloc holds (≈ process anon RSS).
            resident: stats::resident::read().ok()? as u64,
            // Virtual address space mapped by jemalloc.
            mapped: stats::mapped::read().ok()? as u64,
            // Pages jemalloc has released back to the OS but keeps mapped
            // (returned; counts toward VIRT/mapped, not RSS).
            retained: stats::retained::read().ok()? as u64,
        })
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
    pub fn stats() -> Option<super::MemStats> {
        None
    }
}

/// jemalloc memory accounting (bytes). The key diagnostic is `resident` (≈ RSS)
/// vs `allocated` (true live heap): a large gap means the balloon is
/// allocator-retained dirty pages (reclaimable by purge/decay), not live data.
#[derive(Debug, Clone, Copy)]
pub struct MemStats {
    pub allocated: u64,
    pub active: u64,
    pub resident: u64,
    pub mapped: u64,
    pub retained: u64,
}

pub use imp::{enable_background_thread, purge, resident_bytes, stats};

/// Live system memory available to userspace, in bytes, read fresh from
/// `/proc/meminfo` (`MemAvailable`). Unlike the boot-time memory *limit*, this
/// tracks real-time pressure from every tenant on the box, so the adaptive spill
/// can react when *other* processes eat into free RAM. Returns `None` off Linux
/// (no `/proc/meminfo`), where callers fall back to their resident-byte guard.
pub fn available_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}
