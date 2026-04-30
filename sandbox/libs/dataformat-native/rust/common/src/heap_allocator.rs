/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Per-plugin heap tracking via mimalloc first-class heaps.
//!
//! Each plugin creates its own heap via [`create_heap`] and wires it into
//! thread pools via [`set_thread_heap`] or [`scoped_thread_heap`].
//!
//! # Allocator Design
//!
//! [`HeapRoutingAllocator`] is the `#[global_allocator]`. It checks a
//! thread-local [`ACTIVE_HEAP`] on every allocation:
//! - If set → `mi_heap_malloc_aligned(heap, ...)` (tracked by plugin)
//! - If null → `System` allocator (libc malloc, safe on JVM threads)
//!
//! The System default path is critical for JVM interop: when the native
//! library is loaded via dlopen/FFM, JVM threads call into Rust without
//! mimalloc TLS being initialized. Using System for the default path
//! avoids SIGSEGV in `mi_thread_init`. Once `#[ffm_safe]` spawns a Rust
//! thread and `ScopedThreadHeap` sets the active heap, all allocations
//! route through mimalloc heaps for per-plugin tracking.
//!
//! **Dealloc routing:** Uses `mi_is_in_heap_region` to detect whether a
//! pointer belongs to mimalloc. If so, `mi_free`; otherwise `System.dealloc`.
//!
//! **Critical:** `mi_free` resolves the owning heap from the pointer's
//! segment metadata. This prevents cross-heap migration and use-after-free
//! when threads exit.

use core::ffi::{c_char, c_void};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::{Mutex, Once};
use std::thread;

pub use libmimalloc_sys::mi_heap_t;
use native_bridge_macros::ffm_thread;
use libmimalloc_sys::{
    mi_free, mi_heap_area_t, mi_heap_malloc_aligned, mi_heap_new,
    mi_heap_visit_blocks, mi_heap_zalloc_aligned, mi_is_in_heap_region,
    mi_realloc_aligned,
};

// mi_stats_get_json is part of mimalloc's extended API (mimalloc-stats.h) but
// libmimalloc-sys doesn't generate a Rust binding for it even with the "extended"
// feature. We declare it manually — signature matches mimalloc v2.x.
extern "C" {
    fn mi_stats_get_json(buf_size: usize, buf: *mut c_char) -> *mut c_char;
}

// ── Thread-local active heap ────────────────────────────────────────────────

thread_local! {
    static ACTIVE_HEAP: Cell<*mut mi_heap_t> = const { Cell::new(std::ptr::null_mut()) };
}

#[inline(always)]
fn get_active_heap() -> *mut mi_heap_t {
    ACTIVE_HEAP.try_with(|c| c.get()).unwrap_or(std::ptr::null_mut())
}

// ── Custom global allocator ─────────────────────────────────────────────────

pub struct HeapRoutingAllocator;

unsafe impl GlobalAlloc for HeapRoutingAllocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let heap = get_active_heap();
        if heap.is_null() {
            // Default path: use System allocator (libc malloc).
            // This is safe on JVM threads — no mimalloc TLS access.
            System.alloc(layout)
        } else {
            mi_heap_malloc_aligned(heap, layout.size(), layout.align()) as *mut u8
        }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let heap = get_active_heap();
        if heap.is_null() {
            System.alloc_zeroed(layout)
        } else {
            mi_heap_zalloc_aligned(heap, layout.size(), layout.align()) as *mut u8
        }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Route to the correct deallocator based on pointer ownership.
        // mi_is_in_heap_region checks if the pointer belongs to any mimalloc
        // segment — if so, use mi_free; otherwise use System free.
        if mi_is_in_heap_region(ptr as *const c_void) {
            mi_free(ptr as *mut c_void);
        } else {
            System.dealloc(ptr, layout);
        }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if mi_is_in_heap_region(ptr as *const c_void) {
            mi_realloc_aligned(ptr as *mut c_void, new_size, layout.align()) as *mut u8
        } else {
            System.realloc(ptr, layout, new_size)
        }
    }
}

// ── Plugin heap registry ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct PluginHeap {
    pub(crate) ptr: *mut mi_heap_t,
}

unsafe impl Send for PluginHeap {}
unsafe impl Sync for PluginHeap {}

static REGISTRY: Mutex<Vec<(&'static str, PluginHeap)>> = Mutex::new(Vec::new());

struct HeapPtr(*mut mi_heap_t);
unsafe impl Send for HeapPtr {}

static OWNER_TX: Mutex<Option<std::sync::mpsc::SyncSender<std::sync::mpsc::SyncSender<HeapPtr>>>> =
    Mutex::new(None);
static OWNER_STARTED: Once = Once::new();

/// Create a new mimalloc heap for a plugin and register it.
///
/// Heaps are created on a dedicated "heap-owner" thread whose TLD is never
/// destroyed, preventing mimalloc from deleting shared plugin heaps when
/// other threads exit.
///
/// # Name lifetime
/// The `name` parameter must be `&'static str`. Callers typically use a
/// string literal or `OnceLock`-guarded initialization, so no allocation
/// is leaked. The registry stores the reference for the process lifetime —
/// this is intentional since plugin heaps are never unregistered.
pub fn create_heap(name: &'static str) -> PluginHeap {
    let mut registry = REGISTRY.lock().unwrap();
    if let Some(&(_, existing)) = registry.iter().find(|(n, _)| *n == name) {
        return existing;
    }

    OWNER_STARTED.call_once(|| {
        let (tx, rx) = std::sync::mpsc::sync_channel::<std::sync::mpsc::SyncSender<HeapPtr>>(0);
        *OWNER_TX.lock().unwrap() = Some(tx);
        thread::Builder::new()
            .name("heap-owner".into())
            .spawn(move || {
                // This thread lives for the entire process. Its TLD is never
                // destroyed, so heaps created here are never auto-freed.
                loop {
                    match rx.recv() {
                        Ok(reply) => {
                            let _ = reply.send(HeapPtr(unsafe { mi_heap_new() }));
                        }
                        Err(_) => break, // channel closed → process shutting down
                    }
                }
            })
            .expect("Failed to spawn heap-owner thread");
    });

    let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel(1);
    OWNER_TX.lock().unwrap().as_ref().unwrap().send(reply_tx).unwrap();
    let HeapPtr(ptr) = reply_rx.recv().unwrap();
    assert!(!ptr.is_null(), "mi_heap_new failed for '{}'", name);

    let heap = PluginHeap { ptr };
    registry.push((name, heap));
    // Release the mutex lock before logging to avoid holding it during I/O.
    drop(registry);
    crate::log_info!("Created plugin heap for '{}'", name);
    heap
}

/// Set the current thread's active plugin heap permanently.
pub fn set_thread_heap(heap: PluginHeap) {
    ACTIVE_HEAP.with(|c| c.set(heap.ptr));
}

/// Clear the current thread's active heap (allocations go to default mimalloc).
pub fn clear_thread_heap() {
    ACTIVE_HEAP.with(|c| c.set(std::ptr::null_mut()));
}

/// Temporarily set the current thread's active plugin heap.
pub fn scoped_thread_heap(heap: PluginHeap) -> HeapGuard {
    let prev = ACTIVE_HEAP.with(|c| c.replace(heap.ptr));
    HeapGuard { prev }
}

/// RAII guard that restores the previous thread-local heap on drop.
///
/// Deliberately `!Send` — dropping on a different thread would restore the
/// wrong thread's heap pointer. The raw pointer already makes it `!Send`
/// by default; we do NOT override that.
pub struct HeapGuard {
    prev: *mut mi_heap_t,
}

impl Drop for HeapGuard {
    fn drop(&mut self) {
        ACTIVE_HEAP.with(|c| c.set(self.prev));
    }
}

// ── Stats ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
pub struct HeapStats {
    pub used: usize,
    pub committed: usize,
}

#[derive(Debug, Clone)]
pub struct PluginStats {
    pub name: &'static str,
    pub stats: HeapStats,
}

pub fn heap_stats(heap: PluginHeap) -> HeapStats {
    if heap.ptr.is_null() {
        return HeapStats::default();
    }

    struct Acc {
        used: usize,
        committed: usize,
    }

    unsafe extern "C" fn visitor(
        _heap: *const mi_heap_t,
        area: *const mi_heap_area_t,
        block: *mut c_void,
        _block_size: usize,
        arg: *mut c_void,
    ) -> bool {
        // visit_all_blocks=false → called once per area with block=NULL.
        // The guard is defensive — if visit_all_blocks were ever changed to
        // true, we'd still only accumulate stats on the area-level call.
        if block.is_null() {
            let acc = &mut *(arg as *mut Acc);
            let a = &*area;
            // full_block_size includes padding and metadata — gives the true
            // memory cost rather than just the usable block_size.
            acc.used += a.used * a.full_block_size;
            acc.committed += a.committed;
        }
        true
    }

    let mut acc = Acc { used: 0, committed: 0 };
    unsafe {
        mi_heap_visit_blocks(
            heap.ptr,
            false,
            Some(visitor),
            &mut acc as *mut Acc as *mut c_void,
        );
    }
    HeapStats { used: acc.used, committed: acc.committed }
}

pub fn all_plugin_stats() -> Vec<PluginStats> {
    let heaps = REGISTRY.lock().unwrap();
    heaps.iter().map(|&(name, heap)| PluginStats {
        name,
        stats: heap_stats(heap),
    }).collect()
}

// ── FFM exports ─────────────────────────────────────────────────────────────

/// Returns the number of registered plugin heaps.
#[ffm_thread]
#[no_mangle]
pub extern "C" fn native_heap_count() -> i32 {
    REGISTRY.lock().unwrap().len() as i32
}

/// Copies the name of the heap at `index` into `buf` (null-terminated).
/// Returns bytes written (excluding null), or -1 on error.
#[ffm_thread]
#[no_mangle]
pub unsafe extern "C" fn native_heap_name(index: i32, buf: *mut u8, buf_len: i32) -> i32 {
    let registry = REGISTRY.lock().unwrap();
    let i = index as usize;
    if i >= registry.len() || buf.is_null() || buf_len < 1 {
        return -1;
    }
    let name = registry[i].0.as_bytes();
    let n = name.len().min((buf_len - 1) as usize); // reserve 1 byte for null
    std::ptr::copy_nonoverlapping(name.as_ptr(), buf, n);
    *buf.add(n) = 0; // null-terminate
    n as i32
}

/// Returns used bytes for the plugin heap at the given index.
/// Only walks blocks for the requested heap, not all heaps.
#[ffm_thread]
#[no_mangle]
pub extern "C" fn native_heap_used(index: i32) -> i64 {
    let heap = {
        let registry = REGISTRY.lock().unwrap();
        let i = index as usize;
        if i >= registry.len() { return -1; }
        registry[i].1
    }; // lock released before heap walk
    heap_stats(heap).used as i64
}

/// Returns committed bytes for the plugin heap at the given index.
/// Only walks blocks for the requested heap, not all heaps.
#[ffm_thread]
#[no_mangle]
pub extern "C" fn native_heap_committed(index: i32) -> i64 {
    let heap = {
        let registry = REGISTRY.lock().unwrap();
        let i = index as usize;
        if i >= registry.len() { return -1; }
        registry[i].1
    }; // lock released before heap walk
    heap_stats(heap).committed as i64
}

/// Returns total mimalloc committed bytes across all heaps (not just plugin heaps).
/// Uses mimalloc's internal JSON stats API which includes the default heap and
/// thread-local heaps that are not tracked by the plugin registry.
///
/// Avoids full serde_json::Value parse — uses targeted string search for the
/// "committed" field to minimize overhead on a frequently-called stats path.
#[ffm_thread]
#[no_mangle]
pub extern "C" fn native_global_committed() -> i64 {
    let raw = unsafe {
        let ptr = mi_stats_get_json(0, std::ptr::null_mut());
        if ptr.is_null() { return 0; }
        let json = std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned();
        mi_free(ptr as *mut c_void);
        json
    };
    // Fast path: find "committed":{"current":N} without full JSON parse
    if let Some(pos) = raw.find("\"committed\"") {
        if let Some(cur_pos) = raw[pos..].find("\"current\":") {
            let start = pos + cur_pos + "\"current\":".len();
            let end = raw[start..].find(|c: char| !c.is_ascii_digit()).map_or(raw.len(), |e| start + e);
            if let Ok(val) = raw[start..end].parse::<i64>() {
                return val;
            }
        }
    }
    // Fallback to full parse
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap_or_default();
    v["committed"]["current"].as_i64().unwrap_or(0)
}

/// Destroy all plugin heaps and clear the registry.
///
/// # Safety
/// All threads must have stopped using plugin heaps before calling this.
/// Any pointers allocated from plugin heaps become dangling after this call.
/// This is intended for graceful process shutdown only.
#[ffm_thread]
#[no_mangle]
pub extern "C" fn native_heap_destroy_all() {
    let mut registry = REGISTRY.lock().unwrap();
    for &(name, heap) in registry.iter() {
        if !heap.ptr.is_null() {
            unsafe { libmimalloc_sys::mi_heap_destroy(heap.ptr) };
        }
        crate::log_info!("Destroyed plugin heap for '{}'", name);
    }
    registry.clear();
}

// ── Test-only helpers (shared by plugin crates) ─────────────────────────────

/// Allocate a zeroed buffer on the given heap. Returns pointer as i64.
/// The caller must free with [`test_free_buffer`].
pub fn test_allocate_buffer(heap: PluginHeap, size: i64) -> i64 {
    let _guard = scoped_thread_heap(heap);
    let buf: Vec<u8> = vec![0u8; size as usize];
    let ptr = buf.as_ptr() as i64;
    std::mem::forget(buf);
    ptr
}

/// Free a buffer allocated by [`test_allocate_buffer`].
pub fn test_free_buffer(ptr: i64) {
    if ptr != 0 {
        unsafe { mi_free(ptr as *mut c_void) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libmimalloc_sys::mi_heap_malloc;
    use std::thread;

    unsafe fn heap_alloc(heap: PluginHeap, size: usize) -> *mut c_void {
        mi_heap_malloc(heap.ptr, size)
    }

    #[test]
    fn test_single_plugin_tracks_allocations() {
        let heap = create_heap("test-single");
        let ptr = unsafe { heap_alloc(heap, 64 * 1024) };
        assert!(!ptr.is_null());
        let stats = heap_stats(heap);
        assert!(stats.used >= 64 * 1024);
        unsafe { mi_free(ptr) };
    }

    #[test]
    fn test_multiple_plugins_independent() {
        let heap_a = create_heap("test-multi-A");
        let heap_b = create_heap("test-multi-B");
        let ptr_a = unsafe { heap_alloc(heap_a, 128 * 1024) };
        let ptr_b = unsafe { heap_alloc(heap_b, 256 * 1024) };
        let stats_a = heap_stats(heap_a);
        let stats_b = heap_stats(heap_b);
        assert!(stats_a.used >= 128 * 1024);
        assert!(stats_b.used >= 256 * 1024);
        assert!(stats_b.used > stats_a.used);
        unsafe { mi_free(ptr_a); mi_free(ptr_b); }
    }

    #[test]
    fn test_stats_decrease_after_free() {
        let heap = create_heap("test-dealloc");
        let ptr = unsafe { heap_alloc(heap, 512 * 1024) };
        let used_before = heap_stats(heap).used;
        assert!(used_before >= 512 * 1024);
        unsafe { mi_free(ptr) };
        let used_after = heap_stats(heap).used;
        assert!(used_after < used_before);
    }

    #[test]
    fn test_cross_thread_allocation() {
        let heap = create_heap("test-cross-thread");
        struct SendPtr(*mut c_void);
        unsafe impl Send for SendPtr {}
        let wrapped = thread::spawn(move || SendPtr(unsafe { heap_alloc(heap, 128 * 1024) }))
            .join().unwrap();
        let stats = heap_stats(heap);
        assert!(stats.used >= 128 * 1024);
        unsafe { mi_free(wrapped.0) };
    }

    #[test]
    fn test_cross_plugin_free_no_segfault() {
        let heap_a = create_heap("test-cross-free-A");
        let heap_b = create_heap("test-cross-free-B");

        // Plugin A allocates
        let ptr = unsafe { heap_alloc(heap_a, 256 * 1024) };
        let a_used = heap_stats(heap_a).used;
        assert!(a_used >= 256 * 1024);
        assert_eq!(heap_stats(heap_b).used, 0);

        // Plugin B frees on a different thread — must not segfault
        let ptr_addr = ptr as usize;
        thread::Builder::new()
            .name("plugin-b-freer".into())
            .spawn(move || {
                set_thread_heap(heap_b);
                unsafe { mi_free(ptr_addr as *mut c_void) };
            })
            .unwrap()
            .join()
            .unwrap();

        // Memory should be freed from A's heap, B should still be 0
        assert_eq!(heap_stats(heap_b).used, 0);
    }

    #[test]
    fn test_scoped_heap_restores_previous() {
        let heap_a = create_heap("test-scope-A");
        let heap_b = create_heap("test-scope-B");
        set_thread_heap(heap_a);
        assert_eq!(get_active_heap(), heap_a.ptr);
        {
            let _guard = scoped_thread_heap(heap_b);
            assert_eq!(get_active_heap(), heap_b.ptr);
        }
        assert_eq!(get_active_heap(), heap_a.ptr);
    }

    #[test]
    fn test_registry_contains_created_heaps() {
        let _heap = create_heap("test-registry");
        let all = all_plugin_stats();
        assert!(all.iter().any(|ps| ps.name == "test-registry"));
    }
}
