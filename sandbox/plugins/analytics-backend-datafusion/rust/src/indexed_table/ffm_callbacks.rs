/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM upcall surface for index-filter providers and collectors.
//!
//! Four callback slots, populated once at startup by
//! `df_register_filter_tree_callbacks` (see `ffm.rs`):
//!
//! - `createProvider(annotationId) -> providerKey|-1`
//! - `createCollector(providerKey, writerGeneration, minDoc, maxDoc) -> collectorKey|-1`
//! - `collectDocs(collectorKey, minDoc, maxDoc, outBuf, outWordCap) -> wordsWritten|-1`
//! - `releaseCollector(collectorKey)`
//! - `releaseProvider(providerKey)`
//!
//! `ProviderHandle` and `FfmSegmentCollector` are the lifetime wrappers —
//! they call the release callbacks on drop.

use std::sync::atomic::{AtomicPtr, Ordering};

use super::index::RowGroupDocsCollector;

// ── Callback signatures ───────────────────────────────────────────────

type CreateProviderFn = unsafe extern "C" fn(i32) -> i32;
type ReleaseProviderFn = unsafe extern "C" fn(i32);
/// `(provider_key, writer_generation, doc_min, doc_max) -> collector_key | -1`.
///
/// `writer_generation` is the stable per-segment identifier
type CreateCollectorFn = unsafe extern "C" fn(i32, i64, i32, i32) -> i32;
type CollectDocsFn = unsafe extern "C" fn(i32, i32, i32, *mut u64, i64) -> i64;
type ReleaseCollectorFn = unsafe extern "C" fn(i32);

static CREATE_PROVIDER: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
static RELEASE_PROVIDER: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
static CREATE_COLLECTOR: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
static COLLECT_DOCS: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
static RELEASE_COLLECTOR: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Registered by Java at startup. Stores function pointers into atomic
/// slots. Each call to this entry replaces the slots wholesale.
///
/// Not annotated `#[ffm_safe]` because that macro is specific to the
/// `-> i64` error-pointer convention. We use a manual `catch_unwind`
/// instead, though the body (atomic stores) can't realistically panic.
#[no_mangle]
pub unsafe extern "C" fn df_register_filter_tree_callbacks(
    create_provider: CreateProviderFn,
    release_provider: ReleaseProviderFn,
    create_collector: CreateCollectorFn,
    collect_docs: CollectDocsFn,
    release_collector: ReleaseCollectorFn,
) {
    // catch_unwind is defense-in-depth: atomic stores shouldn't panic,
    // but if they ever did (e.g. allocator OOM if we grew the atomics),
    // unwinding across the FFM boundary is UB. Swallow the panic
    // silently — there's no way to report it back to Java for a
    // `-> ()` function.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        CREATE_PROVIDER.store(create_provider as *mut (), Ordering::Release);
        RELEASE_PROVIDER.store(release_provider as *mut (), Ordering::Release);
        CREATE_COLLECTOR.store(create_collector as *mut (), Ordering::Release);
        COLLECT_DOCS.store(collect_docs as *mut (), Ordering::Release);
        RELEASE_COLLECTOR.store(release_collector as *mut (), Ordering::Release);
    }));
}

fn load_create_provider() -> Result<CreateProviderFn, String> {
    let p = CREATE_PROVIDER.load(Ordering::Acquire);
    if p.is_null() {
        return Err("FilterTree callbacks not registered".into());
    }
    Ok(unsafe { std::mem::transmute::<*mut (), CreateProviderFn>(p) })
}
fn load_release_provider() -> Option<ReleaseProviderFn> {
    let p = RELEASE_PROVIDER.load(Ordering::Acquire);
    if p.is_null() {
        None
    } else {
        Some(unsafe { std::mem::transmute::<*mut (), ReleaseProviderFn>(p) })
    }
}
fn load_create_collector() -> Result<CreateCollectorFn, String> {
    let p = CREATE_COLLECTOR.load(Ordering::Acquire);
    if p.is_null() {
        return Err("FilterTree callbacks not registered".into());
    }
    Ok(unsafe { std::mem::transmute::<*mut (), CreateCollectorFn>(p) })
}
fn load_collect_docs() -> Result<CollectDocsFn, String> {
    let p = COLLECT_DOCS.load(Ordering::Acquire);
    if p.is_null() {
        return Err("FilterTree callbacks not registered".into());
    }
    Ok(unsafe { std::mem::transmute::<*mut (), CollectDocsFn>(p) })
}
fn load_release_collector() -> Option<ReleaseCollectorFn> {
    let p = RELEASE_COLLECTOR.load(Ordering::Acquire);
    if p.is_null() {
        None
    } else {
        Some(unsafe { std::mem::transmute::<*mut (), ReleaseCollectorFn>(p) })
    }
}

// ── ProviderHandle — owns `releaseProvider` on drop ───────────────────

/// Returned from `create_provider`. Drop releases the provider.
pub struct ProviderHandle {
    key: i32,
}

impl ProviderHandle {
    pub fn key(&self) -> i32 {
        self.key
    }
}

impl std::fmt::Debug for ProviderHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderHandle")
            .field("key", &self.key)
            .finish()
    }
}

impl Drop for ProviderHandle {
    fn drop(&mut self) {
        if let Some(release) = load_release_provider() {
            unsafe { release(self.key) };
        }
    }
}

/// Create a provider by annotation ID by upcalling Java.
pub fn create_provider(annotation_id: i32) -> Result<ProviderHandle, String> {
    let create = load_create_provider()?;
    let key = unsafe { create(annotation_id) };
    if key < 0 {
        return Err(format!(
            "createProvider failed: annotation_id={} -> {}",
            annotation_id,
            key
        ));
    }
    Ok(ProviderHandle { key })
}

// ── FfmSegmentCollector — owns `releaseCollector` on drop ─────────────

#[derive(Debug)]
pub struct FfmSegmentCollector {
    key: i32,
}

impl FfmSegmentCollector {
    /// Ask Java for a collector keyed by `provider_key` for the given segment/doc range.
    ///
    /// `writer_generation` identifies the segment.
    pub fn create(
        provider_key: i32,
        writer_generation: i64,
        doc_min: i32,
        doc_max: i32,
    ) -> Result<Self, String> {
        let create = load_create_collector()?;
        let key = unsafe { create(provider_key, writer_generation, doc_min, doc_max) };
        if key < 0 {
            return Err(format!(
                "createCollector(provider={}, writer_generation={}) failed: {}",
                provider_key, writer_generation, key
            ));
        }
        Ok(FfmSegmentCollector { key })
    }
}

impl RowGroupDocsCollector for FfmSegmentCollector {
    fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        if max_doc <= min_doc {
            return Ok(Vec::new());
        }
        let span = (max_doc - min_doc) as usize;
        let word_count = span.div_ceil(64);
        let mut buf = vec![0u64; word_count];
        let collect_fn = load_collect_docs()?;
        let n = unsafe {
            collect_fn(
                self.key,
                min_doc,
                max_doc,
                buf.as_mut_ptr(),
                word_count as i64,
            )
        };
        if n < 0 {
            return Err(format!("collectDocs(key={}) failed: {}", self.key, n));
        }
        // Defensive: the Java callback is contracted to return
        // `wordsWritten <= outWordCap`. If it lied, the buffer already
        // overflowed, but truncating won't recover the clobbered heap.
        // Detect the violation and fail loudly so the Java callback bug
        // is surfaced before downstream code consumes the tainted bitset.
        let n = n as usize;
        if n > word_count {
            return Err(format!(
                "collectDocs(key={}) reported wordsWritten={} > capacity={}; \
                 callback contract violated (possible heap overflow)",
                self.key, n, word_count,
            ));
        }
        buf.truncate(n);
        Ok(buf)
    }
}

impl Drop for FfmSegmentCollector {
    fn drop(&mut self) {
        if let Some(release) = load_release_collector() {
            unsafe { release(self.key) };
        }
    }
}
