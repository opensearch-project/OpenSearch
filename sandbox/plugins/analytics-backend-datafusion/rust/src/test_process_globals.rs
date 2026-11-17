/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Test-only discipline for the engine's process-global state.
//!
//! Production installs each of these globals once from `DataFusionService.doStart` and never
//! contends on them. A test binary is the opposite: `cargo test` runs every test in one process on
//! many threads, so a test that replaces, resets or asserts on one of them is racing every other
//! test that reads it. The globals in question are
//!
//! - the runtime manager, `ffm::TOKIO_RUNTIME_MANAGER` — see [`install_runtime_manager`];
//! - the global `RuntimeEnv` registration, `cache::metadata_cache::GLOBAL_RUNTIME_ENV`, which keeps
//!   only a `Weak`, so closing a runtime that registered it leaves every reader with `None`;
//! - the scoped page-index caches, `cache::page_index::{COLUMN_INDEX_CACHE, OFFSET_INDEX_CACHE}`,
//!   whose hit/miss/entry counters are process-wide;
//! - the `memory_guard` spill globals that runtime construction writes.
//!
//! These are not independent — opening a doc-values cursor reads the runtime manager and the
//! `RuntimeEnv` registration and inserts into the page-index caches — so they are covered by ONE
//! lock rather than one lock each. Separate locks would force a test to hold several at once and
//! turn a data race into a lock-ordering problem.
//!
//! Every test that touches any of the above must hold [`lock`] for its whole body. The affected
//! tests take 0.21s in total when run serially, so the lost parallelism is not measurable against
//! a suite that takes ~9s.

use std::cell::Cell;
use std::sync::{Mutex, MutexGuard};

static PROCESS_GLOBALS: Mutex<()> = Mutex::new(());

thread_local! {
    /// How many guards this thread currently holds. A depth rather than a flag so a nested
    /// acquisition — a helper that locks inside a test that already did — releases correctly.
    static HELD_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Proof that the holder has exclusive access to the engine's process-global state. Also records
/// the fact on the holding thread, so [`assert_held`] can tell a guarded caller from an unguarded
/// one instead of trusting a comment.
pub(crate) struct ProcessGlobalsGuard {
    // Dropped after the depth decrement below, which is the order we want: the depth must reach 0
    // before another thread can take the mutex and see a stale non-zero depth of its own.
    _inner: MutexGuard<'static, ()>,
}

impl Drop for ProcessGlobalsGuard {
    fn drop(&mut self) {
        HELD_DEPTH.with(|depth| depth.set(depth.get() - 1));
    }
}

/// Exclusive access to the engine's process-global state for the duration of a test. Hold the
/// returned guard in a binding that lives as long as the test body:
///
/// ```ignore
/// let _globals = crate::test_process_globals::lock();
/// ```
///
/// Poison is deliberately ignored: the guarded data is `()`, so there is nothing a panicking test
/// can leave half-written, and propagating poison would turn one genuine failure into a dozen
/// `PoisonError` panics in unrelated tests and bury the real one.
pub(crate) fn lock() -> ProcessGlobalsGuard {
    let inner = PROCESS_GLOBALS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    HELD_DEPTH.with(|depth| depth.set(depth.get() + 1));
    ProcessGlobalsGuard { _inner: inner }
}

/// Panics unless the calling thread holds [`lock`]. Call this from the entry points that reach the
/// shared globals, so a test that forgets the lock fails loudly at the offending call instead of
/// corrupting an unrelated test's assertion in a later run.
///
/// Thread-local by design, and that is also its limit: it can only be placed where the work runs on
/// the thread that took the guard. A global counter would be useless here — the failure this exists
/// to catch is an unguarded test running *concurrently with* a guarded one, and a global counter
/// reads as "held" in exactly that situation. So do not move these calls onto a path that a
/// DataFusion executor thread polls; assert at the entry point the test itself calls.
///
/// `what` names the state being touched, so the panic says which invariant was broken.
#[track_caller]
pub(crate) fn assert_held(what: &str) {
    assert!(
        HELD_DEPTH.with(Cell::get) > 0,
        "{what} is process-global, but this test does not hold \
         crate::test_process_globals::lock(). Add `let _globals = crate::test_process_globals::lock();` \
         as the first line of the test body and keep the binding alive for the whole test."
    );
}

/// Installs the runtime manager production installs from `DataFusionService.doStart`, once per test
/// process.
///
/// Install-once rather than per-test: `df_init_runtime_manager` replaces the global
/// unconditionally and `df_shutdown_runtime_manager` clears it, so a second call from one test
/// pulls the manager out from under every other test that is mid-read — which surfaces as
/// "no runtime manager registered" from an unrelated test. Nothing tears it down; the manager's
/// threads live until the test process exits. Mirrors the `OnceLock` setup the integration tests
/// in `tests/` already use.
///
/// Cheap and idempotent, so callers may call it unconditionally, but [`lock`] must be held: the
/// first call installs the manager, and `df_init_runtime_manager` asserts the lock for that reason.
pub(crate) fn install_runtime_manager() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| crate::ffm::df_init_runtime_manager(2, 1.5, 1.5));
}
