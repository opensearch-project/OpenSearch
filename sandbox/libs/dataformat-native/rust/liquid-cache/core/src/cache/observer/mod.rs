mod stats;

pub use stats::{CacheStats, RuntimeStats, RuntimeStatsSnapshot};

use stats::{RuntimeStats as RuntimeStatsInner, RuntimeStatsSnapshot as RuntimeStatsSnapshotInner};

#[derive(Debug)]
/// Cache-side observer for runtime stats.
pub struct Observer {
    runtime: RuntimeStatsInner,
}

impl Default for Observer {
    fn default() -> Self {
        Self::new()
    }
}

impl Observer {
    /// Create a new observer with all counters reset.
    pub fn new() -> Self {
        Self {
            runtime: RuntimeStatsInner::default(),
        }
    }

    /// Snapshot runtime counters *without* resetting them (non-destructive).
    ///
    /// Used by [`crate::cache::LiquidCache::stats`] so that reading stats for
    /// observability does not perturb the counters. Safe to call repeatedly
    /// and from multiple consumers.
    pub fn runtime_snapshot(&self) -> RuntimeStatsSnapshotInner {
        self.runtime.snapshot()
    }

    /// Snapshot runtime counters and reset them to zero (destructive).
    ///
    /// Reserved for an explicit read-and-reset (e.g. periodic delta
    /// reporting). Callers that just want to observe current values must use
    /// [`runtime_snapshot`](Self::runtime_snapshot) instead.
    pub fn runtime_consume_snapshot(&self) -> RuntimeStatsSnapshotInner {
        self.runtime.consume_snapshot()
    }

    #[inline]
    pub(crate) fn on_get(&self, selection: bool) {
        self.runtime.incr_get();
        if selection {
            self.runtime.incr_get_with_selection();
        }
    }

    #[inline]
    pub(crate) fn on_try_read_liquid(&self) {
        self.runtime.incr_try_read_liquid();
    }

    #[inline]
    pub(crate) fn on_eval_predicate(&self) {
        self.runtime.incr_eval_predicate();
    }

    #[inline]
    pub(crate) fn on_memory_eviction(&self) {
        self.runtime.incr_memory_evictions();
    }

    #[inline]
    pub(crate) fn on_transcode(&self) {
        self.runtime.incr_transcodes();
    }

    /// Access the underlying runtime statistics counters.
    pub fn runtime_stats(&self) -> &RuntimeStats {
        &self.runtime
    }
}
