use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Macro to define runtime statistics metrics.
///
/// Usage:
/// ```ignore
/// define_runtime_stats! {
///     (field_name, "doc comment", method_name),
///     ...
/// }
/// ```
///
/// This generates:
/// - Fields in `RuntimeStats` struct
/// - Fields in `RuntimeStatsSnapshot` struct
/// - Increment methods (`incr_*`)
/// - `consume_snapshot` implementation
/// - `reset` implementation
macro_rules! define_runtime_stats {
    (
        $(
            ($field:ident, $doc:literal, $method:ident)
        ),* $(,)?
    ) => {
        /// Atomic runtime counters for cache API calls.
        #[derive(Debug, Default)]
        pub struct RuntimeStats {
            $(
                #[doc = $doc]
                pub(crate) $field: AtomicU64,
            )*
        }

        /// Immutable snapshot of [`RuntimeStats`].
        #[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
        pub struct RuntimeStatsSnapshot {
            $(
                #[doc = concat!("Total ", stringify!($field), ".")]
                pub $field: u64,
            )*
        }

        impl RuntimeStats {
            /// Return an immutable snapshot of the current runtime counters
            /// *without* resetting them.
            ///
            /// This is the read used for observability (metrics scraping,
            /// logging, a stats REST endpoint): it is non-destructive, so it
            /// is safe to call repeatedly and from multiple independent
            /// consumers without perturbing the counters. Prefer this over
            /// [`consume_snapshot`](Self::consume_snapshot) unless you
            /// explicitly want a read-and-reset.
            pub fn snapshot(&self) -> RuntimeStatsSnapshot {
                RuntimeStatsSnapshot {
                    $(
                        $field: self.$field.load(Ordering::Relaxed),
                    )*
                }
            }

            /// Return an immutable snapshot of the current runtime counters and reset the stats to 0.
            pub fn consume_snapshot(&self) -> RuntimeStatsSnapshot {
                let v = self.snapshot();
                self.reset();
                v
            }

            $(
                /// Increment counter.
                #[inline]
                pub fn $method(&self) {
                    self.$field.fetch_add(1, Ordering::Relaxed);
                }
            )*

            /// Reset the runtime stats to 0.
            pub fn reset(&self) {
                $(
                    self.$field.store(0, Ordering::Relaxed);
                )*
            }
        }

        impl fmt::Display for RuntimeStats {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                writeln!(f, "RuntimeStats:")?;
                $(
                    writeln!(f, "  {}: {}", stringify!($field), self.$field.load(Ordering::Relaxed))?;
                )*
                Ok(())
            }
        }

        impl fmt::Display for RuntimeStatsSnapshot {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                writeln!(f, "RuntimeStatsSnapshot:")?;
                $(
                    writeln!(f, "  {}: {}", stringify!($field), self.$field)?;
                )*
                Ok(())
            }
        }
    };
}

// Define all runtime statistics metrics here.
// To add a new metric, add a line: (field_name, "doc comment", method_name)
define_runtime_stats! {
    (get, "Number of `get` calls issued via `CachedData`.", incr_get),
    (get_with_selection, "Number of `get_with_selection` calls issued via `CachedData`.", incr_get_with_selection),
    (eval_predicate, "Number of `eval_predicate` calls issued via `CachedData`.", incr_eval_predicate),
    (cache_hit, "Number of cache hits (data found in cache).", incr_cache_hit),
    (cache_miss, "Number of cache misses (data not in cache, fell back to Parquet).", incr_cache_miss),
    (try_read_liquid_calls, "Number of `try_read_liquid` calls issued via `CachedData`.", incr_try_read_liquid),
    (eval_predicate_on_liquid_failed, "Number of `eval_predicate` calls that failed on Liquid array.", incr_eval_predicate_on_liquid_failed),
    (memory_evictions, "Number of cache entries evicted from memory.", incr_memory_evictions),
    (transcodes, "Number of Arrow entries transcoded to Liquid under memory pressure.", incr_transcodes),
}

/// Snapshot of cache statistics.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheStats {
    /// Total number of entries in the cache.
    pub total_entries: usize,
    /// Number of in-memory Arrow entries.
    pub memory_arrow_entries: usize,
    /// Number of in-memory Liquid entries.
    pub memory_liquid_entries: usize,
    /// Total size of in-memory Arrow entries in bytes.
    pub memory_arrow_bytes: usize,
    /// Total size of in-memory Liquid entries in bytes.
    pub memory_liquid_bytes: usize,
    /// Total memory usage of the cache.
    pub memory_usage_bytes: usize,
    /// Maximum memory size.
    pub max_memory_bytes: usize,
    /// Runtime counters snapshot.
    pub runtime: RuntimeStatsSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `snapshot()` must be non-destructive — repeated reads return the same
    /// values and never reset the counters. Only `consume_snapshot()` resets.
    #[test]
    fn snapshot_is_non_destructive_and_consume_resets() {
        let stats = RuntimeStats::default();
        for _ in 0..3 {
            stats.incr_cache_hit();
        }
        stats.incr_cache_miss();

        // Two non-destructive reads must be identical and reflect the counts.
        let first = stats.snapshot();
        let second = stats.snapshot();
        assert_eq!(first.cache_hit, 3);
        assert_eq!(first.cache_miss, 1);
        assert_eq!(
            (second.cache_hit, second.cache_miss),
            (3, 1),
            "snapshot() must not reset counters"
        );

        // A further increment is visible cumulatively (not lost by prior reads).
        stats.incr_cache_hit();
        assert_eq!(stats.snapshot().cache_hit, 4, "counters must accumulate");

        // consume_snapshot() returns the current values then resets to zero.
        let consumed = stats.consume_snapshot();
        assert_eq!((consumed.cache_hit, consumed.cache_miss), (4, 1));
        let after = stats.snapshot();
        assert_eq!(
            (after.cache_hit, after.cache_miss),
            (0, 0),
            "consume_snapshot() must reset counters to zero"
        );
    }
}
