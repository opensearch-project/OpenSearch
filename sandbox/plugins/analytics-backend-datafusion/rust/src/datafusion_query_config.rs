//! Per-query tuning knobs shared by the vanilla and indexed query paths.
//!
//! Populated from Java (cluster / index / request settings) and passed to
//! Rust once at query start via a `#[repr(C)]` wire struct. Read out at
//! setup time and copied into hot-path fields — never dereferenced on a
//! per-batch or per-row hot path.

use crate::indexed_table::stream::FilterStrategy;
use crate::indexed_table::eval::single_collector::CollectorCallStrategy;

/// Query-scoped configuration. Owned by value after FFM decode.
#[derive(Debug, Clone)]
pub struct DatafusionQueryConfig {
    // Common
    pub batch_size: usize,
    // Single query concurrency
    pub target_partitions: usize,
    /// DataFusion's own decode-time predicate pushdown on the vanilla path.
    pub parquet_pushdown_filters: bool,

    // Indexed-only
    pub min_skip_run_default: usize,
    pub min_skip_run_selectivity_threshold: f64,
    /// Whether IndexedStream asks parquet to apply the residual predicate
    /// during decode (via `RowFilter` pushdown). Narrow row-granular
    /// selections benefit; block-granular ones don't.
    pub indexed_pushdown_filters: bool,
    pub force_strategy: Option<FilterStrategy>,
    pub force_pushdown: Option<bool>,
    pub cost_predicate: u32,
    pub cost_collector: u32,
    /// Maximum number of Collector-leaf FFM calls issued in parallel per
    /// RG prefetch. 1 = today's fully-sequential behaviour (lowest CPU,
    /// fastest short-circuit). `target_partitions × max_collector_parallelism`
    /// bounds total concurrent Lucene threads; default is 1
    ///
    /// At higher values, short-circuit savings in AND/OR groups are
    /// sacrificed (see `BitmapTreeEvaluator::prefetch`): collectors
    /// beyond the first may run even if their result is not needed.
    pub max_collector_parallelism: usize,
    /// How the SingleCollectorEvaluator narrows collector doc ranges
    /// relative to page-pruning results. `PageRangeSplit` is the default
    /// — only one collector, so multiple FFM calls per RG is acceptable.
    pub single_collector_strategy: CollectorCallStrategy,
    /// How the bitmap tree evaluator narrows collector doc ranges.
    /// `TightenOuterBounds` is the default — multiple collectors in the
    /// tree means `PageRangeSplit` would multiply FFM calls.
    pub tree_collector_strategy: CollectorCallStrategy,
}

impl Default for DatafusionQueryConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            // TODO: change this default value ?
            target_partitions: 1,
            parquet_pushdown_filters: false,
            min_skip_run_default: 1024, // Todo: tune based on benchmarks
            min_skip_run_selectivity_threshold: 0.03, // Todo : tune based on benchmarks
            indexed_pushdown_filters: true,
            force_strategy: None,
            force_pushdown: None,
            cost_predicate: 1,
            cost_collector: 10, // TODO : should this be collector leaf specific
            max_collector_parallelism: 1,
            single_collector_strategy: CollectorCallStrategy::PageRangeSplit,
            tree_collector_strategy: CollectorCallStrategy::TightenOuterBounds,
        }
    }
}

/// FFM wire format. Must stay in lockstep with the Java `MemoryLayout`.
///
/// All fields have fixed sizes and natural alignment so Java and Rust
/// produce the same byte layout on all target platforms. Enum-ish
/// `Option<_>` fields are encoded with a `-1` sentinel for `None`.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct WireDatafusionQueryConfig {
    pub batch_size: i64,
    pub target_partitions: i64,
    pub min_skip_run_default: i64,
    pub min_skip_run_selectivity_threshold: f64,
    /// 0 = false, 1 = true
    pub parquet_pushdown_filters: i32,
    /// 0 = false, 1 = true
    pub indexed_pushdown_filters: i32,
    /// -1 = None, 0 = RowSelection, 1 = BooleanMask
    pub force_strategy: i32,
    /// -1 = None, 0 = false, 1 = true
    pub force_pushdown: i32,
    pub cost_predicate: i32,
    pub cost_collector: i32,
    pub max_collector_parallelism: i32,
    /// 0 = FullRange, 1 = TightenOuterBounds, 2 = PageRangeSplit
    pub single_collector_strategy: i32,
    /// 0 = FullRange, 1 = TightenOuterBounds, 2 = PageRangeSplit
    pub tree_collector_strategy: i32,
}

impl DatafusionQueryConfig {
    /// Decode from a raw FFM pointer. Null (`0`) returns defaults.
    ///
    /// # Safety
    /// `ptr` must be 0, or a valid pointer to a `WireDatafusionQueryConfig`
    /// whose memory is live for the duration of this call.
    pub unsafe fn from_ffm_ptr(ptr: i64) -> Self {
        if ptr == 0 {
            return Self::default();
        }
        let wire = &*(ptr as *const WireDatafusionQueryConfig);
        Self::from_wire(wire)
    }

    fn from_wire(w: &WireDatafusionQueryConfig) -> Self {
        let force_strategy = match w.force_strategy {
            0 => Some(FilterStrategy::RowSelection),
            1 => Some(FilterStrategy::BooleanMask),
            _ => None,
        };
        let force_pushdown = match w.force_pushdown {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        };
        Self {
            batch_size: w.batch_size as usize,
            target_partitions: w.target_partitions as usize,
            parquet_pushdown_filters: w.parquet_pushdown_filters != 0,
            min_skip_run_default: w.min_skip_run_default as usize,
            min_skip_run_selectivity_threshold: w.min_skip_run_selectivity_threshold,
            indexed_pushdown_filters: w.indexed_pushdown_filters != 0,
            force_strategy,
            force_pushdown,
            cost_predicate: w.cost_predicate as u32,
            cost_collector: w.cost_collector as u32,
            max_collector_parallelism: (w.max_collector_parallelism as usize).max(1),
            single_collector_strategy: match w.single_collector_strategy {
                0 => CollectorCallStrategy::FullRange,
                1 => CollectorCallStrategy::TightenOuterBounds,
                _ => CollectorCallStrategy::PageRangeSplit,
            },
            tree_collector_strategy: match w.tree_collector_strategy {
                0 => CollectorCallStrategy::FullRange,
                2 => CollectorCallStrategy::PageRangeSplit,
                _ => CollectorCallStrategy::TightenOuterBounds,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_legacy_constants() {
        let c = DatafusionQueryConfig::default();
        assert_eq!(c.batch_size, 8192);
        assert_eq!(c.target_partitions, 1);
        assert!(!c.parquet_pushdown_filters);
        assert_eq!(c.min_skip_run_default, 1024);
        assert!((c.min_skip_run_selectivity_threshold - 0.03).abs() < 1e-9);
        assert!(c.indexed_pushdown_filters);
        assert_eq!(c.force_strategy, None);
        assert_eq!(c.force_pushdown, None);
        assert_eq!(c.cost_predicate, 1);
        assert_eq!(c.cost_collector, 10);
    }

    #[test]
    fn wire_decode_null_pointer_gives_defaults() {
        let c = unsafe { DatafusionQueryConfig::from_ffm_ptr(0) };
        let d = DatafusionQueryConfig::default();
        assert_eq!(c.batch_size, d.batch_size);
        assert_eq!(c.min_skip_run_default, d.min_skip_run_default);
        assert_eq!(c.cost_collector, d.cost_collector);
    }

    #[test]
    fn wire_decode_round_trips_all_fields() {
        let wire = WireDatafusionQueryConfig {
            batch_size: 16384,
            target_partitions: 8,
            min_skip_run_default: 512,
            min_skip_run_selectivity_threshold: 0.07,
            parquet_pushdown_filters: 1,
            indexed_pushdown_filters: 0,
            force_strategy: 1,
            force_pushdown: 0,
            cost_predicate: 3,
            cost_collector: 17,
            max_collector_parallelism: 4,
            single_collector_strategy: 2,
            tree_collector_strategy: 1,
        };
        let ptr = &wire as *const _ as i64;
        let c = unsafe { DatafusionQueryConfig::from_ffm_ptr(ptr) };
        assert_eq!(c.batch_size, 16384);
        assert_eq!(c.target_partitions, 8);
        assert_eq!(c.min_skip_run_default, 512);
        assert!((c.min_skip_run_selectivity_threshold - 0.07).abs() < 1e-9);
        assert!(c.parquet_pushdown_filters);
        assert!(!c.indexed_pushdown_filters);
        assert_eq!(c.force_strategy, Some(FilterStrategy::BooleanMask));
        assert_eq!(c.force_pushdown, Some(false));
        assert_eq!(c.cost_predicate, 3);
        assert_eq!(c.cost_collector, 17);
    }

    #[test]
    fn wire_decode_force_fields_none_sentinels() {
        let wire = WireDatafusionQueryConfig {
            batch_size: 8192,
            target_partitions: 4,
            min_skip_run_default: 1024,
            min_skip_run_selectivity_threshold: 0.03,
            parquet_pushdown_filters: 0,
            indexed_pushdown_filters: 1,
            force_strategy: -1,
            force_pushdown: -1,
            cost_predicate: 1,
            cost_collector: 10,
            max_collector_parallelism: 2,
            single_collector_strategy: 2,
            tree_collector_strategy: 1,
        };
        let ptr = &wire as *const _ as i64;
        let c = unsafe { DatafusionQueryConfig::from_ffm_ptr(ptr) };
        assert_eq!(c.force_strategy, None);
        assert_eq!(c.force_pushdown, None);
    }
}
