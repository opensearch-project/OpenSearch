//! Per-query tuning knobs shared by the vanilla and indexed query paths.
//!
//! Populated from Java (cluster / index / request settings) and passed to
//! Rust once at query start via a `#[repr(C)]` wire struct. Read out at
//! setup time and copied into hot-path fields — never dereferenced on a
//! per-batch or per-row hot path.

use crate::indexed_table::eval::single_collector::CollectorCallStrategy;
use crate::indexed_table::stream::FilterStrategy;

/// Selects which execution path computes shard-global row IDs.
///
/// Selects which execution path computes shard-global row IDs.
/// `None` = no row ID computation (baseline — reads ___row_id as a regular column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchStrategy {
    /// No row ID optimizer applied. ___row_id is read as a regular column
    /// without any row_base addition. Returns local (per-file) row IDs only.
    None,
    /// ShardTableProvider + ProjectRowIdOptimizer.
    /// Reads ___row_id from parquet, adds row_base via physical optimizer rewrite.
    /// Produces shard-global absolute row IDs.
    ListingTable,
    /// Predicate-only mode in the indexed executor.
    /// Uses indexed pipeline (segment partitioning, prefetch, PositionMap).
    /// Does NOT read ___row_id from disk — computes from position:
    /// global_base + rg.first_row + position_in_rg. Zero column I/O for row ID.
    IndexedPredicateOnly,
}

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
    /// Strategy for row ID emission on the vanilla path.
    /// Only consulted when the plan requests row IDs (contains _global_row_id() UDF
    /// or projects ___row_id).
    pub fetch_strategy: FetchStrategy,
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
    /// 0 = None (baseline), 1 = ListingTable, 2 = IndexedPredicateOnly
    pub fetch_strategy: i32,
}

impl DatafusionQueryConfig {
    /// Fallback values used when Java passes a null config pointer (0).
    /// Production code should always supply a real config via the wire
    /// struct; this exists only for the transitional period while Java
    /// wiring is incomplete.
    fn fallback() -> Self {
        Self {
            batch_size: 8192,
            target_partitions: 4,
            parquet_pushdown_filters: false,
            min_skip_run_default: 1024,
            min_skip_run_selectivity_threshold: 0.03,
            indexed_pushdown_filters: true,
            force_strategy: None,
            force_pushdown: None,
            cost_predicate: 1,
            cost_collector: 10,
            max_collector_parallelism: 1,
            single_collector_strategy: CollectorCallStrategy::PageRangeSplit,
            tree_collector_strategy: CollectorCallStrategy::TightenOuterBounds,
            fetch_strategy: FetchStrategy::None,
        }
    }

    /// Constructor with sensible defaults for tests and benchmarks.
    /// Production code should use `from_ffm_ptr` with a real wire config.
    pub fn test_default() -> Self {
        Self::fallback()
    }

    /// Returns a builder seeded with fallback defaults for test usage.
    #[cfg(test)]
    pub fn builder() -> DatafusionQueryConfigBuilder {
        DatafusionQueryConfigBuilder::new()
    }

    /// Decode from a raw FFM pointer.
    ///
    /// # Safety
    /// `ptr` must be a valid, non-zero pointer to a `WireDatafusionQueryConfig`
    /// whose memory is live for the duration of this call.
    ///
    /// # Panics
    /// Panics if `ptr` is 0 (null). Java must always supply a valid config pointer.
    pub unsafe fn from_ffm_ptr(ptr: i64) -> Self {
        assert!(
            ptr != 0,
            "from_ffm_ptr: null query config pointer — Java must always provide a valid config"
        );
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
            fetch_strategy: match w.fetch_strategy {
                1 => FetchStrategy::ListingTable,
                2 => FetchStrategy::IndexedPredicateOnly,
                _ => FetchStrategy::None,
            },
        }
    }
}

#[cfg(test)]
pub struct DatafusionQueryConfigBuilder(DatafusionQueryConfig);

#[cfg(test)]
impl DatafusionQueryConfigBuilder {
    fn new() -> Self {
        Self(DatafusionQueryConfig::fallback())
    }
    pub fn batch_size(mut self, v: usize) -> Self {
        self.0.batch_size = v;
        self
    }
    pub fn target_partitions(mut self, v: usize) -> Self {
        self.0.target_partitions = v;
        self
    }
    pub fn parquet_pushdown_filters(mut self, v: bool) -> Self {
        self.0.parquet_pushdown_filters = v;
        self
    }
    pub fn min_skip_run_default(mut self, v: usize) -> Self {
        self.0.min_skip_run_default = v;
        self
    }
    pub fn min_skip_run_selectivity_threshold(mut self, v: f64) -> Self {
        self.0.min_skip_run_selectivity_threshold = v;
        self
    }
    pub fn indexed_pushdown_filters(mut self, v: bool) -> Self {
        self.0.indexed_pushdown_filters = v;
        self
    }
    pub fn force_strategy(mut self, v: Option<FilterStrategy>) -> Self {
        self.0.force_strategy = v;
        self
    }
    pub fn force_pushdown(mut self, v: Option<bool>) -> Self {
        self.0.force_pushdown = v;
        self
    }
    pub fn cost_predicate(mut self, v: u32) -> Self {
        self.0.cost_predicate = v;
        self
    }
    pub fn cost_collector(mut self, v: u32) -> Self {
        self.0.cost_collector = v;
        self
    }
    pub fn max_collector_parallelism(mut self, v: usize) -> Self {
        self.0.max_collector_parallelism = v;
        self
    }
    pub fn single_collector_strategy(mut self, v: CollectorCallStrategy) -> Self {
        self.0.single_collector_strategy = v;
        self
    }
    pub fn tree_collector_strategy(mut self, v: CollectorCallStrategy) -> Self {
        self.0.tree_collector_strategy = v;
        self
    }
    pub fn build(self) -> DatafusionQueryConfig {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_matches_legacy_constants() {
        let c = DatafusionQueryConfig::test_default();
        assert_eq!(c.batch_size, 8192);
        assert_eq!(c.target_partitions, 4);
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
    #[should_panic(expected = "null query config pointer")]
    fn wire_decode_null_pointer_panics() {
        unsafe { DatafusionQueryConfig::from_ffm_ptr(0) };
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
            fetch_strategy: 1,
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
        assert_eq!(c.fetch_strategy, FetchStrategy::ListingTable);
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
            fetch_strategy: 0,
        };
        let ptr = &wire as *const _ as i64;
        let c = unsafe { DatafusionQueryConfig::from_ffm_ptr(ptr) };
        assert_eq!(c.force_strategy, None);
        assert_eq!(c.force_pushdown, None);
        assert_eq!(c.fetch_strategy, FetchStrategy::None);
    }
}
