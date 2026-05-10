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
    // ─── Common (affects both vanilla and indexed paths) ───
    pub batch_size: usize,
    pub target_partitions: usize,
    /// DataFusion's own decode-time predicate pushdown on the vanilla path.
    pub parquet_pushdown_filters: bool,

    // ─── DataFusion execution settings (memory & spill) ───
    /// Minimum file size (bytes) for repartitioning file scans across
    /// `target_partitions`. Files smaller than this are read by a single
    /// partition. Larger values reduce partition count for small files.
    /// Maps to: `config.optimizer.repartition_file_min_size`
    pub repartition_file_min_size: usize,
    /// Whether to repartition file scans (split large files across partitions).
    /// Maps to: `config.optimizer.repartition_file_scans`
    pub repartition_file_scans: bool,
    /// Reserved memory per sort operator for in-memory merge during spill.
    /// Maps to: `config.execution.sort_spill_reservation_bytes`
    pub sort_spill_reservation_bytes: usize,
    /// Use parquet page index for predicate pushdown (skip pages).
    /// Maps to: `config.execution.parquet.enable_page_index`
    pub parquet_enable_page_index: bool,
    /// Use bloom filters during parquet reads to skip row groups.
    /// Maps to: `config.execution.parquet.bloom_filter_on_read`
    pub parquet_bloom_filter_on_read: bool,
    /// Reorder filter predicates to minimize evaluation cost.
    /// Maps to: `config.execution.parquet.reorder_filters`
    pub parquet_reorder_filters: bool,

    // ─── Indexed-only settings ───
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
    pub max_collector_parallelism: usize,
    /// How the SingleCollectorEvaluator narrows collector doc ranges.
    pub single_collector_strategy: CollectorCallStrategy,
    /// How the bitmap tree evaluator narrows collector doc ranges.
    pub tree_collector_strategy: CollectorCallStrategy,
}

impl Default for DatafusionQueryConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            target_partitions: 4,
            parquet_pushdown_filters: false,
            // DataFusion execution defaults
            repartition_file_min_size: 10 * 1024 * 1024, // 10MB
            repartition_file_scans: true,
            sort_spill_reservation_bytes: 10 * 1024 * 1024, // 10MB
            parquet_enable_page_index: true,
            parquet_bloom_filter_on_read: true,
            parquet_reorder_filters: false,
            // Indexed-only
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
            // DataFusion execution configs — use defaults until Java wire struct
            // is extended to pass these. They can be overridden via cluster settings.
            repartition_file_min_size: 10 * 1024 * 1024,
            repartition_file_scans: true,
            sort_spill_reservation_bytes: 10 * 1024 * 1024,
            parquet_enable_page_index: true,
            parquet_bloom_filter_on_read: true,
            parquet_reorder_filters: false,
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
