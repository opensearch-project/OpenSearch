//! Per-query tuning knobs shared by the vanilla and indexed query paths.
//!
//! Populated from Java (cluster / index / request settings) and passed to
//! Rust once at query start via a `#[repr(C)]` wire struct. Read out at
//! setup time and copied into hot-path fields — never dereferenced on a
//! per-batch or per-row hot path.

use crate::indexed_table::stream::FilterStrategy;

/// Engine-internal point lookup driven through the normal `df_execute_query`
/// entry point. When active, the Substrait `plan_ptr` is ignored and the plan
/// is built natively via the DataFrame API with a single pushed-down filter on
/// a stored reserved column — no Substrait, no planner round-trip. Used by the
/// pluggable-dataformat get-by-id path (`GetService`), not by user search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InternalSearch {
    /// Not an internal lookup — decode `plan_ptr` as Substrait as usual.
    Off,
    /// Get-by-row-id: `__row_id__ = bound`, single row. `bound` is the physical
    /// row position resolved from the secondary (Lucene) index.
    ByRowId(i64),
    /// Seq-no scan: `_seq_no > bound`, projecting only id/seq/term/version.
    /// Used by version-map restore on crash recovery.
    SeqNoAbove(i64),
}

impl InternalSearch {
    /// Decodes the FFM wire pair `(mode, bound)`. `mode`: 0 = Off, 1 = ByRowId,
    /// 2 = SeqNoAbove. Any other value is treated as Off (forward-compatible).
    pub fn from_wire(mode: i64, bound: i64) -> Self {
        match mode {
            1 => InternalSearch::ByRowId(bound),
            2 => InternalSearch::SeqNoAbove(bound),
            _ => InternalSearch::Off,
        }
    }

    /// Whether this is an engine-internal point lookup (i.e. not [`InternalSearch::Off`],
    /// the normal user-search path).
    pub fn is_internal_search(self) -> bool {
        !matches!(self, InternalSearch::Off)
    }
}

/// Query-scoped configuration. Owned by value after FFM decode.
#[derive(Debug, Clone)]
pub struct DatafusionQueryConfig {
    // Common
    pub batch_size: usize,
    // Single query concurrency
    pub target_partitions: usize,
    /// DataFusion's own decode-time predicate pushdown on the ListingTable path.
    pub listing_table_pushdown_filters: bool,

    // Indexed-only
    pub min_skip_run_default: usize,
    pub min_skip_run_selectivity_threshold: f64,
    /// Whether IndexedStream asks parquet to apply the residual predicate
    /// during decode (via `RowFilter` pushdown). Narrow row-granular
    /// selections benefit; block-granular ones don't.
    pub indexed_pushdown_filters: bool,
    /// Optional override that pins the per-RG `min_skip_run` choice instead of
    /// letting selectivity decide. Backed by the `datafusion.indexed.force_strategy`
    /// cluster setting: `None` (wire `-1`) lets the selectivity heuristic run,
    /// `RowSelection`/`BooleanMask` pin the choice node-wide. See
    /// `IndexedStream::pick_min_skip_run`.
    pub force_strategy: Option<FilterStrategy>,
    pub cost_predicate: u32,
    pub cost_collector: u32,
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
    pub listing_table_pushdown_filters: i32,
    /// 0 = false, 1 = true
    pub indexed_pushdown_filters: i32,
    /// -1 = None, 0 = RowSelection, 1 = BooleanMask.
    /// Backed by the `datafusion.indexed.force_strategy` cluster setting.
    pub force_strategy: i32,
    pub cost_predicate: i32,
    pub cost_collector: i32,
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
            listing_table_pushdown_filters: false,
            min_skip_run_default: 1024,
            min_skip_run_selectivity_threshold: 0.03,
            indexed_pushdown_filters: true,
            force_strategy: None,
            cost_predicate: 1,
            cost_collector: 10,
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
        Self {
            batch_size: w.batch_size as usize,
            target_partitions: w.target_partitions as usize,
            listing_table_pushdown_filters: w.listing_table_pushdown_filters != 0,
            min_skip_run_default: w.min_skip_run_default as usize,
            min_skip_run_selectivity_threshold: w.min_skip_run_selectivity_threshold,
            indexed_pushdown_filters: w.indexed_pushdown_filters != 0,
            // `force_strategy` is backed by a cluster setting; `-1` means None
            // (selectivity heuristic decides).
            force_strategy: match w.force_strategy {
                0 => Some(FilterStrategy::RowSelection),
                1 => Some(FilterStrategy::BooleanMask),
                _ => None,
            },
            cost_predicate: w.cost_predicate as u32,
            cost_collector: w.cost_collector as u32,
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
    pub fn listing_table_pushdown_filters(mut self, v: bool) -> Self {
        self.0.listing_table_pushdown_filters = v;
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
    pub fn cost_predicate(mut self, v: u32) -> Self {
        self.0.cost_predicate = v;
        self
    }
    pub fn cost_collector(mut self, v: u32) -> Self {
        self.0.cost_collector = v;
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
        assert!(!c.listing_table_pushdown_filters);
        assert_eq!(c.min_skip_run_default, 1024);
        assert!((c.min_skip_run_selectivity_threshold - 0.03).abs() < 1e-9);
        assert!(c.indexed_pushdown_filters);
        assert_eq!(c.force_strategy, None);
        assert_eq!(c.cost_predicate, 1);
        assert_eq!(c.cost_collector, 10);
    }

    #[test]
    #[should_panic(expected = "null query config pointer")]
    fn wire_decode_null_pointer_panics() {
        unsafe { DatafusionQueryConfig::from_ffm_ptr(0) };
    }

    #[test]
    fn internal_search_from_wire_decodes_modes() {
        assert_eq!(InternalSearch::from_wire(0, 99), InternalSearch::Off);
        assert_eq!(
            InternalSearch::from_wire(1, 42),
            InternalSearch::ByRowId(42)
        );
        assert_eq!(
            InternalSearch::from_wire(2, 7),
            InternalSearch::SeqNoAbove(7)
        );
        // Unknown modes are forward-compatible: treated as Off, bound ignored.
        assert_eq!(InternalSearch::from_wire(3, 5), InternalSearch::Off);
        assert!(!InternalSearch::Off.is_internal_search());
        assert!(InternalSearch::ByRowId(0).is_internal_search());
        assert!(InternalSearch::SeqNoAbove(0).is_internal_search());
    }

    #[test]
    fn wire_decode_round_trips_all_fields() {
        let wire = WireDatafusionQueryConfig {
            batch_size: 16384,
            target_partitions: 8,
            min_skip_run_default: 512,
            min_skip_run_selectivity_threshold: 0.07,
            listing_table_pushdown_filters: 1,
            indexed_pushdown_filters: 0,
            force_strategy: 1,
            cost_predicate: 3,
            cost_collector: 17,
        };
        let ptr = &wire as *const _ as i64;
        let c = unsafe { DatafusionQueryConfig::from_ffm_ptr(ptr) };
        assert_eq!(c.batch_size, 16384);
        assert_eq!(c.target_partitions, 8);
        assert_eq!(c.min_skip_run_default, 512);
        assert!((c.min_skip_run_selectivity_threshold - 0.07).abs() < 1e-9);
        assert!(c.listing_table_pushdown_filters);
        assert!(!c.indexed_pushdown_filters);
        assert_eq!(c.force_strategy, Some(FilterStrategy::BooleanMask));
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
            listing_table_pushdown_filters: 0,
            indexed_pushdown_filters: 1,
            force_strategy: -1,
            cost_predicate: 1,
            cost_collector: 10,
        };
        let ptr = &wire as *const _ as i64;
        let c = unsafe { DatafusionQueryConfig::from_ffm_ptr(ptr) };
        assert_eq!(c.force_strategy, None);
    }
}
