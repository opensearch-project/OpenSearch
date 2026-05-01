/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Fuzz fixture configuration — controls size, shape, and cardinality of
//! the generated corpus.
//!
//! One `FixtureConfig` → one parquet file written once per test. Each
//! test fuzz loop then generates many random trees against the same
//! fixture; the corpus is shared, the trees vary per iteration.

/// Kinds of columns the corpus generator can produce. Kept narrow on
/// purpose — the same five cover every Predicate code path our
/// evaluator needs to exercise (string eq/in, int cmp, int64 cmp,
/// float cmp, bool eq + 3VL null handling everywhere).
#[derive(Debug, Clone, Copy)]
pub(in crate::indexed_table::tests_e2e) enum ColumnKind {
    /// Utf8. Generated via a distinct-values pool; controls cardinality.
    Utf8 { num_distinct: usize, max_len: usize },
    /// Int32. Generated from a `[min, max)` value range.
    Int32 { min: i32, max: i32 },
    /// Int64. Same.
    Int64 { min: i64, max: i64 },
    /// Float64. Generated from a `[min, max)` range.
    Float64 { min: f64, max: f64 },
    /// Boolean.
    Boolean,
    /// Date32. Days since epoch; generated from a `[min, max)` range.
    Date32 { min: i32, max: i32 },
    /// Timestamp(Nanosecond, None). Nanoseconds since epoch; generated
    /// from a `[min, max)` range.
    TimestampNanos { min: i64, max: i64 },
}

/// Shape of the fixture. All parameters are deterministic given the
/// `seed`; two `FixtureConfig`s with the same seed + same fields
/// produce the same corpus + same trees.
#[derive(Debug, Clone)]
pub(in crate::indexed_table::tests_e2e) struct FixtureConfig {
    /// Master seed for this fixture. Drives corpus and tree generation.
    pub seed: u64,

    /// Total row count across the fixture. 10_000..=50_000 for our suite.
    pub num_rows: usize,

    /// Number of segments (parquet files). Rows are split round-robin-ish
    /// across segments; each segment becomes its own `SegmentFileInfo`.
    /// Global doc-id is the row's index in the corpus, preserved across
    /// segments via monotonically increasing `segment.first_row`.
    pub num_segments: usize,

    /// Target DataFusion partitions. Values > 1 exercise the
    /// `CoalescePartitionsExec` wrapper path in IndexedExec.
    pub target_partitions: usize,

    /// Max row-group size for the parquet writer. The writer will cut an
    /// RG every `rows_per_row_group` rows; smaller values produce more
    /// RGs and exercise per-RG boundaries harder.
    pub rows_per_row_group: usize,

    /// Writer's page-size limit. Parquet cuts a page when either
    /// `rows_per_page` is reached or the configured byte budget is hit,
    /// whichever is first. Smaller values produce more pages, exercising
    /// the `PagePruner` harder.
    pub rows_per_page: usize,

    /// Columns in order. `columns[0]` becomes the first parquet field.
    pub columns: Vec<(String, ColumnKind)>,

    /// Per-column null probability in `0.0..=1.0`.
    pub null_pct: f64,

    /// Number of Collector leaves the tree generator may emit. Each
    /// leaf is backed by a random RG-relative doc-id set produced at
    /// corpus-gen time so the oracle can mirror it exactly.
    pub num_collector_leaves: usize,

    /// Density of each Collector leaf's matching set, expressed as a
    /// fraction of `num_rows`. 0.01 = 1% of rows match that collector.
    pub collector_density: f64,

    /// Maximum depth of generated trees. A depth-0 tree is a single
    /// leaf; depth-1 allows one layer of connectives; etc.
    pub tree_max_depth: u32,

    /// Maximum fanout at each AND / OR node.
    pub tree_max_fanout: usize,

    /// Override batch_size for the DataFusion query config. `None` uses
    /// the harness default (`[128, 1024, 8192][seed % 3]`).
    pub batch_size: Option<usize>,

    /// Override max_collector_parallelism. `None` uses the harness
    /// default (`[1, 1, 2, 4][seed % 4]`).
    pub max_collector_parallelism: Option<usize>,

    /// Per-column null probability overrides. When non-empty, the
    /// corpus generator uses `null_pct_overrides[col_name]` instead of
    /// the global `null_pct` for that column. Columns not in the map
    /// fall back to `null_pct`.
    pub null_pct_overrides: std::collections::HashMap<String, f64>,
}

impl FixtureConfig {
    /// Small fixture for `fuzz_small`: 10k rows, handful of RGs, a few
    /// columns, moderate null rate.
    pub fn small(seed: u64) -> Self {
        Self {
            seed,
            num_rows: 10_000,
            num_segments: 1,
            target_partitions: 1,
            rows_per_row_group: 2_048,
            rows_per_page: 512,
            columns: default_columns(),
            null_pct: 0.1,
            num_collector_leaves: 3,
            collector_density: 0.05,
            tree_max_depth: 5,
            tree_max_fanout: 5,
            batch_size: None,
            max_collector_parallelism: None,
            null_pct_overrides: std::collections::HashMap::new(),
        }
    }

    /// Mid-size fixture for broader correctness coverage: 50k rows,
    /// more RGs, more pages.
    pub fn mid(seed: u64) -> Self {
        Self {
            seed,
            num_rows: 50_000,
            num_segments: 3,
            target_partitions: 4,
            rows_per_row_group: 4_096,
            rows_per_page: 1_024,
            columns: default_columns(),
            null_pct: 0.15,
            num_collector_leaves: 4,
            collector_density: 0.03,
            tree_max_depth: 6,
            tree_max_fanout: 6,
            batch_size: None,
            max_collector_parallelism: None,
            null_pct_overrides: std::collections::HashMap::new(),
        }
    }

    /// Block-boundary focus: cuts RGs + pages at tight multiples so
    /// `PositionMap` and `min_skip_run` math gets stressed.
    pub fn block_boundaries(seed: u64) -> Self {
        Self {
            seed,
            num_rows: 16_384,
            num_segments: 2,
            target_partitions: 2,
            rows_per_row_group: 1_024,
            rows_per_page: 64,
            columns: default_columns(),
            null_pct: 0.05,
            num_collector_leaves: 3,
            collector_density: 0.01, // very sparse → long skip runs
            tree_max_depth: 5,
            tree_max_fanout: 5,
            batch_size: None,
            max_collector_parallelism: None,
            null_pct_overrides: std::collections::HashMap::new(),
        }
    }

    /// Null-heavy: ~50% null on every column, exercises 3VL everywhere.
    pub fn null_heavy(seed: u64) -> Self {
        Self {
            seed,
            num_rows: 10_000,
            num_segments: 1,
            target_partitions: 1,
            rows_per_row_group: 2_048,
            rows_per_page: 256,
            columns: default_columns(),
            null_pct: 0.5,
            num_collector_leaves: 3,
            collector_density: 0.1,
            tree_max_depth: 5,
            tree_max_fanout: 5,
            batch_size: None,
            max_collector_parallelism: None,
            null_pct_overrides: std::collections::HashMap::new(),
        }
    }

    /// Cardinality extremes: mix of degenerate column shapes to stress
    /// page pruning + stats paths.
    /// - `const_str`: 1 distinct Utf8 value (whole column identical).
    /// - `unique_str`: `num_distinct = num_rows` so every value is
    ///   different — page stats min == max per row, pruning rarely
    ///   helps.
    /// - `tiny_int`: Int32 with range `[0, 2)` so page min/max are
    ///   extremely tight and almost every literal prunes.
    /// - `wide_int`: Int32 across full range — opposite end.
    pub fn cardinality_extremes(seed: u64) -> Self {
        let num_rows = 10_000;
        Self {
            seed,
            num_rows,
            num_segments: 1,
            target_partitions: 1,
            rows_per_row_group: 2_048,
            rows_per_page: 256,
            columns: vec![
                (
                    "const_str".to_string(),
                    ColumnKind::Utf8 {
                        num_distinct: 1,
                        max_len: 4,
                    },
                ),
                (
                    "unique_str".to_string(),
                    ColumnKind::Utf8 {
                        num_distinct: num_rows,
                        max_len: 8,
                    },
                ),
                ("tiny_int".to_string(), ColumnKind::Int32 { min: 0, max: 2 }),
                (
                    "wide_int".to_string(),
                    ColumnKind::Int32 {
                        min: i32::MIN / 2,
                        max: i32::MAX / 2,
                    },
                ),
                // keep one of each common type for tree-gen to also have choices
                ("price".to_string(), ColumnKind::Int32 { min: 0, max: 1000 }),
                (
                    "score".to_string(),
                    ColumnKind::Float64 {
                        min: 0.0,
                        max: 100.0,
                    },
                ),
                ("active".to_string(), ColumnKind::Boolean),
            ],
            null_pct: 0.1,
            num_collector_leaves: 3,
            collector_density: 0.05,
            tree_max_depth: 4,
            tree_max_fanout: 4,
            batch_size: None,
            max_collector_parallelism: None,
            null_pct_overrides: std::collections::HashMap::new(),
        }
    }

    /// Concurrency stress: 8 partitions × 4 segments with mid-size
    /// data. Flushes out any cross-partition / cross-segment ordering
    /// assumptions and exercises `UnionExec + CoalescePartitionsExec`
    /// with realistic fan-out. Each iteration is run TWICE (via
    /// `run_iteration_twice`) to detect non-determinism across runs.
    pub fn concurrency(seed: u64) -> Self {
        Self {
            seed,
            num_rows: 20_000,
            num_segments: 4,
            target_partitions: 8,
            rows_per_row_group: 1_024,
            rows_per_page: 256,
            columns: default_columns(),
            null_pct: 0.1,
            num_collector_leaves: 4,
            collector_density: 0.05,
            tree_max_depth: 5,
            tree_max_fanout: 5,
            batch_size: None,
            max_collector_parallelism: None,
            null_pct_overrides: std::collections::HashMap::new(),
        }
    }

    /// batch_size=1: stresses coalescer and mask-slicing at every row
    /// boundary. Catches off-by-one bugs in `current_mask` indexing.
    pub fn batch_size_one(seed: u64) -> Self {
        Self {
            seed,
            num_rows: 5_000,
            num_segments: 1,
            target_partitions: 1,
            rows_per_row_group: 1_024,
            rows_per_page: 256,
            columns: default_columns(),
            null_pct: 0.1,
            num_collector_leaves: 3,
            collector_density: 0.05,
            tree_max_depth: 4,
            tree_max_fanout: 4,
            batch_size: Some(1),
            max_collector_parallelism: None,
            null_pct_overrides: std::collections::HashMap::new(),
        }
    }

    /// All-null columns: `brand` and `qty` are 100% null, others at
    /// 10%. Exercises page stats with absent min/max and `IS NULL`
    /// predicates that always return TRUE on those columns.
    pub fn all_null_columns(seed: u64) -> Self {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert("brand".to_string(), 1.0);
        overrides.insert("qty".to_string(), 1.0);
        Self {
            seed,
            num_rows: 10_000,
            num_segments: 1,
            target_partitions: 1,
            rows_per_row_group: 2_048,
            rows_per_page: 256,
            columns: default_columns(),
            null_pct: 0.1,
            num_collector_leaves: 3,
            collector_density: 0.05,
            tree_max_depth: 5,
            tree_max_fanout: 5,
            batch_size: None,
            max_collector_parallelism: None,
            null_pct_overrides: overrides,
        }
    }

    /// Empty-result stress: very low collector density (0.1%) and only
    /// 1 collector leaf. Most trees produce zero matching rows,
    /// exercising short-circuit and empty-batch paths.
    pub fn empty_result(seed: u64) -> Self {
        Self {
            seed,
            num_rows: 10_000,
            num_segments: 1,
            target_partitions: 1,
            rows_per_row_group: 2_048,
            rows_per_page: 256,
            columns: default_columns(),
            null_pct: 0.1,
            num_collector_leaves: 1,
            collector_density: 0.001,
            tree_max_depth: 5,
            tree_max_fanout: 5,
            batch_size: None,
            max_collector_parallelism: None,
            null_pct_overrides: std::collections::HashMap::new(),
        }
    }

    /// Single row group: `rows_per_row_group >= num_rows` so the
    /// entire segment is one RG. No RG boundary transitions in the
    /// streaming loop.
    pub fn single_row_group(seed: u64) -> Self {
        Self {
            seed,
            num_rows: 10_000,
            num_segments: 1,
            target_partitions: 1,
            rows_per_row_group: 100_000,
            rows_per_page: 512,
            columns: default_columns(),
            null_pct: 0.1,
            num_collector_leaves: 3,
            collector_density: 0.05,
            tree_max_depth: 5,
            tree_max_fanout: 5,
            batch_size: None,
            max_collector_parallelism: None,
            null_pct_overrides: std::collections::HashMap::new(),
        }
    }

    /// Always-parallel collectors: `max_collector_parallelism = 4` so
    /// `PrecomputedLeafCache` concurrent path is always exercised.
    pub fn parallel_collectors(seed: u64) -> Self {
        Self {
            seed,
            num_rows: 10_000,
            num_segments: 2,
            target_partitions: 2,
            rows_per_row_group: 2_048,
            rows_per_page: 256,
            columns: default_columns(),
            null_pct: 0.1,
            num_collector_leaves: 4,
            collector_density: 0.05,
            tree_max_depth: 5,
            tree_max_fanout: 5,
            batch_size: None,
            max_collector_parallelism: Some(4),
            null_pct_overrides: std::collections::HashMap::new(),
        }
    }
}

/// Default column mix — covers every Predicate code path our evaluator
/// has. Names match what tree-gen expects.
fn default_columns() -> Vec<(String, ColumnKind)> {
    vec![
        (
            "brand".to_string(),
            ColumnKind::Utf8 {
                num_distinct: 8,
                max_len: 8,
            },
        ),
        (
            "status".to_string(),
            ColumnKind::Utf8 {
                num_distinct: 3,
                max_len: 8,
            },
        ),
        ("price".to_string(), ColumnKind::Int32 { min: 0, max: 1000 }),
        (
            "qty".to_string(),
            ColumnKind::Int64 {
                min: 0,
                max: 10_000,
            },
        ),
        (
            "score".to_string(),
            ColumnKind::Float64 {
                min: 0.0,
                max: 100.0,
            },
        ),
        ("active".to_string(), ColumnKind::Boolean),
        // Date32 values in roughly [2020-01-01, 2025-12-31]: days since
        // epoch 1970-01-01 = ~18262..=20454.
        (
            "created_day".to_string(),
            ColumnKind::Date32 {
                min: 18_262,
                max: 20_454,
            },
        ),
        // Timestamp in 2024 calendar year: ns since epoch.
        (
            "ts".to_string(),
            ColumnKind::TimestampNanos {
                min: 1_704_067_200_000_000_000, // 2024-01-01T00:00:00Z
                max: 1_735_689_600_000_000_000, // 2025-01-01T00:00:00Z
            },
        ),
    ]
}
