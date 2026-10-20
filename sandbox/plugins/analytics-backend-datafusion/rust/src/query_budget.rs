/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Per-query budgeting with spill-on-exceed semantics.
//!
//! # TODO: Cardinality-aware operator headroom
//!
//! When parquet column statistics include `distinct_count` (NDV), the budget
//! could leave proportional headroom for hash aggregation / distinct operators:
//!   headroom = NDV × (key_bytes + accumulator_bytes_per_group)
//! This would prevent the phantom from starving high-cardinality GROUP BY
//! queries that need large hash tables. Without NDV, operators rely on
//! DataFusion's spill mechanism (safe but slower). Sources for NDV:
//! - Parquet column chunk statistics (`Statistics::distinct_count()`)
//! - OpenSearch field mappings with known cardinality (keyword fields)
//! - Query-level hint via `DatafusionQueryConfig`
//!
//! # Problem
//!
//! DataFusion's `MemoryPool` only tracks operator-internal state (hash tables,
//! sort buffers). The following are **untracked**:
//! - In-flight `RecordBatch`es between operators
//! - Parquet decode buffers (page decompression, dictionary decode)
//! - Channel buffers (`CoalescePartitionsExec` mpsc, `CrossRtStream` mpsc)
//!
//! More partitions multiply all of the above. Without accounting for untracked
//! memory, the pool's limit underestimates true memory usage, and operators
//! that *could* spill don't trigger spill because the pool appears to have
//! headroom.
//!
//! # Solution: Phantom Reservation + Spill Pressure
//!
//! Instead of clamping `target_partitions` (which reduces throughput), we:
//!
//! 1. **Estimate** the untracked memory for the chosen `target_partitions` and
//!    `batch_size` from the schema.
//! 2. **Reserve** that estimate from the pool as a phantom "untracked overhead"
//!    `MemoryConsumer` (marked `can_spill = true`).
//! 3. Let the query run at full configured parallelism.
//!
//! This phantom reservation **occupies** pool capacity that would otherwise be
//! available to operators. When the pool is under pressure:
//! - Spillable operators (HashAgg, Sort) see less available capacity.
//! - Their `try_grow` calls fail earlier.
//! - They trigger spill-to-disk, freeing tracked memory.
//! - The system stays within the pool limit (tracked + phantom ≤ pool_limit).
//!
//! The net effect: the pool limit now represents the **actual** RSS bound
//! (tracked + untracked), and operators automatically spill to stay within it.
//!
//! # When even spilling isn't enough
//!
//! If the pool is so exhausted that even the phantom reservation itself fails
//! (`try_grow` returns `ResourcesExhausted`), the query is rejected at
//! admission time with a clear error — before any work begins.
//!
//! # Adaptive fallback
//!
//! When the phantom reservation fails, the budget calculator can optionally
//! reduce `target_partitions` to lower the phantom size, retrying with less
//! parallelism. This gives a graceful degradation path: full parallelism when
//! memory allows, reduced parallelism under pressure, rejection only at
//! extreme saturation.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use once_cell::sync::Lazy;
use parquet::file::metadata::ParquetMetaData;

/// Cumulative counters for budget fallbacks and rejections.
pub struct AdaptiveBudgetStats {
    pub fallbacks: AtomicU64,
    pub rejections: AtomicU64,
}

static ADAPTIVE_BUDGET: Lazy<AdaptiveBudgetStats> = Lazy::new(|| AdaptiveBudgetStats {
    fallbacks: AtomicU64::new(0),
    rejections: AtomicU64::new(0),
});

/// Returns a reference to the global budget stats counters.
pub fn adaptive_budget() -> &'static AdaptiveBudgetStats {
    &ADAPTIVE_BUDGET
}

/// Configurable minimum target_partitions floor for adaptive reduction.
/// Updated from Java when the cluster setting `datafusion.min_target_partitions`
/// changes. The floor is clamped per-call to the configured target_partitions,
/// so it cannot raise the starting value above what the caller requested
/// (e.g. derived from `search.concurrent.max_slice_count`).
static MIN_TARGET_PARTITIONS_SETTING: AtomicUsize = AtomicUsize::new(1);

/// Set the minimum target partitions floor at runtime. Called from Java when
/// the cluster setting changes.
pub fn set_min_target_partitions(value: usize) {
    MIN_TARGET_PARTITIONS_SETTING.store(value.max(1), Ordering::Release);
}

/// Read the current minimum target partitions floor.
pub fn get_min_target_partitions() -> usize {
    MIN_TARGET_PARTITIONS_SETTING.load(Ordering::Acquire)
}

/// How many batch-sized buffers exist per partition in the pipeline.
///
/// Derived from the execution pipeline:
/// - 1 batch: in-flight between FileStream and CoalescePartitionsExec
/// - 1 batch: buffered in the CoalescePartitionsExec mpsc channel slot
/// - ~1 batch: parquet decode working buffer (decompressed page → arrow arrays)
const PARTITION_BUFFER_MULTIPLIER: usize = 3;

/// Base per-partition overhead for parquet decode structures (metadata parsing,
/// row group state, page index). Independent of column count.
const PAGE_DECODE_BASE_OVERHEAD_BYTES: usize = 64 * 1024;

/// Per-column overhead within a partition (column reader, decompression buffer,
/// dictionary decode state). ~4KB per active column reader.
const PER_COLUMN_DECODE_OVERHEAD_BYTES: usize = 4 * 1024;

/// Minimum batch_size floor.
const MIN_BATCH_SIZE: usize = 1024;

/// Default minimum target_partitions floor (used if the dynamic setting hasn't been set).
const DEFAULT_MIN_TARGET_PARTITIONS: usize = 1;

/// Computed budget for a query. Carries the phantom reservation that must be
/// held for the query's lifetime.
#[derive(Debug)]
pub struct QueryMemoryBudget {
    /// Effective target_partitions to use.
    pub target_partitions: usize,
    /// Effective batch_size to use.
    pub batch_size: usize,
    /// The phantom reservation occupying pool capacity for untracked memory.
    /// Drop this when the query completes to release the capacity.
    pub phantom_reservation: MemoryReservation,
    /// Bytes reserved by the phantom (informational).
    pub phantom_bytes: usize,
}

/// Compute and reserve a memory budget for a query.
///
/// Attempts to reserve a phantom allocation representing the untracked memory
/// at full configured parallelism. If that fails, iteratively halves
/// `target_partitions` until the reservation succeeds or we hit the minimum.
///
/// Returns `Err` only if even `target_partitions=1` with `batch_size=MIN`
/// cannot fit — the query should be rejected.
///
/// If `projected_columns` is provided, only those columns contribute to the
/// row-width estimate (much tighter for narrow projections on wide tables).
pub fn acquire_budget(
    pool: &Arc<dyn MemoryPool>,
    schema: &SchemaRef,
    configured_target_partitions: usize,
    configured_batch_size: usize,
) -> Result<QueryMemoryBudget, DataFusionError> {
    acquire_budget_with_projection(
        pool,
        schema,
        configured_target_partitions,
        configured_batch_size,
        None,
    )
}

/// Acquire budget using measured row bytes from parquet metadata.
///
/// This is the preferred path when metadata is cached. Uses actual
/// `uncompressed_size / num_rows` per column from the first row group
/// instead of static type-based estimates. Produces tight phantoms
/// (typically within 1.1-1.3× of actual RSS) instead of the 2.5-3×
/// over-estimation from static estimates.
pub fn acquire_budget_from_metadata(
    pool: &Arc<dyn MemoryPool>,
    schema: &SchemaRef,
    metadata: &ParquetMetaData,
    configured_target_partitions: usize,
    configured_batch_size: usize,
) -> Result<QueryMemoryBudget, DataFusionError> {
    let avg_row_bytes = estimate_row_bytes_from_metadata(schema, metadata)
        .unwrap_or_else(|| estimate_avg_row_bytes(schema));
    let num_columns = schema.fields().len();
    acquire_budget_inner(
        pool,
        avg_row_bytes,
        num_columns,
        configured_target_partitions,
        configured_batch_size,
    )
}

/// Same as [`acquire_budget`] but accepts an optional projection.
pub fn acquire_budget_with_projection(
    pool: &Arc<dyn MemoryPool>,
    schema: &SchemaRef,
    configured_target_partitions: usize,
    configured_batch_size: usize,
    projected_columns: Option<&[usize]>,
) -> Result<QueryMemoryBudget, DataFusionError> {
    let avg_row_bytes = match projected_columns {
        Some(indices) => estimate_projected_row_bytes(schema, indices),
        None => estimate_avg_row_bytes(schema),
    };
    let num_columns = match projected_columns {
        Some(indices) => indices.len(),
        None => schema.fields().len(),
    };
    acquire_budget_inner(
        pool,
        avg_row_bytes,
        num_columns,
        configured_target_partitions,
        configured_batch_size,
    )
}

/// Core budget acquisition logic. All public entry points delegate here.
///
/// Tries to reserve a phantom at the configured parallelism. If the pool
/// rejects it, iteratively halves target_partitions until it fits.
///
/// **Proactive RSS check**: before attempting pool reservation, consults
/// jemalloc's resident bytes. If physical memory already exceeds the admission
/// threshold (default 70% of pool limit), immediately reduces partitions to
/// the minimum. This prevents the "20 queries all pass admission simultaneously"
/// burst — each new query arriving when RSS is elevated starts at minimum
/// parallelism, limiting its hash table growth and total memory footprint.
fn acquire_budget_inner(
    pool: &Arc<dyn MemoryPool>,
    avg_row_bytes: usize,
    num_columns: usize,
    configured_target_partitions: usize,
    configured_batch_size: usize,
) -> Result<QueryMemoryBudget, DataFusionError> {
    // The setting acts purely as a floor for adaptive reduction. It must never
    // raise target_partitions above what the caller already configured (e.g.
    // derived from `search.concurrent.max_slice_count`). Clamp so a configured
    // value below the setting is left untouched.
    let min_partitions = get_min_target_partitions().min(configured_target_partitions.max(1));
    let mut target_partitions = configured_target_partitions.max(1);
    let mut batch_size = configured_batch_size.max(MIN_BATCH_SIZE);

    // Proactive admission guard: only consult jemalloc RSS when the pool's own
    // reservation accounting already shows pressure (>= admission threshold).
    // This avoids the ~5µs jemalloc epoch.advance cost on the happy path.
    if let Some(limit) = pool_limit(pool) {
        let reserved = pool.reserved();
        let thresholds = crate::memory_guard::get_thresholds();
        let admission_bytes = (limit as f64 * thresholds.admission_throttle) as usize;
        if reserved >= admission_bytes {
            let resident = crate::memory_guard::cached_resident_bytes();
            if resident > 0 {
                let spill_bytes = (limit as f64 * thresholds.admission_reject) as i64;
                if resident >= spill_bytes {
                    // RSS at spill threshold (85%) — reject immediately.
                    // Even at min partitions this query will hit spill on first batch.
                    // Better to reject with clear backpressure than admit and fail slowly.
                    native_bridge_common::log_info!(
                        "Admission REJECTED: pool reserved={}B, RSS={}B >= spill threshold ({:.0}% of {}B). Node under memory pressure.",
                        reserved, resident, thresholds.admission_reject * 100.0, limit
                    );
                    ADAPTIVE_BUDGET.rejections.fetch_add(1, Ordering::Relaxed);
                    return Err(crate::native_error::admission_rejected_error(
                        compute_untracked_bytes_with_columns(
                            min_partitions,
                            MIN_BATCH_SIZE,
                            avg_row_bytes,
                            num_columns,
                        ),
                        min_partitions,
                        MIN_BATCH_SIZE,
                        avg_row_bytes,
                    ));
                }
                // RSS between admission (70%) and operator (85%) — reduce partitions
                let admission_threshold_bytes =
                    (limit as f64 * thresholds.admission_throttle) as i64;
                if resident >= admission_threshold_bytes {
                    native_bridge_common::log_info!(
                        "Admission: pool reserved={}B, RSS={}B >= admission threshold ({:.0}%) — reducing to min partitions={}",
                        reserved, resident, thresholds.admission_throttle * 100.0, min_partitions
                    );
                    target_partitions = min_partitions;
                }
            }
        }
    }

    loop {
        let phantom_bytes = compute_untracked_bytes_with_columns(
            target_partitions,
            batch_size,
            avg_row_bytes,
            num_columns,
        );

        let consumer = MemoryConsumer::new(format!(
            "query_untracked(partitions={},batch={})",
            target_partitions, batch_size
        ))
        .with_can_spill(true);
        let mut reservation = consumer.register(pool);

        match reservation.try_grow(phantom_bytes) {
            Ok(()) => {
                return Ok(QueryMemoryBudget {
                    target_partitions,
                    batch_size,
                    phantom_reservation: reservation,
                    phantom_bytes,
                });
            }
            Err(_) => {
                drop(reservation);

                // Before reducing: consult jemalloc as second source of truth.
                // If actual process memory is well below the pool limit, the
                // pool's rejection is a false positive (stale phantoms from
                // queries whose Drop hasn't propagated, accounting drift).
                // Proceed at current parallelism via infallible grow.
                if let Some(limit) = pool_limit(pool) {
                    if jemalloc_says_headroom_available(limit) {
                        let consumer = MemoryConsumer::new(format!(
                            "query_untracked(partitions={},batch={},jemalloc_override)",
                            target_partitions, batch_size
                        ))
                        .with_can_spill(true);
                        let reservation = consumer.register(pool);
                        pool.grow(&reservation, phantom_bytes);
                        return Ok(QueryMemoryBudget {
                            target_partitions,
                            batch_size,
                            phantom_reservation: reservation,
                            phantom_bytes,
                        });
                    }
                }

                // Both pool and jemalloc confirm pressure — reduce parallelism
                if target_partitions > min_partitions {
                    let prev = target_partitions;
                    target_partitions = (target_partitions / 2).max(min_partitions);
                    ADAPTIVE_BUDGET.fallbacks.fetch_add(1, Ordering::Relaxed);
                    native_bridge_common::log_info!(
                        "Memory pressure: reducing target_partitions {} -> {} (phantom {} bytes failed, floor={})",
                        prev, target_partitions, phantom_bytes, min_partitions
                    );
                } else if batch_size > MIN_BATCH_SIZE {
                    let prev = batch_size;
                    batch_size = (batch_size / 2).max(MIN_BATCH_SIZE);
                    ADAPTIVE_BUDGET.fallbacks.fetch_add(1, Ordering::Relaxed);
                    native_bridge_common::log_info!(
                        "Memory pressure: reducing batch_size {} -> {} at target_partitions={} (phantom {} bytes failed)",
                        prev, batch_size, target_partitions, phantom_bytes
                    );
                } else {
                    ADAPTIVE_BUDGET.rejections.fetch_add(1, Ordering::Relaxed);
                    return Err(crate::native_error::admission_rejected_error(
                        compute_untracked_bytes(min_partitions, MIN_BATCH_SIZE, avg_row_bytes),
                        min_partitions,
                        MIN_BATCH_SIZE,
                        avg_row_bytes,
                    ));
                }
            }
        }
    }
}

/// Attempt to acquire or grow the coordinator-reduce session's phantom
/// reservation based on a child input's schema. Compares the estimated
/// untracked memory for this schema against the reservation already held
/// from a prior child registration (if any). Skips acquisition when the
/// existing reservation already covers the new estimate (e.g. a prior child
/// had a wider schema). Returns Ok(None) when the existing reservation already
/// covers the estimate. Returns Err if the pool cannot fit the phantom even at
/// target_partitions=1 — the reduce should be rejected.
pub fn try_grow_reduce_budget(
    pool: &Arc<dyn MemoryPool>,
    schema: &SchemaRef,
    batch_size: usize,
    configured_target_partitions: usize,
    prior_partition_reservation_bytes: usize,
) -> Result<Option<QueryMemoryBudget>, DataFusionError> {
    let avg_row_bytes = estimate_avg_row_bytes(schema);
    let num_columns = schema.fields().len();
    let needed = compute_untracked_bytes_with_columns(
        configured_target_partitions,
        batch_size,
        avg_row_bytes,
        num_columns,
    );
    if needed <= prior_partition_reservation_bytes {
        return Ok(None);
    }
    acquire_budget_inner(
        pool,
        avg_row_bytes,
        num_columns,
        configured_target_partitions,
        batch_size,
    )
    .map(Some)
}

/// Compute the untracked byte envelope for given parameters.
fn compute_untracked_bytes(
    target_partitions: usize,
    batch_size: usize,
    avg_row_bytes: usize,
) -> usize {
    compute_untracked_bytes_with_columns(target_partitions, batch_size, avg_row_bytes, 0)
}

/// Compute untracked bytes with column-count-aware decode overhead.
fn compute_untracked_bytes_with_columns(
    target_partitions: usize,
    batch_size: usize,
    avg_row_bytes: usize,
    num_columns: usize,
) -> usize {
    let batch_bytes = batch_size * avg_row_bytes;
    let decode_overhead =
        PAGE_DECODE_BASE_OVERHEAD_BYTES + num_columns * PER_COLUMN_DECODE_OVERHEAD_BYTES;

    if target_partitions == 1 {
        // No CoalescePartitionsExec, no merge channel. Pipeline is:
        // FileStream → CrossRtStream(1 slot) → Java
        // Untracked: 1 decode buffer + 1 in-flight + 1 CrossRtStream slot
        let per_partition = 2 * batch_bytes + decode_overhead;
        let output_channel = batch_bytes;
        return per_partition + output_channel;
    }

    // Multi-partition: CoalescePartitionsExec adds a merge channel with
    // capacity=target_partitions. Each partition has decode + in-flight.
    // Then CrossRtStream adds 1 more output slot.
    let per_partition = PARTITION_BUFFER_MULTIPLIER * batch_bytes + decode_overhead;
    let output_channel = batch_bytes;

    target_partitions * per_partition + output_channel
}

/// Estimate average row size in bytes from a schema.
///
/// For fixed-width types: exact byte width.
/// For variable-width types: conservative static estimate.
/// Includes 1 bit per nullable column for the validity bitmap.
/// Returns at least 8 bytes (minimum for one Int64 column).
pub fn estimate_avg_row_bytes(schema: &SchemaRef) -> usize {
    let mut total: usize = 0;
    let mut nullable_count: usize = 0;
    for field in schema.fields() {
        total += estimate_field_bytes(field.data_type());
        if field.is_nullable() {
            nullable_count += 1;
        }
    }
    // Null bitmaps: 1 bit per nullable column per row, rounded up to bytes
    total += (nullable_count + 7) / 8;
    total.max(8)
}

/// Estimate row bytes considering only the projected columns.
fn estimate_projected_row_bytes(schema: &SchemaRef, indices: &[usize]) -> usize {
    let mut total: usize = 0;
    for &idx in indices {
        if let Some(field) = schema.fields().get(idx) {
            total += estimate_field_bytes(field.data_type());
        }
    }
    total.max(8)
}

/// Refine row-width estimate using actual column sizes from parquet metadata.
///
/// For each column, takes the **minimum** of:
/// - Static type-based estimate (conservative for short strings)
/// - `uncompressed_size / num_rows` from parquet (accurate for long strings)
///
/// This produces the tightest correct estimate regardless of string length:
/// - Short strings (10 bytes): static=64 wins, metadata≈14 (includes offset overhead) → uses 14
/// - Long strings (500 bytes): static=64 loses, metadata≈504 → uses 504... wait, that's
///   an under-estimate from static. So we actually want MAX for variable-length columns
///   (metadata is ground truth) and MIN for fixed-width (both are exact).
///
/// Revised strategy: for variable-length types, use metadata (ground truth).
/// For fixed-width types, use the type size (exact).
///
/// Returns `None` if metadata doesn't have useful stats (empty file, etc.).
pub fn estimate_row_bytes_from_metadata(
    schema: &SchemaRef,
    metadata: &ParquetMetaData,
) -> Option<usize> {
    let first_rg = metadata.row_groups().first()?;
    let num_rows = first_rg.num_rows() as usize;
    if num_rows == 0 {
        return None;
    }

    let mut total: usize = 0;
    for (idx, field) in schema.fields().iter().enumerate() {
        if idx < first_rg.columns().len() {
            let col_chunk = &first_rg.columns()[idx];
            let measured = col_chunk.uncompressed_size() as usize / num_rows;

            if is_variable_length(field.data_type()) {
                // Variable-length: metadata is ground truth (includes offset array overhead
                // which Arrow also has). Use it directly — it's the actual decoded size.
                total += measured;
            } else {
                // Fixed-width: type size is exact. Metadata may include encoding overhead
                // (e.g., RLE/dictionary pages) that inflates the number.
                total += estimate_field_bytes(field.data_type());
            }
        } else {
            total += estimate_field_bytes(field.data_type());
        }
    }
    Some(total.max(8))
}

fn is_variable_length(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8
            | DataType::Binary
            | DataType::LargeUtf8
            | DataType::LargeBinary
            | DataType::Utf8View
            | DataType::BinaryView
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Map(_, _)
    )
}

fn estimate_field_bytes(dt: &DataType) -> usize {
    match dt {
        DataType::Boolean => 1,
        DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => 2,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => 8,
        DataType::Date32 => 4,
        DataType::Date64 | DataType::Time64(_) | DataType::Timestamp(_, _) => 8,
        DataType::Time32(_) => 4,
        DataType::Duration(_) => 8,
        DataType::Interval(_) => 16,
        DataType::Decimal128(_, _) => 16,
        DataType::Decimal256(_, _) => 32,
        DataType::FixedSizeBinary(n) => *n as usize,
        DataType::Utf8 | DataType::Binary => 64,
        DataType::LargeUtf8 | DataType::LargeBinary => 128,
        DataType::Utf8View | DataType::BinaryView => 64,
        DataType::List(inner) | DataType::LargeList(inner) => {
            4 * estimate_field_bytes(inner.data_type())
        }
        DataType::FixedSizeList(inner, n) => *n as usize * estimate_field_bytes(inner.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .map(|f| estimate_field_bytes(f.data_type()))
            .sum(),
        DataType::Map(entry, _) => estimate_field_bytes(entry.data_type()) * 4,
        _ => 32,
    }
}

/// Extract pool limit if finite.
fn pool_limit(pool: &Arc<dyn MemoryPool>) -> Option<usize> {
    match pool.memory_limit() {
        MemoryLimit::Finite(limit) => Some(limit),
        _ => None,
    }
}

/// Delegates to the common memory guard for admission-level override check.
fn jemalloc_says_headroom_available(pool_limit_bytes: usize) -> bool {
    crate::memory_guard::should_override(
        pool_limit_bytes,
        crate::memory_guard::OverrideContext::Admission,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::num::NonZeroUsize;
    use std::sync::Mutex;

    /// Serializes tests that mutate the process-global MIN_TARGET_PARTITIONS_SETTING.
    static MIN_PARTITIONS_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn schema_of(fields: Vec<(&str, DataType)>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn test_pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(datafusion::execution::memory_pool::TrackConsumersPool::new(
            datafusion::execution::memory_pool::GreedyMemoryPool::new(limit),
            NonZeroUsize::new(5).unwrap(),
        ))
    }

    #[test]
    fn estimate_fixed_width_schema() {
        let schema = schema_of(vec![
            ("id", DataType::Int64),
            ("price", DataType::Float64),
            ("count", DataType::Int32),
        ]);
        // 8 + 8 + 4 = 20, plus 3 nullable columns → ceil(3/8) = 1 byte bitmap
        assert_eq!(estimate_avg_row_bytes(&schema), 21);
    }

    #[test]
    fn estimate_variable_width_schema() {
        let schema = schema_of(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        // 8 + 64 = 72, plus 2 nullable → ceil(2/8) = 1 byte bitmap
        assert_eq!(estimate_avg_row_bytes(&schema), 73);
    }

    #[test]
    fn estimate_empty_schema_returns_minimum() {
        let schema = Arc::new(Schema::empty());
        assert_eq!(estimate_avg_row_bytes(&schema), 8);
    }

    #[test]
    fn acquire_budget_succeeds_with_plenty_of_memory() {
        let pool = test_pool(1_000_000_000); // 1GB
        let schema = schema_of(vec![("a", DataType::Int64), ("b", DataType::Int64)]);

        let budget = acquire_budget(&pool, &schema, 4, 8192).unwrap();
        assert_eq!(budget.target_partitions, 4);
        assert_eq!(budget.batch_size, 8192);
        assert!(budget.phantom_bytes > 0);
        assert_eq!(pool.reserved(), budget.phantom_bytes);
    }

    /// Helper for tests with artificially small pools.
    fn acquire_budget_test(
        pool: &Arc<dyn MemoryPool>,
        schema: &SchemaRef,
        target_partitions: usize,
        batch_size: usize,
    ) -> Result<QueryMemoryBudget, DataFusionError> {
        acquire_budget(pool, schema, target_partitions, batch_size)
    }

    #[test]
    fn acquire_budget_reduces_partitions_under_pressure() {
        // Pool with only 2MB — not enough for 8 partitions
        let schema = schema_of(vec![("a", DataType::Int64), ("b", DataType::Int64)]);
        let pool = test_pool(2_000_000);

        let budget = acquire_budget_test(&pool, &schema, 8, 8192).unwrap();
        assert!(budget.target_partitions < 8);
        assert!(budget.target_partitions >= DEFAULT_MIN_TARGET_PARTITIONS);
        assert!(budget.phantom_bytes <= 2_000_000);
    }

    #[test]
    fn acquire_budget_reduces_batch_size_at_min_partitions() {
        let schema = schema_of(vec![("a", DataType::LargeBinary)]); // 128 bytes/row
        let pool = test_pool(1_000_000); // 1MB — forces batch reduction

        let budget = acquire_budget_test(&pool, &schema, 4, 8192).unwrap();
        assert_eq!(budget.target_partitions, DEFAULT_MIN_TARGET_PARTITIONS);
        assert!(budget.batch_size < 8192);
        assert!(budget.batch_size >= MIN_BATCH_SIZE);
    }

    #[test]
    fn acquire_budget_rejects_when_fully_exhausted() {
        let pool = test_pool(1000); // Tiny pool — even minimum won't fit
        let schema = schema_of(vec![("a", DataType::Int64)]);

        let result = acquire_budget_test(&pool, &schema, 4, 8192);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot reserve untracked memory budget"));
    }

    #[test]
    fn phantom_freed_on_drop() {
        let pool = test_pool(100_000_000);
        let schema = schema_of(vec![("a", DataType::Int64)]);

        let budget = acquire_budget(&pool, &schema, 4, 8192).unwrap();
        let reserved_with_budget = pool.reserved();
        assert!(reserved_with_budget > 0);

        drop(budget);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn phantom_creates_spill_pressure_for_operators() {
        // Pool is 10MB. Phantom takes ~5MB. Operator only gets ~5MB before spill.
        let pool = test_pool(10_000_000);
        let schema = schema_of(vec![
            ("a", DataType::Int64),
            ("b", DataType::Int64),
            ("c", DataType::Int64),
            ("d", DataType::Int64),
        ]); // 32 bytes/row

        let budget = acquire_budget(&pool, &schema, 4, 8192).unwrap();
        let after_phantom = pool.reserved();

        // Simulate an operator trying to allocate from the same pool
        let consumer = MemoryConsumer::new("hash_agg");
        let mut reservation = consumer.register(&pool);

        // Try to allocate what's left
        let remaining = 10_000_000 - after_phantom;
        assert!(reservation.try_grow(remaining).is_ok());

        // Now try to exceed — this should fail, triggering spill in a real operator
        assert!(reservation.try_grow(1).is_err());

        drop(reservation);
        drop(budget);
    }

    #[test]
    fn compute_untracked_bytes_formula() {
        let partitions = 4;
        let batch_size = 8192;
        let row_bytes = 16;
        let num_columns = 3;

        let result =
            compute_untracked_bytes_with_columns(partitions, batch_size, row_bytes, num_columns);

        let batch_bytes = batch_size * row_bytes; // 131072
        let decode_overhead =
            PAGE_DECODE_BASE_OVERHEAD_BYTES + num_columns * PER_COLUMN_DECODE_OVERHEAD_BYTES;
        let per_part = 3 * batch_bytes + decode_overhead;
        let expected = 4 * per_part + batch_bytes;
        assert_eq!(result, expected);
    }

    #[test]
    fn single_partition_avoids_coalesce_overhead() {
        let row_bytes = 16;
        let batch_size = 8192;
        let multi = compute_untracked_bytes_with_columns(2, batch_size, row_bytes, 2);
        let single = compute_untracked_bytes_with_columns(1, batch_size, row_bytes, 2);
        // Single partition should use less than half of 2-partition (no merge channel multiplier)
        assert!(single < multi);
    }

    #[test]
    fn try_grow_reduce_budget_skips_when_existing_covers() {
        let pool = test_pool(1_000_000_000);
        let schema = schema_of(vec![("a", DataType::Int64), ("b", DataType::Int64)]);
        let needed = compute_untracked_bytes_with_columns(
            4,
            8192,
            estimate_avg_row_bytes(&schema),
            schema.fields().len(),
        );

        // Existing reservation is larger — should return Ok(None)
        let result = try_grow_reduce_budget(&pool, &schema, 8192, 4, needed + 1).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn try_grow_reduce_budget_acquires_when_needed_exceeds_existing() {
        let pool = test_pool(1_000_000_000);
        let schema = schema_of(vec![("a", DataType::Int64), ("b", DataType::Int64)]);

        // No existing reservation — should acquire
        let result = try_grow_reduce_budget(&pool, &schema, 8192, 4, 0).unwrap();
        assert!(result.is_some());
        let budget = result.unwrap();
        assert_eq!(budget.target_partitions, 4);
        assert!(budget.phantom_bytes > 0);
    }

    #[test]
    fn try_grow_reduce_budget_adapts_partitions_under_pressure() {
        // Small pool forces partition reduction
        let pool = test_pool(500_000);
        let schema = schema_of(vec![
            ("a", DataType::Int64),
            ("b", DataType::Int64),
            ("c", DataType::Utf8),
        ]);

        let result = try_grow_reduce_budget(&pool, &schema, 8192, 4, 0).unwrap();
        assert!(result.is_some());
        let budget = result.unwrap();
        assert!(
            budget.target_partitions < 4,
            "expected partitions < 4, got {}",
            budget.target_partitions
        );
    }

    #[test]
    fn min_target_partitions_setting_does_not_raise_configured_value() {
        // Reproduces the bug where `datafusion.min_target_partitions=8` would
        // override a lower configured target_partitions (derived from
        // search.concurrent.max_slice_count). The setting is a floor for
        // adaptive reduction, so it must be a no-op when configured ≤ floor.
        let _guard = MIN_PARTITIONS_TEST_LOCK.lock().unwrap();
        let prev = get_min_target_partitions();
        set_min_target_partitions(8);

        let pool = test_pool(1_000_000_000);
        let schema = schema_of(vec![("a", DataType::Int64), ("b", DataType::Int64)]);

        // Caller asks for 2 partitions; setting is 8. Result must stay at 2.
        let budget = acquire_budget(&pool, &schema, 2, 8192).unwrap();
        assert_eq!(budget.target_partitions, 2);

        set_min_target_partitions(prev);
    }

    #[test]
    fn min_target_partitions_setting_still_acts_as_reduction_floor() {
        // When the configured target is above the floor and memory pressure
        // forces reduction, target_partitions must not drop below the floor.
        let _guard = MIN_PARTITIONS_TEST_LOCK.lock().unwrap();
        let prev = get_min_target_partitions();
        set_min_target_partitions(2);

        let schema = schema_of(vec![("a", DataType::Int64), ("b", DataType::Int64)]);
        let pool = test_pool(2_000_000);

        let budget = acquire_budget(&pool, &schema, 8, 8192).unwrap();
        assert!(budget.target_partitions < 8);
        assert!(budget.target_partitions >= 2);

        set_min_target_partitions(prev);
    }

    #[test]
    fn try_grow_reduce_budget_rejects_when_pool_exhausted() {
        // Tiny pool that can't fit anything
        let pool = test_pool(1024);
        let schema = schema_of(vec![("a", DataType::Int64), ("b", DataType::Utf8)]);

        let result = try_grow_reduce_budget(&pool, &schema, 8192, 4, 0);
        assert!(result.is_err(), "expected Err when pool is exhausted");
    }

    #[test]
    fn adaptive_budget_counters_increment() {
        let stats = adaptive_budget();
        let fallbacks_before = stats.fallbacks.load(Ordering::Relaxed);
        let rejections_before = stats.rejections.load(Ordering::Relaxed);

        // Successful acquire — should not increment fallbacks/rejections
        let pool = test_pool(1_000_000_000);
        let schema = schema_of(vec![("a", DataType::Int64)]);
        let _ = acquire_budget(&pool, &schema, 4, 8192).unwrap();

        // Acquire with tiny pool that forces fallback
        let small_pool = test_pool(2_000_000);
        let wide_schema = schema_of(vec![("a", DataType::Int64), ("b", DataType::Int64)]);
        let _ = acquire_budget(&small_pool, &wide_schema, 8, 8192).unwrap();
        assert!(stats.fallbacks.load(Ordering::Relaxed) > fallbacks_before);

        // Acquire with exhausted pool — should increment rejections
        let tiny_pool = test_pool(1000);
        let result = acquire_budget(&tiny_pool, &schema, 4, 8192);
        assert!(result.is_err());
        assert!(stats.rejections.load(Ordering::Relaxed) > rejections_before);
    }
}
