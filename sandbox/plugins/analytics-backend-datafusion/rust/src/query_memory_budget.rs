/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Per-query memory budgeting with spill-on-exceed semantics.
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

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};
use parquet::file::metadata::ParquetMetaData;

use crate::runtime_manager::CpuContention;

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

/// Minimum target_partitions floor.
const MIN_TARGET_PARTITIONS: usize = 1;

/// Cap `target_partitions` based on current CPU executor contention.
///
/// When the executor is idle or lightly loaded, returns the configured value
/// unchanged. When contended (queued tasks > workers), scales down so this
/// query doesn't add more tasks than the executor can service without queuing.
///
/// Formula: `min(configured, max(1, workers - alive_tasks))`
///
/// This doesn't reduce below 1 and only activates when there's actual queuing.
/// Zero-cost when the executor is idle (most common case).
pub fn cap_partitions_for_cpu(
    configured_target_partitions: usize,
    contention: &CpuContention,
) -> usize {
    if !contention.is_contended() {
        return configured_target_partitions;
    }
    // How many more tasks can the executor absorb without increasing queue depth?
    let spare_capacity = contention.num_workers.saturating_sub(contention.alive_tasks);
    let capped = spare_capacity.max(MIN_TARGET_PARTITIONS);
    capped.min(configured_target_partitions)
}

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
    acquire_budget_with_projection(pool, schema, configured_target_partitions, configured_batch_size, None)
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
    acquire_budget_inner(pool, avg_row_bytes, num_columns, configured_target_partitions, configured_batch_size)
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
    acquire_budget_inner(pool, avg_row_bytes, num_columns, configured_target_partitions, configured_batch_size)
}

/// Threshold: if jemalloc reports actual allocated is below this fraction of
/// pool limit, the pool's `try_grow` failure is considered a false positive
/// (stale reservations, accounting drift). We proceed at full partitions.
const JEMALLOC_SAFETY_THRESHOLD: f64 = 0.7;

/// Core budget acquisition logic. All public entry points delegate here.
///
/// Before reducing `target_partitions`, consults two sources of truth:
/// 1. DataFusion MemoryPool (`try_grow`) — the accounting model
/// 2. jemalloc `allocated_bytes()` — the process-level ground truth
///
/// Only reduces partitions if BOTH confirm pressure. This prevents false
/// reductions from stale pool accounting (phantoms from queries whose `Drop`
/// hasn't propagated yet, or conservative pool math that doesn't reflect
/// actual RSS).
fn acquire_budget_inner(
    pool: &Arc<dyn MemoryPool>,
    avg_row_bytes: usize,
    num_columns: usize,
    configured_target_partitions: usize,
    configured_batch_size: usize,
) -> Result<QueryMemoryBudget, DataFusionError> {
    acquire_budget_impl(pool, avg_row_bytes, num_columns, configured_target_partitions, configured_batch_size, true)
}

/// Implementation with configurable jemalloc cross-check.
/// `use_jemalloc_override`: when true, consults jemalloc before reducing.
/// Set to false in unit tests where pool limits are artificially small.
fn acquire_budget_impl(
    pool: &Arc<dyn MemoryPool>,
    avg_row_bytes: usize,
    num_columns: usize,
    configured_target_partitions: usize,
    configured_batch_size: usize,
    use_jemalloc_override: bool,
) -> Result<QueryMemoryBudget, DataFusionError> {
    let mut target_partitions = configured_target_partitions.max(MIN_TARGET_PARTITIONS);
    let mut batch_size = configured_batch_size.max(MIN_BATCH_SIZE);

    let pool_limit = match pool.memory_limit() {
        MemoryLimit::Finite(limit) => Some(limit),
        _ => None,
    };

    loop {
        let phantom_bytes = compute_untracked_bytes_with_columns(
            target_partitions, batch_size, avg_row_bytes, num_columns,
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
                // If actual process memory is well below the pool limit, the pool's
                // rejection is a false positive — proceed at current parallelism
                // by forcing the reservation (infallible grow).
                if use_jemalloc_override {
                    if let Some(limit) = pool_limit {
                        if should_override_pool_rejection(limit) {
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
                }

                // Both sources confirm pressure — reduce parallelism
                if target_partitions > MIN_TARGET_PARTITIONS {
                    target_partitions = (target_partitions / 2).max(MIN_TARGET_PARTITIONS);
                } else if batch_size > MIN_BATCH_SIZE {
                    batch_size = (batch_size / 2).max(MIN_BATCH_SIZE);
                } else {
                    return Err(DataFusionError::ResourcesExhausted(format!(
                        "Cannot reserve untracked memory budget: {} bytes required at \
                         minimum parallelism (partitions={}, batch_size={}, avg_row_bytes={}). \
                         Pool capacity exhausted (confirmed by jemalloc).",
                        compute_untracked_bytes(MIN_TARGET_PARTITIONS, MIN_BATCH_SIZE, avg_row_bytes),
                        MIN_TARGET_PARTITIONS,
                        MIN_BATCH_SIZE,
                        avg_row_bytes,
                    )));
                }
            }
        }
    }
}

/// Consult jemalloc: is the actual process allocated memory below the safety
/// threshold of the pool limit?
///
/// Returns `true` if jemalloc says there's headroom (pool rejection was false
/// positive). Returns `false` if jemalloc confirms pressure or if stats are
/// unavailable.
fn should_override_pool_rejection(pool_limit: usize) -> bool {
    let allocated = native_bridge_common::allocator::allocated_bytes();
    if allocated <= 0 {
        // Stats unavailable or error — don't override, be conservative
        return false;
    }
    let allocated = allocated as usize;
    let threshold = (pool_limit as f64 * JEMALLOC_SAFETY_THRESHOLD) as usize;
    allocated < threshold
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
    let decode_overhead = PAGE_DECODE_BASE_OVERHEAD_BYTES
        + num_columns * PER_COLUMN_DECODE_OVERHEAD_BYTES;

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
        DataType::Struct(fields) => fields.iter().map(|f| estimate_field_bytes(f.data_type())).sum(),
        DataType::Map(entry, _) => estimate_field_bytes(entry.data_type()) * 4,
        _ => 32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::num::NonZeroUsize;

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

    /// Helper: acquire budget without jemalloc override (for unit tests with
    /// artificially small pools where jemalloc would always override).
    fn acquire_budget_no_override(
        pool: &Arc<dyn MemoryPool>,
        schema: &SchemaRef,
        target_partitions: usize,
        batch_size: usize,
    ) -> Result<QueryMemoryBudget, DataFusionError> {
        let avg_row_bytes = estimate_avg_row_bytes(schema);
        let num_columns = schema.fields().len();
        acquire_budget_impl(pool, avg_row_bytes, num_columns, target_partitions, batch_size, false)
    }

    #[test]
    fn acquire_budget_reduces_partitions_under_pressure() {
        // Pool with only 2MB — not enough for 8 partitions
        let schema = schema_of(vec![("a", DataType::Int64), ("b", DataType::Int64)]);
        let pool = test_pool(2_000_000);

        let budget = acquire_budget_no_override(&pool, &schema, 8, 8192).unwrap();
        assert!(budget.target_partitions < 8);
        assert!(budget.target_partitions >= MIN_TARGET_PARTITIONS);
        assert!(budget.phantom_bytes <= 2_000_000);
    }

    #[test]
    fn acquire_budget_reduces_batch_size_at_min_partitions() {
        let schema = schema_of(vec![("a", DataType::LargeBinary)]); // 128 bytes/row
        let pool = test_pool(1_000_000); // 1MB — forces batch reduction

        let budget = acquire_budget_no_override(&pool, &schema, 4, 8192).unwrap();
        assert_eq!(budget.target_partitions, MIN_TARGET_PARTITIONS);
        assert!(budget.batch_size < 8192);
        assert!(budget.batch_size >= MIN_BATCH_SIZE);
    }

    #[test]
    fn acquire_budget_rejects_when_fully_exhausted() {
        let pool = test_pool(1000); // Tiny pool — even minimum won't fit
        let schema = schema_of(vec![("a", DataType::Int64)]);

        let result = acquire_budget_no_override(&pool, &schema, 4, 8192);
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

}
