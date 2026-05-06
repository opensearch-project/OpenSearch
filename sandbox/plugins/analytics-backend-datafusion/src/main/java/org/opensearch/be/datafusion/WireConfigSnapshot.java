/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.annotation.ExperimentalApi;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Immutable snapshot of the dynamic indexed query settings, ready to be written
 * into a {@code MemorySegment} matching the Rust {@code WireDatafusionQueryConfig}
 * {@code #[repr(C)]} layout.
 * <p>
 * Hardcoded wire fields (not configurable via cluster settings):
 * <ul>
 *   <li>{@code indexed_pushdown_filters = 1} (true)</li>
 *   <li>{@code force_strategy = -1} (None)</li>
 *   <li>{@code force_pushdown = -1} (None)</li>
 *   <li>{@code single_collector_strategy = 2} (PageRangeSplit)</li>
 *   <li>{@code tree_collector_strategy = 1} (TightenOuterBounds)</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record WireConfigSnapshot(int batchSize, int targetPartitions, boolean parquetPushdownFilters, int minSkipRunDefault,
    double minSkipRunSelectivityThreshold, int costPredicate, int costCollector, int maxCollectorParallelism) {

    /** Total byte size of the wire struct ({@code WireDatafusionQueryConfig}). */
    public static final long BYTE_SIZE = 68;

    /**
     * Writes this snapshot into a {@code MemorySegment} matching the
     * {@code WireDatafusionQueryConfig} {@code #[repr(C)]} layout.
     * <p>
     * The segment must be at least {@link #BYTE_SIZE} bytes and allocated from
     * a confined {@code Arena} scoped to the query lifetime.
     *
     * @param segment the target memory segment (at least 68 bytes)
     */
    public void writeTo(MemorySegment segment) {
        // Offset 0: batch_size (i64)
        segment.set(ValueLayout.JAVA_LONG, 0, (long) batchSize);
        // Offset 8: target_partitions (i64)
        segment.set(ValueLayout.JAVA_LONG, 8, (long) targetPartitions);
        // Offset 16: min_skip_run_default (i64)
        segment.set(ValueLayout.JAVA_LONG, 16, (long) minSkipRunDefault);
        // Offset 24: min_skip_run_selectivity_threshold (f64)
        segment.set(ValueLayout.JAVA_DOUBLE, 24, minSkipRunSelectivityThreshold);
        // Offset 32: parquet_pushdown_filters (i32) — 0 = false, 1 = true
        segment.set(ValueLayout.JAVA_INT, 32, parquetPushdownFilters ? 1 : 0);
        // Offset 36: indexed_pushdown_filters (i32) — always 1 (hardcoded)
        segment.set(ValueLayout.JAVA_INT, 36, 1);
        // Offset 40: force_strategy (i32) — always -1 (None)
        segment.set(ValueLayout.JAVA_INT, 40, -1);
        // Offset 44: force_pushdown (i32) — always -1 (None)
        segment.set(ValueLayout.JAVA_INT, 44, -1);
        // Offset 48: cost_predicate (i32)
        segment.set(ValueLayout.JAVA_INT, 48, costPredicate);
        // Offset 52: cost_collector (i32)
        segment.set(ValueLayout.JAVA_INT, 52, costCollector);
        // Offset 56: max_collector_parallelism (i32)
        segment.set(ValueLayout.JAVA_INT, 56, maxCollectorParallelism);
        // Offset 60: single_collector_strategy (i32) — always 2 (PageRangeSplit)
        segment.set(ValueLayout.JAVA_INT, 60, 2);
        // Offset 64: tree_collector_strategy (i32) — always 1 (TightenOuterBounds)
        segment.set(ValueLayout.JAVA_INT, 64, 1);
    }
}
