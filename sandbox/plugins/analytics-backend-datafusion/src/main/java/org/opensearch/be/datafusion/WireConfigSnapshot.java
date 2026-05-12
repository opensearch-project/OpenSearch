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
 * Use {@link #builder()} to construct instances.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class WireConfigSnapshot {

    /** Total byte size of the wire struct ({@code WireDatafusionQueryConfig}). */
    public static final long BYTE_SIZE = 72;

    private final int batchSize;
    private final int targetPartitions;
    private final boolean parquetPushdownFilters;
    private final int minSkipRunDefault;
    private final double minSkipRunSelectivityThreshold;
    private final int maxCollectorParallelism;
    private final int singleCollectorStrategy;
    private final int treeCollectorStrategy;
    private final int rowIdStrategy;

    private WireConfigSnapshot(Builder builder) {
        this.batchSize = builder.batchSize;
        this.targetPartitions = builder.targetPartitions;
        this.parquetPushdownFilters = builder.parquetPushdownFilters;
        this.minSkipRunDefault = builder.minSkipRunDefault;
        this.minSkipRunSelectivityThreshold = builder.minSkipRunSelectivityThreshold;
        this.maxCollectorParallelism = builder.maxCollectorParallelism;
        this.singleCollectorStrategy = builder.singleCollectorStrategy;
        this.treeCollectorStrategy = builder.treeCollectorStrategy;
        this.rowIdStrategy = builder.rowIdStrategy;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder pre-populated with all values from an existing snapshot.
     * Useful for rebuilding a snapshot with a single field changed.
     */
    public static Builder builder(WireConfigSnapshot current) {
        return new Builder().batchSize(current.batchSize)
            .targetPartitions(current.targetPartitions)
            .parquetPushdownFilters(current.parquetPushdownFilters)
            .minSkipRunDefault(current.minSkipRunDefault)
            .minSkipRunSelectivityThreshold(current.minSkipRunSelectivityThreshold)
            .maxCollectorParallelism(current.maxCollectorParallelism)
            .singleCollectorStrategy(current.singleCollectorStrategy)
            .treeCollectorStrategy(current.treeCollectorStrategy)
            .rowIdStrategy(current.rowIdStrategy);
    }

    public int batchSize() {
        return batchSize;
    }

    public int targetPartitions() {
        return targetPartitions;
    }

    public boolean parquetPushdownFilters() {
        return parquetPushdownFilters;
    }

    public int minSkipRunDefault() {
        return minSkipRunDefault;
    }

    public double minSkipRunSelectivityThreshold() {
        return minSkipRunSelectivityThreshold;
    }

    public int maxCollectorParallelism() {
        return maxCollectorParallelism;
    }

    public int singleCollectorStrategy() {
        return singleCollectorStrategy;
    }

    public int treeCollectorStrategy() {
        return treeCollectorStrategy;
    }

    public int rowIdStrategy() {
        return rowIdStrategy;
    }

    /**
     * Writes this snapshot into a {@code MemorySegment} matching the
     * {@code WireDatafusionQueryConfig} {@code #[repr(C)]} layout.
     * <p>
     * The segment must be at least {@link #BYTE_SIZE} bytes and allocated from
     * a confined {@code Arena} scoped to the query lifetime.
     *
     * <pre>
     * Offset  Size  Field                                Type     Source
     * ──────  ────  ─────────────────────────────────    ──────   ───────────
     * 0       8     batch_size                           i64      from snapshot
     * 8       8     target_partitions                    i64      from snapshot
     * 16      8     min_skip_run_default                 i64      from snapshot
     * 24      8     min_skip_run_selectivity_threshold   f64      from snapshot
     * 32      4     parquet_pushdown_filters             i32      from snapshot (0/1)
     * 36      4     indexed_pushdown_filters             i32      hardcoded 1
     * 40      4     force_strategy                       i32      hardcoded -1
     * 44      4     force_pushdown                       i32      hardcoded -1
     * 48      4     cost_predicate                       i32      hardcoded 1
     * 52      4     cost_collector                       i32      hardcoded 10
     * 56      4     max_collector_parallelism            i32      from snapshot
     * 60      4     single_collector_strategy            i32      from snapshot
     * 64      4     tree_collector_strategy              i32      from snapshot
     * 68      4     row_id_strategy                      i32      from snapshot (0/1/2)
     * ──────  ────
     * Total: 72 bytes
     * </pre>
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
        // Offset 48: cost_predicate (i32) — hardcoded 1
        segment.set(ValueLayout.JAVA_INT, 48, 1);
        // Offset 52: cost_collector (i32) — hardcoded 10
        segment.set(ValueLayout.JAVA_INT, 52, 10);
        // Offset 56: max_collector_parallelism (i32)
        segment.set(ValueLayout.JAVA_INT, 56, maxCollectorParallelism);
        // Offset 60: single_collector_strategy (i32)
        segment.set(ValueLayout.JAVA_INT, 60, singleCollectorStrategy);
        // Offset 64: tree_collector_strategy (i32)
        segment.set(ValueLayout.JAVA_INT, 64, treeCollectorStrategy);
        // Offset 68: row_id_strategy (i32) — 0 = None, 1 = ListingTable, 2 = IndexedPredicateOnly
        segment.set(ValueLayout.JAVA_INT, 68, rowIdStrategy);
    }

    /**
     * Builder for {@link WireConfigSnapshot}. All fields have sensible defaults
     * matching the Rust {@code DatafusionQueryConfig::default()}.
     */
    public static final class Builder {
        private int batchSize = 8192;
        private int targetPartitions = 4;
        private boolean parquetPushdownFilters = false;
        private int minSkipRunDefault = 1024;
        private double minSkipRunSelectivityThreshold = 0.03;
        private int maxCollectorParallelism = 1;
        private int singleCollectorStrategy = 2; // PageRangeSplit
        private int treeCollectorStrategy = 1;   // TightenOuterBounds
        private int rowIdStrategy = 1;           // ListingTable (ShardTableProvider + ProjectRowIdOptimizer)

        private Builder() {}

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder targetPartitions(int targetPartitions) {
            this.targetPartitions = targetPartitions;
            return this;
        }

        public Builder parquetPushdownFilters(boolean parquetPushdownFilters) {
            this.parquetPushdownFilters = parquetPushdownFilters;
            return this;
        }

        public Builder minSkipRunDefault(int minSkipRunDefault) {
            this.minSkipRunDefault = minSkipRunDefault;
            return this;
        }

        public Builder minSkipRunSelectivityThreshold(double minSkipRunSelectivityThreshold) {
            this.minSkipRunSelectivityThreshold = minSkipRunSelectivityThreshold;
            return this;
        }

        public Builder maxCollectorParallelism(int maxCollectorParallelism) {
            this.maxCollectorParallelism = maxCollectorParallelism;
            return this;
        }

        public Builder singleCollectorStrategy(int singleCollectorStrategy) {
            this.singleCollectorStrategy = singleCollectorStrategy;
            return this;
        }

        public Builder treeCollectorStrategy(int treeCollectorStrategy) {
            this.treeCollectorStrategy = treeCollectorStrategy;
            return this;
        }

        public Builder rowIdStrategy(int rowIdStrategy) {
            this.rowIdStrategy = rowIdStrategy;
            return this;
        }

        public WireConfigSnapshot build() {
            return new WireConfigSnapshot(this);
        }
    }
}
