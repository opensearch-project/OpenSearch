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
    public static final long BYTE_SIZE = 52;

    private final int batchSize;
    private final int targetPartitions;
    private final boolean listingTablePushdownFilters;
    private final int minSkipRunDefault;
    private final double minSkipRunSelectivityThreshold;
    private final boolean indexedPushdownFilters;
    private final int forceStrategy;

    private WireConfigSnapshot(Builder builder) {
        this.batchSize = builder.batchSize;
        this.targetPartitions = builder.targetPartitions;
        this.listingTablePushdownFilters = builder.listingTablePushdownFilters;
        this.minSkipRunDefault = builder.minSkipRunDefault;
        this.minSkipRunSelectivityThreshold = builder.minSkipRunSelectivityThreshold;
        this.indexedPushdownFilters = builder.indexedPushdownFilters;
        this.forceStrategy = builder.forceStrategy;
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
            .listingTablePushdownFilters(current.listingTablePushdownFilters)
            .minSkipRunDefault(current.minSkipRunDefault)
            .minSkipRunSelectivityThreshold(current.minSkipRunSelectivityThreshold)
            .indexedPushdownFilters(current.indexedPushdownFilters)
            .forceStrategy(current.forceStrategy);
    }

    public int batchSize() {
        return batchSize;
    }

    public int targetPartitions() {
        return targetPartitions;
    }

    public boolean listingTablePushdownFilters() {
        return listingTablePushdownFilters;
    }

    public int minSkipRunDefault() {
        return minSkipRunDefault;
    }

    public double minSkipRunSelectivityThreshold() {
        return minSkipRunSelectivityThreshold;
    }

    public boolean indexedPushdownFilters() {
        return indexedPushdownFilters;
    }

    /** -1 = None (selectivity heuristic), 0 = RowSelection, 1 = BooleanMask. */
    public int forceStrategy() {
        return forceStrategy;
    }

    /**
     * Writes this snapshot into a {@code MemorySegment} matching the
     * {@code WireDatafusionQueryConfig} {@code #[repr(C)]} layout.
     * <p>
     * The segment must be at least {@link #BYTE_SIZE} bytes and allocated from
     * a confined {@code Arena} scoped to the query lifetime. Fields that the Rust
     * side no longer exposes as settings ({@code cost_predicate},
     * {@code cost_collector}) are written as their fixed hardcoded values.
     *
     * <pre>
     * Offset  Size  Field                                Type     Source
     * ──────  ────  ─────────────────────────────────    ──────   ───────────
     * 0       8     batch_size                           i64      from snapshot
     * 8       8     target_partitions                    i64      from snapshot
     * 16      8     min_skip_run_default                 i64      from snapshot
     * 24      8     min_skip_run_selectivity_threshold   f64      from snapshot
     * 32      4     listing_table_pushdown_filters       i32      from snapshot (0/1)
     * 36      4     indexed_pushdown_filters             i32      from snapshot (0/1)
     * 40      4     force_strategy                       i32      from snapshot (-1/0/1)
     * 44      4     cost_predicate                       i32      hardcoded 1
     * 48      4     cost_collector                       i32      hardcoded 10
     * ──────  ────
     * Total: 52 bytes
     * </pre>
     *
     * @param segment the target memory segment (at least {@link #BYTE_SIZE} bytes)
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
        // Offset 32: listing_table_pushdown_filters (i32) — 0 = false, 1 = true
        segment.set(ValueLayout.JAVA_INT, 32, listingTablePushdownFilters ? 1 : 0);
        // Offset 36: indexed_pushdown_filters (i32) — 0 = false, 1 = true
        segment.set(ValueLayout.JAVA_INT, 36, indexedPushdownFilters ? 1 : 0);
        // Offset 40: force_strategy (i32) — -1 = None, 0 = RowSelection, 1 = BooleanMask
        segment.set(ValueLayout.JAVA_INT, 40, forceStrategy);
        // Offset 44: cost_predicate (i32) — hardcoded 1
        segment.set(ValueLayout.JAVA_INT, 44, 1);
        // Offset 48: cost_collector (i32) — hardcoded 10
        segment.set(ValueLayout.JAVA_INT, 48, 10);
    }

    /**
     * Builder for {@link WireConfigSnapshot}. All fields have sensible defaults
     * matching the Rust {@code DatafusionQueryConfig::default()}.
     */
    public static final class Builder {
        private int batchSize = 8192;
        private int targetPartitions = 4;
        private boolean listingTablePushdownFilters = false;
        private int minSkipRunDefault = 1024;
        private double minSkipRunSelectivityThreshold = 0.03;
        private boolean indexedPushdownFilters = true;
        private int forceStrategy = -1;

        private Builder() {}

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder targetPartitions(int targetPartitions) {
            this.targetPartitions = targetPartitions;
            return this;
        }

        public Builder listingTablePushdownFilters(boolean listingTablePushdownFilters) {
            this.listingTablePushdownFilters = listingTablePushdownFilters;
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

        public Builder indexedPushdownFilters(boolean indexedPushdownFilters) {
            this.indexedPushdownFilters = indexedPushdownFilters;
            return this;
        }

        /** @param forceStrategy -1 = None (heuristic), 0 = RowSelection, 1 = BooleanMask. */
        public Builder forceStrategy(int forceStrategy) {
            this.forceStrategy = forceStrategy;
            return this;
        }

        public WireConfigSnapshot build() {
            return new WireConfigSnapshot(this);
        }
    }
}
