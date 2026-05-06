/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Unit tests for {@link WireConfigSnapshot} — verifies the wire struct byte size,
 * correct field placement at known offsets, and hardcoded field values.
 * <p>
 * These are example-based tests using fixed/known values (not randomized).
 * <p>
 * Requirements: 5.3
 */
public class WireConfigSnapshotTests extends OpenSearchTestCase {

    /**
     * Verifies that {@code BYTE_SIZE} equals 68, matching the Rust
     * {@code WireDatafusionQueryConfig} {@code #[repr(C)]} layout.
     */
    public void testByteSizeEquals68() {
        assertEquals("WireConfigSnapshot.BYTE_SIZE must be 68 bytes", 68L, WireConfigSnapshot.BYTE_SIZE);
    }

    /**
     * Verifies that {@code writeTo} writes all dynamic fields at the correct
     * offsets for a known snapshot with specific values.
     */
    public void testWriteToWritesCorrectValuesAtCorrectOffsets() {
        WireConfigSnapshot snapshot = new WireConfigSnapshot(
            8192,   // batchSize
            4,      // targetPartitions
            true,   // parquetPushdownFilters
            1024,   // minSkipRunDefault
            0.03,   // minSkipRunSelectivityThreshold
            1,      // costPredicate
            10,     // costCollector
            4       // maxCollectorParallelism
        );

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            // Offset 0: batch_size (i64)
            assertEquals(8192L, segment.get(ValueLayout.JAVA_LONG, 0));
            // Offset 8: target_partitions (i64)
            assertEquals(4L, segment.get(ValueLayout.JAVA_LONG, 8));
            // Offset 16: min_skip_run_default (i64)
            assertEquals(1024L, segment.get(ValueLayout.JAVA_LONG, 16));
            // Offset 24: min_skip_run_selectivity_threshold (f64)
            assertEquals(0.03, segment.get(ValueLayout.JAVA_DOUBLE, 24), 1e-15);
            // Offset 32: parquet_pushdown_filters (i32) — true maps to 1
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 32));
            // Offset 48: cost_predicate (i32)
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 48));
            // Offset 52: cost_collector (i32)
            assertEquals(10, segment.get(ValueLayout.JAVA_INT, 52));
            // Offset 56: max_collector_parallelism (i32)
            assertEquals(4, segment.get(ValueLayout.JAVA_INT, 56));
        }
    }

    /**
     * Verifies that {@code writeTo} writes {@code parquet_pushdown_filters = 0}
     * when the boolean field is false.
     */
    public void testWriteToWritesParquetPushdownFalseAsZero() {
        WireConfigSnapshot snapshot = new WireConfigSnapshot(
            4096,   // batchSize
            2,      // targetPartitions
            false,  // parquetPushdownFilters
            512,    // minSkipRunDefault
            0.5,    // minSkipRunSelectivityThreshold
            5,      // costPredicate
            20,     // costCollector
            2       // maxCollectorParallelism
        );

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            // Offset 32: parquet_pushdown_filters (i32) — false maps to 0
            assertEquals(0, segment.get(ValueLayout.JAVA_INT, 32));
        }
    }

    /**
     * Verifies that all hardcoded fields are written with their expected constant
     * values regardless of the dynamic field inputs:
     * <ul>
     *   <li>{@code indexed_pushdown_filters} (offset 36) = 1</li>
     *   <li>{@code force_strategy} (offset 40) = -1</li>
     *   <li>{@code force_pushdown} (offset 44) = -1</li>
     *   <li>{@code single_collector_strategy} (offset 60) = 2</li>
     *   <li>{@code tree_collector_strategy} (offset 64) = 1</li>
     * </ul>
     */
    public void testHardcodedFieldsAreWrittenCorrectly() {
        WireConfigSnapshot snapshot = new WireConfigSnapshot(
            16384,  // batchSize
            8,      // targetPartitions
            true,   // parquetPushdownFilters
            2048,   // minSkipRunDefault
            0.1,    // minSkipRunSelectivityThreshold
            3,      // costPredicate
            15,     // costCollector
            6       // maxCollectorParallelism
        );

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            // Offset 36: indexed_pushdown_filters — always 1
            assertEquals("indexed_pushdown_filters must be 1", 1, segment.get(ValueLayout.JAVA_INT, 36));
            // Offset 40: force_strategy — always -1 (None)
            assertEquals("force_strategy must be -1", -1, segment.get(ValueLayout.JAVA_INT, 40));
            // Offset 44: force_pushdown — always -1 (None)
            assertEquals("force_pushdown must be -1", -1, segment.get(ValueLayout.JAVA_INT, 44));
            // Offset 60: single_collector_strategy — always 2 (PageRangeSplit)
            assertEquals("single_collector_strategy must be 2", 2, segment.get(ValueLayout.JAVA_INT, 60));
            // Offset 64: tree_collector_strategy — always 1 (TightenOuterBounds)
            assertEquals("tree_collector_strategy must be 1", 1, segment.get(ValueLayout.JAVA_INT, 64));
        }
    }

    /**
     * Verifies that hardcoded fields remain constant even with different dynamic
     * field values — ensures they are truly independent of constructor arguments.
     */
    public void testHardcodedFieldsAreIndependentOfDynamicValues() {
        WireConfigSnapshot snapshot = new WireConfigSnapshot(
            1,      // batchSize (minimum)
            1,      // targetPartitions (minimum)
            false,  // parquetPushdownFilters
            1,      // minSkipRunDefault (minimum)
            0.0,    // minSkipRunSelectivityThreshold (minimum)
            0,      // costPredicate (minimum)
            0,      // costCollector (minimum)
            1       // maxCollectorParallelism (minimum)
        );

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals("indexed_pushdown_filters must be 1", 1, segment.get(ValueLayout.JAVA_INT, 36));
            assertEquals("force_strategy must be -1", -1, segment.get(ValueLayout.JAVA_INT, 40));
            assertEquals("force_pushdown must be -1", -1, segment.get(ValueLayout.JAVA_INT, 44));
            assertEquals("single_collector_strategy must be 2", 2, segment.get(ValueLayout.JAVA_INT, 60));
            assertEquals("tree_collector_strategy must be 1", 1, segment.get(ValueLayout.JAVA_INT, 64));
        }
    }
}
