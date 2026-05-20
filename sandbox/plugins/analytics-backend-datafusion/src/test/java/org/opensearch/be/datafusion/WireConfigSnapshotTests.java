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

public class WireConfigSnapshotTests extends OpenSearchTestCase {

    public void testByteSizeEquals68() {
        assertEquals(68L, WireConfigSnapshot.BYTE_SIZE);
    }

    public void testWriteToWritesCorrectValuesAtCorrectOffsets() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder()
            .batchSize(8192)
            .targetPartitions(4)
            .parquetPushdownFilters(true)
            .minSkipRunDefault(1024)
            .minSkipRunSelectivityThreshold(0.03)
            .maxCollectorParallelism(4)
            .singleCollectorStrategy(2)
            .treeCollectorStrategy(1)
            .build();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(8192L, segment.get(ValueLayout.JAVA_LONG, 0));
            assertEquals(4L, segment.get(ValueLayout.JAVA_LONG, 8));
            assertEquals(1024L, segment.get(ValueLayout.JAVA_LONG, 16));
            assertEquals(0.03, segment.get(ValueLayout.JAVA_DOUBLE, 24), 1e-15);
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 32)); // parquet_pushdown = true
            assertEquals(4, segment.get(ValueLayout.JAVA_INT, 56)); // max_collector_parallelism
            assertEquals(2, segment.get(ValueLayout.JAVA_INT, 60)); // single_collector_strategy
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 64)); // tree_collector_strategy
        }
    }

    public void testWriteToWritesParquetPushdownFalseAsZero() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder().parquetPushdownFilters(false).build();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(0, segment.get(ValueLayout.JAVA_INT, 32));
        }
    }

    public void testHardcodedFieldsAreWrittenCorrectly() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder().batchSize(16384).targetPartitions(8).maxCollectorParallelism(6).build();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 36));  // indexed_pushdown_filters
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 40)); // force_strategy
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 44)); // force_pushdown
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 48));  // cost_predicate (hardcoded)
            assertEquals(10, segment.get(ValueLayout.JAVA_INT, 52)); // cost_collector (hardcoded)
        }
    }

    public void testBuilderDefaultsMatchExpected() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder().build();

        assertEquals(8192, snapshot.batchSize());
        assertEquals(4, snapshot.targetPartitions());
        assertEquals(false, snapshot.parquetPushdownFilters());
        assertEquals(1024, snapshot.minSkipRunDefault());
        assertEquals(0.03, snapshot.minSkipRunSelectivityThreshold(), 1e-15);
        assertEquals(1, snapshot.maxCollectorParallelism());
        assertEquals(2, snapshot.singleCollectorStrategy());  // page_range_split
        assertEquals(1, snapshot.treeCollectorStrategy());    // tighten_outer_bounds
    }

    public void testBuilderCopyPreservesAllFields() {
        WireConfigSnapshot original = WireConfigSnapshot.builder()
            .batchSize(4096)
            .targetPartitions(16)
            .parquetPushdownFilters(true)
            .minSkipRunDefault(512)
            .minSkipRunSelectivityThreshold(0.5)
            .maxCollectorParallelism(8)
            .singleCollectorStrategy(0)
            .treeCollectorStrategy(2)
            .build();

        WireConfigSnapshot copy = WireConfigSnapshot.builder(original).build();

        assertEquals(original.batchSize(), copy.batchSize());
        assertEquals(original.targetPartitions(), copy.targetPartitions());
        assertEquals(original.parquetPushdownFilters(), copy.parquetPushdownFilters());
        assertEquals(original.minSkipRunDefault(), copy.minSkipRunDefault());
        assertEquals(original.minSkipRunSelectivityThreshold(), copy.minSkipRunSelectivityThreshold(), 0.0);
        assertEquals(original.maxCollectorParallelism(), copy.maxCollectorParallelism());
        assertEquals(original.singleCollectorStrategy(), copy.singleCollectorStrategy());
        assertEquals(original.treeCollectorStrategy(), copy.treeCollectorStrategy());
    }
}
