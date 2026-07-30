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

    public void testByteSize() {
        assertEquals(52L, WireConfigSnapshot.BYTE_SIZE);
    }

    public void testWriteToWritesCorrectValuesAtCorrectOffsets() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder()
            .batchSize(8192)
            .targetPartitions(4)
            .listingTablePushdownFilters(true)
            .minSkipRunDefault(1024)
            .minSkipRunSelectivityThreshold(0.03)
            .build();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(8192L, segment.get(ValueLayout.JAVA_LONG, 0));
            assertEquals(4L, segment.get(ValueLayout.JAVA_LONG, 8));
            assertEquals(1024L, segment.get(ValueLayout.JAVA_LONG, 16));
            assertEquals(0.03, segment.get(ValueLayout.JAVA_DOUBLE, 24), 1e-15);
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 32)); // listing_table_pushdown = true
        }
    }

    public void testWriteToWritesListingTablePushdownFalseAsZero() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder().listingTablePushdownFilters(false).build();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(0, segment.get(ValueLayout.JAVA_INT, 32));
        }
    }

    public void testHardcodedFieldsAreWrittenCorrectly() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder().batchSize(16384).targetPartitions(8).build();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 36));  // indexed_pushdown_filters default (true)
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 40)); // force_strategy default (None)
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 44));  // cost_predicate (hardcoded)
            assertEquals(10, segment.get(ValueLayout.JAVA_INT, 48)); // cost_collector (hardcoded)
        }
    }

    public void testIndexedPushdownFiltersIsWrittenFromSnapshot() {
        for (boolean v : new boolean[] { true, false }) {
            WireConfigSnapshot snapshot = WireConfigSnapshot.builder().indexedPushdownFilters(v).build();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
                snapshot.writeTo(segment);
                assertEquals(v ? 1 : 0, segment.get(ValueLayout.JAVA_INT, 36));
            }
            assertEquals(v, snapshot.indexedPushdownFilters());
        }
    }

    public void testForceStrategyIsWrittenFromSnapshot() {
        // 0 = RowSelection, 1 = BooleanMask, -1 = None
        for (int wire : new int[] { -1, 0, 1 }) {
            WireConfigSnapshot snapshot = WireConfigSnapshot.builder().forceStrategy(wire).build();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
                snapshot.writeTo(segment);
                assertEquals(wire, segment.get(ValueLayout.JAVA_INT, 40));
            }
            assertEquals(wire, snapshot.forceStrategy());
        }
    }

    public void testBuilderDefaultsMatchExpected() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder().build();

        assertEquals(8192, snapshot.batchSize());
        assertEquals(4, snapshot.targetPartitions());
        assertEquals(false, snapshot.listingTablePushdownFilters());
        assertEquals(1024, snapshot.minSkipRunDefault());
        assertEquals(0.03, snapshot.minSkipRunSelectivityThreshold(), 1e-15);
        assertEquals(true, snapshot.indexedPushdownFilters());
    }

    public void testBuilderCopyPreservesAllFields() {
        WireConfigSnapshot original = WireConfigSnapshot.builder()
            .batchSize(4096)
            .targetPartitions(16)
            .listingTablePushdownFilters(true)
            .minSkipRunDefault(512)
            .minSkipRunSelectivityThreshold(0.5)
            .indexedPushdownFilters(false)
            .forceStrategy(1)
            .build();

        WireConfigSnapshot copy = WireConfigSnapshot.builder(original).build();

        assertEquals(original.batchSize(), copy.batchSize());
        assertEquals(original.targetPartitions(), copy.targetPartitions());
        assertEquals(original.listingTablePushdownFilters(), copy.listingTablePushdownFilters());
        assertEquals(original.minSkipRunDefault(), copy.minSkipRunDefault());
        assertEquals(original.minSkipRunSelectivityThreshold(), copy.minSkipRunSelectivityThreshold(), 0.0);
        assertEquals(original.indexedPushdownFilters(), copy.indexedPushdownFilters());
        assertEquals(original.forceStrategy(), copy.forceStrategy());
    }
}
