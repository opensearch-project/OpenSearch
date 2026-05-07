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
        WireConfigSnapshot snapshot = new WireConfigSnapshot(8192, 4, true, 1024, 0.03, 1, 10, 4);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(8192L, segment.get(ValueLayout.JAVA_LONG, 0));
            assertEquals(4L, segment.get(ValueLayout.JAVA_LONG, 8));
            assertEquals(1024L, segment.get(ValueLayout.JAVA_LONG, 16));
            assertEquals(0.03, segment.get(ValueLayout.JAVA_DOUBLE, 24), 1e-15);
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 32));
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 48));
            assertEquals(10, segment.get(ValueLayout.JAVA_INT, 52));
            assertEquals(4, segment.get(ValueLayout.JAVA_INT, 56));
        }
    }

    public void testWriteToWritesParquetPushdownFalseAsZero() {
        WireConfigSnapshot snapshot = new WireConfigSnapshot(4096, 2, false, 512, 0.5, 5, 20, 2);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(0, segment.get(ValueLayout.JAVA_INT, 32));
        }
    }

    public void testHardcodedFieldsAreWrittenCorrectly() {
        WireConfigSnapshot snapshot = new WireConfigSnapshot(16384, 8, true, 2048, 0.1, 3, 15, 6);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 36));
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 40));
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 44));
            assertEquals(2, segment.get(ValueLayout.JAVA_INT, 60));
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 64));
        }
    }

    public void testHardcodedFieldsAreIndependentOfDynamicValues() {
        WireConfigSnapshot snapshot = new WireConfigSnapshot(1, 1, false, 1, 0.0, 0, 0, 1);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 36));
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 40));
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 44));
            assertEquals(2, segment.get(ValueLayout.JAVA_INT, 60));
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 64));
        }
    }
}
