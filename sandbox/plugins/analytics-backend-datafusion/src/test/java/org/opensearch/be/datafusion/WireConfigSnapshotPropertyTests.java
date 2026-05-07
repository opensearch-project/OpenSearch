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

public class WireConfigSnapshotPropertyTests extends OpenSearchTestCase {

    private static final double DOUBLE_EPSILON = 1e-15;
    private static final int ITERATIONS = 200;

    public void testWireFormatRoundTripProperty() {
        for (int i = 0; i < ITERATIONS; i++) {
            int batchSize = randomIntBetween(1, 1_000_000);
            int targetPartitions = randomIntBetween(1, 128);
            boolean parquetPushdownFilters = randomBoolean();
            int minSkipRunDefault = randomIntBetween(1, 100_000);
            double minSkipRunSelectivityThreshold = randomDoubleBetween(0.0, 1.0, true);
            int costPredicate = randomIntBetween(0, 1000);
            int costCollector = randomIntBetween(0, 1000);
            int maxCollectorParallelism = randomIntBetween(1, 64);

            WireConfigSnapshot snapshot = new WireConfigSnapshot(
                batchSize,
                targetPartitions,
                parquetPushdownFilters,
                minSkipRunDefault,
                minSkipRunSelectivityThreshold,
                costPredicate,
                costCollector,
                maxCollectorParallelism
            );

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
                snapshot.writeTo(segment);

                assertEquals((long) batchSize, segment.get(ValueLayout.JAVA_LONG, 0));
                assertEquals((long) targetPartitions, segment.get(ValueLayout.JAVA_LONG, 8));
                assertEquals((long) minSkipRunDefault, segment.get(ValueLayout.JAVA_LONG, 16));
                assertEquals(minSkipRunSelectivityThreshold, segment.get(ValueLayout.JAVA_DOUBLE, 24), DOUBLE_EPSILON);
                assertEquals(parquetPushdownFilters ? 1 : 0, segment.get(ValueLayout.JAVA_INT, 32));
                assertEquals(costPredicate, segment.get(ValueLayout.JAVA_INT, 48));
                assertEquals(costCollector, segment.get(ValueLayout.JAVA_INT, 52));
                assertEquals(maxCollectorParallelism, segment.get(ValueLayout.JAVA_INT, 56));

                assertEquals(1, segment.get(ValueLayout.JAVA_INT, 36));
                assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 40));
                assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 44));
                assertEquals(2, segment.get(ValueLayout.JAVA_INT, 60));
                assertEquals(1, segment.get(ValueLayout.JAVA_INT, 64));
            }
        }
    }

    public void testWireFormatRoundTripAtMinimumBounds() {
        for (int i = 0; i < ITERATIONS; i++) {
            boolean parquetPushdownFilters = randomBoolean();

            WireConfigSnapshot snapshot = new WireConfigSnapshot(1, 1, parquetPushdownFilters, 1, 0.0, 0, 0, 1);

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
                snapshot.writeTo(segment);

                assertEquals(1L, segment.get(ValueLayout.JAVA_LONG, 0));
                assertEquals(1L, segment.get(ValueLayout.JAVA_LONG, 8));
                assertEquals(1L, segment.get(ValueLayout.JAVA_LONG, 16));
                assertEquals(0.0, segment.get(ValueLayout.JAVA_DOUBLE, 24), DOUBLE_EPSILON);
                assertEquals(0, segment.get(ValueLayout.JAVA_INT, 48));
                assertEquals(0, segment.get(ValueLayout.JAVA_INT, 52));
                assertEquals(1, segment.get(ValueLayout.JAVA_INT, 56));
            }
        }
    }

    public void testWireFormatRoundTripAtMaximumBounds() {
        for (int i = 0; i < ITERATIONS; i++) {
            int batchSize = randomIntBetween(500_000, Integer.MAX_VALUE);
            int targetPartitions = randomIntBetween(64, 256);
            int minSkipRunDefault = randomIntBetween(50_000, Integer.MAX_VALUE);
            int costPredicate = randomIntBetween(500, Integer.MAX_VALUE);
            int costCollector = randomIntBetween(500, Integer.MAX_VALUE);
            int maxCollectorParallelism = randomIntBetween(32, 128);

            WireConfigSnapshot snapshot = new WireConfigSnapshot(
                batchSize,
                targetPartitions,
                true,
                minSkipRunDefault,
                1.0,
                costPredicate,
                costCollector,
                maxCollectorParallelism
            );

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
                snapshot.writeTo(segment);

                assertEquals((long) batchSize, segment.get(ValueLayout.JAVA_LONG, 0));
                assertEquals((long) targetPartitions, segment.get(ValueLayout.JAVA_LONG, 8));
                assertEquals((long) minSkipRunDefault, segment.get(ValueLayout.JAVA_LONG, 16));
                assertEquals(1.0, segment.get(ValueLayout.JAVA_DOUBLE, 24), DOUBLE_EPSILON);
                assertEquals(1, segment.get(ValueLayout.JAVA_INT, 32));
                assertEquals(costPredicate, segment.get(ValueLayout.JAVA_INT, 48));
                assertEquals(costCollector, segment.get(ValueLayout.JAVA_INT, 52));
                assertEquals(maxCollectorParallelism, segment.get(ValueLayout.JAVA_INT, 56));
            }
        }
    }
}
