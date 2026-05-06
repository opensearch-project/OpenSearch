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
 * Property-based test for {@link WireConfigSnapshot} wire format round-trip.
 * <p>
 * Feature: dynamic-indexed-query-settings, Property 2: Wire format round-trip
 * <p>
 * Validates: Requirements 3.4, 5.3
 * <p>
 * For any valid {@code WireConfigSnapshot} (with {@code batchSize >= 1},
 * {@code 0.0 <= selectivityThreshold <= 1.0}, {@code maxCollectorParallelism >= 1},
 * and all other fields within their defined bounds), writing the snapshot to a
 * {@code MemorySegment} via {@code writeTo()} and then reading back field-by-field
 * at known offsets SHALL produce values matching the original snapshot exactly
 * (within floating-point epsilon for doubles).
 */
public class WireConfigSnapshotPropertyTests extends OpenSearchTestCase {

    private static final double DOUBLE_EPSILON = 1e-15;
    private static final int ITERATIONS = 200;

    /**
     * **Validates: Requirements 3.4, 5.3**
     * <p>
     * Property 2: Wire format round-trip — for any valid WireConfigSnapshot,
     * writing to a MemorySegment and reading back at known offsets produces
     * matching values for all dynamic fields.
     */
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

                // Read back dynamic fields at known offsets and assert round-trip
                long readBatchSize = segment.get(ValueLayout.JAVA_LONG, 0);
                assertEquals("batch_size at offset 0", (long) batchSize, readBatchSize);

                long readTargetPartitions = segment.get(ValueLayout.JAVA_LONG, 8);
                assertEquals("target_partitions at offset 8", (long) targetPartitions, readTargetPartitions);

                long readMinSkipRunDefault = segment.get(ValueLayout.JAVA_LONG, 16);
                assertEquals("min_skip_run_default at offset 16", (long) minSkipRunDefault, readMinSkipRunDefault);

                double readSelectivityThreshold = segment.get(ValueLayout.JAVA_DOUBLE, 24);
                assertEquals(
                    "min_skip_run_selectivity_threshold at offset 24",
                    minSkipRunSelectivityThreshold,
                    readSelectivityThreshold,
                    DOUBLE_EPSILON
                );

                int readParquetPushdown = segment.get(ValueLayout.JAVA_INT, 32);
                assertEquals("parquet_pushdown_filters at offset 32", parquetPushdownFilters ? 1 : 0, readParquetPushdown);

                int readCostPredicate = segment.get(ValueLayout.JAVA_INT, 48);
                assertEquals("cost_predicate at offset 48", costPredicate, readCostPredicate);

                int readCostCollector = segment.get(ValueLayout.JAVA_INT, 52);
                assertEquals("cost_collector at offset 52", costCollector, readCostCollector);

                int readMaxCollectorParallelism = segment.get(ValueLayout.JAVA_INT, 56);
                assertEquals("max_collector_parallelism at offset 56", maxCollectorParallelism, readMaxCollectorParallelism);

                // Verify hardcoded fields are written correctly
                int readIndexedPushdown = segment.get(ValueLayout.JAVA_INT, 36);
                assertEquals("indexed_pushdown_filters at offset 36 must be 1", 1, readIndexedPushdown);

                int readForceStrategy = segment.get(ValueLayout.JAVA_INT, 40);
                assertEquals("force_strategy at offset 40 must be -1", -1, readForceStrategy);

                int readForcePushdown = segment.get(ValueLayout.JAVA_INT, 44);
                assertEquals("force_pushdown at offset 44 must be -1", -1, readForcePushdown);

                int readSingleCollectorStrategy = segment.get(ValueLayout.JAVA_INT, 60);
                assertEquals("single_collector_strategy at offset 60 must be 2", 2, readSingleCollectorStrategy);

                int readTreeCollectorStrategy = segment.get(ValueLayout.JAVA_INT, 64);
                assertEquals("tree_collector_strategy at offset 64 must be 1", 1, readTreeCollectorStrategy);
            }
        }
    }

    /**
     * **Validates: Requirements 3.4, 5.3**
     * <p>
     * Property 2 boundary case: verify round-trip at minimum valid bounds.
     */
    public void testWireFormatRoundTripAtMinimumBounds() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Use minimum valid values for bounded fields
            int batchSize = 1;
            int targetPartitions = 1;
            boolean parquetPushdownFilters = randomBoolean();
            int minSkipRunDefault = 1;
            double minSkipRunSelectivityThreshold = 0.0;
            int costPredicate = 0;
            int costCollector = 0;
            int maxCollectorParallelism = 1;

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

                assertEquals("batch_size min", 1L, segment.get(ValueLayout.JAVA_LONG, 0));
                assertEquals("target_partitions min", 1L, segment.get(ValueLayout.JAVA_LONG, 8));
                assertEquals("min_skip_run_default min", 1L, segment.get(ValueLayout.JAVA_LONG, 16));
                assertEquals("selectivity_threshold min", 0.0, segment.get(ValueLayout.JAVA_DOUBLE, 24), DOUBLE_EPSILON);
                assertEquals("cost_predicate min", 0, segment.get(ValueLayout.JAVA_INT, 48));
                assertEquals("cost_collector min", 0, segment.get(ValueLayout.JAVA_INT, 52));
                assertEquals("max_collector_parallelism min", 1, segment.get(ValueLayout.JAVA_INT, 56));
            }
        }
    }

    /**
     * **Validates: Requirements 3.4, 5.3**
     * <p>
     * Property 2 boundary case: verify round-trip at maximum valid bounds.
     */
    public void testWireFormatRoundTripAtMaximumBounds() {
        for (int i = 0; i < ITERATIONS; i++) {
            int batchSize = randomIntBetween(500_000, Integer.MAX_VALUE);
            int targetPartitions = randomIntBetween(64, 256);
            boolean parquetPushdownFilters = true;
            int minSkipRunDefault = randomIntBetween(50_000, Integer.MAX_VALUE);
            double minSkipRunSelectivityThreshold = 1.0;
            int costPredicate = randomIntBetween(500, Integer.MAX_VALUE);
            int costCollector = randomIntBetween(500, Integer.MAX_VALUE);
            int maxCollectorParallelism = randomIntBetween(32, 128);

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

                assertEquals("batch_size max range", (long) batchSize, segment.get(ValueLayout.JAVA_LONG, 0));
                assertEquals("target_partitions max range", (long) targetPartitions, segment.get(ValueLayout.JAVA_LONG, 8));
                assertEquals("min_skip_run_default max range", (long) minSkipRunDefault, segment.get(ValueLayout.JAVA_LONG, 16));
                assertEquals("selectivity_threshold max", 1.0, segment.get(ValueLayout.JAVA_DOUBLE, 24), DOUBLE_EPSILON);
                assertEquals("parquet_pushdown true", 1, segment.get(ValueLayout.JAVA_INT, 32));
                assertEquals("cost_predicate max range", costPredicate, segment.get(ValueLayout.JAVA_INT, 48));
                assertEquals("cost_collector max range", costCollector, segment.get(ValueLayout.JAVA_INT, 52));
                assertEquals("max_collector_parallelism max range", maxCollectorParallelism, segment.get(ValueLayout.JAVA_INT, 56));
            }
        }
    }
}
