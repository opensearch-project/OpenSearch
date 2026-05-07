/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.HashSet;
import java.util.Set;

public class ShardScanInstructionHandlerTests extends OpenSearchTestCase {

    public void testSnapshotIsReadableFromSettings() {
        Settings settings = Settings.builder()
            .put("datafusion.indexed.batch_size", 4096)
            .put("datafusion.indexed.parquet_pushdown_filters", true)
            .put("datafusion.indexed.min_skip_run_default", 512)
            .put("datafusion.indexed.min_skip_run_selectivity_threshold", 0.05)
            .put("datafusion.indexed.cost_predicate", 3)
            .put("datafusion.indexed.cost_collector", 20)
            .put("datafusion.indexed.max_collector_parallelism", 2)
            .build();

        DatafusionSettings datafusionSettings = new DatafusionSettings(settings);
        WireConfigSnapshot snapshot = datafusionSettings.getSnapshot();

        assertNotNull(snapshot);
        assertEquals(4096, snapshot.batchSize());
        assertEquals(true, snapshot.parquetPushdownFilters());
        assertEquals(512, snapshot.minSkipRunDefault());
        assertEquals(0.05, snapshot.minSkipRunSelectivityThreshold(), 1e-15);
        assertEquals(3, snapshot.costPredicate());
        assertEquals(20, snapshot.costCollector());
        assertEquals(2, snapshot.maxCollectorParallelism());
    }

    public void testSnapshotWriteToProducesValidSegment() {
        WireConfigSnapshot snapshot = new WireConfigSnapshot(8192, 4, false, 1024, 0.03, 1, 10, 4);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertTrue(segment.address() != 0);
            assertEquals(WireConfigSnapshot.BYTE_SIZE, segment.byteSize());
        }
    }

    public void testVolatileSnapshotReflectsLatestUpdate() {
        Settings initialSettings = Settings.builder()
            .put("datafusion.indexed.batch_size", 8192)
            .put("datafusion.indexed.cost_collector", 10)
            .build();

        DatafusionSettings datafusionSettings = new DatafusionSettings(initialSettings);

        Set<Setting<?>> registeredSettings = new HashSet<>(DatafusionSettings.ALL_SETTINGS);
        registeredSettings.add(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING);
        registeredSettings.add(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE);
        ClusterSettings clusterSettings = new ClusterSettings(initialSettings, registeredSettings);
        datafusionSettings.registerListeners(clusterSettings);

        WireConfigSnapshot before = datafusionSettings.getSnapshot();
        assertEquals(8192, before.batchSize());
        assertEquals(10, before.costCollector());

        clusterSettings.applySettings(
            Settings.builder().put("datafusion.indexed.batch_size", 16384).put("datafusion.indexed.cost_collector", 10).build()
        );

        WireConfigSnapshot after = datafusionSettings.getSnapshot();
        assertEquals(16384, after.batchSize());
        assertEquals(10, after.costCollector());

        clusterSettings.applySettings(
            Settings.builder().put("datafusion.indexed.batch_size", 16384).put("datafusion.indexed.cost_collector", 50).build()
        );

        WireConfigSnapshot afterSecond = datafusionSettings.getSnapshot();
        assertEquals(50, afterSecond.costCollector());
        assertEquals(16384, afterSecond.batchSize());
    }

    public void testSegmentSizeMatchesByteSize() {
        WireConfigSnapshot snapshot = new WireConfigSnapshot(
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            true,
            Integer.MAX_VALUE,
            1.0,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE
        );

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals((long) Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_LONG, 0));
            assertEquals((long) Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_LONG, 8));
            assertEquals((long) Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_LONG, 16));
            assertEquals(1.0, segment.get(ValueLayout.JAVA_DOUBLE, 24), 1e-15);
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 32));
            assertEquals(Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_INT, 48));
            assertEquals(Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_INT, 52));
            assertEquals(Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_INT, 56));

            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 36));
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 40));
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 44));
            assertEquals(2, segment.get(ValueLayout.JAVA_INT, 60));
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 64));

            assertEquals(68L, WireConfigSnapshot.BYTE_SIZE);
        }
    }
}
