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

/**
 * Unit tests for the config-passing contract used by {@link ShardScanInstructionHandler}.
 * <p>
 * Since the instruction handler is tightly coupled to native code (it calls
 * {@code NativeBridge.createSessionContext}), these tests verify the <em>data flow</em>
 * that the handler relies on:
 * <ol>
 *   <li>{@code DatafusionSettings.getSnapshot()} returns a non-null snapshot with expected values</li>
 *   <li>{@code WireConfigSnapshot.writeTo()} produces a segment with a non-zero address</li>
 *   <li>The volatile snapshot reflects the latest setting update (proving volatile read works)</li>
 *   <li>{@code WireConfigSnapshot.BYTE_SIZE} (68) is sufficient to hold all fields</li>
 * </ol>
 * <p>
 * Requirements: 3.3, 3.4
 */
public class ShardScanInstructionHandlerTests extends OpenSearchTestCase {

    /**
     * Verifies that {@code DatafusionSettings.getSnapshot()} returns a non-null snapshot
     * with expected values when constructed with custom settings. This mirrors what the
     * instruction handler does: it reads the snapshot from the settings holder.
     * <p>
     * Validates: Requirement 3.3 — the handler reads the pre-computed snapshot, not ClusterService.
     */
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

        assertNotNull("getSnapshot() must return a non-null snapshot", snapshot);
        assertEquals("batchSize must reflect custom setting", 4096, snapshot.batchSize());
        assertEquals("parquetPushdownFilters must reflect custom setting", true, snapshot.parquetPushdownFilters());
        assertEquals("minSkipRunDefault must reflect custom setting", 512, snapshot.minSkipRunDefault());
        assertEquals("minSkipRunSelectivityThreshold must reflect custom setting", 0.05, snapshot.minSkipRunSelectivityThreshold(), 1e-15);
        assertEquals("costPredicate must reflect custom setting", 3, snapshot.costPredicate());
        assertEquals("costCollector must reflect custom setting", 20, snapshot.costCollector());
        assertEquals("maxCollectorParallelism must reflect custom setting", 2, snapshot.maxCollectorParallelism());
    }

    /**
     * Verifies that writing a snapshot to a {@code MemorySegment} produces a segment
     * with a non-zero address and correct size. This mirrors the instruction handler's
     * allocation pattern: allocate a confined-arena segment, write the snapshot, then
     * pass the address to the native bridge.
     * <p>
     * Validates: Requirement 3.4 — the wire config is written to a valid memory segment
     * whose address can be passed to native code.
     */
    public void testSnapshotWriteToProducesValidSegment() {
        WireConfigSnapshot snapshot = new WireConfigSnapshot(
            8192,   // batchSize
            4,      // targetPartitions
            false,  // parquetPushdownFilters
            1024,   // minSkipRunDefault
            0.03,   // minSkipRunSelectivityThreshold
            1,      // costPredicate
            10,     // costCollector
            4       // maxCollectorParallelism
        );

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            // The segment address must be non-zero — this is what gets passed to NativeBridge
            assertTrue("MemorySegment address must be non-zero for native bridge", segment.address() != 0);
            // The segment byte size must match BYTE_SIZE
            assertEquals("Allocated segment must have exactly BYTE_SIZE bytes", WireConfigSnapshot.BYTE_SIZE, segment.byteSize());
        }
    }

    /**
     * Verifies that the volatile snapshot reflects the latest setting update after
     * a listener fires. This proves the instruction handler's volatile read pattern
     * works: it always sees the most recent configuration without a ClusterService lookup.
     * <p>
     * Validates: Requirement 3.3 — the handler reads the volatile snapshot which reflects
     * the latest operator-configured values.
     */
    public void testVolatileSnapshotReflectsLatestUpdate() {
        Settings initialSettings = Settings.builder()
            .put("datafusion.indexed.batch_size", 8192)
            .put("datafusion.indexed.cost_collector", 10)
            .build();

        DatafusionSettings datafusionSettings = new DatafusionSettings(initialSettings);

        // Register listeners on a ClusterSettings instance (same as createComponents does)
        Set<Setting<?>> registeredSettings = new HashSet<>(DatafusionSettings.ALL_SETTINGS);
        registeredSettings.add(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(initialSettings, registeredSettings);
        datafusionSettings.registerListeners(clusterSettings);

        // Verify initial state
        WireConfigSnapshot before = datafusionSettings.getSnapshot();
        assertEquals("initial batchSize must be 8192", 8192, before.batchSize());
        assertEquals("initial costCollector must be 10", 10, before.costCollector());

        // Simulate a cluster settings update (as if operator called PUT _cluster/settings)
        clusterSettings.applySettings(
            Settings.builder().put("datafusion.indexed.batch_size", 16384).put("datafusion.indexed.cost_collector", 10).build()
        );

        // The volatile snapshot must now reflect the updated value
        WireConfigSnapshot after = datafusionSettings.getSnapshot();
        assertEquals("batchSize must reflect the updated value after listener fires", 16384, after.batchSize());
        // Other fields must remain unchanged
        assertEquals("costCollector must remain unchanged after batch_size update", 10, after.costCollector());

        // Update another setting to prove multiple volatile writes work
        clusterSettings.applySettings(
            Settings.builder().put("datafusion.indexed.batch_size", 16384).put("datafusion.indexed.cost_collector", 50).build()
        );

        WireConfigSnapshot afterSecond = datafusionSettings.getSnapshot();
        assertEquals("costCollector must reflect the second update", 50, afterSecond.costCollector());
        assertEquals("batchSize must still reflect the first update", 16384, afterSecond.batchSize());
    }

    /**
     * Verifies that {@code WireConfigSnapshot.BYTE_SIZE} (68 bytes) is sufficient to
     * hold all fields written by {@code writeTo()}. This is validated by writing a
     * snapshot with maximum-range values and reading back every field to confirm no
     * overlap or truncation.
     * <p>
     * Validates: Requirement 3.4 — the segment size is correct for the wire struct layout.
     */
    public void testSegmentSizeMatchesByteSize() {
        // Use large values to exercise the full range of each field
        WireConfigSnapshot snapshot = new WireConfigSnapshot(
            Integer.MAX_VALUE,  // batchSize
            Integer.MAX_VALUE,  // targetPartitions
            true,               // parquetPushdownFilters
            Integer.MAX_VALUE,  // minSkipRunDefault
            1.0,                // minSkipRunSelectivityThreshold (max)
            Integer.MAX_VALUE,  // costPredicate
            Integer.MAX_VALUE,  // costCollector
            Integer.MAX_VALUE   // maxCollectorParallelism
        );

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            // Read back all fields to verify no overlap or truncation
            // Dynamic fields
            assertEquals((long) Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_LONG, 0));   // batch_size
            assertEquals((long) Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_LONG, 8));   // target_partitions
            assertEquals((long) Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_LONG, 16));  // min_skip_run_default
            assertEquals(1.0, segment.get(ValueLayout.JAVA_DOUBLE, 24), 1e-15);              // selectivity_threshold
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 32));                          // parquet_pushdown_filters
            assertEquals(Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_INT, 48));          // cost_predicate
            assertEquals(Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_INT, 52));          // cost_collector
            assertEquals(Integer.MAX_VALUE, segment.get(ValueLayout.JAVA_INT, 56));          // max_collector_parallelism

            // Hardcoded fields — verify they are not corrupted by large dynamic values
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 36));   // indexed_pushdown_filters
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 40));  // force_strategy
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 44));  // force_pushdown
            assertEquals(2, segment.get(ValueLayout.JAVA_INT, 60));   // single_collector_strategy
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 64));   // tree_collector_strategy

            // The last field ends at offset 64 + 4 = 68, which equals BYTE_SIZE
            assertEquals("BYTE_SIZE must equal the end offset of the last field (68)", 68L, WireConfigSnapshot.BYTE_SIZE);
        }
    }
}
