/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.plugin.stats.AnalyticsBackendTaskCancellationStats;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;

/**
 * Unit and property-based tests for {@link NativeNodeStatsLayout}.
 *
 * <p>Task 3.3: Unit tests verifying layout size and decode correctness.
 * <p>Task 3.4: Property test verifying decode matches byte offsets for random values.
 */
public class NativeNodeStatsLayoutTests extends OpenSearchTestCase {

    // ========== Task 3.3: Unit Tests ==========

    /**
     * Validates: Requirements 4.2
     * Verify LAYOUT.byteSize() == 32 (4 × JAVA_LONG).
     */
    public void testLayoutByteSizeIs32() {
        assertEquals(32L, NativeNodeStatsLayout.LAYOUT.byteSize());
        assertEquals(4 * Long.BYTES, (int) NativeNodeStatsLayout.LAYOUT.byteSize());
    }

    /**
     * Validates: Requirements 4.4
     * Verify readNativeNodeStats decodes known byte patterns correctly.
     * Writes known long values at offsets 0, 8, 16, 24 and asserts decoded fields match.
     */
    public void testReadNativeNodeStatsDecodesKnownValues() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(NativeNodeStatsLayout.LAYOUT);

            // Write known values at the 4 field positions (little-endian, native order for JAVA_LONG)
            seg.setAtIndex(ValueLayout.JAVA_LONG, 0, 42L);   // offset 0: native_search_task_current
            seg.setAtIndex(ValueLayout.JAVA_LONG, 1, 100L);  // offset 8: native_search_task_total
            seg.setAtIndex(ValueLayout.JAVA_LONG, 2, 7L);    // offset 16: native_search_shard_task_current
            seg.setAtIndex(ValueLayout.JAVA_LONG, 3, 999L);  // offset 24: native_search_shard_task_total

            AnalyticsBackendTaskCancellationStats stats = NativeNodeStatsLayout.readNativeNodeStats(seg);

            assertEquals(42L, stats.getSearchTaskCurrent());
            assertEquals(100L, stats.getSearchTaskTotal());
            assertEquals(7L, stats.getSearchShardTaskCurrent());
            assertEquals(999L, stats.getSearchShardTaskTotal());
        }
    }

    /**
     * Validates: Requirements 4.4
     * Verify readNativeNodeStats handles zero values correctly.
     */
    public void testReadNativeNodeStatsDecodesZeros() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(NativeNodeStatsLayout.LAYOUT);
            // All zeros by default after allocation

            AnalyticsBackendTaskCancellationStats stats = NativeNodeStatsLayout.readNativeNodeStats(seg);

            assertEquals(0L, stats.getSearchTaskCurrent());
            assertEquals(0L, stats.getSearchTaskTotal());
            assertEquals(0L, stats.getSearchShardTaskCurrent());
            assertEquals(0L, stats.getSearchShardTaskTotal());
        }
    }

    /**
     * Validates: Requirements 4.4
     * Verify readNativeNodeStats handles Long.MAX_VALUE correctly.
     */
    public void testReadNativeNodeStatsDecodesMaxValues() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(NativeNodeStatsLayout.LAYOUT);

            seg.setAtIndex(ValueLayout.JAVA_LONG, 0, Long.MAX_VALUE);
            seg.setAtIndex(ValueLayout.JAVA_LONG, 1, Long.MAX_VALUE);
            seg.setAtIndex(ValueLayout.JAVA_LONG, 2, Long.MAX_VALUE);
            seg.setAtIndex(ValueLayout.JAVA_LONG, 3, Long.MAX_VALUE);

            AnalyticsBackendTaskCancellationStats stats = NativeNodeStatsLayout.readNativeNodeStats(seg);

            assertEquals(Long.MAX_VALUE, stats.getSearchTaskCurrent());
            assertEquals(Long.MAX_VALUE, stats.getSearchTaskTotal());
            assertEquals(Long.MAX_VALUE, stats.getSearchShardTaskCurrent());
            assertEquals(Long.MAX_VALUE, stats.getSearchShardTaskTotal());
        }
    }

    // ========== Task 3.4: Property Test ==========

    /**
     * **Property 4: NativeNodeStatsLayout decode matches byte offsets**
     *
     * <p>Generate random 32-byte segments, verify decoded fields match values at offsets 0, 8, 16, 24.
     *
     * <p>**Validates: Requirements 4.4**
     */
    public void testPropertyDecodeMatchesByteOffsets() {
        final int iterations = 100;

        for (int i = 0; i < iterations; i++) {
            long searchTaskCurrent = randomLong();
            long searchTaskTotal = randomLong();
            long shardTaskCurrent = randomLong();
            long shardTaskTotal = randomLong();

            try (var arena = Arena.ofConfined()) {
                var seg = arena.allocate(NativeNodeStatsLayout.LAYOUT);

                // Write random values at the expected byte offsets
                seg.setAtIndex(ValueLayout.JAVA_LONG, 0, searchTaskCurrent);   // offset 0
                seg.setAtIndex(ValueLayout.JAVA_LONG, 1, searchTaskTotal);     // offset 8
                seg.setAtIndex(ValueLayout.JAVA_LONG, 2, shardTaskCurrent);    // offset 16
                seg.setAtIndex(ValueLayout.JAVA_LONG, 3, shardTaskTotal);      // offset 24

                AnalyticsBackendTaskCancellationStats stats = NativeNodeStatsLayout.readNativeNodeStats(seg);

                assertEquals("Iteration " + i + ": native_search_task_current mismatch", searchTaskCurrent, stats.getSearchTaskCurrent());
                assertEquals("Iteration " + i + ": native_search_task_total mismatch", searchTaskTotal, stats.getSearchTaskTotal());
                assertEquals(
                    "Iteration " + i + ": native_search_shard_task_current mismatch",
                    shardTaskCurrent,
                    stats.getSearchShardTaskCurrent()
                );
                assertEquals(
                    "Iteration " + i + ": native_search_shard_task_total mismatch",
                    shardTaskTotal,
                    stats.getSearchShardTaskTotal()
                );
            }
        }
    }
}
