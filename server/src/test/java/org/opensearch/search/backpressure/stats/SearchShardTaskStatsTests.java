/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.search.backpressure.trackers.HeapUsageTracker;
import org.opensearch.search.backpressure.trackers.NativeMemoryUsageTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackers.TaskResourceUsageTracker;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;
import java.util.Map;

public class SearchShardTaskStatsTests extends AbstractWireSerializingTestCase<SearchShardTaskStats> {
    @Override
    protected Writeable.Reader<SearchShardTaskStats> instanceReader() {
        return SearchShardTaskStats::new;
    }

    @Override
    protected SearchShardTaskStats createTestInstance() {
        return randomInstance();
    }

    public static SearchShardTaskStats randomInstance() {
        Map<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> resourceUsageTrackerStats = Map.of(
            TaskResourceUsageTrackerType.CPU_USAGE_TRACKER,
            new CpuUsageTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER,
            new HeapUsageTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            TaskResourceUsageTrackerType.ELAPSED_TIME_TRACKER,
            new ElapsedTimeTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER,
            new NativeMemoryUsageTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
        );

        return new SearchShardTaskStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            resourceUsageTrackerStats
        );
    }

    /**
     * Pre-V_3_7_0 nodes don't recognize the {@code NATIVE_MEMORY_USAGE_TRACKER} entry. A round
     * trip via the older wire version MUST drop that entry rather than corrupting the stream;
     * the remaining trackers MUST round-trip unchanged.
     *
     * <p>To assert this without reaching into private fields, we construct a deterministic
     * input plus a parallel "expected" instance that omits the native-memory entry. Equality
     * via {@link SearchShardTaskStats#equals} then exercises the same invariants.
     */
    public void testNativeMemoryEntryDroppedOnPreV37Wire() throws Exception {
        long cancellations = 7L;
        long limitReached = 3L;
        long completions = 11L;

        TaskResourceUsageTracker.Stats cpu = new CpuUsageTracker.Stats(1L, 2L, 3L);
        TaskResourceUsageTracker.Stats heap = new HeapUsageTracker.Stats(4L, 5L, 6L, 7L);
        TaskResourceUsageTracker.Stats elapsed = new ElapsedTimeTracker.Stats(8L, 9L, 10L);
        TaskResourceUsageTracker.Stats nativeMem = new NativeMemoryUsageTracker.Stats(11L, 12L, 13L);

        Map<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> withNative = new HashMap<>();
        withNative.put(TaskResourceUsageTrackerType.CPU_USAGE_TRACKER, cpu);
        withNative.put(TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER, heap);
        withNative.put(TaskResourceUsageTrackerType.ELAPSED_TIME_TRACKER, elapsed);
        withNative.put(TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER, nativeMem);

        Map<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> withoutNative = new HashMap<>(withNative);
        withoutNative.remove(TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER);

        SearchShardTaskStats original = new SearchShardTaskStats(cancellations, limitReached, completions, withNative);
        SearchShardTaskStats expected = new SearchShardTaskStats(cancellations, limitReached, completions, withoutNative);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_3_6_0);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(Version.V_3_6_0);
                SearchShardTaskStats deserialized = new SearchShardTaskStats(in);
                assertEquals("pre-V_3_7_0 wire must drop the native-memory tracker entry but preserve the others", expected, deserialized);
            }
        }
    }

    /**
     * V_3_7_0 round-trip must preserve the native-memory entry verbatim.
     */
    public void testNativeMemoryEntryRoundTripsOnV37Wire() throws Exception {
        SearchShardTaskStats original = randomInstance();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_3_7_0);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(Version.V_3_7_0);
                SearchShardTaskStats deserialized = new SearchShardTaskStats(in);
                assertEquals(original, deserialized);
            }
        }
    }
}
