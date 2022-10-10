/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.search.backpressure.trackers.HeapUsageTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SearchShardTaskStatsTests extends AbstractWireSerializingTestCase<SearchShardTaskStats> {
    @Override
    protected Writeable.Reader<SearchShardTaskStats> instanceReader() {
        return SearchShardTaskStats::new;
    }

    @Override
    protected SearchShardTaskStats createTestInstance() {
        return randomInstance();
    }

    public void testLastCancelledTaskIsNull() throws IOException {
        SearchShardTaskStats expected = new SearchShardTaskStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            null,
            Collections.emptyMap()
        );

        SearchShardTaskStats actual = copyInstance(expected);
        assertEquals(expected, actual);
    }

    public static SearchShardTaskStats randomInstance() {
        Map<String, Long> shardCancellationCount = Stream.generate(() -> RandomStrings.randomAsciiLettersOfLength(random(), 8))
            .limit(randomIntBetween(1, 10))
            .collect(Collectors.toMap(Function.identity(), s -> randomNonNegativeLong()));

        Map<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> resourceUsageTrackerStats = Map.of(
            TaskResourceUsageTrackerType.CPU_USAGE_TRACKER,
            new CpuUsageTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER,
            new HeapUsageTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            TaskResourceUsageTrackerType.ELAPSED_TIME_TRACKER,
            new ElapsedTimeTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
        );

        return new SearchShardTaskStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            CancelledTaskStatsTests.randomInstance(),
            resourceUsageTrackerStats
        );
    }
}
