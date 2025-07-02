/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.autoforcemerge;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.resource.tracker.AverageCpuUsageTracker;
import org.opensearch.node.resource.tracker.AverageMemoryUsageTracker;
import org.opensearch.threadpool.ThreadPool;

/**
 * Provider for creating resource usage trackers used in auto force merge operations.
 *
 * @opensearch.internal
 */
public class ResourceTrackerProvider {

    public static final TimeValue AVERAGE_WINDOW_ONE_SECOND = TimeValue.timeValueSeconds(1);
    public static final TimeValue AVERAGE_WINDOW_FIVE_SECOND = TimeValue.timeValueSeconds(5);
    public static final TimeValue AVERAGE_WINDOW_ONE_MINUTE = TimeValue.timeValueMinutes(1);
    public static final TimeValue AVERAGE_WINDOW_FIVE_MINUTE = TimeValue.timeValueMinutes(5);

    public static ResourceTrackers resourceTrackers;

    public static ResourceTrackers create(ThreadPool threadPool) {
        return resourceTrackers = new ResourceTrackers(
            new AverageCpuUsageTracker(threadPool, AVERAGE_WINDOW_ONE_SECOND, AVERAGE_WINDOW_ONE_MINUTE),
            new AverageCpuUsageTracker(threadPool, AVERAGE_WINDOW_FIVE_SECOND, AVERAGE_WINDOW_FIVE_MINUTE),
            new AverageMemoryUsageTracker(threadPool, AVERAGE_WINDOW_ONE_SECOND, AVERAGE_WINDOW_ONE_MINUTE),
            new AverageMemoryUsageTracker(threadPool, AVERAGE_WINDOW_FIVE_SECOND, AVERAGE_WINDOW_FIVE_MINUTE)
        );
    }

    public static class ResourceTrackers {
        public final AverageCpuUsageTracker cpuOneMinute;
        public final AverageCpuUsageTracker cpuFiveMinute;
        public final AverageMemoryUsageTracker jvmOneMinute;
        public final AverageMemoryUsageTracker jvmFiveMinute;

        ResourceTrackers(
            AverageCpuUsageTracker cpuOneMinute,
            AverageCpuUsageTracker cpuFiveMinute,
            AverageMemoryUsageTracker jvmOneMinute,
            AverageMemoryUsageTracker jvmFiveMinute
        ) {
            this.cpuOneMinute = cpuOneMinute;
            this.cpuFiveMinute = cpuFiveMinute;
            this.jvmOneMinute = jvmOneMinute;
            this.jvmFiveMinute = jvmFiveMinute;
        }
    }
}
