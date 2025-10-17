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

    public static final TimeValue SHORT_POLL_INTERVAL = TimeValue.timeValueSeconds(6);
    public static final TimeValue LONG_POLL_INTERVAL = TimeValue.timeValueSeconds(30);
    public static final TimeValue SHORT_AVERAGE_WINDOW = TimeValue.timeValueMinutes(1);
    public static final TimeValue LONG_AVERAGE_WINDOW = TimeValue.timeValueMinutes(5);

    public static ResourceTrackers resourceTrackers;

    public static ResourceTrackers create(ThreadPool threadPool) {
        return resourceTrackers = new ResourceTrackers(
            new AverageCpuUsageTracker(threadPool, SHORT_POLL_INTERVAL, SHORT_AVERAGE_WINDOW),
            new AverageCpuUsageTracker(threadPool, LONG_POLL_INTERVAL, LONG_AVERAGE_WINDOW),
            new AverageMemoryUsageTracker(threadPool, SHORT_POLL_INTERVAL, SHORT_AVERAGE_WINDOW),
            new AverageMemoryUsageTracker(threadPool, LONG_POLL_INTERVAL, LONG_AVERAGE_WINDOW)
        );
    }

    /**
     * Container for resource usage trackers used in auto force merge operations.
     * Provides access to CPU and JVM memory usage trackers with different time windows.
     *
     * @opensearch.internal
     */
    public static class ResourceTrackers {
        public final AverageCpuUsageTracker cpuOneMinute;
        public final AverageCpuUsageTracker cpuFiveMinute;
        public final AverageMemoryUsageTracker jvmOneMinute;
        public final AverageMemoryUsageTracker jvmFiveMinute;

        /**
         * Creates a new ResourceTrackers instance.
         *
         * @param cpuOneMinute  CPU tracker with 1-minute window
         * @param cpuFiveMinute CPU tracker with 5-minute window
         * @param jvmOneMinute  JVM memory tracker with 1-minute window
         * @param jvmFiveMinute JVM memory tracker with 5-minute window
         */
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

        public void start() {
            cpuOneMinute.start();
            cpuFiveMinute.start();
            jvmOneMinute.start();
            jvmFiveMinute.start();
        }

        public void stop() {
            cpuOneMinute.stop();
            cpuFiveMinute.stop();
            jvmOneMinute.stop();
            jvmFiveMinute.stop();
        }
    }
}
