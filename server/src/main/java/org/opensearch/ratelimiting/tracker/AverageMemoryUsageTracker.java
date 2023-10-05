/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimiting.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

/**
 * AverageMemoryUsageTracker tracks the average JVM usage by polling the JVM usage every (pollingInterval)
 * and keeping track of the rolling average over a defined time window (windowDuration).
 */
public class AverageMemoryUsageTracker extends AbstractAverageUsageTracker {

    private static final Logger LOGGER = LogManager.getLogger(AverageMemoryUsageTracker.class);

    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

    public AverageMemoryUsageTracker(ThreadPool threadPool, TimeValue pollingInterval, TimeValue windowDuration) {
        super(threadPool, pollingInterval, windowDuration);
    }

    /**
     * Get current memory usage percentage calculated against max heap memory
     */
    @Override
    public long getUsage() {
        long usage = MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed() * 100 / MEMORY_MX_BEAN.getHeapMemoryUsage().getMax();
        LOGGER.debug("Recording memory usage: {}%", usage);
        return usage;
    }
}
