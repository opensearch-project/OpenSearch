/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.resource.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.threadpool.ThreadPool;

/**
 * AverageCpuUsageTracker tracks the average CPU usage by polling the CPU usage every (pollingInterval)
 * and keeping track of the rolling average over a defined time window (windowDuration).
 */
public class AverageCpuUsageTracker extends AbstractAverageUsageTracker {
    private static final Logger LOGGER = LogManager.getLogger(AverageCpuUsageTracker.class);

    public AverageCpuUsageTracker(ThreadPool threadPool, TimeValue pollingInterval, TimeValue windowDuration) {
        super(threadPool, pollingInterval, windowDuration);
    }

    /**
     * Returns the process CPU usage in percent
     */
    @Override
    public long getUsage() {
        long usage = ProcessProbe.getInstance().getProcessCpuPercent();
        LOGGER.debug("Recording cpu usage: {}%", usage);
        return usage;
    }

}
