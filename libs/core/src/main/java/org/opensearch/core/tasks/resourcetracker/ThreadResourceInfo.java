/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.tasks.resourcetracker;

import org.opensearch.common.annotation.PublicApi;

/**
 * Resource consumption information about a particular execution of thread.
 * <p>
 * It captures the resource usage information about a particular execution of thread
 * for a specific stats type like worker_stats or response_stats etc.,
 *
 *  @opensearch.api
 */
@PublicApi(since = "2.1.0")
public class ThreadResourceInfo {
    private final long threadId;
    private volatile boolean isActive = true;
    private final ResourceStatsType statsType;
    private final ResourceUsageInfo resourceUsageInfo;

    public ThreadResourceInfo(long threadId, ResourceStatsType statsType, ResourceUsageMetric... resourceUsageMetrics) {
        this.threadId = threadId;
        this.statsType = statsType;
        this.resourceUsageInfo = new ResourceUsageInfo(resourceUsageMetrics);
    }

    /**
     * Updates thread's resource consumption information.
     */
    public void recordResourceUsageMetrics(ResourceUsageMetric... resourceUsageMetrics) {
        resourceUsageInfo.recordResourceUsageMetrics(resourceUsageMetrics);
    }

    public void setActive(boolean isActive) {
        this.isActive = isActive;
    }

    public boolean isActive() {
        return isActive;
    }

    public ResourceStatsType getStatsType() {
        return statsType;
    }

    public long getThreadId() {
        return threadId;
    }

    public ResourceUsageInfo getResourceUsageInfo() {
        return resourceUsageInfo;
    }

    @Override
    public String toString() {
        return resourceUsageInfo + ", stats_type=" + statsType + ", is_active=" + isActive + ", threadId=" + threadId;
    }
}
