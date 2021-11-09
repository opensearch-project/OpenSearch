/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread resource usage information for particular resource stats type.
 * <p>
 * It captures the resource usage information about a particular execution of thread
 * for a specific stats type.
 */
public class ResourceUsageInfo {
    private static final Logger logger = LogManager.getLogger(ResourceUsageInfo.class);
    private final EnumMap<ResourceStats, ResourceStatsInfo> statsInfo = new EnumMap<>(ResourceStats.class);

    public ResourceUsageInfo(ResourceUsageMetric... resourceUsageMetrics) {
        for (ResourceUsageMetric resourceUsageMetric : resourceUsageMetrics) {
            this.statsInfo.put(resourceUsageMetric.getStats(), new ResourceStatsInfo(resourceUsageMetric.getValue()));
        }
    }

    public void recordResourceUsageMetrics(ResourceUsageMetric... resourceUsageMetrics) {
        for (ResourceUsageMetric resourceUsageMetric : resourceUsageMetrics) {
            ResourceStatsInfo resourceStatsInfo = statsInfo.get(resourceUsageMetric.getStats());
            if (resourceStatsInfo == null) {
                statsInfo.put(resourceUsageMetric.getStats(), new ResourceStatsInfo(resourceUsageMetric.getValue()));
            } else {
                updateResourceUsageInfo(resourceStatsInfo, resourceUsageMetric);
            }
        }
    }

    private void updateResourceUsageInfo(ResourceStatsInfo resourceStatsInfo, ResourceUsageMetric resourceUsageMetric) {
        long currentEndValue;
        long newEndValue;
        do {
            currentEndValue = resourceStatsInfo.endValue.get();
            newEndValue = resourceUsageMetric.getValue();
            if (currentEndValue > newEndValue) {
                logger.debug(
                    "Dropping resource usage update as the new value is lower than current value ["
                        + "resource_stats=[{}], "
                        + "current_end_value={}, "
                        + "new_end_value={}]",
                    resourceUsageMetric.getStats(),
                    currentEndValue,
                    newEndValue
                );
                return;
            }
        } while (!resourceStatsInfo.endValue.compareAndSet(currentEndValue, newEndValue));
        logger.debug(
            "Updated resource usage info [resource_stats=[{}], " + "old_end_value={}, new_end_value={}]",
            resourceUsageMetric.getStats(),
            currentEndValue,
            newEndValue
        );
    }

    public EnumMap<ResourceStats, ResourceStatsInfo> getStatsInfo() {
        return statsInfo;
    }

    @Override
    public String toString() {
        return statsInfo.toString();
    }

    /**
     *  Defines resource stats information.
     */
    static class ResourceStatsInfo {
        private final long startValue;
        private final AtomicLong endValue;

        private ResourceStatsInfo(long startValue) {
            this.startValue = startValue;
            this.endValue = new AtomicLong(startValue);
        }

        public long getTotalValue() {
            return endValue.get() - startValue;
        }

        @Override
        public String toString() {
            return String.valueOf(getTotalValue());
        }
    }
}
