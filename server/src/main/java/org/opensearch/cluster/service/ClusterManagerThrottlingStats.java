/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.common.metrics.CounterMetric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains stats of Cluster Manager Task Throttling.
 * It stores the total cumulative count of throttled tasks per task type.
 */
public class ClusterManagerThrottlingStats implements ClusterManagerTaskThrottlerListener {

    private Map<String, CounterMetric> throttledTasksCount = new ConcurrentHashMap<>();

    private void incrementThrottlingCount(String type, final int counts) {
        throttledTasksCount.computeIfAbsent(type, k -> new CounterMetric()).inc(counts);
    }

    public long getThrottlingCount(String type) {
        return throttledTasksCount.get(type) == null ? 0 : throttledTasksCount.get(type).count();
    }

    public long getTotalThrottledTaskCount() {
        CounterMetric totalCount = new CounterMetric();
        throttledTasksCount.forEach((aClass, counterMetric) -> { totalCount.inc(counterMetric.count()); });
        return totalCount.count();
    }

    @Override
    public void onThrottle(String type, int counts) {
        incrementThrottlingCount(type, counts);
    }
}
