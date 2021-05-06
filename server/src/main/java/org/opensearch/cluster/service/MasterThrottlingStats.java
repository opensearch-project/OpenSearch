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
 * Contains stats of Master Task Throttling.
 * It stores the total cumulative count of throttled tasks per task type.
 */
public class MasterThrottlingStats {

    private Map<String, CounterMetric> throttledTasksCount = new ConcurrentHashMap<>();

    public void incrementThrottlingCount(String type, final int permits) {
        if(!throttledTasksCount.containsKey(type)) {
            throttledTasksCount.put(type, new CounterMetric());
        }
        throttledTasksCount.get(type).inc(permits);
    }

    public long getThrottlingCount(Class type) {
        return throttledTasksCount.get(type).count();
    }

    public long getTotalThrottledTaskCount() {
        CounterMetric totalCount = new CounterMetric();
        throttledTasksCount.forEach((aClass, counterMetric) -> {totalCount.inc(counterMetric.count());});
        return totalCount.count();
    }

}
