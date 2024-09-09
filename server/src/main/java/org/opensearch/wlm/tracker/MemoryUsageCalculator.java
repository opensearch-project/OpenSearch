/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.wlm.QueryGroupTask;

import java.util.List;

/**
 * class to help make memory usage calculations for the query group
 */
public class MemoryUsageCalculator extends ResourceUsageCalculator {
    public static final long HEAP_SIZE_BYTES = JvmStats.jvmStats().getMem().getHeapMax().getBytes();
    public static final MemoryUsageCalculator INSTANCE = new MemoryUsageCalculator();

    private MemoryUsageCalculator() {}

    @Override
    public double calculateResourceUsage(List<QueryGroupTask> tasks) {
        return tasks.stream().mapToDouble(this::calculateTaskResourceUsage).sum();
    }

    @Override
    public double calculateTaskResourceUsage(QueryGroupTask task) {
        return (1.0f * task.getTotalResourceUtilization(ResourceStats.MEMORY)) / HEAP_SIZE_BYTES;
    }
}
