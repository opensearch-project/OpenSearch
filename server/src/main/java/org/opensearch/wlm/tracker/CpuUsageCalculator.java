/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.wlm.QueryGroupTask;

import java.util.List;
import java.util.function.Supplier;

/**
 * class to help make cpu usage calculations for the query group
 */
public class CpuUsageCalculator implements ResourceUsageCalculator {
    // This value should be initialised at the start time of the process and be used throughout the codebase
    public static final int PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();
    public static final CpuUsageCalculator INSTANCE = new CpuUsageCalculator();

    private CpuUsageCalculator() {}

    @Override
    public double calculateResourceUsage(List<QueryGroupTask> tasks, Supplier<Long> timeSupplier) {
        double usage = tasks.stream().mapToDouble(task -> calculateTaskResourceUsage(task, timeSupplier)).sum();

        usage /= PROCESSOR_COUNT;
        return usage;
    }

    @Override
    public double calculateTaskResourceUsage(QueryGroupTask task, Supplier<Long> nanoTimeSupplier) {
        return (1.0f * task.getTotalResourceUtilization(ResourceStats.CPU)) / (nanoTimeSupplier.get() - task.getStartTimeNanos());
    }
}
