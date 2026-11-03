/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.wlm.WorkloadGroupTask;

import java.util.List;

/**
 * class to help make cpu usage calculations for the workload group
 */
public class CpuUsageCalculator extends ResourceUsageCalculator {
    // This value should be initialised at the start time of the process and be used throughout the codebase
    public static final int PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();
    public static final CpuUsageCalculator INSTANCE = new CpuUsageCalculator();

    private CpuUsageCalculator() {}

    @Override
    public double calculateResourceUsage(List<WorkloadGroupTask> tasks) {
        return tasks.stream().mapToDouble(this::calculateTaskResourceUsage).sum();
    }

    /**
     * Returns the task's share of the node's CPU, so it is in the same units as the workload group usage
     */
    @Override
    public double calculateTaskResourceUsage(WorkloadGroupTask task) {
        final double coresUsed = (1.0f * task.getTotalResourceUtilization(ResourceStats.CPU)) / task.getElapsedTime();
        return coresUsed / PROCESSOR_COUNT;
    }
}
