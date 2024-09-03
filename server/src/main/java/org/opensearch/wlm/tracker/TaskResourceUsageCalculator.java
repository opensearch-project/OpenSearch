/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.tasks.Task;
import org.opensearch.wlm.ResourceType;

import java.util.function.Supplier;

import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService.HEAP_SIZE_BYTES;

/**
 * Utility class to calculate task level resource usage
 */
public abstract class TaskResourceUsageCalculator {
    public static TaskResourceUsageCalculator from(final ResourceType resourceType) {
        if (resourceType == ResourceType.CPU) {
            return new TaskCpuUsageCalculator();
        } else if (resourceType == ResourceType.MEMORY) {
            return new TaskMemoryUsageCalculator();
        }
        throw new IllegalArgumentException("Invalid resource type " + resourceType + " . It is not supported in wlm");
    }

    /**
     * calculates the resource usage for the task
     * @param task {@link Task} instance
     * @param nanoTimeSupplier time supplier in nano second unit
     * @return task resource usage
     */
    public abstract double calculateFor(Task task, Supplier<Long> nanoTimeSupplier);

    /**
     * This class will return per core cpu usage for a task
     */
    public static class TaskCpuUsageCalculator extends TaskResourceUsageCalculator {
        @Override
        public double calculateFor(Task task, Supplier<Long> nanoTimeSupplier) {
            return ((1.0f * task.getTotalResourceUtilization(ResourceStats.CPU)) / (nanoTimeSupplier.get() - task.getStartTimeNanos()));
        }
    }

    /**
     * This class will return allocated bytes by the task since task has been created
     */
    public static class TaskMemoryUsageCalculator extends TaskResourceUsageCalculator {
        @Override
        public double calculateFor(Task task, Supplier<Long> nanoTimeSupplier) {
            return (1.0f * task.getTotalResourceUtilization(ResourceStats.MEMORY)) / HEAP_SIZE_BYTES;
        }
    }
}

