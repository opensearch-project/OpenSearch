/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadManagementSettings;

import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService.PROCESSOR_COUNT;

/**
 * This class is used to track query group level resource usage
 */
public abstract class QueryGroupResourceUsage {
    private double currentUsage;

    /**
     * getter for value field
     * @return resource usage value
     */
    public double getCurrentUsage() {
        return currentUsage;
    }

    public void setCurrentUsage(double currentUsage) {
        this.currentUsage = currentUsage;
    }

    public static QueryGroupResourceUsage from(ResourceType resourceType) {
        if (resourceType == ResourceType.CPU) {
            return new QueryGroupCpuUsage();
        } else if (resourceType == ResourceType.MEMORY) {
            return new QueryGroupMemoryUsage();
        }
        throw new IllegalArgumentException("Invalid resource type: " + resourceType + ". It is currently not supported in wlm");
    }

    /**
     * Determines whether {@link QueryGroup} is breaching its threshold for the resource
     * @param queryGroup
     * @return whether the query group is breaching threshold for this resource
     */
    public boolean isBreachingThresholdFor(QueryGroup queryGroup, WorkloadManagementSettings settings) {
        return getCurrentUsage() > getNormalisedThresholdFor(queryGroup, settings);
    }

    /**
     * returns the value by which the resource usage is beyond the configured limit for the query group
     * @param queryGroup instance
     * @param settings {@link WorkloadManagementSettings} instance
     * @return the overshooting limit for the resource
     */
    public double getReduceByFor(QueryGroup queryGroup, WorkloadManagementSettings settings) {
        return getCurrentUsage() - getNormalisedThresholdFor(queryGroup, settings);
    }

    /**
     * initialises the member variable currentUsage
     * @param tasks list of tasks in the query group
     * @param timeSupplier nano time supplier
     */
    public void initialise(List<QueryGroupTask> tasks, Supplier<Long> timeSupplier) {
        this.setCurrentUsage(this.calculateResourceUsage(tasks, timeSupplier));
    }

    /**
     * normalises configured value with respect to node level cancellation thresholds
     * @param queryGroup instance
     * @param settings {@link WorkloadManagementSettings} instance
     * @return normalised value with respect to node level cancellation thresholds
     */
    public abstract double getNormalisedThresholdFor(QueryGroup queryGroup, WorkloadManagementSettings settings);

    /**
     * calculates the current resource usage for the query group
     * @param tasks list of tasks in the query group
     * @param timeSupplier nano time supplier
     */
    public abstract double calculateResourceUsage(List<QueryGroupTask> tasks, Supplier<Long> timeSupplier);

    /**
     * class to store cpu usage for the query group
     */
    public static class QueryGroupCpuUsage extends QueryGroupResourceUsage {
        @Override
        public double getNormalisedThresholdFor(QueryGroup queryGroup, WorkloadManagementSettings settings) {
            return settings.getNodeLevelCpuCancellationThreshold() * queryGroup.getResourceLimits().get(ResourceType.CPU);
        }

        @Override
        public double calculateResourceUsage(List<QueryGroupTask> tasks, Supplier<Long> timeSupplier) {
            double usage = tasks.stream().mapToDouble(task -> {
                return TaskResourceUsageCalculator.from(ResourceType.CPU).calculateFor(task, timeSupplier);
            }).sum();

            usage /= PROCESSOR_COUNT;
            return usage;
        }
    }

    /**
     * class to store memory usage for the query group
     */
    public static class QueryGroupMemoryUsage extends QueryGroupResourceUsage {
        @Override
        public double getNormalisedThresholdFor(QueryGroup queryGroup, WorkloadManagementSettings settings) {
            return settings.getNodeLevelMemoryCancellationThreshold() * queryGroup.getResourceLimits().get(ResourceType.MEMORY);
        }

        @Override
        public double calculateResourceUsage(List<QueryGroupTask> tasks, Supplier<Long> timeSupplier) {
            double usage = tasks.stream().mapToDouble(task -> {
                return TaskResourceUsageCalculator.from(ResourceType.MEMORY).calculateFor(task, timeSupplier);
            }).sum();
            return usage;
        }
    }

}
