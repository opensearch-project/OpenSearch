/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.querygroup.cancellation;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.search.querygroup.QueryGroupLevelResourceUsageView;
import org.opensearch.search.resourcetypes.ResourceType;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.search.querygroup.tracker.QueryGroupResourceUsageTrackerService.TRACKED_RESOURCES;

/**
 * Manages the cancellation of tasks enforced by QueryGroup thresholds on resource usage criteria.
 * This class utilizes a strategy pattern through {@link TaskSelectionStrategy} to identify tasks that exceed
 * predefined resource usage limits and are therefore eligible for cancellation.
 *
 * <p>The cancellation process is initiated by evaluating the resource usage of each QueryGroup against its
 * resource limits. Tasks that contribute to exceeding these limits are selected for cancellation based on the
 * implemented task selection strategy.</p>
 *
 * <p>Instances of this class are configured with a map linking QueryGroup IDs to their corresponding resource usage
 * views, a set of active QueryGroups, and a task selection strategy. These components collectively facilitate the
 * identification and cancellation of tasks that threaten to breach QueryGroup resource limits.</p>
 *
 * @see TaskSelectionStrategy
 * @see QueryGroup
 * @see ResourceType
 */
public class DefaultTaskCancellation {
    protected final TaskSelectionStrategy taskSelectionStrategy;
    // a map of QueryGroupId to its corresponding QueryGroupLevelResourceUsageView object
    protected final Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelViews;
    protected final Set<QueryGroup> activeQueryGroups;

    public DefaultTaskCancellation(
        TaskSelectionStrategy taskSelectionStrategy,
        Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelViews,
        Set<QueryGroup> activeQueryGroups
    ) {
        this.taskSelectionStrategy = taskSelectionStrategy;
        this.queryGroupLevelViews = queryGroupLevelViews;
        this.activeQueryGroups = activeQueryGroups;
    }

    /**
     * Cancel tasks based on the implemented strategy.
     */
    public final void cancelTasks() {
        List<TaskCancellation> cancellableTasks = getAllCancellableTasks();
        for (TaskCancellation taskCancellation : cancellableTasks) {
            taskCancellation.cancel();
        }
    }

    /**
     * returns the list of QueryGroups breaching their resource limits.
     *
     * @return List of QueryGroups
     */
    public List<QueryGroup> getQueryGroupsToCancelFrom() {
        final List<QueryGroup> queryGroupsToCancelFrom = new ArrayList<>();

        for (QueryGroup queryGroup : this.activeQueryGroups) {
            Map<ResourceType, Long> currentResourceUsage = getResourceUsage(queryGroup.get_id());
            // if(currentResourceUsage == null) {
            // // skip if the QueryGroup is not found
            // continue;
            // }

            Map<ResourceType, Object> resourceLimits = queryGroup.getResourceLimits();
            for (ResourceType resourceType : TRACKED_RESOURCES) {
                if (resourceLimits.containsKey(resourceType)) {
                    long threshold = queryGroup.getThresholdInLong(resourceType);

                    if (isBreachingThreshold(currentResourceUsage, resourceType, threshold)) {
                        queryGroupsToCancelFrom.add(queryGroup);
                        break;
                    }
                }
            }

        }

        return queryGroupsToCancelFrom;
    }

    /**
     * Get all cancellable tasks from the QueryGroups.
     *
     * @return List of tasks that can be cancelled
     */
    protected List<TaskCancellation> getAllCancellableTasks() {
        return getQueryGroupsToCancelFrom().stream()
            .flatMap(queryGroup -> getCancellableTasksFrom(queryGroup).stream())
            .collect(Collectors.toList());
    }

    /**
     * Get cancellable tasks from a specific queryGroup.
     *
     * @param queryGroup The QueryGroup from which to get cancellable tasks
     * @return List of tasks that can be cancelled
     */
    protected List<TaskCancellation> getCancellableTasksFrom(QueryGroup queryGroup) {
        return TRACKED_RESOURCES.stream()
            .filter(resourceType -> shouldCancelTasks(queryGroup, resourceType))
            .flatMap(resourceType -> getTaskCancellations(queryGroup, resourceType).stream())
            .collect(Collectors.toList());
    }

    private boolean shouldCancelTasks(QueryGroup queryGroup, ResourceType resourceType) {
        long reduceBy = getReduceBy(queryGroup, resourceType);
        return reduceBy > 0;
    }

    private List<TaskCancellation> getTaskCancellations(QueryGroup queryGroup, ResourceType resourceType) {
        return taskSelectionStrategy.selectTasksForCancellation(
            getAllTasksInQueryGroup(queryGroup.get_id()),
            getReduceBy(queryGroup, resourceType),
            resourceType
        );
    }

    private long getReduceBy(QueryGroup queryGroup, ResourceType resourceType) {
        Long usage = getUsage(queryGroup, resourceType);
        if (usage == null) {
            return 0;
        }
        return getUsage(queryGroup, resourceType) - queryGroup.getThresholdInLong(resourceType);
    }

    private Long getUsage(QueryGroup queryGroup, ResourceType resourceType) {
        return queryGroupLevelViews.get(queryGroup.get_id()).getResourceUsageData().get(resourceType);
    }

    private List<Task> getAllTasksInQueryGroup(String queryGroupId) {
        return queryGroupLevelViews.get(queryGroupId).getActiveTasks();
    }

    /**
     * Checks if the current resource usage is breaching the threshold of the provided resource limit.
     *
     * @param currentResourceUsage The current resource usage
     * @param resourceType The resource type to check against
     * @param threshold Threshold of the query group
     * @return true if the current resource usage is breaching the threshold, false otherwise
     */
    private boolean isBreachingThreshold(Map<ResourceType, Long> currentResourceUsage, ResourceType resourceType, long threshold) {
        return currentResourceUsage.get(resourceType) > threshold;
    }

    /**
     * Returns the resource usage of the QueryGroup with the provided ID.
     *
     * @param queryGroupId The ID of the QueryGroup
     * @return The resource usage of the QueryGroup
     */
    private Map<ResourceType, Long> getResourceUsage(String queryGroupId) {
        // if(QueryGroupLevelViews.get(queryGroupId) == null) {
        // return null;
        // }
        return queryGroupLevelViews.get(queryGroupId).getResourceUsageData();
    }
}
