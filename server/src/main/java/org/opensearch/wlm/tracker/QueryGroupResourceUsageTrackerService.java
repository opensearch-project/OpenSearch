/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.search.ResourceType;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.wlm.QueryGroupHelper;
import org.opensearch.wlm.QueryGroupLevelResourceUsageView;
import org.opensearch.wlm.QueryGroupTask;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class tracks resource usage per QueryGroup
 */
public class QueryGroupResourceUsageTrackerService implements QueryGroupUsageTracker, TaskManager.TaskEventListeners {

    public static final List<ResourceType> TRACKED_RESOURCES = List.of(ResourceType.values());
    private final TaskManager taskManager;
    private final TaskResourceTrackingService taskResourceTrackingService;

    /**
     * QueryGroupResourceTrackerService constructor
     *
     * @param taskManager Task Manager service for keeping track of currently running tasks on the nodes
     * @param taskResourceTrackingService Service that helps track resource usage of tasks running on a node.
     */
    public QueryGroupResourceUsageTrackerService(
        final TaskManager taskManager,
        final TaskResourceTrackingService taskResourceTrackingService
    ) {
        this.taskManager = taskManager;
        this.taskResourceTrackingService = taskResourceTrackingService;
    }

    /**
     * Constructs a map of QueryGroupLevelResourceUsageView instances for each QueryGroup.
     *
     * @return Map of QueryGroup views
     */
    @Override
    public Map<String, QueryGroupLevelResourceUsageView> constructQueryGroupLevelUsageViews() {
        final Map<String, List<Task>> tasksByQueryGroup = getTasksGroupedByQueryGroup();
        final Map<String, QueryGroupLevelResourceUsageView> queryGroupViews = new HashMap<>();

        // Iterate over each QueryGroup entry
        for (Map.Entry<String, List<Task>> queryGroupEntry : tasksByQueryGroup.entrySet()) {
            // Compute the QueryGroup usage
            final EnumMap<ResourceType, Long> queryGroupUsage = new EnumMap<>(ResourceType.class);
            for (ResourceType resourceType : TRACKED_RESOURCES) {
                long queryGroupResourceUsage = 0;
                for (Task task : queryGroupEntry.getValue()) {
                    queryGroupResourceUsage += QueryGroupHelper.getResourceUsage(resourceType, task);
                }
                queryGroupUsage.put(resourceType, queryGroupResourceUsage);
            }

            // Add to the QueryGroup View
            queryGroupViews.put(
                queryGroupEntry.getKey(),
                new QueryGroupLevelResourceUsageView(queryGroupUsage, queryGroupEntry.getValue())
            );
        }
        return queryGroupViews;
    }

    /**
     * Groups tasks by their associated QueryGroup.
     *
     * @return Map of tasks grouped by QueryGroup
     */
    private Map<String, List<Task>> getTasksGroupedByQueryGroup() {
        return taskResourceTrackingService.getResourceAwareTasks()
            .values()
            .stream()
            .filter(QueryGroupTask.class::isInstance)
            .map(QueryGroupTask.class::cast)
            .collect(Collectors.groupingBy(QueryGroupTask::getQueryGroupId, Collectors.mapping(task -> (Task) task, Collectors.toList())));
    }

    /**
     * Handles the completion of a task.
     *
     * @param task The completed task
     */
    @Override
    public void onTaskCompleted(Task task) {}
}
