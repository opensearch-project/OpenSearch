/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.querygroup.tracker;

import org.opensearch.common.inject.Inject;
import org.opensearch.search.querygroup.QueryGroupLevelResourceUsageView;
import org.opensearch.search.querygroup.QueryGroupTask;
import org.opensearch.search.resourcetypes.ResourceType;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class tracks requests per QueryGroup
 */
// @ExperimentalApi
public class QueryGroupResourceUsageTrackerService implements QueryGroupUsageTracker, TaskManager.TaskEventListeners {

    public static final List<ResourceType> TRACKED_RESOURCES = List.of(ResourceType.fromName("JVM"), ResourceType.fromName("CPU"));

    private final TaskManager taskManager;
    private final TaskResourceTrackingService taskResourceTrackingService;

    /**
     * QueryGroupResourceTrackerService constructor
     *
     * @param taskManager Task Manager service for keeping track of currently running tasks on the nodes
     * @param taskResourceTrackingService Service that helps track resource usage of tasks running on a node.
     */
    @Inject
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
        Map<String, QueryGroupLevelResourceUsageView> queryGroupViews = new HashMap<>();

        Map<String, List<Task>> tasksByQueryGroup = getTasksGroupedByQueryGroup();
        Map<String, Map<ResourceType, Long>> queryGroupResourceUsage = getResourceUsageOfQueryGroups(tasksByQueryGroup);

        for (String queryGroupId : tasksByQueryGroup.keySet()) {
            QueryGroupLevelResourceUsageView queryGroupLevelResourceUsageView = new QueryGroupLevelResourceUsageView(
                queryGroupId,
                queryGroupResourceUsage.get(queryGroupId),
                tasksByQueryGroup.get(queryGroupId)
            );
            queryGroupViews.put(queryGroupId, queryGroupLevelResourceUsageView);
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
     * Calculates the resource usage of each QueryGroup.
     *
     * @param tasksByQueryGroup Map of tasks grouped by QueryGroup
     * @return Map of resource usage for each QueryGroup
     */
    private Map<String, Map<ResourceType, Long>> getResourceUsageOfQueryGroups(Map<String, List<Task>> tasksByQueryGroup) {
        Map<String, Map<ResourceType, Long>> resourceUsageOfQueryGroups = new HashMap<>();

        // Iterate over each QueryGroup entry
        for (Map.Entry<String, List<Task>> queryGroupEntry : tasksByQueryGroup.entrySet()) {
            String queryGroupId = queryGroupEntry.getKey();
            List<Task> tasks = queryGroupEntry.getValue();

            // Prepare a usage map for the current QueryGroup, or retrieve the existing one
            Map<ResourceType, Long> queryGroupUsage = resourceUsageOfQueryGroups.computeIfAbsent(queryGroupId, k -> new HashMap<>());

            // Accumulate resource usage for each task in the QueryGroup
            for (Task task : tasks) {
                for (ResourceType resourceType : TRACKED_RESOURCES) {
                    long currentUsage = queryGroupUsage.getOrDefault(resourceType, 0L);
                    long taskUsage = resourceType.getResourceUsage(task);
                    // task.getTotalResourceStats().getCpuTimeInNanos();
                    queryGroupUsage.put(resourceType, currentUsage + taskUsage);
                }
            }
        }
        return resourceUsageOfQueryGroups;
    }

    /**
     * Handles the completion of a task.
     *
     * @param task The completed task
     */
    @Override
    public void onTaskCompleted(Task task) {}
}
