/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.tracker;

import org.opensearch.common.inject.Inject;
import org.opensearch.search.sandboxing.SandboxLevelResourceUsageView;
import org.opensearch.search.sandboxing.resourcetype.SandboxResourceType;
import org.opensearch.search.sandboxing.SandboxTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class tracks requests per Sandbox
 */
//@ExperimentalApi
public class SandboxResourceUsageTrackerService implements SandboxUsageTracker, TaskManager.TaskEventListeners {

    public static final List<SandboxResourceType> TRACKED_RESOURCES = List.of(SandboxResourceType.fromString("JVM"));

    private final TaskManager taskManager;
    private final TaskResourceTrackingService taskResourceTrackingService;

    /**
     * SandboxResourceTrackerService constructor
     *
     * @param taskManager
     * @param taskResourceTrackingService
     */
    @Inject
    public SandboxResourceUsageTrackerService(
        final TaskManager taskManager,
        final TaskResourceTrackingService taskResourceTrackingService
    ) {
        this.taskManager = taskManager;
        this.taskResourceTrackingService = taskResourceTrackingService;
    }

    /**
     * Constructs a map of SandboxLevelResourceUsageView instances for each sandbox.
     *
     * @return Map of sandbox views
     */
    @Override
    public Map<String, SandboxLevelResourceUsageView> constructSandboxLevelUsageViews() {
        Map<String, SandboxLevelResourceUsageView> sandboxViews = new HashMap<>();

        Map<String, List<Task>> tasksBySandbox = getTasksGroupedBySandbox();
        Map<String, Map<SandboxResourceType, Long>> sandboxResourceUsage = getResourceUsageOfSandboxes(tasksBySandbox);

        for(String sandboxId : tasksBySandbox.keySet()) {
            SandboxLevelResourceUsageView sandboxLevelResourceUsageView = new SandboxLevelResourceUsageView(
                sandboxId,
                sandboxResourceUsage.get(sandboxId),
                tasksBySandbox.get(sandboxId)
                );
            sandboxViews.put(sandboxId, sandboxLevelResourceUsageView);
        }
        return sandboxViews;
    }

    /**
     * Groups tasks by their associated sandbox.
     *
     * @return Map of tasks grouped by sandbox
     */
    private Map<String, List<Task>> getTasksGroupedBySandbox() {
        return taskResourceTrackingService.getResourceAwareTasks()
            .values()
            .stream()
            .filter(SandboxTask.class::isInstance)
            .map(SandboxTask.class::cast)
            .collect(Collectors.groupingBy(
                SandboxTask::getSandboxId,
                Collectors.mapping(task -> (Task) task, Collectors.toList())
            ));
    }

    /**
     * Calculates the resource usage of each sandbox.
     *
     * @param tasksBySandbox Map of tasks grouped by sandbox
     * @return Map of resource usage for each sandbox
     */
    private Map<String, Map<SandboxResourceType, Long>> getResourceUsageOfSandboxes(Map<String, List<Task>> tasksBySandbox) {
        Map<String, Map<SandboxResourceType, Long>> resourceUsageOfSandboxes = new HashMap<>();

        // Iterate over each sandbox entry
        for (Map.Entry<String, List<Task>> sandboxEntry : tasksBySandbox.entrySet()) {
            String sandboxId = sandboxEntry.getKey();
            List<Task> tasks = sandboxEntry.getValue();

            // Prepare a usage map for the current sandbox, or retrieve the existing one
            Map<SandboxResourceType, Long> sandboxUsage = resourceUsageOfSandboxes.computeIfAbsent(sandboxId, k -> new HashMap<>());

            // Accumulate resource usage for each task in the sandbox
            for (Task task : tasks) {
                for (SandboxResourceType resourceType : TRACKED_RESOURCES) {
                    long currentUsage = sandboxUsage.getOrDefault(resourceType, 0L);
                    long taskUsage = resourceType.getResourceUsage(task);
                    sandboxUsage.put(resourceType, currentUsage + taskUsage);
                }
            }
        }
        return resourceUsageOfSandboxes;
    }

    /**
     * Handles the completion of a task.
     *
     * @param task The completed task
     */
    @Override
    public void onTaskCompleted(Task task) {
    }
}
