/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.cancellation;

import org.opensearch.cluster.metadata.Sandbox;
import org.opensearch.search.sandboxing.SandboxLevelResourceUsageView;
import org.opensearch.search.sandboxing.resourcetype.SandboxResourceType;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.search.sandboxing.tracker.SandboxResourceUsageTrackerService.TRACKED_RESOURCES;

/**
 * Abstract class that provides a structure for task cancellation.
 * This class is extended by other classes to provide specific task cancellation strategies.
 */
public abstract class AbstractTaskCancellation {
    // Strategy for selecting tasks to cancel
    protected final TaskSelectionStrategy taskSelectionStrategy;
    // Views of resource usage at the sandbox level
    protected final Map<String, SandboxLevelResourceUsageView> sandboxLevelViews;
    // Set of active sandboxes
    protected final Set<Sandbox> activeSandboxes;

    public AbstractTaskCancellation(
        TaskSelectionStrategy taskSelectionStrategy,
        Map<String, SandboxLevelResourceUsageView> sandboxLevelViews,
        Set<Sandbox> activeSandboxes
    ) {
        this.taskSelectionStrategy = taskSelectionStrategy;
        this.sandboxLevelViews = sandboxLevelViews;
        this.activeSandboxes = activeSandboxes;
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
     * Abstract method to get the list of sandboxes from which tasks can be cancelled.
     * This method needs to be implemented by subclasses.
     *
     * @return List of sandboxes
     */
    abstract List<Sandbox> getSandboxesToCancelFrom();

    /**
     * Get all cancellable tasks from the sandboxes.
     *
     * @return List of tasks that can be cancelled
     */
    protected List<TaskCancellation> getAllCancellableTasks() {
        return getSandboxesToCancelFrom().stream()
            .flatMap(sandbox -> getCancellableTasksFrom(sandbox).stream())
            .collect(Collectors.toList());
    }

    /**
     * Get cancellable tasks from a specific sandbox.
     *
     * @param sandbox The sandbox from which to get cancellable tasks
     * @return List of tasks that can be cancelled
     */
    protected List<TaskCancellation> getCancellableTasksFrom(Sandbox sandbox) {
        return TRACKED_RESOURCES.stream()
            .filter(resourceType -> shouldCancelTasks(sandbox, resourceType))
            .flatMap(resourceType -> getTaskCancellations(sandbox, resourceType).stream())
            .collect(Collectors.toList());
    }

    private boolean shouldCancelTasks(Sandbox sandbox, SandboxResourceType resourceType) {
        long reduceBy = getReduceBy(sandbox, resourceType);
        return reduceBy > 0;
    }

    private List<TaskCancellation> getTaskCancellations(Sandbox sandbox, SandboxResourceType resourceType) {
        return taskSelectionStrategy.selectTasksForCancellation(
                getAllTasksInSandbox(sandbox.getId()),
                getReduceBy(sandbox, resourceType),
                resourceType)
            .stream()
            .map(task -> createTaskCancellation((CancellableTask) task))
            .collect(Collectors.toList());
    }

    private long getReduceBy(Sandbox sandbox, SandboxResourceType resourceType) {
        return getUsage(sandbox, resourceType) - sandbox.getResourceLimitFor(resourceType).getThresholdInLong();
    }

    private Long getUsage(Sandbox sandbox, SandboxResourceType resourceType) {
        return sandboxLevelViews.get(sandbox.getId()).getResourceUsageData().get(resourceType);
    }

    private List<Task> getAllTasksInSandbox(String sandboxId) {
        return sandboxLevelViews.get(sandboxId).getActiveTasks();
    }

    private TaskCancellation createTaskCancellation(CancellableTask task) {
        // todo add reasons and callbacks
        return new TaskCancellation(task, List.of(), List.of(this::callbackOnCancel));
    }

    private void callbackOnCancel() {
        // todo Implement callback logic here
        System.out.println("Task was cancelled.");
    }
}
