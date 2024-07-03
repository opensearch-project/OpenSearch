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
import org.opensearch.search.sandboxing.resourcetype.SystemResource;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.search.sandboxing.tracker.SandboxResourceUsageTrackerService.TRACKED_RESOURCES;

/**
 * Manages the cancellation of tasks enforced by sandbox thresholds on resource usage criteria.
 * This class utilizes a strategy pattern through {@link TaskSelectionStrategy} to identify tasks that exceed
 * predefined resource usage limits and are therefore eligible for cancellation.
 *
 * <p>The cancellation process is initiated by evaluating the resource usage of each sandbox against its
 * resource limits. Tasks that contribute to exceeding these limits are selected for cancellation based on the
 * implemented task selection strategy.</p>
 *
 * <p>Instances of this class are configured with a map linking sandbox IDs to their corresponding resource usage
 * views, a set of active sandboxes, and a task selection strategy. These components collectively facilitate the
 * identification and cancellation of tasks that threaten to breach sandbox resource limits.</p>
 *
 * @see TaskSelectionStrategy
 * @see Sandbox
 * @see SystemResource
 */
public class DefaultTaskCancellation {
    protected final TaskSelectionStrategy taskSelectionStrategy;
    // a map of sandboxId to its corresponding SandboxLevelResourceUsageView object
    protected final Map<String, SandboxLevelResourceUsageView> sandboxLevelViews;
    protected final Set<Sandbox> activeSandboxes;

    public DefaultTaskCancellation(
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
     * returns the list of sandboxes breaching their resource limits.
     *
     * @return List of sandboxes
     */
    public List<Sandbox> getSandboxesToCancelFrom() {
        final List<Sandbox> sandboxesToCancelFrom = new ArrayList<>();

        for (Sandbox sandbox : this.activeSandboxes) {
            Map<SystemResource, Long> currentResourceUsage = getResourceUsage(sandbox.getId());
            // if(currentResourceUsage == null) {
            // // skip if the sandbox is not found
            // continue;
            // }

            for (Sandbox.ResourceLimit resourceLimit : sandbox.getResourceLimits()) {
                if (isBreachingThreshold(currentResourceUsage, resourceLimit)) {
                    sandboxesToCancelFrom.add(sandbox);
                    break;
                }
            }
        }

        return sandboxesToCancelFrom;
    }

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

    private boolean shouldCancelTasks(Sandbox sandbox, SystemResource resourceType) {
        long reduceBy = getReduceBy(sandbox, resourceType);
        return reduceBy > 0;
    }

    private List<TaskCancellation> getTaskCancellations(Sandbox sandbox, SystemResource resourceType) {
        return taskSelectionStrategy.selectTasksForCancellation(
            getAllTasksInSandbox(sandbox.getId()),
            getReduceBy(sandbox, resourceType),
            resourceType
        );
    }

    private long getReduceBy(Sandbox sandbox, SystemResource resourceType) {
        Long usage = getUsage(sandbox, resourceType);
        if (usage == null) {
            return 0;
        }
        return getUsage(sandbox, resourceType) - sandbox.getResourceLimitFor(resourceType).getThresholdInLong();
    }

    private Long getUsage(Sandbox sandbox, SystemResource resourceType) {
        return sandboxLevelViews.get(sandbox.getId()).getResourceUsageData().get(resourceType);
    }

    private List<Task> getAllTasksInSandbox(String sandboxId) {
        return sandboxLevelViews.get(sandboxId).getActiveTasks();
    }

    /**
     * Checks if the current resource usage is breaching the threshold of the provided resource limit.
     *
     * @param currentResourceUsage The current resource usage
     * @param resourceLimit The resource limit to check against
     * @return true if the current resource usage is breaching the threshold, false otherwise
     */
    private boolean isBreachingThreshold(Map<SystemResource, Long> currentResourceUsage, Sandbox.ResourceLimit resourceLimit) {
        return currentResourceUsage.get(resourceLimit.getResourceType()) > resourceLimit.getThreshold();
    }

    /**
     * Returns the resource usage of the sandbox with the provided ID.
     *
     * @param sandboxId The ID of the sandbox
     * @return The resource usage of the sandbox
     */
    private Map<SystemResource, Long> getResourceUsage(String sandboxId) {
        // if(sandboxLevelViews.get(sandboxId) == null) {
        // return null;
        // }
        return sandboxLevelViews.get(sandboxId).getResourceUsageData();
    }
}
