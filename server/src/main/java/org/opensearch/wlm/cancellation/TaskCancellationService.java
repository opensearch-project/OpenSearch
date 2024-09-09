/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.wlm.MutableQueryGroupFragment.ResiliencyMode;
import org.opensearch.wlm.QueryGroupLevelResourceUsageView;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService.TRACKED_RESOURCES;

/**
 * Manages the cancellation of tasks enforced by QueryGroup thresholds on resource usage criteria.
 * This class utilizes a strategy pattern through {@link MaximumResourceTaskSelectionStrategy} to identify tasks that exceed
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
 * @see MaximumResourceTaskSelectionStrategy
 * @see QueryGroup
 * @see ResourceType
 */
public class TaskCancellationService {
    public static final double MIN_VALUE = 1e-9;

    private final WorkloadManagementSettings workloadManagementSettings;
    private final TaskSelectionStrategy taskSelectionStrategy;
    private final QueryGroupResourceUsageTrackerService resourceUsageTrackerService;
    // a map of QueryGroupId to its corresponding QueryGroupLevelResourceUsageView object
    Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelResourceUsageViews;
    private final Collection<QueryGroup> activeQueryGroups;
    private final Collection<QueryGroup> deletedQueryGroups;
    private BooleanSupplier isNodeInDuress;

    public TaskCancellationService(
        WorkloadManagementSettings workloadManagementSettings,
        TaskSelectionStrategy taskSelectionStrategy,
        QueryGroupResourceUsageTrackerService resourceUsageTrackerService,
        Collection<QueryGroup> activeQueryGroups,
        Collection<QueryGroup> deletedQueryGroups,
        BooleanSupplier isNodeInDuress
    ) {
        this.workloadManagementSettings = workloadManagementSettings;
        this.taskSelectionStrategy = taskSelectionStrategy;
        this.resourceUsageTrackerService = resourceUsageTrackerService;
        this.activeQueryGroups = activeQueryGroups;
        this.deletedQueryGroups = deletedQueryGroups;
        this.isNodeInDuress = isNodeInDuress;
    }

    /**
     * Cancel tasks based on the implemented strategy.
     */
    public final void cancelTasks() {
        queryGroupLevelResourceUsageViews = resourceUsageTrackerService.constructQueryGroupLevelUsageViews();
        // cancel tasks from QueryGroups that are in Enforced mode that are breaching their resource limits
        cancelTasks(ResiliencyMode.ENFORCED);
        // if the node is in duress, cancel tasks accordingly.
        handleNodeDuress();
    }

    private void handleNodeDuress() {
        if (!isNodeInDuress.getAsBoolean()) {
            return;
        }
        // List of tasks to be executed in order if the node is in duress
        List<Consumer<Void>> duressActions = List.of(v -> cancelTasksFromDeletedQueryGroups(), v -> cancelTasks(ResiliencyMode.SOFT));

        for (Consumer<Void> duressAction : duressActions) {
            if (!isNodeInDuress.getAsBoolean()) {
                break;
            }
            duressAction.accept(null);
        }
    }

    private void cancelTasksFromDeletedQueryGroups() {
        cancelTasks(getAllCancellableTasks(this.deletedQueryGroups));
    }

    /**
     * Get all cancellable tasks from the QueryGroups.
     *
     * @return List of tasks that can be cancelled
     */
    List<TaskCancellation> getAllCancellableTasks(ResiliencyMode resiliencyMode) {
        return getAllCancellableTasks(getQueryGroupsToCancelFrom(resiliencyMode));
    }

    /**
     * Get all cancellable tasks from the given QueryGroups.
     *
     * @return List of tasks that can be cancelled
     */
    List<TaskCancellation> getAllCancellableTasks(Collection<QueryGroup> queryGroups) {
        return queryGroups.stream().flatMap(queryGroup -> getCancellableTasksFrom(queryGroup).stream()).collect(Collectors.toList());
    }

    /**
     * returns the list of QueryGroups breaching their resource limits.
     *
     * @return List of QueryGroups
     */
    private List<QueryGroup> getQueryGroupsToCancelFrom(ResiliencyMode resiliencyMode) {
        final List<QueryGroup> queryGroupsToCancelFrom = new ArrayList<>();

        for (QueryGroup queryGroup : this.activeQueryGroups) {
            if (queryGroup.getResiliencyMode() != resiliencyMode) {
                continue;
            }
            for (ResourceType resourceType : TRACKED_RESOURCES) {
                if (queryGroup.getResourceLimits().containsKey(resourceType)) {
                    if (shouldCancelTasks(queryGroup, resourceType)) {
                        queryGroupsToCancelFrom.add(queryGroup);
                        break;
                    }

                }
            }
        }

        return queryGroupsToCancelFrom;
    }

    private void cancelTasks(ResiliencyMode resiliencyMode) {
        cancelTasks(getAllCancellableTasks(resiliencyMode));
    }

    private void cancelTasks(List<TaskCancellation> cancellableTasks) {
        cancellableTasks.forEach(TaskCancellation::cancel);
    }

    /**
     * Get cancellable tasks from a specific queryGroup.
     *
     * @param queryGroup The QueryGroup from which to get cancellable tasks
     * @return List of tasks that can be cancelled
     */
    List<TaskCancellation> getCancellableTasksFrom(QueryGroup queryGroup) {
        return TRACKED_RESOURCES.stream()
            .filter(resourceType -> shouldCancelTasks(queryGroup, resourceType))
            .flatMap(resourceType -> getTaskCancellations(queryGroup, resourceType).stream())
            .collect(Collectors.toList());
    }

    private boolean shouldCancelTasks(QueryGroup queryGroup, ResourceType resourceType) {
        return getExcessUsage(queryGroup, resourceType) > 0;
    }

    private List<TaskCancellation> getTaskCancellations(QueryGroup queryGroup, ResourceType resourceType) {
        List<QueryGroupTask> selectedTasksToCancel = taskSelectionStrategy.selectTasksForCancellation(
            queryGroupLevelResourceUsageViews.get(queryGroup.get_id()).getActiveTasks(),
            getExcessUsage(queryGroup, resourceType),
            resourceType
        );
        List<TaskCancellation> taskCancellations = new ArrayList<>();
        for (QueryGroupTask task : selectedTasksToCancel) {
            String cancellationReason = createCancellationReason(queryGroup, task, resourceType);
            taskCancellations.add(createTaskCancellation(task, cancellationReason));
        }
        return taskCancellations;
    }

    private String createCancellationReason(QueryGroup querygroup, QueryGroupTask task, ResourceType resourceType) {
        Double thresholdInPercent = getThresholdInPercent(querygroup, resourceType);
        return "[Workload Management] Cancelling Task ID : "
            + task.getId()
            + " from QueryGroup ID : "
            + querygroup.get_id()
            + " breached the resource limit of : "
            + thresholdInPercent
            + " for resource type : "
            + resourceType.getName();
    }

    private Double getThresholdInPercent(QueryGroup querygroup, ResourceType resourceType) {
        return querygroup.getResourceLimits().get(resourceType) * 100;
    }

    private TaskCancellation createTaskCancellation(CancellableTask task, String cancellationReason) {
        return new TaskCancellation(task, List.of(new TaskCancellation.Reason(cancellationReason, 5)), List.of(this::callbackOnCancel));
    }

    List<TaskCancellation> getTaskCancellationsForDeletedQueryGroup(QueryGroup queryGroup) {
        List<QueryGroupTask> tasks = queryGroupLevelResourceUsageViews.get(queryGroup.get_id()).getActiveTasks();

        List<TaskCancellation> taskCancellations = new ArrayList<>();
        for (QueryGroupTask task : tasks) {
            String cancellationReason = "[Workload Management] Cancelling Task ID : "
                + task.getId()
                + " from QueryGroup ID : "
                + queryGroup.get_id();
            taskCancellations.add(createTaskCancellation(task, cancellationReason));
        }
        return taskCancellations;
    }

    private double getExcessUsage(QueryGroup queryGroup, ResourceType resourceType) {
        if (queryGroup.getResourceLimits().get(resourceType) == null
            || !queryGroupLevelResourceUsageViews.containsKey(queryGroup.get_id())) {
            return 0;
        }

        final QueryGroupLevelResourceUsageView queryGroupResourceUsageView = queryGroupLevelResourceUsageViews.get(queryGroup.get_id());
        final double currentUsage = queryGroupResourceUsageView.getResourceUsageData().get(resourceType);
        return currentUsage - getNormalisedThreshold(queryGroup, resourceType);
    }

    /**
     * normalises configured value with respect to node level cancellation thresholds
     * @param queryGroup instance
     * @return normalised value with respect to node level cancellation thresholds
     */
    private double getNormalisedThreshold(QueryGroup queryGroup, ResourceType resourceType) {
        double nodeLevelCancellationThreshold = resourceType.getNodeLevelThreshold(workloadManagementSettings);
        return queryGroup.getResourceLimits().get(resourceType) * nodeLevelCancellationThreshold;
    }

    private void callbackOnCancel() {
        // TODO Implement callback logic here mostly used for Stats
    }
}
