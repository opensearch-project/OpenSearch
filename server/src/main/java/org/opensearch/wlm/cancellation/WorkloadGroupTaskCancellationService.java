/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.WorkloadGroupLevelResourceUsageView;
import org.opensearch.wlm.WorkloadGroupTask;
import org.opensearch.wlm.WorkloadGroupsStateAccessor;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.stats.WorkloadGroupState;
import org.opensearch.wlm.tracker.WorkloadGroupResourceUsageTrackerService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opensearch.wlm.tracker.WorkloadGroupResourceUsageTrackerService.TRACKED_RESOURCES;

/**
 * Manages the cancellation of tasks enforced by WorkloadGroup thresholds on resource usage criteria.
 * This class utilizes a strategy pattern through {@link MaximumResourceTaskSelectionStrategy} to identify tasks that exceed
 * predefined resource usage limits and are therefore eligible for cancellation.
 *
 * <p>The cancellation process is initiated by evaluating the resource usage of each WorkloadGroup against its
 * resource limits. Tasks that contribute to exceeding these limits are selected for cancellation based on the
 * implemented task selection strategy.</p>
 *
 * <p>Instances of this class are configured with a map linking WorkloadGroup IDs to their corresponding resource usage
 * views, a set of active WorkloadGroups, and a task selection strategy. These components collectively facilitate the
 * identification and cancellation of tasks that threaten to breach WorkloadGroup resource limits.</p>
 *
 * @see MaximumResourceTaskSelectionStrategy
 * @see WorkloadGroup
 * @see ResourceType
 */
public class WorkloadGroupTaskCancellationService {
    public static final double MIN_VALUE = 1e-9;
    private static final Logger log = LogManager.getLogger(WorkloadGroupTaskCancellationService.class);

    private final WorkloadManagementSettings workloadManagementSettings;
    private final TaskSelectionStrategy taskSelectionStrategy;
    private final WorkloadGroupResourceUsageTrackerService resourceUsageTrackerService;
    // a map of WorkloadGroupId to its corresponding WorkloadGroupLevelResourceUsageView object
    Map<String, WorkloadGroupLevelResourceUsageView> workloadGroupLevelResourceUsageViews;
    private final WorkloadGroupsStateAccessor workloadGroupStateAccessor;

    public WorkloadGroupTaskCancellationService(
        WorkloadManagementSettings workloadManagementSettings,
        TaskSelectionStrategy taskSelectionStrategy,
        WorkloadGroupResourceUsageTrackerService resourceUsageTrackerService,
        WorkloadGroupsStateAccessor workloadGroupStateAccessor
    ) {
        this.workloadManagementSettings = workloadManagementSettings;
        this.taskSelectionStrategy = taskSelectionStrategy;
        this.resourceUsageTrackerService = resourceUsageTrackerService;
        this.workloadGroupStateAccessor = workloadGroupStateAccessor;
    }

    /**
     * Cancel tasks based on the implemented strategy.
     */
    public void cancelTasks(
        BooleanSupplier isNodeInDuress,
        Collection<WorkloadGroup> activeWorkloadGroups,
        Collection<WorkloadGroup> deletedWorkloadGroups
    ) {
        workloadGroupLevelResourceUsageViews = resourceUsageTrackerService.constructWorkloadGroupLevelUsageViews();
        // cancel tasks from WorkloadGroups that are in Enforced mode that are breaching their resource limits
        cancelTasks(ResiliencyMode.ENFORCED, activeWorkloadGroups);
        // if the node is in duress, cancel tasks accordingly.
        handleNodeDuress(isNodeInDuress, activeWorkloadGroups, deletedWorkloadGroups);

        updateResourceUsageInWorkloadGroupState(activeWorkloadGroups);
    }

    private void updateResourceUsageInWorkloadGroupState(Collection<WorkloadGroup> activeWorkloadGroups) {
        Set<String> isSearchWorkloadRunning = new HashSet<>();
        for (Map.Entry<String, WorkloadGroupLevelResourceUsageView> queryGroupLevelResourceUsageViewEntry : workloadGroupLevelResourceUsageViews
            .entrySet()) {
            isSearchWorkloadRunning.add(queryGroupLevelResourceUsageViewEntry.getKey());
            WorkloadGroupState queryGroupState = getWorkloadGroupState(queryGroupLevelResourceUsageViewEntry.getKey());
            TRACKED_RESOURCES.forEach(resourceType -> {
                final double currentUsage = queryGroupLevelResourceUsageViewEntry.getValue().getResourceUsageData().get(resourceType);
                queryGroupState.getResourceState().get(resourceType).setLastRecordedUsage(currentUsage);
            });
        }

        activeWorkloadGroups.forEach(queryGroup -> {
            if (!isSearchWorkloadRunning.contains(queryGroup.get_id())) {
                TRACKED_RESOURCES.forEach(
                    resourceType -> getWorkloadGroupState(queryGroup.get_id()).getResourceState().get(resourceType).setLastRecordedUsage(0.0)
                );
            }
        });
    }

    private void handleNodeDuress(
        BooleanSupplier isNodeInDuress,
        Collection<WorkloadGroup> activeWorkloadGroups,
        Collection<WorkloadGroup> deletedWorkloadGroups
    ) {
        if (!isNodeInDuress.getAsBoolean()) {
            return;
        }
        // List of tasks to be executed in order if the node is in duress
        List<Consumer<Void>> duressActions = List.of(
            v -> cancelTasksFromDeletedWorkloadGroups(deletedWorkloadGroups),
            v -> cancelTasks(ResiliencyMode.SOFT, activeWorkloadGroups)
        );

        for (Consumer<Void> duressAction : duressActions) {
            if (!isNodeInDuress.getAsBoolean()) {
                break;
            }
            duressAction.accept(null);
        }
    }

    private void cancelTasksFromDeletedWorkloadGroups(Collection<WorkloadGroup> deletedWorkloadGroups) {
        cancelTasks(getAllCancellableTasks(deletedWorkloadGroups));
    }

    /**
     * Get all cancellable tasks from the WorkloadGroups.
     *
     * @return List of tasks that can be cancelled
     */
    List<TaskCancellation> getAllCancellableTasks(ResiliencyMode resiliencyMode, Collection<WorkloadGroup> queryGroups) {
        return getAllCancellableTasks(
            queryGroups.stream().filter(queryGroup -> queryGroup.getResiliencyMode() == resiliencyMode).collect(Collectors.toList())
        );
    }

    /**
     * Get all cancellable tasks from the given WorkloadGroups.
     *
     * @return List of tasks that can be cancelled
     */
    List<TaskCancellation> getAllCancellableTasks(Collection<WorkloadGroup> queryGroups) {
        List<TaskCancellation> taskCancellations = new ArrayList<>();
        final List<Runnable> onCancelCallbacks = new ArrayList<>();
        for (WorkloadGroup queryGroup : queryGroups) {
            final List<TaskCancellation.Reason> reasons = new ArrayList<>();
            List<WorkloadGroupTask> selectedTasks = new ArrayList<>();
            for (ResourceType resourceType : TRACKED_RESOURCES) {
                // We need to consider the already selected tasks since those tasks also consumed the resources
                double excessUsage = getExcessUsage(queryGroup, resourceType) - resourceType.getResourceUsageCalculator()
                    .calculateResourceUsage(selectedTasks);
                if (excessUsage > MIN_VALUE) {
                    reasons.add(new TaskCancellation.Reason(generateReasonString(queryGroup, resourceType), 1));
                    onCancelCallbacks.add(this.getResourceTypeOnCancelCallback(queryGroup.get_id(), resourceType));
                    // Only add tasks not already added to avoid double cancellations
                    selectedTasks.addAll(
                        taskSelectionStrategy.selectTasksForCancellation(getTasksFor(queryGroup), excessUsage, resourceType)
                            .stream()
                            .filter(x -> selectedTasks.stream().noneMatch(y -> x.getId() != y.getId()))
                            .collect(Collectors.toList())
                    );
                }
            }

            if (!reasons.isEmpty()) {
                onCancelCallbacks.add(getWorkloadGroupState(queryGroup.get_id()).totalCancellations::inc);
                taskCancellations.addAll(
                    selectedTasks.stream().map(task -> new TaskCancellation(task, reasons, onCancelCallbacks)).collect(Collectors.toList())
                );
            }
        }
        return taskCancellations;
    }

    private String generateReasonString(WorkloadGroup queryGroup, ResourceType resourceType) {
        final double currentUsage = getCurrentUsage(queryGroup, resourceType);
        return "WorkloadGroup ID : "
            + queryGroup.get_id()
            + " breached the resource limit: ("
            + currentUsage
            + " > "
            + queryGroup.getResourceLimits().get(resourceType)
            + ") for resource type : "
            + resourceType.getName();
    }

    private List<WorkloadGroupTask> getTasksFor(WorkloadGroup queryGroup) {
        return workloadGroupLevelResourceUsageViews.get(queryGroup.get_id()).getActiveTasks();
    }

    private void cancelTasks(ResiliencyMode resiliencyMode, Collection<WorkloadGroup> queryGroups) {
        cancelTasks(getAllCancellableTasks(resiliencyMode, queryGroups));
    }

    private void cancelTasks(List<TaskCancellation> cancellableTasks) {

        Consumer<TaskCancellation> cancellationLoggingConsumer = (taskCancellation -> {
            log.warn(
                "Task {} is eligible for cancellation for reason {}",
                taskCancellation.getTask().getId(),
                taskCancellation.getReasonString()
            );
        });
        Consumer<TaskCancellation> cancellationConsumer = cancellationLoggingConsumer;
        if (workloadManagementSettings.getWlmMode() == WlmMode.ENABLED) {
            cancellationConsumer = (taskCancellation -> {
                cancellationLoggingConsumer.accept(taskCancellation);
                taskCancellation.cancel();
            });
        }
        cancellableTasks.forEach(cancellationConsumer);
    }

    private double getExcessUsage(WorkloadGroup queryGroup, ResourceType resourceType) {
        if (queryGroup.getResourceLimits().get(resourceType) == null
            || !workloadGroupLevelResourceUsageViews.containsKey(queryGroup.get_id())) {
            return 0;
        }
        return getCurrentUsage(queryGroup, resourceType) - getNormalisedThreshold(queryGroup, resourceType);
    }

    private double getCurrentUsage(WorkloadGroup queryGroup, ResourceType resourceType) {
        final WorkloadGroupLevelResourceUsageView queryGroupResourceUsageView = workloadGroupLevelResourceUsageViews.get(queryGroup.get_id());
        return queryGroupResourceUsageView.getResourceUsageData().get(resourceType);
    }

    /**
     * normalises configured value with respect to node level cancellation thresholds
     * @param queryGroup instance
     * @return normalised value with respect to node level cancellation thresholds
     */
    private double getNormalisedThreshold(WorkloadGroup queryGroup, ResourceType resourceType) {
        double nodeLevelCancellationThreshold = resourceType.getNodeLevelThreshold(workloadManagementSettings);
        return queryGroup.getResourceLimits().get(resourceType) * nodeLevelCancellationThreshold;
    }

    private Runnable getResourceTypeOnCancelCallback(String queryGroupId, ResourceType resourceType) {
        WorkloadGroupState queryGroupState = getWorkloadGroupState(queryGroupId);
        return queryGroupState.getResourceState().get(resourceType).cancellations::inc;
    }

    private WorkloadGroupState getWorkloadGroupState(String queryGroupId) {
        assert queryGroupId != null : "queryGroupId should never be null at this point.";

        return workloadGroupStateAccessor.getWorkloadGroupState(queryGroupId);
    }

    /**
     * Removes the queryGroups from deleted list if it doesn't have any tasks running
     */
    public void pruneDeletedWorkloadGroups(Collection<WorkloadGroup> deletedWorkloadGroups) {
        List<WorkloadGroup> currentDeletedWorkloadGroups = new ArrayList<>(deletedWorkloadGroups);
        for (WorkloadGroup queryGroup : currentDeletedWorkloadGroups) {
            if (workloadGroupLevelResourceUsageViews.get(queryGroup.get_id()).getActiveTasks().isEmpty()) {
                deletedWorkloadGroups.remove(queryGroup);
            }
        }
    }
}
