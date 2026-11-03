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
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadGroupLevelResourceUsageView;
import org.opensearch.wlm.WorkloadGroupTask;
import org.opensearch.wlm.WorkloadGroupsStateAccessor;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.stats.WorkloadGroupState;
import org.opensearch.wlm.tracker.WorkloadGroupResourceUsageTrackerService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
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
        // Read the raw state map, because getWorkloadGroupState falls back to DEFAULT once a deleted group's state is removed,
        // which would write the deleted group's usage into DEFAULT's. Groups with no state of their own are skipped
        final Map<String, WorkloadGroupState> workloadGroupStateMap = workloadGroupStateAccessor.getWorkloadGroupStateMap();
        Set<String> isSearchWorkloadRunning = new HashSet<>();
        for (Map.Entry<
            String,
            WorkloadGroupLevelResourceUsageView> workloadGroupLevelResourceUsageViewEntry : workloadGroupLevelResourceUsageViews
                .entrySet()) {
            isSearchWorkloadRunning.add(workloadGroupLevelResourceUsageViewEntry.getKey());
            WorkloadGroupState workloadGroupState = workloadGroupStateMap.get(workloadGroupLevelResourceUsageViewEntry.getKey());
            if (workloadGroupState == null) {
                continue;
            }
            TRACKED_RESOURCES.forEach(resourceType -> {
                final double currentUsage = workloadGroupLevelResourceUsageViewEntry.getValue().getResourceUsageData().get(resourceType);
                workloadGroupState.getResourceState().get(resourceType).setLastRecordedUsage(currentUsage);
            });
        }

        activeWorkloadGroups.forEach(workloadGroup -> {
            final WorkloadGroupState workloadGroupState = workloadGroupStateMap.get(workloadGroup.get_id());
            if (!isSearchWorkloadRunning.contains(workloadGroup.get_id()) && workloadGroupState != null) {
                TRACKED_RESOURCES.forEach(
                    resourceType -> workloadGroupState.getResourceState().get(resourceType).setLastRecordedUsage(0.0)
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
    List<TaskCancellation> getAllCancellableTasks(ResiliencyMode resiliencyMode, Collection<WorkloadGroup> workloadGroups) {
        return getAllCancellableTasks(
            workloadGroups.stream()
                .filter(workloadGroup -> workloadGroup.getResiliencyMode() == resiliencyMode)
                .collect(Collectors.toList())
        );
    }

    /**
     * Get all cancellable tasks from the given WorkloadGroups.
     *
     * @return List of tasks that can be cancelled
     */
    List<TaskCancellation> getAllCancellableTasks(Collection<WorkloadGroup> workloadGroups) {
        List<TaskCancellation> taskCancellations = new ArrayList<>();
        for (WorkloadGroup workloadGroup : workloadGroups) {
            final List<TaskCancellation.Reason> reasons = new ArrayList<>();
            List<WorkloadGroupTask> selectedTasks = new ArrayList<>();
            final Set<Long> selectedTaskIds = new HashSet<>();
            // The tasks selected for each breached resource. Each task is selected for one resource only
            final Map<ResourceType, List<WorkloadGroupTask>> selectedTasksByResource = new EnumMap<>(ResourceType.class);
            for (ResourceType resourceType : TRACKED_RESOURCES) {
                // We need to consider the already selected tasks since those tasks also consumed the resources
                double excessUsage = getExcessUsage(workloadGroup, resourceType) - resourceType.getResourceUsageCalculator()
                    .calculateResourceUsage(selectedTasks);
                if (excessUsage > MIN_VALUE) {
                    reasons.add(new TaskCancellation.Reason(generateReasonString(workloadGroup, resourceType), 1));
                    // Only offer the tasks not selected yet. The excess already deducts the usage of the selected tasks, so
                    // offering them again would count that usage twice, and could select a task twice
                    final List<WorkloadGroupTask> unselectedTasks = getTasksFor(workloadGroup).stream()
                        .filter(task -> selectedTaskIds.contains(task.getId()) == false)
                        .collect(Collectors.toList());
                    final List<WorkloadGroupTask> tasksForResource = taskSelectionStrategy.selectTasksForCancellation(
                        unselectedTasks,
                        excessUsage,
                        resourceType
                    );
                    tasksForResource.forEach(task -> selectedTaskIds.add(task.getId()));
                    selectedTasks.addAll(tasksForResource);
                    selectedTasksByResource.put(resourceType, tasksForResource);
                }
            }

            if (!reasons.isEmpty()) {
                // Built per group, so a task's cancellation is counted under its own group and not under the other breaching
                // groups. A deleted group has no state left, so its cancellations go to DEFAULT, like its completions
                final Runnable totalCancellationsCallback = getWorkloadGroupState(workloadGroup.get_id()).totalCancellations::inc;
                selectedTasksByResource.forEach((resourceType, tasks) -> {
                    // Every task carries the reasons of all the breached resources, but counts as a cancellation only for the
                    // resource it was selected for, so the per resource cancellations add up to the total
                    final List<Runnable> onCancelCallbacks = List.of(
                        getResourceTypeOnCancelCallback(workloadGroup.get_id(), resourceType),
                        totalCancellationsCallback
                    );
                    tasks.forEach(task -> taskCancellations.add(new TaskCancellation(task, reasons, onCancelCallbacks)));
                });
            }
        }
        return taskCancellations;
    }

    private String generateReasonString(WorkloadGroup workloadGroup, ResourceType resourceType) {
        final double currentUsage = getCurrentUsage(workloadGroup, resourceType);
        return "WorkloadGroup ID : "
            + workloadGroup.get_id()
            + " breached the resource limit: ("
            + currentUsage
            + " > "
            + workloadGroup.getResourceLimits().get(resourceType)
            + ") for resource type : "
            + resourceType.getName();
    }

    private List<WorkloadGroupTask> getTasksFor(WorkloadGroup workloadGroup) {
        return workloadGroupLevelResourceUsageViews.get(workloadGroup.get_id()).getActiveTasks();
    }

    private void cancelTasks(ResiliencyMode resiliencyMode, Collection<WorkloadGroup> workloadGroups) {
        cancelTasks(getAllCancellableTasks(resiliencyMode, workloadGroups));
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

    private double getExcessUsage(WorkloadGroup workloadGroup, ResourceType resourceType) {
        if (workloadGroup.getResourceLimits().get(resourceType) == null
            || !workloadGroupLevelResourceUsageViews.containsKey(workloadGroup.get_id())) {
            return 0;
        }
        return getCurrentUsage(workloadGroup, resourceType) - getNormalisedThreshold(workloadGroup, resourceType);
    }

    private double getCurrentUsage(WorkloadGroup workloadGroup, ResourceType resourceType) {
        final WorkloadGroupLevelResourceUsageView workloadGroupResourceUsageView = workloadGroupLevelResourceUsageViews.get(
            workloadGroup.get_id()
        );
        return workloadGroupResourceUsageView.getResourceUsageData().get(resourceType);
    }

    /**
     * normalises configured value with respect to node level cancellation thresholds
     * @param workloadGroup instance
     * @return normalised value with respect to node level cancellation thresholds
     */
    private double getNormalisedThreshold(WorkloadGroup workloadGroup, ResourceType resourceType) {
        double nodeLevelCancellationThreshold = resourceType.getNodeLevelThreshold(workloadManagementSettings);
        return workloadGroup.getResourceLimits().get(resourceType) * nodeLevelCancellationThreshold;
    }

    private Runnable getResourceTypeOnCancelCallback(String workloadGroupId, ResourceType resourceType) {
        WorkloadGroupState workloadGroupState = getWorkloadGroupState(workloadGroupId);
        return workloadGroupState.getResourceState().get(resourceType).cancellations::inc;
    }

    private WorkloadGroupState getWorkloadGroupState(String workloadGroupId) {
        assert workloadGroupId != null : "workloadGroupId should never be null at this point.";

        return workloadGroupStateAccessor.getWorkloadGroupState(workloadGroupId);
    }

    /**
     * Removes the workloadGroups from deleted list if it doesn't have any tasks running
     */
    public void pruneDeletedWorkloadGroups(Collection<WorkloadGroup> deletedWorkloadGroups) {
        // No views until cancelTasks first runs, so nothing is known about the tasks yet. Keep the groups for the next run
        if (workloadGroupLevelResourceUsageViews == null) {
            return;
        }
        List<WorkloadGroup> currentDeletedWorkloadGroups = new ArrayList<>(deletedWorkloadGroups);
        for (WorkloadGroup workloadGroup : currentDeletedWorkloadGroups) {
            // Views exist only for the groups with tasks on this node, so a group without a view has no active tasks
            final WorkloadGroupLevelResourceUsageView view = workloadGroupLevelResourceUsageViews.get(workloadGroup.get_id());
            if (view == null || view.getActiveTasks().isEmpty()) {
                deletedWorkloadGroups.remove(workloadGroup);
            }
        }
    }
}
