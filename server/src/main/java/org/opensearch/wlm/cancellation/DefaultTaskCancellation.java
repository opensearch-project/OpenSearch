/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.wlm.ResourceType;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.wlm.QueryGroupLevelResourceUsageView;
import org.opensearch.wlm.WorkloadManagementSettings;

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
 * This class utilizes a strategy pattern through {@link DefaultTaskSelectionStrategy} to identify tasks that exceed
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
 * @see DefaultTaskSelectionStrategy
 * @see QueryGroup
 * @see ResourceType
 */
public class DefaultTaskCancellation {
    private static final long HEAP_SIZE_BYTES = JvmStats.jvmStats().getMem().getHeapMax().getBytes();

    protected final WorkloadManagementSettings workloadManagementSettings;
    protected final DefaultTaskSelectionStrategy defaultTaskSelectionStrategy;
    // a map of QueryGroupId to its corresponding QueryGroupLevelResourceUsageView object
    protected final Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelResourceUsageViews;
    protected final Collection<QueryGroup> activeQueryGroups;
    protected final Collection<QueryGroup> deletedQueryGroups;
    protected BooleanSupplier isNodeInDuress;

    public DefaultTaskCancellation(
        WorkloadManagementSettings workloadManagementSettings,
        DefaultTaskSelectionStrategy defaultTaskSelectionStrategy,
        Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelResourceUsageViews,
        Collection<QueryGroup> activeQueryGroups,
        Collection<QueryGroup> deletedQueryGroups,
        BooleanSupplier isNodeInDuress
    ) {
        this.workloadManagementSettings = workloadManagementSettings;
        this.defaultTaskSelectionStrategy = defaultTaskSelectionStrategy;
        this.queryGroupLevelResourceUsageViews = queryGroupLevelResourceUsageViews;
        this.activeQueryGroups = activeQueryGroups;
        this.deletedQueryGroups = deletedQueryGroups;
        this.isNodeInDuress = isNodeInDuress;
    }

    /**
     * Cancel tasks based on the implemented strategy.
     */
    public final void cancelTasks() {
        // cancel tasks from QueryGroups that are in Enforced mode that are breaching their resource limits
        cancelTasks(QueryGroup.ResiliencyMode.ENFORCED);
        // if the node is in duress, cancel tasks accordingly.
        handleNodeDuress();
    }

    private void handleNodeDuress() {
        if (!isNodeInDuress.getAsBoolean()) {
            return;
        }
        // List of tasks to be executed in order if the node is in duress
        List<Consumer<Void>> duressActions = List.of(
            v -> cancelTasksFromDeletedQueryGroups(),
            v -> cancelTasks(QueryGroup.ResiliencyMode.SOFT)
        );

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
    protected List<TaskCancellation> getAllCancellableTasks(QueryGroup.ResiliencyMode resiliencyMode) {
        return getAllCancellableTasks(getQueryGroupsToCancelFrom(resiliencyMode));
    }

    /**
     * Get all cancellable tasks from the given QueryGroups.
     *
     * @return List of tasks that can be cancelled
     */
    protected List<TaskCancellation> getAllCancellableTasks(Collection<QueryGroup> queryGroups) {
        return queryGroups.stream().flatMap(queryGroup -> getCancellableTasksFrom(queryGroup).stream()).collect(Collectors.toList());
    }

    /**
     * returns the list of QueryGroups breaching their resource limits.
     *
     * @return List of QueryGroups
     */
    private List<QueryGroup> getQueryGroupsToCancelFrom(QueryGroup.ResiliencyMode resiliencyMode) {
        final List<QueryGroup> queryGroupsToCancelFrom = new ArrayList<>();

        for (QueryGroup queryGroup : this.activeQueryGroups) {
            if (queryGroup.getResiliencyMode() != resiliencyMode) {
                continue;
            }
            Map<ResourceType, Long> queryGroupResourceUsage = queryGroupLevelResourceUsageViews.get(queryGroup.get_id())
                .getResourceUsageData();

            for (ResourceType resourceType : TRACKED_RESOURCES) {
                if (queryGroup.getResourceLimits().containsKey(resourceType) && queryGroupResourceUsage.containsKey(resourceType)) {
                    Double resourceLimit = (Double) queryGroup.getResourceLimits().get(resourceType);
                    Long resourceUsage = queryGroupResourceUsage.get(resourceType);

                    if (isBreachingThreshold(resourceType, resourceLimit, resourceUsage)) {
                        queryGroupsToCancelFrom.add(queryGroup);
                        break;
                    }
                }
            }
        }

        return queryGroupsToCancelFrom;
    }

    private void cancelTasks(QueryGroup.ResiliencyMode resiliencyMode) {
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
    protected List<TaskCancellation> getCancellableTasksFrom(QueryGroup queryGroup) {
        return TRACKED_RESOURCES.stream()
            .filter(resourceType -> shouldCancelTasks(queryGroup, resourceType))
            .flatMap(resourceType -> getTaskCancellations(queryGroup, resourceType).stream())
            .collect(Collectors.toList());
    }

    private boolean shouldCancelTasks(QueryGroup queryGroup, ResourceType resourceType) {
        return getReduceBy(queryGroup, resourceType) > 0;
    }

    private List<TaskCancellation> getTaskCancellations(QueryGroup queryGroup, ResourceType resourceType) {
        List<Task> selectedTasksToCancel = defaultTaskSelectionStrategy.selectTasksForCancellation(
            queryGroupLevelResourceUsageViews.get(queryGroup.get_id()).getActiveTasks(),
            getReduceBy(queryGroup, resourceType),
            resourceType
        );
        List<TaskCancellation> taskCancellations = new ArrayList<>();
        for (Task task : selectedTasksToCancel) {
            String cancellationReason = createCancellationReason(queryGroup, task, resourceType);
            taskCancellations.add(createTaskCancellation((CancellableTask) task, cancellationReason));
        }
        return taskCancellations;
    }

    private String createCancellationReason(QueryGroup querygroup, Task task, ResourceType resourceType) {
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
        return ((Double) (querygroup.getResourceLimits().get(resourceType))) * 100;
    }

    private TaskCancellation createTaskCancellation(CancellableTask task, String cancellationReason) {
        return new TaskCancellation(task, List.of(new TaskCancellation.Reason(cancellationReason, 5)), List.of(this::callbackOnCancel));
    }

    protected List<TaskCancellation> getTaskCancellationsForDeletedQueryGroup(QueryGroup queryGroup) {
        List<Task> tasks = defaultTaskSelectionStrategy.selectTasksFromDeletedQueryGroup(
            queryGroupLevelResourceUsageViews.get(queryGroup.get_id()).getActiveTasks()
        );
        List<TaskCancellation> taskCancellations = new ArrayList<>();
        for (Task task : tasks) {
            String cancellationReason = "[Workload Management] Cancelling Task ID : "
                + task.getId()
                + " from QueryGroup ID : "
                + queryGroup.get_id();
            taskCancellations.add(createTaskCancellation((CancellableTask) task, cancellationReason));
        }
        return taskCancellations;
    }

    private long getReduceBy(QueryGroup queryGroup, ResourceType resourceType) {
        if (queryGroup.getResourceLimits().get(resourceType) == null) {
            return 0;
        }
        Double threshold = (Double) queryGroup.getResourceLimits().get(resourceType);
        return getResourceUsage(queryGroup, resourceType) - convertThresholdIntoLong(resourceType, threshold);
    }

    private Long convertThresholdIntoLong(ResourceType resourceType, Double resourceThresholdInPercentage) {
        Long threshold = null;
        if (resourceType == ResourceType.MEMORY) {
            // Check if resource usage is breaching the threshold
            double nodeLevelCancellationThreshold = this.workloadManagementSettings.getNodeLevelMemoryCancellationThreshold()
                * HEAP_SIZE_BYTES;
            threshold = (long) (resourceThresholdInPercentage * nodeLevelCancellationThreshold);
        } else if (resourceType == ResourceType.CPU) {
            // Get the total CPU time of the process in milliseconds
            long cpuTotalTimeInMillis = ProcessProbe.getInstance().getProcessCpuTotalTime();
            double nodeLevelCancellationThreshold = this.workloadManagementSettings.getNodeLevelCpuCancellationThreshold()
                * cpuTotalTimeInMillis;
            // Check if resource usage is breaching the threshold
            threshold = (long) (resourceThresholdInPercentage * nodeLevelCancellationThreshold);
        }
        return threshold;
    }

    private Long getResourceUsage(QueryGroup queryGroup, ResourceType resourceType) {
        if (!queryGroupLevelResourceUsageViews.containsKey(queryGroup.get_id())) {
            return 0L;
        }
        return queryGroupLevelResourceUsageViews.get(queryGroup.get_id()).getResourceUsageData().get(resourceType);
    }

    private boolean isBreachingThreshold(ResourceType resourceType, Double resourceThresholdInPercentage, long resourceUsage) {
        if (resourceType == ResourceType.MEMORY) {
            // Check if resource usage is breaching the threshold
            return resourceUsage > convertThresholdIntoLong(resourceType, resourceThresholdInPercentage);
        }
        // Resource types should be CPU, resourceUsage is in nanoseconds, convert to milliseconds
        long resourceUsageInMillis = resourceUsage / 1_000_000;
        // Check if resource usage is breaching the threshold
        return resourceUsageInMillis > convertThresholdIntoLong(resourceType, resourceThresholdInPercentage);
    }

    private void callbackOnCancel() {
        // TODO Implement callback logic here mostly used for Stats
    }
}
