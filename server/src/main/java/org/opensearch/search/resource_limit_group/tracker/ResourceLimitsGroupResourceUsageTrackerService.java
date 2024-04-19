/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resource_limit_group.tracker;

import org.opensearch.common.inject.Inject;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.search.resource_limit_group.ResourceLimitGroupPruner;
import org.opensearch.search.resource_limit_group.cancellation.ResourceLimitGroupRequestCanceller;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * This class tracks requests per resourceLimitGroups
 */
public class ResourceLimitsGroupResourceUsageTrackerService
    implements
        TaskManager.TaskEventListeners,
        ResourceLimitGroupResourceUsageTracker,
        ResourceLimitGroupRequestCanceller,
        ResourceLimitGroupPruner {

    private static final String CPU = "CPU";
    private static final String JVM_ALLOCATIONS = "JVM_Allocations";
    private static final int numberOfAvailableProcessors = Runtime.getRuntime().availableProcessors();
    private static final long totalAvailableJvmMemory = Runtime.getRuntime().totalMemory();
    private final LongSupplier timeNanosSupplier;
    /**
     * ResourceLimitGroup ids which are marked for deletion in between the
     * {@link org.opensearch.search.resource_limit_group.ResourceLimitGroupService} runs
     */
    private List<String> toDeleteResourceLimitGroups;
    private List<Object> activeResourceLimitGroups;
    private final TaskManager taskManager;
    private final TaskResourceTrackingService taskResourceTrackingService;

    /**
     * ResourceLimitsGroupResourceUsageTrackerService constructor
     * @param taskManager
     * @param taskResourceTrackingService
     */
    @Inject
    public ResourceLimitsGroupResourceUsageTrackerService(
        TaskManager taskManager,
        TaskResourceTrackingService taskResourceTrackingService
    ) {
        this.taskManager = taskManager;
        this.taskResourceTrackingService = taskResourceTrackingService;
        toDeleteResourceLimitGroups = Collections.synchronizedList(new ArrayList<>());
        this.timeNanosSupplier = System::nanoTime;
    }

    @Override
    public void updateResourceLimitGroupsResourceUsage() {

    }

    private AbsoluteResourceUsage calculateAbsoluteResourceUsageFor(Task task) {
        TaskResourceUsage taskResourceUsage = task.getTotalResourceStats();
        long cpuTimeInNanos = taskResourceUsage.getCpuTimeInNanos();
        long jvmAllocations = taskResourceUsage.getMemoryInBytes();
        long taskElapsedTime = timeNanosSupplier.getAsLong() - task.getStartTimeNanos();
        return new AbsoluteResourceUsage(
            (cpuTimeInNanos * 1.0f) / (taskElapsedTime * numberOfAvailableProcessors),
            ((jvmAllocations * 1.0f) / totalAvailableJvmMemory)
        );
    }

    /**
     * Value holder class for resource usage in absolute terms with respect to system/process mem
     */
    private static class AbsoluteResourceUsage {
        private final double absoluteCpuUsage;
        private final double absoluteJvmAllocationsUsage;

        public AbsoluteResourceUsage(double absoluteCpuUsage, double absoluteJvmAllocationsUsage) {
            this.absoluteCpuUsage = absoluteCpuUsage;
            this.absoluteJvmAllocationsUsage = absoluteJvmAllocationsUsage;
        }

        public static AbsoluteResourceUsage merge(AbsoluteResourceUsage a, AbsoluteResourceUsage b) {
            return new AbsoluteResourceUsage(
                a.absoluteCpuUsage + b.absoluteCpuUsage,
                a.absoluteJvmAllocationsUsage + b.absoluteJvmAllocationsUsage
            );
        }

        public double getAbsoluteCpuUsageInPercentage() {
            return absoluteCpuUsage * 100;
        }

        public double getAbsoluteJvmAllocationsUsageInPercent() {
            return absoluteJvmAllocationsUsage * 100;
        }
    }

    /**
     * filter out the deleted Resource Limit Groups which still has unfinished tasks
     */
    public void pruneResourceLimitGroup() {
        toDeleteResourceLimitGroups = toDeleteResourceLimitGroups.stream().filter(this::hasUnfinishedTasks).collect(Collectors.toList());
    }

    private boolean hasUnfinishedTasks(String sandboxId) {
        return false;
    }

    /**
     * method to handle the completed tasks
     * @param task represents completed task on the node
     */
    @Override
    public void onTaskCompleted(Task task) {}

    /**
     * This method will select the Resource Limit Groups violating the enforced constraints
     * and cancel the tasks from the violating Resource Limit Groups
     * Cancellation happens in two scenarios
     * <ol>
     *     <li> If the Resource Limit Group is of enforced type and it is breaching its cancellation limit for the threshold </li>
     *     <li> Node is in duress and Resource Limit Groups which are breaching the cancellation thresholds will have cancellations </li>
     * </ol>
     */
    @Override
    public void cancelViolatingTasks() {
        List<TaskCancellation> cancellableTasks = getCancellableTasks();
        for (TaskCancellation taskCancellation : cancellableTasks) {
            taskCancellation.cancel();
        }
    }

    /**
     *
     * @return list of cancellable tasks
     */
    private List<TaskCancellation> getCancellableTasks() {
        // get cancellations from enforced type Resource Limit Groups
        List<String> inViolationSandboxes = getBreachingSandboxIds();
        List<TaskCancellation> cancellableTasks = new ArrayList<>();
        for (String sandboxId : inViolationSandboxes) {
            cancellableTasks.addAll(getCancellableTasksFrom(sandboxId));
        }
        return cancellableTasks;
    }

    public void deleteSandbox(String sandboxId) {
        if (hasUnfinishedTasks(sandboxId)) {
            toDeleteResourceLimitGroups.add(sandboxId);
        }
        // remove this sandbox from the active sandboxes
    }

    private List<String> getBreachingSandboxIds() {
        return Collections.emptyList();
    }

    private List<TaskCancellation> getCancellableTasksFrom(String sandboxId) {
        return Collections.emptyList();
    }
}
