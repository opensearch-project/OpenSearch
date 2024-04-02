/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandbox;

import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
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
 * This class tracks requests per sandboxes
 */
public class SandboxedRequestTrackerService implements TaskManager.TaskEventListeners, SandboxResourceTracker, SandboxStatsReporter {

    private static final String CPU = "CPU";
    private static final String JVM_ALLOCATIONS = "JVM_Allocations";
    private static final int numberOfAvailableProcessors = Runtime.getRuntime().availableProcessors();
    private static final long totalAvailableJvmMemory = Runtime.getRuntime().totalMemory();
    private final LongSupplier timeNanosSupplier;
    /**
     * Sandbox ids which are marked for deletion in between the @link SandboxService runs
     */
    private List<String> toDeleteSandboxes;
    /**
     * It is used to track the task to sandbox mapping which will be useful to remove it from the @link tasksPerSandbox
     */
    private final TaskManager taskManager;
    private final TaskResourceTrackingService taskResourceTrackingService;

    public SandboxedRequestTrackerService(
        TaskManager taskManager,
        TaskResourceTrackingService taskResourceTrackingService,
        LongSupplier timeNanosSupplier
    ) {
        this.taskManager = taskManager;
        this.taskResourceTrackingService = taskResourceTrackingService;
        toDeleteSandboxes = Collections.synchronizedList(new ArrayList<>());
        this.timeNanosSupplier = timeNanosSupplier;
    }

    @Override
    public void updateSandboxResourceUsages() {

    }

    @Override
    public SandboxStatsHolder getStats() {
        return null;
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

    public void pruneSandboxes() {
        toDeleteSandboxes = toDeleteSandboxes.stream().filter(this::hasUnfinishedTasks).collect(Collectors.toList());
    }

    public boolean hasUnfinishedTasks(String sandboxId) {
        return false;
    }

    @Override
    public void onTaskCompleted(Task task) {}

    public void cancelViolatingTasks() {
        List<TaskCancellation> cancellableTasks = getCancellableTasks();
        for (TaskCancellation taskCancellation : cancellableTasks) {
            taskCancellation.cancel();
        }
    }

    private List<TaskCancellation> getCancellableTasks() {
        List<String> inViolationSandboxes = getBreachingSandboxIds();
        List<TaskCancellation> cancellableTasks = new ArrayList<>();
        for (String sandboxId : inViolationSandboxes) {
            cancellableTasks.addAll(getCancellableTasksFrom(sandboxId));
        }
        return cancellableTasks;
    }

    public SandboxStatsHolder getSandboxLevelStats() {
        return null;
    }

    private List<String> getBreachingSandboxIds() {
        return Collections.emptyList();
    }

    private List<TaskCancellation> getCancellableTasksFrom(String sandboxId) {
        return Collections.emptyList();
    }
}
