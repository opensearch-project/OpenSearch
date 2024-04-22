/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resource_limit_group.tracker;

import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.search.resource_limit_group.ResourceLimitGroupPruner;
import org.opensearch.search.resource_limit_group.ResourceLimitGroupTask;
import org.opensearch.search.resource_limit_group.cancellation.CancellableTaskSelector;
import org.opensearch.search.resource_limit_group.cancellation.ResourceLimitGroupTaskCanceller;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * This class tracks requests per resourceLimitGroups
 */
public class ResourceLimitsGroupResourceUsageTrackerService extends ResourceLimitGroupTaskCanceller
    implements
        TaskManager.TaskEventListeners,
        ResourceLimitGroupResourceUsageTracker,
        ResourceLimitGroupPruner {

    private static final String CPU = "cpu";
    private static final String JVM = "jvm";
    private static final List<String> TRACKED_RESOURCES = List.of(JVM);
    private static final int numberOfAvailableProcessors = Runtime.getRuntime().availableProcessors();
    private static final long totalAvailableJvmMemory = Runtime.getRuntime().totalMemory();
    private final LongSupplier timeNanosSupplier;
    /**
     * ResourceLimitGroup ids which are marked for deletion in between the
     * {@link org.opensearch.search.resource_limit_group.ResourceLimitGroupService} runs
     */
    private List<String> toDeleteResourceLimitGroups;
    private List<ResourceLimitGroup> activeResourceLimitGroups;
    /**
     * This var will hold the sandbox level resource usage as per last run of
     * {@link org.opensearch.search.resource_limit_group.ResourceLimitGroupService}
     */
    private Map<String, Map<String, Double>> resourceUsage;
    /**
     * We are keeping this as instance member as we will need this to identify the contended resource limit groups
     * resourceLimitGroupName -> List&lt;ResourceLimitGroupTask&gt;
     */
    private Map<String, List<Task>> resourceLimitGroupTasks;
    private final TaskManager taskManager;
    private final TaskResourceTrackingService taskResourceTrackingService;
    private final ClusterService clusterService;
    private final CancellableTaskSelector taskSelector;

    /**
     * SandboxResourceTrackerService constructor
     * @param taskManager
     * @param taskResourceTrackingService
     * @param clusterService
     */
    @Inject
    public ResourceLimitsGroupResourceUsageTrackerService(
        final TaskManager taskManager,
        final TaskResourceTrackingService taskResourceTrackingService,
        final ClusterService clusterService,
        final LongSupplier timeNanosSupplier,
        final CancellableTaskSelector taskSelector
    ) {
        super(taskSelector);
        this.taskManager = taskManager;
        this.taskResourceTrackingService = taskResourceTrackingService;
        toDeleteResourceLimitGroups = new ArrayList<>();
        this.clusterService = clusterService;
        this.timeNanosSupplier = timeNanosSupplier;
        this.taskSelector = taskSelector;
    }

    @Override
    public void updateResourceLimitGroupsResourceUsage() {
        activeResourceLimitGroups = new ArrayList<>(clusterService.state().metadata().resourceLimitGroups().values());

        updateResourceLimitGroupTasks();

        refreshResourceLimitGroupsUsage(resourceLimitGroupTasks);
    }

    private void updateResourceLimitGroupTasks() {
        List<ResourceLimitGroupTask> activeTasks = taskResourceTrackingService.getResourceAwareTasks()
            .values()
            .stream()
            .filter(task -> task instanceof ResourceLimitGroupTask)
            .map(task -> (ResourceLimitGroupTask) task)
            .collect(Collectors.toList());

        Map<String, List<Task>> newResourceLimitGroupTasks = new HashMap<>();
        for (Map.Entry<String, List<ResourceLimitGroupTask>> entry : activeTasks.stream()
            .collect(Collectors.groupingBy(ResourceLimitGroupTask::getResourceLimitGroupName))
            .entrySet()) {
            newResourceLimitGroupTasks.put(entry.getKey(), entry.getValue().stream().map(task -> (Task) task).collect(Collectors.toList()));
        }

        resourceLimitGroupTasks = newResourceLimitGroupTasks;
    }

    private void refreshResourceLimitGroupsUsage(Map<String, List<Task>> resourceLimitGroupTasks) {
        /**
         * remove the deleted resource limit groups
         */
        final List<String> nonExistingResourceLimitGroups = new ArrayList<>(resourceUsage.keySet());
        for (String activeResourceLimitGroup : resourceLimitGroupTasks.keySet()) {
            nonExistingResourceLimitGroups.remove(activeResourceLimitGroup);
        }
        nonExistingResourceLimitGroups.forEach(resourceUsage::remove);

        for (Map.Entry<String, List<Task>> resourceLimitGroup : resourceLimitGroupTasks.entrySet()) {
            final String resourceLimitGroupName = resourceLimitGroup.getKey();

            Map<String, Double> resourceLimitGroupUsage = resourceLimitGroup.getValue()
                .stream()
                .map(this::calculateAbsoluteResourceUsageFor)
                .reduce(new AbsoluteResourceUsage(0, 0), AbsoluteResourceUsage::merge)
                .toMap();

            resourceUsage.put(resourceLimitGroupName, resourceLimitGroupUsage);

        }
    }

    // @Override
    // public SandboxStatsHolder getStats() {
    // return null;
    // }

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

        /**
         *
         * @return {@code Map<String, Double>}
         * which captures key as resource and value as the percentage value
         */
        public Map<String, Double> toMap() {
            Map<String, Double> map = new HashMap<>();
            // We can put the additional resources into this map in the future
            map.put(JVM, getAbsoluteJvmAllocationsUsageInPercent());
            return map;
        }
    }

    /**
     * filter out the deleted sandboxes which still has unfi
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
     * This method will select the sandboxes violating the enforced constraints
     * and cancel the tasks from the violating sandboxes
     * Cancellation happens in two scenarios
     * <ol>
     *     <li> If the sandbox is of enforced type and it is breaching its cancellation limit for the threshold </li>
     *     <li> Node is in duress and sandboxes which are breaching the cancellation thresholds will have cancellations </li>
     * </ol>
     */
    @Override
    public void cancelTasks() {
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
        // get cancellations from enforced type sandboxes
        final List<ResourceLimitGroup> inViolationResourceLimitGroups = getBreachingResourceLimitGroups();
        final List<ResourceLimitGroup> enforcedResourceLimitGroups = inViolationResourceLimitGroups.stream()
            .filter(resourceLimitGroup -> resourceLimitGroup.getMode().equals(ResourceLimitGroup.ResourceLimitGroupMode.ENFORCED))
            .collect(Collectors.toList());
        List<TaskCancellation> cancellableTasks = new ArrayList<>();

        for (ResourceLimitGroup resourceLimitGroup : enforcedResourceLimitGroups) {
            cancellableTasks.addAll(getCancellableTasksFrom(resourceLimitGroup));
        }

        // get cancellations from soft type sandboxes if the node is in duress (hitting node level cancellation
        // threshold)

        return cancellableTasks;
    }

    public void deleteSandbox(String sandboxName) {
        if (hasUnfinishedTasks(sandboxName)) {
            toDeleteResourceLimitGroups.add(sandboxName);
        }
        // remove this sandbox from the active sandboxes
        activeResourceLimitGroups = activeResourceLimitGroups.stream()
            .filter(resourceLimitGroup -> !resourceLimitGroup.getName().equals(sandboxName))
            .collect(Collectors.toList());
    }

    private List<ResourceLimitGroup> getBreachingResourceLimitGroups() {
        final List<ResourceLimitGroup> breachingResourceLimitGroupNames = new ArrayList<>();

        for (ResourceLimitGroup resourceLimitGroup : activeResourceLimitGroups) {
            Map<String, Double> currentResourceUsage = resourceUsage.get(resourceLimitGroup.getName());
            boolean isBreaching = false;

            for (ResourceLimitGroup.ResourceLimit resourceLimit : resourceLimitGroup.getResourceLimits()) {
                if (currentResourceUsage.get(resourceLimit.getResourceName()) > resourceLimit.getValue()) {
                    isBreaching = true;
                    break;
                }
            }

            if (isBreaching) breachingResourceLimitGroupNames.add(resourceLimitGroup);
        }

        return breachingResourceLimitGroupNames;
    }

    List<TaskCancellation> getCancellableTasksFrom(ResourceLimitGroup resourceLimitGroup) {
        List<TaskCancellation> cancellations = new ArrayList<>();
        for (String resource : TRACKED_RESOURCES) {
            final double reduceBy = resourceUsage.get(resourceLimitGroup.getName()).get(resource) - resourceLimitGroup.getResourceLimitFor(
                resource
            ).getValue();
            /**
             * if the resource is not defined for this sandbox then ignore cancellations from it
             */
            if (reduceBy < 0.0) {
                continue;
            }
            cancellations.addAll(taskSelector.selectTasks(resourceLimitGroupTasks.get(resourceLimitGroup.getName()), reduceBy, resource));
        }
        return cancellations;
    }
}
