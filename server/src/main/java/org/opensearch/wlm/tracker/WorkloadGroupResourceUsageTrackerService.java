/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupLevelResourceUsageView;
import org.opensearch.wlm.WorkloadGroupTask;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class tracks resource usage per WorkloadGroup
 */
public class WorkloadGroupResourceUsageTrackerService {
    public static final EnumSet<ResourceType> TRACKED_RESOURCES = EnumSet.allOf(ResourceType.class);
    private final TaskResourceTrackingService taskResourceTrackingService;

    /**
     * WorkloadGroupResourceTrackerService constructor
     *
     * @param taskResourceTrackingService Service that helps track resource usage of tasks running on a node.
     */
    public WorkloadGroupResourceUsageTrackerService(TaskResourceTrackingService taskResourceTrackingService) {
        this.taskResourceTrackingService = taskResourceTrackingService;
    }

    /**
     * Constructs a map of WorkloadGroupLevelResourceUsageView instances for each WorkloadGroup.
     *
     * @return Map of WorkloadGroup views
     */
    public Map<String, WorkloadGroupLevelResourceUsageView> constructWorkloadGroupLevelUsageViews() {
        final Map<String, List<WorkloadGroupTask>> tasksByWorkloadGroup = getTasksGroupedByWorkloadGroup();
        final Map<String, WorkloadGroupLevelResourceUsageView> workloadGroupViews = new HashMap<>();

        // Iterate over each WorkloadGroup entry
        for (Map.Entry<String, List<WorkloadGroupTask>> workloadGroupEntry : tasksByWorkloadGroup.entrySet()) {
            // refresh the resource stats
            taskResourceTrackingService.refreshResourceStats(workloadGroupEntry.getValue().toArray(new WorkloadGroupTask[0]));
            // Compute the WorkloadGroup resource usage
            final Map<ResourceType, Double> workloadGroupUsage = new EnumMap<>(ResourceType.class);
            for (ResourceType resourceType : TRACKED_RESOURCES) {
                double usage = resourceType.getResourceUsageCalculator().calculateResourceUsage(workloadGroupEntry.getValue());
                workloadGroupUsage.put(resourceType, usage);
            }

            // Add to the WorkloadGroup View
            workloadGroupViews.put(
                workloadGroupEntry.getKey(),
                new WorkloadGroupLevelResourceUsageView(workloadGroupUsage, workloadGroupEntry.getValue())
            );
        }
        return workloadGroupViews;
    }

    /**
     * Groups tasks by their associated WorkloadGroup.
     *
     * @return Map of tasks grouped by WorkloadGroup
     */
    private Map<String, List<WorkloadGroupTask>> getTasksGroupedByWorkloadGroup() {
        return taskResourceTrackingService.getResourceAwareTasks()
            .values()
            .stream()
            .filter(WorkloadGroupTask.class::isInstance)
            .map(WorkloadGroupTask.class::cast)
            .filter(WorkloadGroupTask::isWorkloadGroupSet)
            .collect(Collectors.groupingBy(WorkloadGroupTask::getWorkloadGroupId, Collectors.mapping(task -> task, Collectors.toList())));
    }
}
