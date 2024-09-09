/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.wlm.QueryGroupLevelResourceUsageView;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * This class tracks resource usage per QueryGroup
 */
public class QueryGroupResourceUsageTrackerService {
    public static final EnumSet<ResourceType> TRACKED_RESOURCES = EnumSet.allOf(ResourceType.class);
    private final TaskResourceTrackingService taskResourceTrackingService;

    /**
     * QueryGroupResourceTrackerService constructor
     *
     * @param taskResourceTrackingService Service that helps track resource usage of tasks running on a node.
     */
    public QueryGroupResourceUsageTrackerService(TaskResourceTrackingService taskResourceTrackingService, LongSupplier nanoTimeSupplier) {
        this.taskResourceTrackingService = taskResourceTrackingService;
        ResourceType.CPU.getResourceUsageCalculator().setNanoTimeSupplier(nanoTimeSupplier);
    }

    /**
     * Constructs a map of QueryGroupLevelResourceUsageView instances for each QueryGroup.
     *
     * @return Map of QueryGroup views
     */
    public Map<String, QueryGroupLevelResourceUsageView> constructQueryGroupLevelUsageViews() {
        final Map<String, List<QueryGroupTask>> tasksByQueryGroup = getTasksGroupedByQueryGroup();
        final Map<String, QueryGroupLevelResourceUsageView> queryGroupViews = new HashMap<>();

        // Iterate over each QueryGroup entry
        for (Map.Entry<String, List<QueryGroupTask>> queryGroupEntry : tasksByQueryGroup.entrySet()) {
            // Compute the QueryGroup resource usage
            final Map<ResourceType, Double> queryGroupUsage = new EnumMap<>(ResourceType.class);
            for (ResourceType resourceType : TRACKED_RESOURCES) {
                double usage = resourceType.getResourceUsageCalculator().calculateResourceUsage(queryGroupEntry.getValue());
                queryGroupUsage.put(resourceType, usage);
            }

            // Add to the QueryGroup View
            queryGroupViews.put(
                queryGroupEntry.getKey(),
                new QueryGroupLevelResourceUsageView(queryGroupUsage, queryGroupEntry.getValue())
            );
        }
        return queryGroupViews;
    }

    /**
     * Groups tasks by their associated QueryGroup.
     *
     * @return Map of tasks grouped by QueryGroup
     */
    private Map<String, List<QueryGroupTask>> getTasksGroupedByQueryGroup() {
        return taskResourceTrackingService.getResourceAwareTasks()
            .values()
            .stream()
            .filter(QueryGroupTask.class::isInstance)
            .map(QueryGroupTask.class::cast)
            .collect(Collectors.groupingBy(QueryGroupTask::getQueryGroupId, Collectors.mapping(task -> task, Collectors.toList())));
    }
}
