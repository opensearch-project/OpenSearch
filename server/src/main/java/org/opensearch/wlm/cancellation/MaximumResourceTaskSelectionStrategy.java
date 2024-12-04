/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.wlm.cancellation.QueryGroupTaskCancellationService.MIN_VALUE;

/**
 * Represents the highest resource consuming task first selection strategy.
 */
public class MaximumResourceTaskSelectionStrategy implements TaskSelectionStrategy {

    public MaximumResourceTaskSelectionStrategy() {}

    /**
     * Returns a comparator that defines the sorting condition for tasks.
     * This is the default implementation since the most resource consuming tasks are the likely to regress the performance.
     * from resiliency point of view it makes sense to cancel them first
     *
     * @return The comparator
     */
    private Comparator<QueryGroupTask> sortingCondition(ResourceType resourceType) {
        return Comparator.comparingDouble(task -> resourceType.getResourceUsageCalculator().calculateTaskResourceUsage(task));
    }

    /**
     * Selects tasks for cancellation based on the provided limit and resource type.
     * The tasks are sorted based on the sorting condition and then selected until the accumulated resource usage reaches the limit.
     *
     * @param tasks The list of tasks from which to select
     * @param limit The limit on the accumulated resource usage
     * @param resourceType The type of resource to consider
     * @return The list of selected tasks
     * @throws IllegalArgumentException If the limit is less than zero
     */
    public List<QueryGroupTask> selectTasksForCancellation(List<QueryGroupTask> tasks, double limit, ResourceType resourceType) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit has to be greater than zero");
        }
        if (limit < MIN_VALUE) {
            return Collections.emptyList();
        }

        List<QueryGroupTask> sortedTasks = tasks.stream().sorted(sortingCondition(resourceType).reversed()).collect(Collectors.toList());

        List<QueryGroupTask> selectedTasks = new ArrayList<>();
        double accumulated = 0;
        for (QueryGroupTask task : sortedTasks) {
            selectedTasks.add(task);
            accumulated += resourceType.getResourceUsageCalculator().calculateTaskResourceUsage(task);
            if ((accumulated - limit) > MIN_VALUE) {
                break;
            }
        }
        return selectedTasks;
    }
}
