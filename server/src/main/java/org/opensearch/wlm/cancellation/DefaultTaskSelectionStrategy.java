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
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.TaskCancellation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents an abstract task selection strategy.
 * This class implements the DefaultTaskSelectionStrategy interface and provides a method to select tasks for cancellation based on a sorting condition.
 * The specific sorting condition depends on the implementation.
 */
public class DefaultTaskSelectionStrategy {

    /**
     * Returns a comparator that defines the sorting condition for tasks.
     * This is the default implementation since the longest running tasks are the ones that consume the most resources.
     *
     * @return The comparator
     */
    public Comparator<QueryGroupTask> sortingCondition() {
        return Comparator.comparingLong(QueryGroupTask::getStartTime);
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
    public List<QueryGroupTask> selectTasksForCancellation(List<QueryGroupTask> tasks, long limit, ResourceType resourceType) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit has to be greater than zero");
        }
        if (limit == 0) {
            return Collections.emptyList();
        }

        List<QueryGroupTask> sortedTasks = tasks.stream().sorted(sortingCondition()).collect(Collectors.toList());

        List<QueryGroupTask> selectedTasks = new ArrayList<>();
        long accumulated = 0;
        for (QueryGroupTask task : sortedTasks) {
            selectedTasks.add(task);
            accumulated += resourceType.getResourceUsage(task);
            if (accumulated >= limit) {
                break;
            }
        }
        return selectedTasks;
    }

    /**
     * Selects tasks for cancellation from deleted query group.
     * This method iterates over the provided list of tasks and selects those that are instances of
     * {@link CancellableTask}. For each selected task, it creates a cancellation reason and adds
     * a {@link TaskCancellation} object to the list of selected tasks.
     *
     * @param tasks The list of {@link Task} objects to be evaluated for cancellation.
     * @return A list of {@link TaskCancellation} objects representing the tasks selected for cancellation.
     */
    public List<QueryGroupTask> selectTasksFromDeletedQueryGroup(List<QueryGroupTask> tasks) {
        return tasks.stream().filter(task -> task instanceof CancellableTask).collect(Collectors.toList());
    }
}
