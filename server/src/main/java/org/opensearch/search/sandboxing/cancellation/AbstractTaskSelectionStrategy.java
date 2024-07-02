/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.cancellation;

import org.opensearch.search.sandboxing.resourcetype.SandboxResourceType;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents an abstract task selection strategy.
 * This class implements the TaskSelectionStrategy interface and provides a method to select tasks for cancellation based on a sorting condition.
 * The specific sorting condition depends on the implementation.
 */
public abstract class AbstractTaskSelectionStrategy implements TaskSelectionStrategy {

    /**
     * Returns a comparator that defines the sorting condition for tasks.
     * The specific sorting condition depends on the implementation.
     *
     * @return The comparator
     */
    public abstract Comparator<Task> sortingCondition();

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
    @Override
    public List<Task> selectTasksForCancellation(List<Task> tasks, long limit, SandboxResourceType resourceType) {
        if (limit < 0) {
            throw new IllegalArgumentException("reduceBy has to be greater than zero");
        }
        if (limit == 0) {
            return Collections.emptyList();
        }

        List<Task> sortedTasks = tasks.stream().sorted(sortingCondition()).collect(Collectors.toList());

        List<Task> selectedTasks = new ArrayList<>();
        long accumulated = 0;

        for (Task task : sortedTasks) {
            selectedTasks.add(task);
            accumulated += resourceType.getResourceUsage(task);
            if (accumulated >= limit) {
                break;
            }
        }
        return selectedTasks;
    }
}
