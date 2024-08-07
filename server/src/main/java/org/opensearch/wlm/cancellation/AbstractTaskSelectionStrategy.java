/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.opensearch.search.ResourceType;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

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
    public List<TaskCancellation> selectTasksForCancellation(List<Task> tasks, long limit, ResourceType resourceType) {
        if (limit < 0) {
            throw new IllegalArgumentException("reduceBy has to be greater than zero");
        }
        if (limit == 0) {
            return Collections.emptyList();
        }

        List<Task> sortedTasks = tasks.stream().sorted(sortingCondition()).collect(Collectors.toList());

        List<TaskCancellation> selectedTasks = new ArrayList<>();
        long accumulated = 0;

        for (Task task : sortedTasks) {
            if (task instanceof CancellableTask) {
                selectedTasks.add(createTaskCancellation((CancellableTask) task));
                accumulated += resourceType.getResourceUsage(task);
                if (accumulated >= limit) {
                    break;
                }
            }
        }
        return selectedTasks;
    }

    private TaskCancellation createTaskCancellation(CancellableTask task) {
        // TODO add correct reason and callbacks
        return new TaskCancellation(task, List.of(new TaskCancellation.Reason("limits exceeded", 5)), List.of(this::callbackOnCancel));
    }

    private void callbackOnCancel() {
        // todo Implement callback logic here mostly used for Stats
    }
}
