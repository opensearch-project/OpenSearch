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
import org.opensearch.wlm.tracker.TaskResourceUsageCalculator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.wlm.cancellation.DefaultTaskCancellation.MIN_VALUE;

/**
 * Represents an abstract task selection strategy.
 * This class implements the DefaultTaskSelectionStrategy interface and provides a method to select tasks for cancellation based on a sorting condition.
 * The specific sorting condition depends on the implementation.
 */
public class DefaultTaskSelectionStrategy {

    private final Supplier<Long> nanoTimeSupplier;

    public DefaultTaskSelectionStrategy() {
        this(System::nanoTime);
    }

    public DefaultTaskSelectionStrategy(Supplier<Long> nanoTimeSupplier) {
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

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
    public List<QueryGroupTask> selectTasksForCancellation(List<QueryGroupTask> tasks, double limit, ResourceType resourceType) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit has to be greater than zero");
        }
        if (limit < MIN_VALUE) {
            return Collections.emptyList();
        }

        List<QueryGroupTask> sortedTasks = tasks.stream().sorted(sortingCondition()).collect(Collectors.toList());

        List<QueryGroupTask> selectedTasks = new ArrayList<>();
        double accumulated = 0;
        for (QueryGroupTask task : sortedTasks) {
            selectedTasks.add(task);
            accumulated += TaskResourceUsageCalculator.from(resourceType).calculateFor(task, nanoTimeSupplier);
            if ((accumulated - limit) > MIN_VALUE) {
                break;
            }
        }
        return selectedTasks;
    }
}
