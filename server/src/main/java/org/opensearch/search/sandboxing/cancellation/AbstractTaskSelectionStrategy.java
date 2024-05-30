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

public abstract class AbstractTaskSelectionStrategy implements TaskSelectionStrategy {

    public abstract Comparator<Task> sortingCondition();

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
