/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.opensearch.tasks.Task;

import java.util.Comparator;

/**
 * Represents a task selection strategy that prioritizes the longest running tasks first.
 */
public class LongestRunningTaskFirstSelectionStrategy extends AbstractTaskSelectionStrategy {

    /**
     * Returns a comparator that sorts tasks based on their start time in descending order.
     *
     * @return The comparator
     */
    @Override
    public Comparator<Task> sortingCondition() {
        return Comparator.comparingLong(Task::getStartTime);
    }
}
