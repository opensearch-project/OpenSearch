/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.cancellation;

import org.opensearch.search.sandboxing.resourcetype.SystemResource;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.util.List;

/**
 * Interface for strategies to select tasks for cancellation.
 * Implementations of this interface define how tasks are selected for cancellation based on resource usage.
 */
public interface TaskSelectionStrategy {
    /**
     * Determines which tasks should be cancelled based on the provided criteria.
     *
     * @param tasks List of tasks available for cancellation.
     * @param limit The amount of tasks to select whose resources reach this limit
     * @param resourceType The type of resource that needs to be reduced, guiding the selection process.
     *
     * @return List of tasks that should be cancelled.
     */
    List<TaskCancellation> selectTasksForCancellation(List<Task> tasks, long limit, SystemResource resourceType);
}
