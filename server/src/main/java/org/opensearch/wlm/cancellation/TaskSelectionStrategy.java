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

import java.util.List;

/**
 * This interface exposes a method which implementations can use
 */
public interface TaskSelectionStrategy {
    /**
     * Determines how the tasks are selected from the list of given tasks based on resource type
     * @param tasks to select from
     * @param limit min cumulative resource usage sum of selected tasks
     * @param resourceType
     * @return list of tasks
     */
    List<QueryGroupTask> selectTasksForCancellation(List<QueryGroupTask> tasks, double limit, ResourceType resourceType);
}
