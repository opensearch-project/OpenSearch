/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resource_limit_group.cancellation;

import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.util.List;

public interface CancellableTaskSelector {
    /**
     * This method selects tasks which can be cancelled
     * @param tasks is list of available tasks to select from
     * @param reduceBy is meant to select enough number of tasks consuming {@param reduceBy} resource
     * @param resource it is a system resource e,g; "jvm" or "cpu"
     * @return
     */
    public List<TaskCancellation> selectTasks(List<Task> tasks, Double reduceBy, String resource);

}
