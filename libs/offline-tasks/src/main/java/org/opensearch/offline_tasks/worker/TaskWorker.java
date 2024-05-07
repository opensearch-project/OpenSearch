/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks.worker;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.offline_tasks.task.Task;
import org.opensearch.offline_tasks.task.TaskParams;

/**
 * Task Worker that executes the Task
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface TaskWorker {

    /**
     * Execute the Task
     *
     * @param task Task to be execute
     * @param taskParams TaskParams to be used while executing the task
     */
    void executeTask(Task task, TaskParams taskParams);

}
