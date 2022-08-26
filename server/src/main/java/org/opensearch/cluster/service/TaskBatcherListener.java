/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import java.util.List;

/**
 * Listener class for callback on various events of TaskBatcher.
 */
public interface TaskBatcherListener {
    /**
     * Callback called before submitting tasks.
     * @param tasks list of tasks which will be submitted.
     */
    void onBeginSubmit(List<? extends TaskBatcher.BatchedTask> tasks);

    /**
     * Callback called if tasks submission due to any reason
     * for e.g. failing due to duplicate tasks.
     * @param tasks list of tasks which was failed to submit.
     */
    void onSubmitFailure(List<? extends TaskBatcher.BatchedTask> tasks);

    /**
     * Callback called whenver task is marked as processed
     * @param tasks list of tasks which will be executed.
     */
    void onProcessed(List<? extends TaskBatcher.BatchedTask> tasks);

}
