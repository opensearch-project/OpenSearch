/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.SearchBackpressureTask;

/**
 * Listener interface for search backpressure task cancellation events.
 * Implementations can be registered with {@link SearchBackpressureService} to receive
 * notifications when tasks are cancelled due to resource usage violations.
 *
 * @opensearch.internal
 */
public interface SearchBackpressureCancellationListener {

    /**
     * Called when a task is cancelled by the search backpressure service.
     *
     * @param task the task that was cancelled
     * @param taskType the type of the task (SearchTask.class or SearchShardTask.class)
     * @param reasonString the combined cancellation reason string describing which limits were breached
     *                     (e.g., "cpu usage exceeded [30s >= 30s], heap usage exceeded [2.5% >= 2%]")
     */
    void onTaskCancelled(CancellableTask task, Class<? extends SearchBackpressureTask> taskType, String reasonString);

    /**
     * Called when task cancellation is skipped due to rate/ratio limits being reached.
     *
     * @param task the task that would have been cancelled
     * @param taskType the type of the task (SearchTask.class or SearchShardTask.class)
     * @param reasonString the cancellation reason string
     */
    default void onCancellationLimitReached(CancellableTask task, Class<? extends SearchBackpressureTask> taskType, String reasonString) {
        // Default no-op implementation
    }
}
