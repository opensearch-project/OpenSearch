/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Data-node shard task representing a single shard fragment execution.
 * Analogous to {@link org.opensearch.action.search.SearchShardTask}.
 * Cancelling this task does not cascade to children.
 *
 * @opensearch.internal
 */
public class AnalyticsShardTask extends CancellableTask {

    private final AtomicReference<Runnable> cancellationListener = new AtomicReference<>();

    public AnalyticsShardTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return false;
    }

    @Override
    public boolean supportsResourceTracking() {
        return true;
    }

    /**
     * Registers a listener that is called exactly once when this task is cancelled.
     * If the task is already cancelled at registration time, the listener fires immediately.
     */
    public void setCancellationListener(Runnable listener) {
        cancellationListener.set(listener);
        if (isCancelled()) {
            Runnable taken = cancellationListener.getAndSet(null);
            if (taken != null) {
                taken.run();
            }
        }
    }

    /** Removes the cancellation listener, e.g. after the query completes normally. */
    public void clearCancellationListener() {
        cancellationListener.set(null);
    }

    @Override
    protected void onCancelled() {
        super.onCancelled();
        Runnable listener = cancellationListener.getAndSet(null);
        if (listener != null) {
            listener.run();
        }
    }
}
