/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.search.SearchTask;
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.SearchBackpressureTask;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinator-level cancellable task representing a running analytics query.
 * Analogous to {@link SearchTask}.
 * Cancelling this task cascades cancellation to all child shard tasks.
 *
 * @opensearch.internal
 */
public class AnalyticsQueryTask extends CancellableTask implements SearchBackpressureTask {

    private static final Logger logger = LogManager.getLogger(AnalyticsQueryTask.class);

    private final String queryId;
    private final TimeValue cancelAfterTimeInterval;
    private final AtomicReference<Runnable> onCancelCallback = new AtomicReference<>();

    public AnalyticsQueryTask(
        long id,
        String type,
        String action,
        String queryId,
        TaskId parentTaskId,
        Map<String, String> headers,
        @Nullable TimeValue cancelAfterTimeInterval
    ) {
        super(
            id,
            type,
            action,
            "queryId[" + queryId + "]",
            parentTaskId,
            headers,
            cancelAfterTimeInterval != null ? cancelAfterTimeInterval : TimeValue.MINUS_ONE
        );
        this.queryId = queryId;
        this.cancelAfterTimeInterval = cancelAfterTimeInterval;
    }

    public AnalyticsQueryTask(long id, String type, String action, String queryId, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, queryId, parentTaskId, headers, null);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    public String getQueryId() {
        return queryId;
    }

    @Nullable
    public TimeValue getCancelAfterTimeInterval() {
        return cancelAfterTimeInterval;
    }

    /**
     * Install a callback to be run when this task is cancelled. Typically called right
     * after task registration by the query driver. The callback runs on whatever thread
     * invokes cancel (transport thread, timeout scheduler, parent cascade); it must be
     * non-blocking and safe from any thread.
     *
     * <p>Replaces any previously installed callback — multi-phase drivers (e.g. M1 broadcast
     * dispatch) install a temporary callback targeting the phase 1 execution, then replace
     * it when phase 2 begins so cancel routes to the active phase's walker.
     *
     * <p>Late-install replay: if this task has already been cancelled by the time
     * {@code setOnCancelCallback} is called, run the new callback immediately on the
     * caller's thread. {@link #onCancelled()} is one-shot, so without replay a callback
     * installed after cancellation would never fire — losing cancel semantics across the
     * pass-1 → pass-2 handoff in {@code BroadcastDispatch}.
     */
    public void setOnCancelCallback(Runnable callback) {
        onCancelCallback.set(callback);
        if (callback != null && isCancelled()) {
            try {
                callback.run();
            } catch (Exception e) {
                logger.warn(
                    new ParameterizedMessage("[AnalyticsQueryTask] late-install onCancel callback failed for queryId={}", queryId),
                    e
                );
            }
        }
    }

    @Override
    protected void onCancelled() {
        Runnable cb = onCancelCallback.get();
        if (cb != null) {
            try {
                cb.run();
            } catch (Exception e) {
                logger.warn(new ParameterizedMessage("[AnalyticsQueryTask] onCancelled callback failed for queryId={}", queryId), e);
            }
        }
    }
}
