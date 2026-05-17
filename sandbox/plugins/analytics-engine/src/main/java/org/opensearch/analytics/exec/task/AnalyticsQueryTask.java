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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Coordinator-level cancellable task representing a running analytics query.
 * Analogous to {@link SearchTask}.
 * Cancelling this task cascades cancellation to all child shard tasks.
 *
 * @opensearch.internal
 */
public class AnalyticsQueryTask extends SearchTask {

    private static final Logger logger = LogManager.getLogger(AnalyticsQueryTask.class);

    private final String queryId;
    private final List<String> indices;
    private final TimeValue cancelAfterTimeInterval;
    private final AtomicReference<Runnable> onCancelCallback = new AtomicReference<>();

    public AnalyticsQueryTask(
        long id,
        String type,
        String action,
        String queryId,
        List<String> indices,
        TaskId parentTaskId,
        Map<String, String> headers,
        @Nullable TimeValue cancelAfterTimeInterval
    ) {
        super(
            id,
            type,
            action,
            (Supplier<String>) () -> "queryId[" + queryId + "] indices[" + String.join(",", indices) + "]",
            parentTaskId,
            headers,
            cancelAfterTimeInterval != null ? cancelAfterTimeInterval : TimeValue.MINUS_ONE
        );
        this.queryId = queryId;
        this.indices = List.copyOf(indices);
        this.cancelAfterTimeInterval = cancelAfterTimeInterval;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    public String getQueryId() {
        return queryId;
    }

    public List<String> getIndices() {
        return indices;
    }

    @Nullable
    public TimeValue getCancelAfterTimeInterval() {
        return cancelAfterTimeInterval;
    }

    /**
     * Install a callback to be run when this task is cancelled. Must be called
     * at most once per task instance — typically right after task registration
     * by the query driver. The callback runs on whatever thread invokes cancel
     * (transport thread, timeout scheduler, parent cascade); it must be
     * non-blocking and safe from any thread.
     */
    public void setOnCancelCallback(Runnable callback) {
        if (onCancelCallback.compareAndSet(null, callback) == false) {
            throw new IllegalStateException("onCancelCallback already set for AnalyticsQueryTask " + queryId);
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
