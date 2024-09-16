/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*
*/

package org.opensearch.tasks;

import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * A protobuf task that can be canceled
*
* @opensearch.internal
*/
public abstract class ProtobufCancellableTask extends ProtobufTask {

    private volatile String reason;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final TimeValue cancelAfterTimeInterval;

    public ProtobufCancellableTask(
        long id,
        String type,
        String action,
        String description,
        ProtobufTaskId parentTaskId,
        Map<String, String> headers
    ) {
        this(id, type, action, description, parentTaskId, headers, NO_TIMEOUT);
    }

    public ProtobufCancellableTask(
        long id,
        String type,
        String action,
        String description,
        ProtobufTaskId parentTaskId,
        Map<String, String> headers,
        TimeValue cancelAfterTimeInterval
    ) {
        super(id, type, action, description, parentTaskId, headers);
        this.cancelAfterTimeInterval = cancelAfterTimeInterval;
    }

    /**
     * This method is called by the task manager when this task is cancelled.
    */
    public void cancel(String reason) {
        assert reason != null;
        if (cancelled.compareAndSet(false, true)) {
            this.reason = reason;
            onCancelled();
        }
    }

    /**
     * Returns true if this task should be automatically cancelled if the coordinating node that
    * requested this task left the cluster.
    */
    public boolean cancelOnParentLeaving() {
        return true;
    }

    /**
     * Returns true if this task can potentially have children that need to be cancelled when it parent is cancelled.
    */
    public abstract boolean shouldCancelChildrenOnCancellation();

    public boolean isCancelled() {
        return cancelled.get();
    }

    public TimeValue getCancellationTimeout() {
        return cancelAfterTimeInterval;
    }

    /**
     * The reason the task was cancelled or null if it hasn't been cancelled.
    */
    @Nullable
    public final String getReasonCancelled() {
        return reason;
    }

    /**
     * Called after the task is cancelled so that it can take any actions that it has to take.
    */
    protected void onCancelled() {}
}
