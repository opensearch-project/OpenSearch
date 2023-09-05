/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.tasks;

import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.unit.TimeValue;

import java.util.Map;

import static org.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * A task that can be canceled
 *
 * @opensearch.internal
 */
public abstract class CancellableTask extends Task {

    private static class CancelledInfo {
        String reason;
        /**
         * The time this task was cancelled as a wall clock time since epoch ({@link System#currentTimeMillis()} style).
         */
        Long cancellationStartTime;
        /**
         * The time this task was cancelled as a relative time ({@link System#nanoTime()} style).
         */
        Long cancellationStartTimeNanos;

        public CancelledInfo(String reason) {
            this.reason = reason;
            this.cancellationStartTime = System.currentTimeMillis();
            this.cancellationStartTimeNanos = System.nanoTime();
        }
    }

    private final SetOnce<CancelledInfo> cancelledInfo = new SetOnce<>();
    private final TimeValue cancelAfterTimeInterval;

    public CancellableTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, description, parentTaskId, headers, NO_TIMEOUT);
    }

    public CancellableTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
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
        if (cancelledInfo.trySet(new CancelledInfo(reason))) {
            onCancelled();
        }
    }

    public boolean isCancelled() {
        return cancelledInfo.get() != null;
    }

    /**
     * Returns true if this task can potentially have children that need to be cancelled when it parent is cancelled.
     */
    public abstract boolean shouldCancelChildrenOnCancellation();

    public TimeValue getCancellationTimeout() {
        return cancelAfterTimeInterval;
    }

    /**
     * Called after the task is cancelled so that it can take any actions that it has to take.
     */
    protected void onCancelled() {}

    /**
     * Returns true if this task should be automatically cancelled if the coordinating node that
     * requested this task left the cluster.
     */
    public boolean cancelOnParentLeaving() {
        return true;
    }

    @Nullable
    public Long getCancellationStartTime() {
        CancelledInfo info = cancelledInfo.get();
        return (info != null) ? info.cancellationStartTime : null;
    }

    @Nullable
    public Long getCancellationStartTimeNanos() {
        CancelledInfo info = cancelledInfo.get();
        return (info != null) ? info.cancellationStartTimeNanos : null;
    }

    /**
     * The reason the task was cancelled or null if it hasn't been cancelled.
     */
    @Nullable
    public String getReasonCancelled() {
        CancelledInfo info = cancelledInfo.get();
        return (info != null) ? info.reason : null;
    }
}
