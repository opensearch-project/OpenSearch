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
import org.opensearch.common.unit.TimeValue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * A task that can be canceled
 *
 * @opensearch.internal
 */
public abstract class CancellableTask extends Task {

    private volatile String reason;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final TimeValue cancelAfterTimeInterval;
    /**
     * The time this task was cancelled as a wall clock time since epoch ({@link System#currentTimeMillis()} style).
     */
    private long cancelledAt = -1;
    /**
     * The time this task was cancelled as a relative time ({@link System#nanoTime()} style).
     */
    private long cancelledAtNanos = -1;

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
        if (cancelled.compareAndSet(false, true)) {
            this.cancelledAt = System.currentTimeMillis();
            this.cancelledAtNanos = System.nanoTime();
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

    public long getCancelledAt() {
        return cancelledAt;
    }

    public long getCancelledAtNanos() {
        return cancelledAtNanos;
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
