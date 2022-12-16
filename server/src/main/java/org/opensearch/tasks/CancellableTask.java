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
import org.opensearch.rest.RestStatus;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * A task that can be canceled
 *
 * @opensearch.internal
 */
public abstract class CancellableTask extends Task {

    private final AtomicReference<Reason> reasonReference = new AtomicReference<>();
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
    public void cancel(String message) {
        assert message != null;
        cancel(new Reason(message));
    }

    public void cancel(Reason reason) {
        assert reason != null;
        if (reasonReference.compareAndSet(null, reason)) {
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
        return reasonReference.get() != null;
    }

    public TimeValue getCancellationTimeout() {
        return cancelAfterTimeInterval;
    }

    /**
     * The {@link Reason} the task was cancelled or null if it hasn't been cancelled.
     */
    @Nullable
    public Reason getReasonCancelled() {
        return reasonReference.get();
    }

    /**
     * The {@link Reason#getMessage()} the task was cancelled or null if it hasn't been cancelled.
     */
    @Nullable
    public String getReasonCancelledMessage() {
        Reason reason = reasonReference.get();
        return reason != null ? reason.getMessage() : null;
    }

    /**
     * Called after the task is cancelled so that it can take any actions that it has to take.
     */
    protected void onCancelled() {}

    /**
     * Reason represents the cancellation reason for the {@link CancellableTask}.
     */
    public static class Reason {
        private final String message;
        private final RestStatus restStatus;

        public Reason(String message) {
            this(message, RestStatus.INTERNAL_SERVER_ERROR);
        }

        public Reason(String message, RestStatus restStatus) {
            this.message = message;
            this.restStatus = restStatus;
        }

        public String getMessage() {
            return message;
        }

        public RestStatus getRestStatus() {
            return restStatus;
        }
    }
}
