/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

import org.opensearch.action.ActionRequest;
import org.opensearch.tasks.Task;

/**
 * Carries per-request context through the concurrency limiter's acquire path so that
 * partition resolvers can inspect the request (e.g. read a header) to determine which
 * partition the request belongs to.
 */
public final class SearchRequestContext {
    private final Task task;
    private final String action;
    private final ActionRequest request;

    /**
     * Creates a new context for the given request.
     *
     * @param task the current task
     * @param action the transport action name
     * @param request the action request
     */
    public SearchRequestContext(Task task, String action, ActionRequest request) {
        this.task = task;
        this.action = action;
        this.request = request;
    }

    /** Returns the task associated with this request. */
    public Task task() {
        return task;
    }

    /** Returns the transport action name. */
    public String action() {
        return action;
    }

    /** Returns the action request. */
    public ActionRequest request() {
        return request;
    }
}
