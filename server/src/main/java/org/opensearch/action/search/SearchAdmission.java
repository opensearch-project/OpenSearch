/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.tasks.Task;

/**
 * Decides when the coordinator may start a search or a scroll page: at once, after waiting, or never. Workload management
 * provides the node's admission; the search actions run each request through it with {@link SearchExecutionGate}, which
 * owns running the request and releasing its permit.
 *
 * @opensearch.internal
 */
@InternalApi
@FunctionalInterface
public interface SearchAdmission {

    /** The permit for a request admitted without being charged against any limit. */
    Releasable NOOP_PERMIT = () -> {};

    /**
     * Whether requests currently need admission. While {@code false}, {@link SearchExecutionGate#execute} runs each request
     * directly and never calls {@link #admit}. Read on every search, so it must be cheap; it may change at any time.
     */
    default boolean isActive() {
        return true;
    }

    /**
     * Decides whether {@code request} may start, completing {@code listener} exactly once with either:
     * <ul>
     *   <li>a non-null permit, which the caller closes once the request completes ({@link #NOOP_PERMIT} when nothing was
     *       charged). Closing runs before the request's listener is notified, so it must be cheap;</li>
     *   <li>the exception the request fails with, without having run. It reaches the client unchanged, so a throttle
     *       rejection is an {@link OpenSearchRejectedExecutionException} (a 429).</li>
     * </ul>
     * Must not block. It may complete inline or later on any thread, with the caller's thread context restored; the request
     * starts on the completing thread, so an asynchronous grant should be delivered on an executor, not a transport thread.
     * The caller never cancels an admission: one that can wait indefinitely must fail once {@code task} is cancelled, and
     * must close any permit it can no longer deliver. {@code task} is not always registered or cancellable (a create
     * point-in-time request runs on an unregistered {@link SearchTask}), so such a wait also needs a bound of its own.
     */
    void admit(Task task, ActionRequest request, ActionListener<Releasable> listener);
}
