/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.Task;

/**
 * Decides a search's {@code cancel_after_time_interval}. {@link TransportSearchAction} consults the node's resolver once per
 * search, before admission, writes the result onto the request, and arms the coordinator's cancellation timer with it unless the
 * task's {@code createTask} chose its own timeout. One per node, bound in {@code Node} and injected into the search actions.
 *
 * @opensearch.internal
 */
@InternalApi
@FunctionalInterface
public interface SearchCancelAfterTimeIntervalResolver {

    /** A resolver that keeps the interval the request asked for. */
    SearchCancelAfterTimeIntervalResolver NOOP = (task, request, requested) -> requested;

    /**
     * Returns the search's {@code cancel_after_time_interval}. Called on the thread that runs the search action, with the
     * request's thread context, so it must be cheap and must not block.
     * <p>
     * Implementations should not throw. An exception thrown here does not fail the search: {@link TransportSearchAction} logs
     * it and falls back to {@code requested}, the interval the search would run with under {@link #NOOP}.
     *
     * @param task      the search's task
     * @param request   the search request
     * @param requested the request's {@code cancel_after_time_interval}, or {@code null} if it set none
     * @return the interval to cancel the search after, {@link SearchService#NO_TIMEOUT} to never cancel it, or {@code null}
     *         to use the cluster's {@code search.cancel_after_time_interval}
     */
    @Nullable
    TimeValue resolve(Task task, SearchRequest request, @Nullable TimeValue requested);
}
