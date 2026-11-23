/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.Collections;

/**
 * Marks a search as a sub-search that {@link IndexAwareSearchRequestProcessor}s do not process.
 * <p>
 * A processor, or the query it produces, may issue searches of its own: a lookup the processor needs
 * before it can decide what to do, or the separate searches a composite query runs for each of its parts.
 * Those searches go through the same coordinating-node code as any other search, so without a marker the
 * processors would run for them too, and a processor that issues a search from {@code process} would
 * recurse. Code that issues such a search wraps the call in {@link #markForSubSearch}; processors are
 * skipped for a search that {@link #isMarked}.
 * <p>
 * The marker is a thread-context transient. Setting it does not stash the caller's context: request
 * headers and other transients stay in place for the sub-search, so it runs with the caller's identity,
 * and response headers the sub-search adds are still there after the marker is removed.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class SubSearchMarker {

    /** Thread-context transient key. Public so that code outside this package can recognize the marker. */
    public static final String SUB_SEARCH_TRANSIENT_NAME = "search_request_processor.sub_search";

    private SubSearchMarker() {}

    /**
     * Marks {@code threadContext} as issuing a sub-search until the returned context is closed. Use it in
     * a try-with-resources around the call that issues the sub-search:
     * <pre>{@code
     * try (ThreadContext.StoredContext ignored = SubSearchMarker.markForSubSearch(threadContext)) {
     *     client.search(subSearchRequest, listener);
     * }
     * }</pre>
     * The marker only needs to cover the call itself, not the wait for its response: the search captures
     * the thread context when it is issued.
     * <p>
     * Closing the returned context restores the transients that were set before the call, so a context
     * that was already marked stays marked. Response headers added in the meantime are kept.
     *
     * @param threadContext the calling thread's context
     * @return a handle that removes the marker again when closed
     */
    public static ThreadContext.StoredContext markForSubSearch(ThreadContext threadContext) {
        // Restores transients on close, keeps response headers added in between (the first argument).
        final ThreadContext.StoredContext restorable = threadContext.newStoredContext(true, false, Collections.emptyList());
        // putTransient throws if the key already has a value, so only set it when it is absent.
        if (threadContext.getTransient(SUB_SEARCH_TRANSIENT_NAME) == null) {
            threadContext.putTransient(SUB_SEARCH_TRANSIENT_NAME, Boolean.TRUE);
        }
        return restorable;
    }

    /**
     * Whether the current thread context is marked as issuing a sub-search.
     *
     * @param threadContext the calling thread's context
     * @return {@code true} if {@link #markForSubSearch} is in effect
     */
    public static boolean isMarked(ThreadContext threadContext) {
        return Boolean.TRUE.equals(threadContext.getTransient(SUB_SEARCH_TRANSIENT_NAME));
    }
}
