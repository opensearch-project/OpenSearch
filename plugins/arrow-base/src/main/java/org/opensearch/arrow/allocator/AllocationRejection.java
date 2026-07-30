/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.apache.arrow.memory.OutOfMemoryException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

import java.util.function.Supplier;

/**
 * Translates Arrow's {@link OutOfMemoryException} into OpenSearch's standard
 * {@link OpenSearchRejectedExecutionException} at allocation boundaries.
 *
 * <p>Arrow throws {@code OutOfMemoryException} when an allocation would exceed
 * a parent's {@code allocationLimit}. This is a per-request, recoverable
 * condition — the caller should back off and retry — but Arrow's exception type
 * is not recognized by OpenSearch's REST layer, so it surfaces to clients as a
 * generic 500. {@link OpenSearchRejectedExecutionException} (a subclass of
 * {@link java.util.concurrent.RejectedExecutionException}) maps to HTTP 429
 * (Too Many Requests) and is the framework's standard rejection signal.
 *
 * <p>Apply this wrapper at <em>per-request</em> allocation sites — places where
 * a failure represents "this request didn't fit" rather than "the framework
 * failed to start". Examples:
 * <ul>
 *   <li>per-query child allocator creation in
 *       {@code DefaultPlanExecutor.executeInternal}</li>
 *   <li>per-VSR child allocator creation in
 *       {@code ArrowBufferPool.createChildAllocator}</li>
 *   <li>request-handler-side allocations in transport/RPC layers</li>
 * </ul>
 *
 * <p><strong>Do not</strong> wrap startup-time / lifetime-of-component
 * allocations (e.g. transport server/client allocators created in
 * {@code doStart}, service-level allocators created in plugin constructors).
 * Those failing during node init is a configuration error, not a per-request
 * back-pressure signal — they should propagate as the original exception.
 */
public final class AllocationRejection {

    private AllocationRejection() {}

    /**
     * Runs {@code body} and translates any Arrow {@link OutOfMemoryException}
     * into {@link OpenSearchRejectedExecutionException}.
     *
     * @param context short label included in the rejection message (typically the
     *                pool or operation name; e.g. "query-pool", "ingest-vsr")
     * @param body    the allocation site to invoke
     * @param <T>     return type of {@code body}
     * @return whatever {@code body} returns on success
     * @throws OpenSearchRejectedExecutionException if {@code body} throws
     *         {@link OutOfMemoryException}; the original Arrow exception is
     *         attached as the cause
     */
    public static <T> T wrap(String context, Supplier<T> body) {
        try {
            return body.get();
        } catch (OutOfMemoryException e) {
            throw rejection(context, e);
        }
    }

    /**
     * Runs {@code body} and translates any Arrow {@link OutOfMemoryException}
     * into {@link OpenSearchRejectedExecutionException}. Use this overload for
     * void allocation sites.
     *
     * @param context short label included in the rejection message (typically the
     *                pool or operation name; e.g. "query-pool", "ingest-vsr")
     * @param body    the allocation site to invoke
     * @throws OpenSearchRejectedExecutionException if {@code body} throws
     *         {@link OutOfMemoryException}; the original Arrow exception is
     *         attached as the cause
     */
    public static void wrap(String context, Runnable body) {
        try {
            body.run();
        } catch (OutOfMemoryException e) {
            throw rejection(context, e);
        }
    }

    private static OpenSearchRejectedExecutionException rejection(String context, OutOfMemoryException cause) {
        OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException(
            "native memory allocation rejected at [" + context + "]: " + cause.getMessage()
        );
        rejection.initCause(cause);
        return rejection;
    }
}
