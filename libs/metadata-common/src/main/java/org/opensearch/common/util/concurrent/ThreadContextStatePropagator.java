/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.common.annotation.PublicApi;

import java.util.Map;

/**
 * The propagator for {@link ThreadContext} that helps to carry-over the state from one
 * thread to another (tasks, tracing context, etc).
 *
 * @opensearch.api
 */
@PublicApi(since = "2.8.0")
public interface ThreadContextStatePropagator {
    /**
     * Returns the list of transient headers that needs to be propagated from current context to new thread context.
     *
     * @param source          current context transient headers
     * @return the list of transient headers that needs to be propagated from current context to new thread context
     */
    @Deprecated(since = "2.12.0", forRemoval = true)
    Map<String, Object> transients(Map<String, Object> source);

    /**
     * Returns the list of transient headers that needs to be propagated from current context to new thread context.
     *
     * @param source          current context transient headers
     * @param isSystemContext if the propagation is for system context.
     * @return the list of transient headers that needs to be propagated from current context to new thread context
     */
    default Map<String, Object> transients(Map<String, Object> source, boolean isSystemContext) {
        return transients(source);
    };

    /**
     * Returns the list of request headers that needs to be propagated from current context to request.
     *
     * @param source          current context headers
     * @return the list of request headers that needs to be propagated from current context to request
     */
    @Deprecated(since = "2.12.0", forRemoval = true)
    Map<String, String> headers(Map<String, Object> source);

    /**
     * Returns the list of request headers that needs to be propagated from current context to request.
     *
     * @param source          current context headers
     * @param isSystemContext if the propagation is for system context.
     * @return the list of request headers that needs to be propagated from current context to request
     */
    default Map<String, String> headers(Map<String, Object> source, boolean isSystemContext) {
        return headers(source);
    }
}
