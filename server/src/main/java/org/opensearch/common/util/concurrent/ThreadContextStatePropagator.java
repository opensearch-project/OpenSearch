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
     * @param source current context transient headers
     * @return the list of transient headers that needs to be propagated from current context to new thread context
     */
    Map<String, Object> transients(Map<String, Object> source);

    /**
     * Returns the list of request headers that needs to be propagated from current context to request.
     * @param source current context headers
     * @return the list of request headers that needs to be propagated from current context to request
     */
    Map<String, String> headers(Map<String, Object> source);
}
