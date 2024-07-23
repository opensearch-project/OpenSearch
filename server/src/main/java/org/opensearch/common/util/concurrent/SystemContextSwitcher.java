/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;

/**
 * InternalContextSwitcher is an internal class used to switch into a fresh
 * internal system context
 *
 * @opensearch.internal
 */
@InternalApi
public class SystemContextSwitcher implements ContextSwitcher {
    private final ThreadPool threadPool;

    public SystemContextSwitcher(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public ThreadContext.StoredContext switchContext() {
        return threadPool.getThreadContext().stashContext();
    }

    public ThreadContext.StoredContext stashAndMergeHeaders(Map<String, String> headers) {
        return threadPool.getThreadContext().stashAndMergeHeaders(headers);
    }
}
