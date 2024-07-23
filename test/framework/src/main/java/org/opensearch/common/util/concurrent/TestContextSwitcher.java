/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.threadpool.ThreadPool;

/**
 * Test Context Switcher
 */
public class TestContextSwitcher implements ContextSwitcher {
    private final ThreadPool threadPool;
    private final Class<?> pluginClass;

    public TestContextSwitcher(ThreadPool threadPool, Class<?> pluginClass) {
        this.threadPool = threadPool;
        this.pluginClass = pluginClass;
    }

    @Override
    public ThreadContext.StoredContext switchContext() {
        return threadPool.getThreadContext().stashContext(pluginClass);
    }
}
