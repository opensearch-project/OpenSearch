/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ThreadPool;

/**
 * ContextSwitcher that is passed to plugins in order to switch to a fresh context
 * from any existing context
 *
 * @opensearch.api
 */
@PublicApi(since = "2.17.0")
public class PluginContextSwitcher implements ContextSwitcher {

    private final ThreadPool threadPool;
    private final Plugin plugin;

    public PluginContextSwitcher(ThreadPool threadPool, Plugin plugin) {
        this.threadPool = threadPool;
        this.plugin = plugin;
    }

    @Override
    public ThreadContext.StoredContext switchContext() {
        return threadPool.getThreadContext().stashContext(plugin.getClass());
    }
}
