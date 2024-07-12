/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.util.concurrent.PluginAwareThreadContextWrapper;
import org.opensearch.plugins.Plugin;

public class PluginAwareThreadPoolWrapper {

    private ThreadPool threadPool;
    private Plugin plugin;

    public PluginAwareThreadPoolWrapper(ThreadPool threadPool, Plugin plugin) {
        this.threadPool = threadPool;
        this.plugin = plugin;
    }

    public PluginAwareThreadContextWrapper getPluginAwareThreadContext() {
        return new PluginAwareThreadContextWrapper(threadPool.getThreadContext(), plugin);
    }
}
