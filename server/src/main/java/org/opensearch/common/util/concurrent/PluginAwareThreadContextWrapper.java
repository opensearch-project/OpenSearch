/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.plugins.Plugin;

public class PluginAwareThreadContextWrapper {

    private final ThreadContext threadContext;
    private final Plugin plugin;

    public PluginAwareThreadContextWrapper(ThreadContext threadContext, Plugin plugin) {
        this.threadContext = threadContext;
        this.plugin = plugin;
    }

    public ThreadContext.StoredContext stashContext() {
        return threadContext.stashContext(plugin.getClass());
    }
}
