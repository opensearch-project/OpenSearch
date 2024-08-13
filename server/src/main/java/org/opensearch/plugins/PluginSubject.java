/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.threadpool.ThreadPool;

import java.security.Principal;

/**
 * PluginSubject is passed to plugins within createComponents and does not require
 * an auth token to authenticate
 *
 * @opensearch.api
 */
public class PluginSubject implements Subject {
    public static final String PLUGIN_EXECUTION_CONTEXT = "_plugin_execution_context";

    private final NamedPrincipal pluginCanonicalName;
    private final ThreadPool threadPool;

    PluginSubject(Class<?> pluginClass, ThreadPool threadPool) {
        this.pluginCanonicalName = new NamedPrincipal(pluginClass.getCanonicalName());
        this.threadPool = threadPool;
    }

    @Override
    public Principal getPrincipal() {
        return pluginCanonicalName;
    }

    @Override
    public void authenticate(AuthToken token) {
        // no-op
    }

    @Override
    public Session runAs() {
        ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext();
        threadPool.getThreadContext().putHeader(PLUGIN_EXECUTION_CONTEXT, pluginCanonicalName.getName());
        return ctx::restore;
    }
}
