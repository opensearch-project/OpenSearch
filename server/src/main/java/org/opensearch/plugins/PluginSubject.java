/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.identity.AbstractSubject;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.threadpool.ThreadPool;

import java.security.Principal;

/**
 * PluginSubject is passed to plugins within createComponents and does not require
 * an auth token to authenticate
 *
 * @opensearch.api
 */
public class PluginSubject extends AbstractSubject {
    private final NamedPrincipal pluginCanonicalName;

    PluginSubject(Class<?> pluginClass, ThreadPool threadPool) {
        super(threadPool);
        this.pluginCanonicalName = new NamedPrincipal(pluginClass.getCanonicalName());
    }

    @Override
    public Principal getPrincipal() {
        return pluginCanonicalName;
    }

    @Override
    public void authenticate(AuthToken token) {
        // no-op
    }
}
