/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.securityplugin;

import org.opensearch.authn.AccessTokenManager;
import org.opensearch.authn.AuthenticationManager;
import org.opensearch.authn.Subject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.threadpool.ThreadPool;

public class SecurityPluginAuthenticationManager implements AuthenticationManager {
    private final ThreadPool threadPool;

    /**
     * Security manager is loaded with default user set,
     * and this instantiation uses the default security manager
     */
    public SecurityPluginAuthenticationManager(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public Subject getSubject() {
        final Object userObj = getThreadContext().getTransient(SecurityPluginConstants.OPENDISTRO_SECURITY_USER);
        return null;
    }

    @Override
    public AccessTokenManager getAccessTokenManager() {
        return null;
    }

    private ThreadContext getThreadContext() {
        return threadPool.getThreadContext();
    }
}
