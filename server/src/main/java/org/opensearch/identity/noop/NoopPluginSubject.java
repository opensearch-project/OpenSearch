/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.threadpool.ThreadPool;

import java.security.Principal;
import java.util.concurrent.Callable;

/**
 * Implementation of subject that is always authenticated
 * <p>
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * This class is used by the NoopIdentityPlugin to initialize IdentityAwarePlugins
 *
 * @opensearch.internal
 */
public class NoopPluginSubject implements Subject {
    private final ThreadPool threadPool;

    public NoopPluginSubject(ThreadPool threadPool) {
        super();
        this.threadPool = threadPool;
    }

    @Override
    public Principal getPrincipal() {
        return NamedPrincipal.UNAUTHENTICATED;
    }

    @Override
    public void authenticate(AuthToken token) {
        // Do nothing as noop subject is always logged in
    }

    @Override
    public void runAs(Callable<Void> callable) throws Exception {
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            callable.call();
        }
    }
}
