/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.identity.AbstractSubject;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.threadpool.ThreadPool;

import java.security.Principal;
import java.util.concurrent.Callable;

public class NoopPluginSubject extends AbstractSubject {
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
