/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.PluginSubject;
import org.opensearch.threadpool.ThreadPool;

import java.security.Principal;

/**
 * Implementation of subject that is always authenticated
 * <p>
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * This class is used by the ShiroIdentityPlugin to initialize IdentityAwarePlugins
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ShiroPluginSubject implements PluginSubject {
    private final ThreadPool threadPool;

    ShiroPluginSubject(ThreadPool threadPool) {
        super();
        this.threadPool = threadPool;
    }

    @Override
    public Principal getPrincipal() {
        return NamedPrincipal.UNAUTHENTICATED;
    }

    @Override
    public <E extends Exception> void runAs(CheckedRunnable<E> r) throws E {
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            r.run();
        }
    }
}
