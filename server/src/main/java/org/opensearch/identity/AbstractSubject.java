/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.Callable;

/**
 * An AbstractSubject provides a default implementation for runAs which populates the _subject header of the
 * ThreadContext with the subject's principal name
 */
public abstract class AbstractSubject implements Subject {

    public static final String SUBJECT_HEADER = "_subject";

    private final ThreadPool threadPool;

    public AbstractSubject(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public AbstractSubject() {
        this.threadPool = null;
    }

    @Override
    public void runAs(Callable<Void> callable) throws Exception {
        if (threadPool != null) {
            try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().putHeader(SUBJECT_HEADER, getPrincipal().getName());
                callable.call();
            }
        } else {
            callable.call();
        }
    }
}
