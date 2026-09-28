/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.common.SuppressForbidden;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An {@link ExecutorService} wrapper that preserves {@link ThreadContext} across task submissions.
 *
 * @opensearch.internal
 */
class ContextPreservingExecutorService extends AbstractExecutorService {

    private final ExecutorService delegate;
    private final ThreadContext threadContext;

    @SuppressForbidden(reason = "properly rethrowing errors, see OpenSearchExecutors.rethrowErrors")
    ContextPreservingExecutorService(ExecutorService delegate, ThreadContext threadContext) {
        super();
        this.delegate = delegate;
        this.threadContext = threadContext;
    }

    @Override
    public void execute(Runnable command) {
        final Runnable contextPreserving = threadContext.preserveContext(command);
        delegate.execute(() -> {
            try {
                contextPreserving.run();
                // Tasks submitted via submit()/invokeAll()/invokeAny() are RunnableFutures that capture any thrown
                // Error instead of letting it propagate, so unwrap and rethrow it here the way
                // OpenSearchThreadPoolExecutor does.
                OpenSearchExecutors.rethrowErrors(threadContext.unwrap(contextPreserving));
            } finally {
                onTaskFinished();
            }
        });
    }

    /**
     * Invoked on the executing thread once a task has finished, whether it completed normally or threw. Rethrowing a
     * fatal {@link Error} happens before this, so it is called from a finally block. Does nothing by default.
     */
    protected void onTaskFinished() {}

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }
}
