/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

abstract class OpenSearchForwardingExecutorService implements ExecutorService {

    private final ExecutorService delegate;

    protected OpenSearchForwardingExecutorService(ExecutorService delegate) {
        this.delegate = delegate;
    }

    ExecutorService delegate() {
        return delegate;
    }

    @Override
    public final void shutdown() {
        delegate.shutdown();
    }

    @Override
    public final List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public final boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public final boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public final boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }
}
