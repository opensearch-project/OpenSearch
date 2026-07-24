/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * An {@link ExecutorService} wrapper that preserves {@link ThreadContext} across task submissions.
 * Each submitted task is wrapped via {@link ThreadContext#preserveContext(Runnable)} so that the
 * thread context at submission time is restored on the executing thread.
 *
 * @opensearch.internal
 */
class ContextPreservingExecutorService implements ExecutorService {

    private final ExecutorService delegate;
    private final ThreadContext threadContext;

    ContextPreservingExecutorService(ExecutorService delegate, ThreadContext threadContext) {
        this.delegate = delegate;
        this.threadContext = threadContext;
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(threadContext.preserveContext(command));
    }

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

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return delegate.submit(preserveContext(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return delegate.submit(threadContext.preserveContext(task), result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return delegate.submit(threadContext.preserveContext(task));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return delegate.invokeAll(tasks.stream().map(this::preserveContext).collect(Collectors.toList()));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.invokeAll(tasks.stream().map(this::preserveContext).collect(Collectors.toList()), timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return delegate.invokeAny(tasks.stream().map(this::preserveContext).collect(Collectors.toList()));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException,
        ExecutionException, TimeoutException {
        return delegate.invokeAny(tasks.stream().map(this::preserveContext).collect(Collectors.toList()), timeout, unit);
    }

    private <T> Callable<T> preserveContext(Callable<T> task) {
        final ThreadContext.StoredContext ctx = threadContext.newStoredContext(false);
        return () -> {
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                ctx.restore();
                return task.call();
            }
        };
    }
}
