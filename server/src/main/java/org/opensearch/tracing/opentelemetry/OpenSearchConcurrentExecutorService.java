/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.opensearch.tracing.TaskEventListener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final public class OpenSearchConcurrentExecutorService extends OpenSearchForwardingExecutorService {
    private final List<TaskEventListener> taskEventListeners;

    OpenSearchConcurrentExecutorService(ExecutorService delegate) {
        this(delegate, OpenTelemetryService.TaskEventListeners.getInstance(null));
    }

    OpenSearchConcurrentExecutorService(ExecutorService delegate, List<TaskEventListener> taskEventListeners) {
        super(delegate);
        this.taskEventListeners = taskEventListeners;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return delegate().submit(wrapTask(task, taskEventListeners));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return delegate().submit(wrapTask(task, taskEventListeners), result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return delegate().submit(wrapTask(task, taskEventListeners));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
        return delegate().invokeAll(wrap(tasks, taskEventListeners));
    }

    @Override
    public <T> List<Future<T>> invokeAll(
        Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException {
        return delegate().invokeAll(wrap(tasks, taskEventListeners), timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
        return delegate().invokeAny(wrap(tasks, taskEventListeners));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return delegate().invokeAny(wrap(tasks, taskEventListeners), timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        delegate().execute(wrapTask(command, taskEventListeners));
    }

    private static <T> Collection<? extends Callable<T>> wrap(Collection<? extends Callable<T>> tasks,
                                                                List<TaskEventListener> taskEventListeners) {
        List<Callable<T>> wrapped = new ArrayList<>();
        for (Callable<T> task : tasks) {
            wrapped.add(wrapTask(task, taskEventListeners));
        }
        return wrapped;
    }

    private static <T> Callable<T> wrapTask(Callable<T> callable, List<TaskEventListener> taskEventListeners) {
        return () -> {
            try (Scope ignored = Context.current().makeCurrent()) {
                OpenTelemetryService.callTaskEventListeners(true, "", Span.current().getSpanContext().getSpanId() + "-" +
                    Thread.currentThread().getName() + "-Start", Thread.currentThread(), taskEventListeners);
                return callable.call();
            } finally {
                OpenTelemetryService.callTaskEventListeners(false, "", Span.current().getSpanContext().getSpanId() + "-" +
                    Thread.currentThread().getName() + "-End", Thread.currentThread(), taskEventListeners);
            }
        };
    }

    static Runnable wrapTask(Runnable runnable, List<TaskEventListener> taskEventListeners) {
        return () -> {
            try (Scope ignored = Context.current().makeCurrent()) {
                OpenTelemetryService.callTaskEventListeners(true, "", Span.current().getSpanContext().getSpanId() + "-" +
                    Thread.currentThread().getName() + "-Start", Thread.currentThread(), taskEventListeners);
                runnable.run();
            } finally {
                OpenTelemetryService.callTaskEventListeners(false, "", Span.current().getSpanContext().getSpanId() + "-" +
                    Thread.currentThread().getName() + "-End", Thread.currentThread(), taskEventListeners);
            }
        };
    }


}
