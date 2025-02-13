/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.ExceptionsHelper;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.Node;
import org.opensearch.threadpool.RunnableTaskExecutionListener;
import org.opensearch.threadpool.TaskAwareRunnable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Executors.
 *
 * @opensearch.internal
 */
public class OpenSearchExecutors {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(OpenSearchExecutors.class);

    /**
     * Setting to manually set the number of available processors. This setting is used to adjust thread pool sizes per node.
     */
    public static final Setting<Integer> PROCESSORS_SETTING = new Setting<>(
        "processors",
        s -> Integer.toString(Runtime.getRuntime().availableProcessors()),
        processorsParser("processors"),
        Property.Deprecated,
        Property.NodeScope
    );

    /**
     * Setting to manually control the number of allocated processors. This setting is used to adjust thread pool sizes per node. The
     * default value is {@link Runtime#availableProcessors()} but should be manually controlled if not all processors on the machine are
     * available to OpenSearch (e.g., because of CPU limits).
     */
    public static final Setting<Integer> NODE_PROCESSORS_SETTING = new Setting<>(
        "node.processors",
        PROCESSORS_SETTING,
        processorsParser("node.processors"),
        Property.NodeScope
    );

    private static Function<String, Integer> processorsParser(final String name) {
        return s -> {
            final int value = Setting.parseInt(s, 1, name);
            final int availableProcessors = Runtime.getRuntime().availableProcessors();
            if (value > availableProcessors) {
                deprecationLogger.deprecate(
                    "processors_" + name,
                    "setting [{}] to value [{}] which is more than available processors [{}] is deprecated",
                    name,
                    value,
                    availableProcessors
                );
            }
            return value;
        };
    }

    /**
     * Returns the number of allocated processors. Defaults to {@link Runtime#availableProcessors()} but can be overridden by passing a
     * {@link Settings} instance with the key {@code node.processors} set to the desired value.
     *
     * @param settings a {@link Settings} instance from which to derive the allocated processors
     * @return the number of allocated processors
     */
    public static int allocatedProcessors(final Settings settings) {
        return NODE_PROCESSORS_SETTING.get(settings);
    }

    public static PrioritizedOpenSearchThreadPoolExecutor newSinglePrioritizing(
        String name,
        ThreadFactory threadFactory,
        ThreadContext contextHolder,
        ScheduledExecutorService timer
    ) {
        return new PrioritizedOpenSearchThreadPoolExecutor(name, 1, 1, 0L, TimeUnit.MILLISECONDS, threadFactory, contextHolder, timer);
    }

    public static OpenSearchThreadPoolExecutor newScaling(
        String name,
        int min,
        int max,
        long keepAliveTime,
        TimeUnit unit,
        ThreadFactory threadFactory,
        ThreadContext contextHolder
    ) {
        ExecutorScalingQueue<Runnable> queue = new ExecutorScalingQueue<>();
        OpenSearchThreadPoolExecutor executor = new OpenSearchThreadPoolExecutor(
            name,
            min,
            max,
            keepAliveTime,
            unit,
            queue,
            threadFactory,
            new ForceQueuePolicy(),
            contextHolder
        );
        queue.executor = executor;
        return executor;
    }

    public static OpenSearchThreadPoolExecutor newFixed(
        String name,
        int size,
        int queueCapacity,
        ThreadFactory threadFactory,
        ThreadContext contextHolder
    ) {
        BlockingQueue<Runnable> queue;
        if (queueCapacity < 0) {
            queue = ConcurrentCollections.newBlockingQueue();
        } else {
            queue = new SizeBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), queueCapacity);
        }
        return new OpenSearchThreadPoolExecutor(
            name,
            size,
            size,
            0,
            TimeUnit.MILLISECONDS,
            queue,
            threadFactory,
            new OpenSearchAbortPolicy(),
            contextHolder
        );
    }

    public static OpenSearchThreadPoolExecutor newResizable(
        String name,
        int size,
        int queueCapacity,
        ThreadFactory threadFactory,
        ThreadContext contextHolder,
        AtomicReference<RunnableTaskExecutionListener> runnableTaskListener
    ) {

        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queue capacity for [" + name + "] executor must be positive, got: " + queueCapacity);
        }

        Function<Runnable, WrappedRunnable> runnableWrapper;
        if (runnableTaskListener != null) {
            runnableWrapper = (runnable) -> {
                TaskAwareRunnable taskAwareRunnable = new TaskAwareRunnable(contextHolder, runnable, runnableTaskListener);
                return new TimedRunnable(taskAwareRunnable);
            };
        } else {
            runnableWrapper = TimedRunnable::new;
        }

        return new QueueResizableOpenSearchThreadPoolExecutor(
            name,
            size,
            size,
            0,
            TimeUnit.MILLISECONDS,
            new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), queueCapacity),
            runnableWrapper,
            threadFactory,
            new OpenSearchAbortPolicy(),
            contextHolder
        );
    }

    /**
     * Return a new executor that will automatically adjust the queue size based on queue throughput.
     *
     * @param size number of fixed threads to use for executing tasks
     * @param initialQueueCapacity initial size of the executor queue
     * @param minQueueSize minimum queue size that the queue can be adjusted to
     * @param maxQueueSize maximum queue size that the queue can be adjusted to
     * @param frameSize number of tasks during which stats are collected before adjusting queue size
     */
    public static OpenSearchThreadPoolExecutor newAutoQueueFixed(
        String name,
        int size,
        int initialQueueCapacity,
        int minQueueSize,
        int maxQueueSize,
        int frameSize,
        TimeValue targetedResponseTime,
        ThreadFactory threadFactory,
        ThreadContext contextHolder
    ) {
        if (initialQueueCapacity <= 0) {
            throw new IllegalArgumentException(
                "initial queue capacity for [" + name + "] executor must be positive, got: " + initialQueueCapacity
            );
        }
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(
            ConcurrentCollections.<Runnable>newBlockingQueue(),
            initialQueueCapacity
        );
        return new QueueResizingOpenSearchThreadPoolExecutor(
            name,
            size,
            size,
            0,
            TimeUnit.MILLISECONDS,
            queue,
            minQueueSize,
            maxQueueSize,
            TimedRunnable::new,
            frameSize,
            targetedResponseTime,
            threadFactory,
            new OpenSearchAbortPolicy(),
            contextHolder
        );
    }

    /**
     * Checks if the runnable arose from asynchronous submission of a task to an executor. If an uncaught exception was thrown
     * during the execution of this task, we need to inspect this runnable and see if it is an error that should be propagated
     * to the uncaught exception handler.
     *
     * @param runnable the runnable to inspect, should be a RunnableFuture
     * @return non fatal exception or null if no exception.
     */
    public static Throwable rethrowErrors(Runnable runnable) {
        if (runnable instanceof RunnableFuture) {
            assert ((RunnableFuture) runnable).isDone();
            try {
                ((RunnableFuture) runnable).get();
            } catch (final Exception e) {
                /*
                 * In theory, Future#get can only throw a cancellation exception, an interrupted exception, or an execution
                 * exception. We want to ignore cancellation exceptions, restore the interrupt status on interrupted exceptions, and
                 * inspect the cause of an execution. We are going to be extra paranoid here though and completely unwrap the
                 * exception to ensure that there is not a buried error anywhere. We assume that a general exception has been
                 * handled by the executed task or the task submitter.
                 */
                assert e instanceof CancellationException || e instanceof InterruptedException || e instanceof ExecutionException : e;
                final Optional<Error> maybeError = ExceptionsHelper.maybeError(e);
                if (maybeError.isPresent()) {
                    // throw this error where it will propagate to the uncaught exception handler
                    throw maybeError.get();
                }
                if (e instanceof InterruptedException) {
                    // restore the interrupt status
                    Thread.currentThread().interrupt();
                }
                if (e instanceof ExecutionException) {
                    return e.getCause();
                }
            }
        }

        return null;
    }

    private static final class DirectExecutorService extends AbstractExecutorService {

        @SuppressForbidden(reason = "properly rethrowing errors, see OpenSearchExecutors.rethrowErrors")
        DirectExecutorService() {
            super();
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
            command.run();
            rethrowErrors(command);
        }
    }

    private static final ExecutorService DIRECT_EXECUTOR_SERVICE = new DirectExecutorService();

    /**
     * Returns an {@link ExecutorService} that executes submitted tasks on the current thread. This executor service does not support being
     * shutdown.
     *
     * @return an {@link ExecutorService} that executes submitted tasks on the current thread
     */
    public static ExecutorService newDirectExecutorService() {
        return DIRECT_EXECUTOR_SERVICE;
    }

    public static String threadName(Settings settings, String namePrefix) {
        if (Node.NODE_NAME_SETTING.exists(settings)) {
            return threadName(Node.NODE_NAME_SETTING.get(settings), namePrefix);
        } else {
            // TODO this should only be allowed in tests
            return threadName("", namePrefix);
        }
    }

    public static String threadName(final String nodeName, final String namePrefix) {
        // TODO missing node names should only be allowed in tests
        return "opensearch" + (nodeName.isEmpty() ? "" : "[") + nodeName + (nodeName.isEmpty() ? "" : "]") + "[" + namePrefix + "]";
    }

    public static ThreadFactory daemonThreadFactory(Settings settings, String namePrefix) {
        return daemonThreadFactory(threadName(settings, namePrefix));
    }

    public static ThreadFactory daemonThreadFactory(String nodeName, String namePrefix) {
        assert nodeName != null && false == nodeName.isEmpty();
        return daemonThreadFactory(threadName(nodeName, namePrefix));
    }

    public static ThreadFactory daemonThreadFactory(String namePrefix) {
        return new OpenSearchThreadFactory(namePrefix);
    }

    /**
     * A thread factory
     *
     * @opensearch.internal
     */
    static class OpenSearchThreadFactory implements ThreadFactory {

        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        @SuppressWarnings("removal")
        OpenSearchThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + "[T#" + threadNumber.getAndIncrement() + "]", 0);
            t.setDaemon(true);
            return t;
        }

    }

    /**
     * Cannot instantiate.
     */
    private OpenSearchExecutors() {}

    /**
     * A scaling queue for executors
     *
     * @opensearch.internal
     */
    static class ExecutorScalingQueue<E> extends LinkedTransferQueue<E> {

        ThreadPoolExecutor executor;

        ExecutorScalingQueue() {}

        @Override
        public boolean offer(E e) {
            // first try to transfer to a waiting worker thread
            if (!tryTransfer(e)) {
                // check if there might be spare capacity in the thread
                // pool executor
                int left = executor.getMaximumPoolSize() - executor.getCorePoolSize();
                if (left > 0) {
                    // reject queuing the task to force the thread pool
                    // executor to add a worker if it can; combined
                    // with ForceQueuePolicy, this causes the thread
                    // pool to always scale up to max pool size and we
                    // only queue when there is no spare capacity
                    return false;
                } else {
                    return super.offer(e);
                }
            } else {
                return true;
            }
        }

        /**
         * Workaround for https://bugs.openjdk.org/browse/JDK-8323659 regression, introduced in JDK-21.0.2.
         */
        @Override
        public void put(E e) {
            super.offer(e);
        }

        /**
         * Workaround for https://bugs.openjdk.org/browse/JDK-8323659 regression, introduced in JDK-21.0.2.
         */
        @Override
        public boolean offer(E e, long timeout, TimeUnit unit) {
            return super.offer(e);
        }

        /**
         * Workaround for https://bugs.openjdk.org/browse/JDK-8323659 regression, introduced in JDK-21.0.2.
         */
        @Override
        public boolean add(E e) {
            return super.offer(e);
        }

    }

    /**
     * A handler for rejected tasks that adds the specified element to this queue,
     * waiting if necessary for space to become available.
     *
     * @opensearch.internal
     */
    static class ForceQueuePolicy implements XRejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                // force queue policy should only be used with a scaling queue
                assert executor.getQueue() instanceof ExecutorScalingQueue;
                executor.getQueue().put(r);
            } catch (final InterruptedException e) {
                // a scaling queue never blocks so a put to it can never be interrupted
                throw new AssertionError(e);
            }
        }

        @Override
        public long rejected() {
            return 0;
        }

    }

}
