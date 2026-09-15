/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Guards the invariant that work which calls into the native backend never runs on a virtual thread.
 *
 * <p>The Arrow C Data Interface invokes its release callbacks synchronously on whichever thread drops
 * an exported array, so such a thread always has a native frame on its stack. A virtual thread in that
 * state cannot unmount; if it blocks on a Netty allocator lock it holds its carrier for good, and once
 * every carrier is pinned the thread owning the lock can never be scheduled — the node deadlocks at 0%
 * CPU. Reverting any of these to virtual threads reintroduces that deadlock, which reproduces only
 * under wide shard fan-in and is therefore easy to miss in review.
 */
public class NativeCallPlatformThreadTests extends OpenSearchTestCase {

    /** Local tasks execute fragments through the native backend, so their threads must be platform threads. */
    public void testLocalTaskThreadsAreNotVirtual() throws Exception {
        ExecutorService exec = Executors.newThreadPerTaskExecutor(QueryContext.localTaskThreadFactory("query-1"));
        try {
            CompletableFuture<Thread> observed = new CompletableFuture<>();
            exec.execute(() -> observed.complete(Thread.currentThread()));
            Thread t = observed.get(10, TimeUnit.SECONDS);
            assertFalse("local-task threads must be platform threads, not virtual", t.isVirtual());
            assertTrue("thread name should identify the query", t.getName().startsWith("analytics-local-task-query-1-"));
            // Virtual threads were unconditionally daemon; platform threads inherit the flag from
            // whichever thread submits the task, so a non-daemon local task stuck in a native call
            // would keep the JVM alive. This test's submitter is JUnit's non-daemon main thread, so
            // an inherited flag would show up here.
            assertTrue("local-task threads must be daemon so a stuck native call cannot block JVM exit", t.isDaemon());
        } finally {
            exec.shutdownNow();
        }
    }

    /**
     * The pool named by the stream response handlers must be registered and must hand out platform
     * threads. Without registration the handlers' {@code executor()} name resolves to nothing and
     * dispatch fails; with virtual threads the deadlock returns.
     *
     * <p>That the handlers actually name this pool — rather than {@link ThreadPool.Names#SAME} — is
     * asserted against the real dispatch path in
     * {@code AnalyticsSearchTransportServiceTests#testStreamHandlersDrainOnTheStreamPool}, which is
     * where the handler-capture harness lives.
     */
    public void testStreamDrainPoolIsRegisteredAndUsesPlatformThreads() throws Exception {
        List<ExecutorBuilder<?>> builders = new AnalyticsPlugin().getExecutorBuilders(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName(), builders.toArray(new ExecutorBuilder<?>[0]));
        try {
            // Throws IllegalArgumentException if the pool was never registered, which is the failure
            // this guards; there is no null return to check for.
            ExecutorService exec = threadPool.executor(AnalyticsPlugin.STREAM_THREAD_POOL_NAME);
            CompletableFuture<Thread> observed = new CompletableFuture<>();
            exec.execute(() -> observed.complete(Thread.currentThread()));
            Thread t = observed.get(10, TimeUnit.SECONDS);
            assertFalse("stream-drain threads must be platform threads, not virtual", t.isVirtual());
        } finally {
            terminate(threadPool);
        }
    }
}
