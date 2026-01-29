/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Settings;

import java.util.Locale;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Executor builder for a resizable threadpool, where each thread is produced by a virtual thread.
 */
public class VirtualThreadExecutorBuilder extends AbstractResizableExecutorBuilder {

    // TODO - do we want this other ctor in abstract class instead?
    VirtualThreadExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final AtomicReference<RunnableTaskExecutionListener> runnableTaskListener
    ) {
        super(settings, name, size, queueSize, "thread_pool." + name, runnableTaskListener);
    }

    public VirtualThreadExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final String prefix,
        final AtomicReference<RunnableTaskExecutionListener> runnableTaskListener
    ) {
        super(settings, name, size, queueSize, prefix, runnableTaskListener);
    }

    @Override
    ThreadFactory getThreadFactory(ResizableExecutorSettings settings) {
        return Thread.ofVirtual().factory();
    }

    @Override
    ThreadPool.ThreadPoolType getThreadPoolType() {
        return ThreadPool.ThreadPoolType.VIRTUAL;
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], size [%d], queue size [%s], virtual threads",
            info.getName(),
            info.getMax(),
            info.getQueueSize() == null ? "unbounded" : info.getQueueSize()
        );
    }
}
