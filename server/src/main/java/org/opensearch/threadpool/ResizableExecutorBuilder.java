/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;

import java.util.Locale;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A builder for resizable executors.
 *
 * @opensearch.internal
 */
public final class ResizableExecutorBuilder extends AbstractResizableExecutorBuilder {
    ResizableExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final AtomicReference<RunnableTaskExecutionListener> runnableTaskListener
    ) {
        super(settings, name, size, queueSize, "thread_pool." + name, runnableTaskListener);
    }

    public ResizableExecutorBuilder(
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
        return OpenSearchExecutors.daemonThreadFactory(OpenSearchExecutors.threadName(settings.nodeName, name()));
    }

    @Override
    ThreadPool.ThreadPoolType getThreadPoolType() {
        return ThreadPool.ThreadPoolType.RESIZABLE;
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], size [%d], queue size [%s]",
            info.getName(),
            info.getMax(),
            info.getQueueSize() == null ? "unbounded" : info.getQueueSize()
        );
    }
}
