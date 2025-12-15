/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.node.Node;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Executor builder for a "threadpool" using per-task virtual threads.
 */
public class VirtualThreadExecutorBuilder extends ExecutorBuilder<VirtualThreadExecutorBuilder.VirtualThreadExecutorSettings> {

    public VirtualThreadExecutorBuilder(final String name, Settings settings) { // TODO - settings not needed?
        super(name);
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return List.of();
    }

    @Override
    VirtualThreadExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        return new VirtualThreadExecutorSettings(nodeName);
    }

    @Override
    ThreadPool.ExecutorHolder build(VirtualThreadExecutorSettings settings, ThreadContext threadContext) {
        // Create virtual thread executor - each task gets its own virtual thread
        final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        final ThreadPool.Info info = new ThreadPool.Info(
            name(),
            ThreadPool.ThreadPoolType.VIRTUAL,
            0,
            Integer.MAX_VALUE,  // Unlimited virtual threads
            null,
            null  // No queue - tasks execute immediately on virtual threads
        );

        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], type [virtual], per-task virtual threads",
            info.getName()
        );
    }

    static class VirtualThreadExecutorSettings extends ExecutorBuilder.ExecutorSettings {
        VirtualThreadExecutorSettings(final String nodeName) {
            super(nodeName);
        }
    }
}
