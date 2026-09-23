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
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.node.Node;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

/**
 * A builder for executors backed by a virtual thread per task.
 *
 * @opensearch.internal
 */
public final class VirtualExecutorBuilder extends ExecutorBuilder<VirtualExecutorBuilder.VirtualExecutorSettings> {

    /**
     * Construct a virtual thread-per-task executor builder.
     *
     * @param name the name of the executor
     */
    public VirtualExecutorBuilder(final String name) {
        super(name);
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return List.of();
    }

    @Override
    VirtualExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        return new VirtualExecutorSettings(nodeName);
    }

    @Override
    ThreadPool.ExecutorHolder build(final VirtualExecutorSettings settings, final ThreadContext threadContext) {
        final ExecutorService executor = OpenSearchExecutors.newVirtualThreadPerTaskExecutor(settings.nodeName, name(), threadContext);
        final ThreadPool.Info info = new ThreadPool.Info(name(), ThreadPool.ThreadPoolType.VIRTUAL);
        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(Locale.ROOT, "name [%s], virtual thread per task", info.getName());
    }

    static class VirtualExecutorSettings extends ExecutorBuilder.ExecutorSettings {
        VirtualExecutorSettings(final String nodeName) {
            super(nodeName);
        }
    }

}
