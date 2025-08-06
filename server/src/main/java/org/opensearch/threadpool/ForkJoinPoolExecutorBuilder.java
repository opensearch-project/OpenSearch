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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;

/**
 * A builder for fork join executors.
 *
 * @opensearch.internal
 */
public final class ForkJoinPoolExecutorBuilder extends ExecutorBuilder<ForkJoinPoolExecutorBuilder.ForkJoinPoolExecutorSettings> {

    private final Setting<Integer> parallelismSetting;

    public ForkJoinPoolExecutorBuilder(final String name, final int parallelism) {
        this(name, parallelism, "thread_pool." + name);
    }

    public ForkJoinPoolExecutorBuilder(final String name, final int parallelism, final String prefix) {
        super(name);
        this.parallelismSetting = Setting.intSetting(settingsKey(prefix, "parallelism"), parallelism, Setting.Property.NodeScope);
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return Arrays.asList(parallelismSetting);
    }

    @Override
    ForkJoinPoolExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        final int parallelism = parallelismSetting.get(settings);
        return new ForkJoinPoolExecutorSettings(nodeName, parallelism);
    }

    @Override
    ThreadPool.ExecutorHolder build(final ForkJoinPoolExecutorSettings settings, final ThreadContext threadContext) {
        int parallelism = settings.parallelism;
        ForkJoinWorkerThreadFactory factory = pool -> {
            ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
            worker.setName(OpenSearchExecutors.threadName(settings.nodeName, name()));
            return worker;
        };
        final ForkJoinPool executor = new ForkJoinPool(parallelism, factory, null, false);

        final ThreadPool.Info info = new ThreadPool.Info(
            name(),
            ThreadPool.ThreadPoolType.FORK_JOIN,
            parallelism,
            parallelism,
            null,
            null
        );
        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], parallelism [%d]",
            info.getName(),
            info.getMax()
        );
    }

    static class ForkJoinPoolExecutorSettings extends ExecutorBuilder.ExecutorSettings {
        private final int parallelism;

        ForkJoinPoolExecutorSettings(final String nodeName, final int parallelism) {
            super(nodeName);
            this.parallelism = parallelism;
        }
    }
}
