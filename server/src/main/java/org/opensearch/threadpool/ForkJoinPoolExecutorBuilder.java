/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.node.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;

/**
 * A builder for fork join executors.
 *
 * @opensearch.internal
 */
public final class ForkJoinPoolExecutorBuilder extends ExecutorBuilder<ForkJoinPoolExecutorBuilder.ForkJoinPoolExecutorSettings> {

    // Mandatory: parallelism, must be >= 1
    private final Setting<Integer> parallelismSetting;
    // Optional settings
    private final Setting<Boolean> asyncModeSetting;
    private final Setting<String> threadFactorySetting;
    private final Setting<Boolean> enableExceptionHandlingSetting;

    // Logger for uncaught exception handler
    private static final Logger logger = LogManager.getLogger(ForkJoinPoolExecutorBuilder.class);

    public ForkJoinPoolExecutorBuilder(final String name, final int parallelism) {
        this(name, parallelism, "thread_pool." + name);
    }

    public ForkJoinPoolExecutorBuilder(final String name, final int parallelism, final String prefix) {
        super(name);
        this.parallelismSetting = Setting.intSetting(
            settingsKey(prefix, "parallelism"),
            parallelism,
            1, // Enforce minimum of 1 (non-zero)
            Setting.Property.NodeScope
        );
        this.asyncModeSetting = Setting.boolSetting(settingsKey(prefix, "async_mode"), false, Setting.Property.NodeScope);
        this.threadFactorySetting = Setting.simpleString(settingsKey(prefix, "thread_factory"), "", Setting.Property.NodeScope);
        this.enableExceptionHandlingSetting = Setting.boolSetting(
            settingsKey(prefix, "enable_exception_handling"),
            true,
            Setting.Property.NodeScope
        );
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return Arrays.asList(parallelismSetting, asyncModeSetting, threadFactorySetting, enableExceptionHandlingSetting);
    }

    @Override
    ForkJoinPoolExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        final int parallelism = parallelismSetting.get(settings); // always >= 1
        final boolean asyncMode = asyncModeSetting.get(settings); // optional, default false
        final String threadFactoryClassName = threadFactorySetting.get(settings); // optional, default ""
        final boolean enableExceptionHandling = enableExceptionHandlingSetting.get(settings); // optional, default true
        return new ForkJoinPoolExecutorSettings(nodeName, parallelism, asyncMode, threadFactoryClassName, enableExceptionHandling);
    }

    @Override
    ThreadPool.ExecutorHolder build(final ForkJoinPoolExecutorSettings settings, final ThreadContext threadContext) {
        int parallelism = settings.parallelism;
        boolean asyncMode = settings.asyncMode;
        String threadFactoryClassName = settings.threadFactoryClassName;
        boolean enableExceptionHandling = settings.enableExceptionHandling;

        ForkJoinWorkerThreadFactory factory;
        if (threadFactoryClassName != null && !threadFactoryClassName.isEmpty()) {
            // Try to instantiate a custom thread factory by class name (must implement ForkJoinWorkerThreadFactory)
            try {
                Class<?> clazz = Class.forName(threadFactoryClassName);
                factory = (ForkJoinWorkerThreadFactory) clazz.getConstructor().newInstance();
            } catch (Exception e) {
                logger.warn(
                    "Unable to instantiate custom ForkJoinWorkerThreadFactory '{}', using default. Error: {}",
                    threadFactoryClassName,
                    e.toString()
                );
                factory = pool -> {
                    ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    worker.setName(OpenSearchExecutors.threadName(settings.nodeName, name()));
                    return worker;
                };
            }
        } else {
            factory = pool -> {
                ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                worker.setName(OpenSearchExecutors.threadName(settings.nodeName, name()));
                return worker;
            };
        }

        Thread.UncaughtExceptionHandler exceptionHandler = enableExceptionHandling
            ? (thread, throwable) -> logger.error("Uncaught exception in ForkJoinPool thread [" + thread.getName() + "]", throwable)
            : null;

        final ForkJoinPool executor = new ForkJoinPool(parallelism, factory, exceptionHandler, asyncMode);

        final ThreadPool.Info info = new ThreadPool.Info(name(), ThreadPool.ThreadPoolType.FORK_JOIN, parallelism, parallelism, null, null);
        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(Locale.ROOT, "name [%s], parallelism [%d]", info.getName(), info.getMax());
    }

    static class ForkJoinPoolExecutorSettings extends ExecutorBuilder.ExecutorSettings {
        private final int parallelism;
        private final boolean asyncMode;
        private final String threadFactoryClassName;
        private final boolean enableExceptionHandling;

        ForkJoinPoolExecutorSettings(
            final String nodeName,
            final int parallelism,
            final boolean asyncMode,
            final String threadFactoryClassName,
            final boolean enableExceptionHandling
        ) {
            super(nodeName);
            this.parallelism = parallelism;
            this.asyncMode = asyncMode;
            this.threadFactoryClassName = threadFactoryClassName;
            this.enableExceptionHandling = enableExceptionHandling;
        }
    }
}
