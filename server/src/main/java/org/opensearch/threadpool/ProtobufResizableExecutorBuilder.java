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
import org.opensearch.common.unit.ProtobufSizeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.node.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A builder for resizable executors.
*
* @opensearch.internal
*/
public final class ProtobufResizableExecutorBuilder extends ProtobufExecutorBuilder<
    ProtobufResizableExecutorBuilder.ResizableExecutorSettings> {

    private final Setting<Integer> sizeSetting;
    private final Setting<Integer> queueSizeSetting;
    private final AtomicReference<RunnableTaskExecutionListener> runnableTaskListener;

    ProtobufResizableExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final AtomicReference<RunnableTaskExecutionListener> runnableTaskListener
    ) {
        this(settings, name, size, queueSize, "thread_pool." + name, runnableTaskListener);
    }

    public ProtobufResizableExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final String prefix,
        final AtomicReference<RunnableTaskExecutionListener> runnableTaskListener
    ) {
        super(name);
        final String sizeKey = settingsKey(prefix, "size");
        this.sizeSetting = new Setting<>(
            sizeKey,
            s -> Integer.toString(size),
            s -> Setting.parseInt(s, 1, applyHardSizeLimit(settings, name), sizeKey),
            Setting.Property.NodeScope
        );
        final String queueSizeKey = settingsKey(prefix, "queue_size");
        this.queueSizeSetting = Setting.intSetting(
            queueSizeKey,
            queueSize,
            new Setting.Property[] { Setting.Property.NodeScope, Setting.Property.Dynamic }
        );
        this.runnableTaskListener = runnableTaskListener;
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return Arrays.asList(sizeSetting, queueSizeSetting);
    }

    @Override
    ResizableExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        final int size = sizeSetting.get(settings);
        final int queueSize = queueSizeSetting.get(settings);
        return new ResizableExecutorSettings(nodeName, size, queueSize);
    }

    @Override
    ProtobufThreadPool.ExecutorHolder build(final ResizableExecutorSettings settings, final ThreadContext threadContext) {
        int size = settings.size;
        int queueSize = settings.queueSize;
        final ThreadFactory threadFactory = OpenSearchExecutors.daemonThreadFactory(
            OpenSearchExecutors.threadName(settings.nodeName, name())
        );
        final ExecutorService executor = OpenSearchExecutors.newResizable(
            settings.nodeName + "/" + name(),
            size,
            queueSize,
            threadFactory,
            threadContext,
            runnableTaskListener
        );
        final ProtobufThreadPool.Info info = new ProtobufThreadPool.Info(
            name(),
            ProtobufThreadPool.ThreadPoolType.RESIZABLE,
            size,
            size,
            null,
            queueSize < 0 ? null : new ProtobufSizeValue(queueSize)
        );
        return new ProtobufThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ProtobufThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], size [%d], queue size [%s]",
            info.getName(),
            info.getMax(),
            info.getQueueSize() == null ? "unbounded" : info.getQueueSize()
        );
    }

    static class ResizableExecutorSettings extends ProtobufExecutorBuilder.ExecutorSettings {

        private final int size;
        private final int queueSize;

        ResizableExecutorSettings(final String nodeName, final int size, final int queueSize) {
            super(nodeName);
            this.size = size;
            this.queueSize = queueSize;
        }

    }
}
