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

/**
 * A builder for fixed executors.
*
* @opensearch.internal
*/
public final class ProtobufFixedExecutorBuilder extends ProtobufExecutorBuilder<ProtobufFixedExecutorBuilder.FixedExecutorSettings> {

    private final Setting<Integer> sizeSetting;
    private final Setting<Integer> queueSizeSetting;

    /**
     * Construct a fixed executor builder; the settings will have the key prefix "thread_pool." followed by the executor name.
    *
    * @param settings  the node-level settings
    * @param name      the name of the executor
    * @param size      the fixed number of threads
    * @param queueSize the size of the backing queue, -1 for unbounded
    */
    ProtobufFixedExecutorBuilder(final Settings settings, final String name, final int size, final int queueSize) {
        this(settings, name, size, queueSize, false);
    }

    /**
     * Construct a fixed executor builder; the settings will have the key prefix "thread_pool." followed by the executor name.
    *
    * @param settings   the node-level settings
    * @param name       the name of the executor
    * @param size       the fixed number of threads
    * @param queueSize  the size of the backing queue, -1 for unbounded
    * @param deprecated whether or not the thread pool is deprecated
    */
    ProtobufFixedExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final boolean deprecated
    ) {
        this(settings, name, size, queueSize, "thread_pool." + name, deprecated);
    }

    /**
     * Construct a fixed executor builder.
    *
    * @param settings  the node-level settings
    * @param name      the name of the executor
    * @param size      the fixed number of threads
    * @param queueSize the size of the backing queue, -1 for unbounded
    * @param prefix    the prefix for the settings keys
    */
    public ProtobufFixedExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final String prefix
    ) {
        this(settings, name, size, queueSize, prefix, false);
    }

    /**
     * Construct a fixed executor builder.
    *
    * @param settings   the node-level settings
    * @param name       the name of the executor
    * @param size       the fixed number of threads
    * @param queueSize  the size of the backing queue, -1 for unbounded
    * @param prefix     the prefix for the settings keys
    * @param deprecated whether or not the thread pool is deprecated
    */
    public ProtobufFixedExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final int queueSize,
        final String prefix,
        final boolean deprecated
    ) {
        super(name);
        final String sizeKey = settingsKey(prefix, "size");
        final Setting.Property[] properties;
        if (deprecated) {
            properties = new Setting.Property[] { Setting.Property.NodeScope, Setting.Property.Deprecated };
        } else {
            properties = new Setting.Property[] { Setting.Property.NodeScope };
        }
        this.sizeSetting = new Setting<>(
            sizeKey,
            s -> Integer.toString(size),
            s -> Setting.parseInt(s, 1, applyHardSizeLimit(settings, name), sizeKey),
            properties
        );
        final String queueSizeKey = settingsKey(prefix, "queue_size");
        this.queueSizeSetting = Setting.intSetting(queueSizeKey, queueSize, properties);
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return Arrays.asList(sizeSetting, queueSizeSetting);
    }

    @Override
    FixedExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        final int size = sizeSetting.get(settings);
        final int queueSize = queueSizeSetting.get(settings);
        return new FixedExecutorSettings(nodeName, size, queueSize);
    }

    @Override
    ProtobufThreadPool.ExecutorHolder build(final FixedExecutorSettings settings, final ThreadContext threadContext) {
        int size = settings.size;
        int queueSize = settings.queueSize;
        final ThreadFactory threadFactory = OpenSearchExecutors.daemonThreadFactory(
            OpenSearchExecutors.threadName(settings.nodeName, name())
        );
        final ExecutorService executor = OpenSearchExecutors.newFixed(
            settings.nodeName + "/" + name(),
            size,
            queueSize,
            threadFactory,
            threadContext
        );
        final ProtobufThreadPool.Info info = new ProtobufThreadPool.Info(
            name(),
            ProtobufThreadPool.ThreadPoolType.FIXED,
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

    static class FixedExecutorSettings extends ProtobufExecutorBuilder.ExecutorSettings {

        private final int size;
        private final int queueSize;

        FixedExecutorSettings(final String nodeName, final int size, final int queueSize) {
            super(nodeName);
            this.size = size;
            this.queueSize = queueSize;
        }

    }

}
