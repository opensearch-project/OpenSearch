/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.SizeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

public class VirtualThreadExecutorBuilderTests extends OpenSearchTestCase {

    public void testVirtualThreadFactory() {
        Settings settings = Settings.builder().put("node.name", "test").build();
        VirtualThreadExecutorBuilder builder = new VirtualThreadExecutorBuilder(settings, "test_pool", 4, 100, new AtomicReference<>());

        AbstractResizableExecutorBuilder.ResizableExecutorSettings executorSettings =
            new AbstractResizableExecutorBuilder.ResizableExecutorSettings("test", 4, 100);
        ThreadFactory factory = builder.getThreadFactory(executorSettings);

        Thread thread = factory.newThread(() -> {});
        assertTrue(thread.isVirtual());
    }

    public void testThreadPoolType() {
        Settings settings = Settings.builder().put("node.name", "test").build();
        VirtualThreadExecutorBuilder builder = new VirtualThreadExecutorBuilder(settings, "test_pool", 4, 100, new AtomicReference<>());

        assertEquals(ThreadPool.ThreadPoolType.VIRTUAL, builder.getThreadPoolType());
    }

    public void testFormatInfo() {
        Settings settings = Settings.builder().put("node.name", "test").build();
        VirtualThreadExecutorBuilder builder = new VirtualThreadExecutorBuilder(settings, "test_pool", 4, 100, new AtomicReference<>());

        ThreadPool.Info info = new ThreadPool.Info("test_pool", ThreadPool.ThreadPoolType.VIRTUAL, 4, 4, null, new SizeValue(100));
        String formatted = builder.formatInfo(info);

        assertTrue(formatted.contains("virtual threads"));
        assertTrue(formatted.contains("test_pool"));
        assertTrue(formatted.contains("size [4]"));
        assertTrue(formatted.contains("queue size [100]"));
    }

    public void testBuild() {
        Settings nodeSettings = Settings.builder()
            .put("node.name", "test_node")
            .put("thread_pool.test_pool.size", 6)
            .put("thread_pool.test_pool.queue_size", 150)
            .build();
        VirtualThreadExecutorBuilder builder = new VirtualThreadExecutorBuilder(nodeSettings, "test_pool", 4, 100, new AtomicReference<>());

        AbstractResizableExecutorBuilder.ResizableExecutorSettings settings = builder.getSettings(nodeSettings);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool.ExecutorHolder holder = builder.build(settings, threadContext);

        assertEquals("test_pool", holder.info.getName());
        assertEquals(ThreadPool.ThreadPoolType.VIRTUAL, holder.info.getThreadPoolType());
        assertEquals(6, holder.info.getMax());
        assertEquals(new SizeValue(150), holder.info.getQueueSize());

        holder.executor().shutdown();
    }
}
