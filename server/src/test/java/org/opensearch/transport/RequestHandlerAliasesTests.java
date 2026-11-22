/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RequestHandlerAliasesTests extends OpenSearchTestCase {
    public void testAliasUsesCanonicalHandlerAndTask() throws Exception {
        ThreadPool threadPool = new TestThreadPool("aliases");
        try {
            TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());
            AtomicReference<String> taskAction = new AtomicReference<>();
            RequestHandlerRegistry<TransportRequest> canonical = new RequestHandlerRegistry<>(
                "canonical:action",
                TransportRequest.Empty::new,
                taskManager,
                (request, channel, task) -> {
                    taskAction.set(task.getAction());
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                },
                ThreadPool.Names.SAME,
                false,
                true
            );
            Transport.RequestHandlers handlers = new Transport.RequestHandlers();
            handlers.registerHandler(canonical);
            handlers.registerAlias("legacy:action", "canonical:action");
            assertSame(canonical, handlers.getHandler("legacy:action"));
            TransportChannel channel = mock(TransportChannel.class);
            when(channel.getChannelType()).thenReturn("transport");
            handlers.getHandler("legacy:action").processMessageReceived(new TransportRequest.Empty(), channel);
            assertEquals("canonical:action", taskAction.get());
            assertTrue(taskManager.getTasks().isEmpty());
            RequestHandlerRegistry<TransportRequest> replacement = RequestHandlerRegistry.replaceHandler(canonical, canonical.getHandler());
            handlers.forceRegister(replacement);
            assertSame(replacement, handlers.getHandler("canonical:action"));
            assertSame(replacement, handlers.getHandler("legacy:action"));
            expectThrows(IllegalArgumentException.class, () -> handlers.registerAlias("legacy:action", "canonical:action"));
            expectThrows(IllegalArgumentException.class, () -> handlers.registerAlias("other:action", "missing:action"));
        } finally {
            terminate(threadPool);
        }
    }
}
