/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public class TransportSearchScrollActionTests extends OpenSearchTestCase {

    public void testUnexpectedAdmissionExceptionTakesTheListenerPath() {
        IllegalStateException boom = new IllegalStateException("boom");
        assertSame(
            "an exception thrown by admission must be delivered to the listener",
            boom,
            executeScroll(new SearchExecutionGate((task, request, listener) -> {
                throw boom;
            }))
        );
    }

    public void testScrollSkipsAdmissionWhileGateIsInactive() {
        IllegalStateException boom = new IllegalStateException("boom");
        AtomicBoolean admitted = new AtomicBoolean();
        Exception failure = executeScroll(new SearchExecutionGate(new SearchAdmission() {
            @Override
            public boolean isActive() {
                return false;
            }

            @Override
            public void admit(Task task, ActionRequest request, ActionListener<Releasable> listener) {
                admitted.set(true);
                throw boom;
            }
        }));
        assertFalse("an inactive admission must not be consulted", admitted.get());
        // The page itself still ran: the invalid scroll id fails past admission, never with the admission exception.
        assertTrue(failure instanceof IllegalArgumentException);
    }

    // Runs one scroll page with an invalid scroll id and returns the failure it completes with.
    private Exception executeScroll(SearchExecutionGate searchExecutionGate) {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            TransportSearchScrollAction action = new TransportSearchScrollAction(
                mock(TransportService.class),
                mock(ClusterService.class),
                new ActionFilters(Collections.emptySet()),
                mock(SearchTransportService.class),
                new SearchPhaseController(new NamedWriteableRegistry(Collections.emptyList()), (searchSourceBuilder) -> null),
                threadPool,
                searchExecutionGate
            );
            SearchTask task = new SearchTask(
                1,
                "transport",
                SearchScrollAction.NAME,
                () -> "test",
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap()
            );
            AtomicReference<Exception> failure = new AtomicReference<>();
            action.doExecute(task, new SearchScrollRequest("scroll-id"), ActionListener.wrap(r -> fail("must not succeed"), failure::set));
            return failure.get();
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }
}
