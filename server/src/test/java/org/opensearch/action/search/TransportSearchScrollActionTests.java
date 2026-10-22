/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.WorkloadGroupService;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportSearchScrollActionTests extends OpenSearchTestCase {

    public void testUnexpectedAdmissionExceptionTakesTheListenerPath() {
        WorkloadGroupService workloadGroupService = mock(WorkloadGroupService.class);
        when(workloadGroupService.isThrottlingActive()).thenReturn(true);
        IllegalStateException boom = new IllegalStateException("boom");
        doThrow(boom).when(workloadGroupService).acquireThrottlePermit(any(), any(), any());
        // The real helper runs over the stubbed admission, so the exception reaches it as it would in production.
        doCallRealMethod().when(workloadGroupService).executeWithThrottlePermit(any(), any(), any(), any());
        assertSame("an exception thrown by admission must be delivered to the listener", boom, executeScroll(workloadGroupService));
    }

    public void testScrollSkipsAdmissionWhileThrottlingIsInactive() {
        WorkloadGroupService workloadGroupService = mock(WorkloadGroupService.class);
        when(workloadGroupService.isThrottlingActive()).thenReturn(false);
        IllegalStateException boom = new IllegalStateException("boom");
        doThrow(boom).when(workloadGroupService).executeWithThrottlePermit(any(), any(), any(), any());
        Exception failure = executeScroll(workloadGroupService);
        verify(workloadGroupService, never()).executeWithThrottlePermit(any(), any(), any(), any());
        // The page itself still ran: the invalid scroll id fails past admission, never with the admission exception.
        assertTrue(failure instanceof IllegalArgumentException);
    }

    // Runs one scroll page with an invalid scroll id and returns the failure it completes with.
    private Exception executeScroll(WorkloadGroupService workloadGroupService) {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            TransportSearchScrollAction action = new TransportSearchScrollAction(
                mock(TransportService.class),
                mock(ClusterService.class),
                new ActionFilters(Collections.emptySet()),
                mock(SearchTransportService.class),
                new SearchPhaseController(new NamedWriteableRegistry(Collections.emptyList()), (searchSourceBuilder) -> null),
                threadPool,
                workloadGroupService
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
