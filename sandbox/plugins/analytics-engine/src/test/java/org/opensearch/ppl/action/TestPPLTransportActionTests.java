/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.TestPPLTransportAction;
import org.opensearch.ppl.action.UnifiedQueryService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TestPPLTransportAction}.
 *
 * <p>Uses reflection to replace the private {@code unifiedQueryService} field with a mock,
 * so we can test the transport action's listener contract in isolation without going
 * through the real pipeline (PushDownPlanner → DefaultPlanExecutor → QueryPlanExecutor).
 */
@SuppressWarnings("unchecked")
public class TestPPLTransportActionTests extends OpenSearchTestCase {

    private ClusterService mockClusterService;
    private ClusterState mockClusterState;
    private UnifiedQueryService mockUnifiedQueryService;
    private TestPPLTransportAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockClusterService = mock(ClusterService.class);
        mockClusterState = mock(ClusterState.class);
        mockUnifiedQueryService = mock(UnifiedQueryService.class);

        when(mockClusterService.state()).thenReturn(mockClusterState);

        action = new TestPPLTransportAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockClusterService,
            mockUnifiedQueryService
        );
    }

    /**
     * Success path: {@code unifiedQueryService.execute()} returns a response →
     * {@code listener.onResponse()} is called with that response.
     */
    public void testSuccessPathCallsOnResponse() {
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "server-1", 200 });
        PPLResponse expectedResponse = new PPLResponse(List.of("host", "status"), rows);
        when(mockUnifiedQueryService.execute(eq("source=logs"), any(ClusterState.class))).thenReturn(expectedResponse);

        ActionListener<PPLResponse> listener = mock(ActionListener.class);
        action.execute(null, new PPLRequest("source=logs"), listener);

        verify(listener).onResponse(expectedResponse);
        verify(mockUnifiedQueryService).execute("source=logs", mockClusterState);
    }

    /**
     * Failure path: {@code unifiedQueryService.execute()} throws →
     * {@code listener.onFailure()} is called with the exception.
     */
    public void testFailurePathCallsOnFailure() {
        RuntimeException expectedException = new RuntimeException("PPL execution failed");
        when(mockUnifiedQueryService.execute(any(String.class), any(ClusterState.class))).thenThrow(expectedException);

        ActionListener<PPLResponse> listener = mock(ActionListener.class);
        action.execute(null, new PPLRequest("invalid query"), listener);

        verify(listener).onFailure(expectedException);
        verify(mockUnifiedQueryService).execute("invalid query", mockClusterState);
    }

    /**
     * Exactly-one-callback on success: only {@code onResponse} is called, never {@code onFailure}.
     */
    public void testExactlyOneCallbackOnSuccess() {
        PPLResponse response = new PPLResponse(Collections.emptyList(), Collections.emptyList());
        when(mockUnifiedQueryService.execute(any(String.class), any(ClusterState.class))).thenReturn(response);

        AtomicInteger responseCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        ActionListener<PPLResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(PPLResponse r) {
                responseCount.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                failureCount.incrementAndGet();
            }
        };

        action.execute(null, new PPLRequest("source=test"), listener);

        assertEquals("onResponse should be called exactly once", 1, responseCount.get());
        assertEquals("onFailure should not be called", 0, failureCount.get());
    }

    /**
     * Exactly-one-callback on failure: only {@code onFailure} is called, never {@code onResponse}.
     */
    public void testExactlyOneCallbackOnFailure() {
        when(mockUnifiedQueryService.execute(any(String.class), any(ClusterState.class))).thenThrow(new RuntimeException("fail"));

        AtomicInteger responseCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicReference<Exception> capturedError = new AtomicReference<>();

        ActionListener<PPLResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(PPLResponse r) {
                responseCount.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                failureCount.incrementAndGet();
                capturedError.set(e);
            }
        };

        action.execute(null, new PPLRequest("source=test"), listener);

        assertEquals("onResponse should not be called", 0, responseCount.get());
        assertEquals("onFailure should be called exactly once", 1, failureCount.get());
        assertNotNull("Exception should be captured", capturedError.get());
    }

    /**
     * Verify that the correct PPL text and cluster state are forwarded to
     * {@code unifiedQueryService.execute()}.
     */
    public void testCorrectArgumentsPassedToUnifiedQueryService() {
        PPLResponse response = new PPLResponse(Collections.emptyList(), Collections.emptyList());
        when(mockUnifiedQueryService.execute(any(String.class), any(ClusterState.class))).thenReturn(response);

        ActionListener<PPLResponse> listener = mock(ActionListener.class);
        action.execute(null, new PPLRequest("source=metrics | where status=500"), listener);

        // Verify exact arguments: the PPL text from the request and the cluster state from ClusterService
        verify(mockUnifiedQueryService).execute("source=metrics | where status=500", mockClusterState);
        verifyNoMoreInteractions(mockUnifiedQueryService);
    }
}
