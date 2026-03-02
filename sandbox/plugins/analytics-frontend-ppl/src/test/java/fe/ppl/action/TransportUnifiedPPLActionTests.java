/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package fe.ppl.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.fe.ppl.action.TransportUnifiedPPLAction;
import org.opensearch.fe.ppl.action.UnifiedPPLRequest;
import org.opensearch.fe.ppl.action.UnifiedPPLResponse;
import org.opensearch.fe.ppl.action.UnifiedQueryService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TransportUnifiedPPLAction}.
 */
@SuppressWarnings("unchecked")
public class TransportUnifiedPPLActionTests extends OpenSearchTestCase {

    private ClusterService mockClusterService;
    private UnifiedQueryService mockQueryService;
    private ClusterState mockClusterState;
    private TransportUnifiedPPLAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockClusterService = mock(ClusterService.class);
        mockQueryService = mock(UnifiedQueryService.class);
        mockClusterState = mock(ClusterState.class);

        when(mockClusterService.state()).thenReturn(mockClusterState);

        action = new TransportUnifiedPPLAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockClusterService,
            mockQueryService
        );
    }

    /**
     * Test success path: UnifiedQueryService returns a response, listener.onResponse is called.
     */
    public void testSuccessPathCallsOnResponse() {
        List<Object[]> rows = new java.util.ArrayList<>();
        rows.add(new Object[] { "server-1", 200 });
        UnifiedPPLResponse expectedResponse = new UnifiedPPLResponse(List.of("host", "status"), rows);
        when(mockQueryService.execute(eq("source=logs"), any(ClusterState.class))).thenReturn(expectedResponse);

        ActionListener<UnifiedPPLResponse> listener = mock(ActionListener.class);
        action.execute(null, new UnifiedPPLRequest("source=logs"), listener);

        verify(listener).onResponse(expectedResponse);
        verify(mockQueryService).execute("source=logs", mockClusterState);
    }

    /**
     * Test failure path: UnifiedQueryService throws exception, listener.onFailure is called.
     */
    public void testFailurePathCallsOnFailure() {
        RuntimeException expectedException = new RuntimeException("PPL execution failed");
        when(mockQueryService.execute(any(String.class), any(ClusterState.class))).thenThrow(expectedException);

        ActionListener<UnifiedPPLResponse> listener = mock(ActionListener.class);
        action.execute(null, new UnifiedPPLRequest("invalid query"), listener);

        verify(listener).onFailure(expectedException);
    }

    /**
     * Test that exactly one of onResponse or onFailure is called per request on success.
     * Validates: Requirement 6.4
     */
    public void testExactlyOneCallbackOnSuccess() {
        UnifiedPPLResponse response = new UnifiedPPLResponse(Collections.emptyList(), Collections.emptyList());
        when(mockQueryService.execute(any(String.class), any(ClusterState.class))).thenReturn(response);

        AtomicInteger responseCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        ActionListener<UnifiedPPLResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(UnifiedPPLResponse r) {
                responseCount.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                failureCount.incrementAndGet();
            }
        };

        action.execute(null, new UnifiedPPLRequest("source=test"), listener);

        assertEquals("onResponse should be called exactly once", 1, responseCount.get());
        assertEquals("onFailure should not be called", 0, failureCount.get());
    }

    /**
     * Test that exactly one of onResponse or onFailure is called per request on failure.
     */
    public void testExactlyOneCallbackOnFailure() {
        when(mockQueryService.execute(any(String.class), any(ClusterState.class))).thenThrow(new RuntimeException("fail"));

        AtomicInteger responseCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicReference<Exception> capturedError = new AtomicReference<>();

        ActionListener<UnifiedPPLResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(UnifiedPPLResponse r) {
                responseCount.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                failureCount.incrementAndGet();
                capturedError.set(e);
            }
        };

        action.execute(null, new UnifiedPPLRequest("source=test"), listener);

        assertEquals("onResponse should not be called", 0, responseCount.get());
        assertEquals("onFailure should be called exactly once", 1, failureCount.get());
        assertNotNull("Exception should be captured", capturedError.get());
    }
}
