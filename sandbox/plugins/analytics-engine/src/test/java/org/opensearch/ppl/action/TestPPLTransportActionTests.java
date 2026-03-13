/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TestPPLTransportAction}.
 *
 * <p>Uses the test-only constructor to inject a mock {@code UnifiedQueryService},
 * so we can test the transport action's listener contract in isolation.
 */
@SuppressWarnings("unchecked")
public class TestPPLTransportActionTests extends OpenSearchTestCase {

    private UnifiedQueryService mockUnifiedQueryService;
    private TestPPLTransportAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockUnifiedQueryService = mock(UnifiedQueryService.class);

        action = new TestPPLTransportAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
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
        when(mockUnifiedQueryService.execute("source=logs")).thenReturn(expectedResponse);

        ActionListener<PPLResponse> listener = mock(ActionListener.class);
        action.execute(null, new PPLRequest("source=logs"), listener);

        verify(listener).onResponse(expectedResponse);
        verify(mockUnifiedQueryService).execute("source=logs");
    }

    /**
     * Failure path: {@code unifiedQueryService.execute()} throws →
     * {@code listener.onFailure()} is called with the exception.
     */
    public void testFailurePathCallsOnFailure() {
        RuntimeException expectedException = new RuntimeException("PPL execution failed");
        when(mockUnifiedQueryService.execute(any(String.class))).thenThrow(expectedException);

        ActionListener<PPLResponse> listener = mock(ActionListener.class);
        action.execute(null, new PPLRequest("invalid query"), listener);

        verify(listener).onFailure(expectedException);
        verify(mockUnifiedQueryService).execute("invalid query");
    }

    /**
     * Exactly-one-callback on success: only {@code onResponse} is called, never {@code onFailure}.
     */
    public void testExactlyOneCallbackOnSuccess() {
        PPLResponse response = new PPLResponse(Collections.emptyList(), Collections.emptyList());
        when(mockUnifiedQueryService.execute(any(String.class))).thenReturn(response);

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
        when(mockUnifiedQueryService.execute(any(String.class))).thenThrow(new RuntimeException("fail"));

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
     * Verify that the correct PPL text is forwarded to
     * {@code unifiedQueryService.execute()}.
     */
    public void testCorrectArgumentsPassedToUnifiedQueryService() {
        PPLResponse response = new PPLResponse(Collections.emptyList(), Collections.emptyList());
        when(mockUnifiedQueryService.execute(any(String.class))).thenReturn(response);

        ActionListener<PPLResponse> listener = mock(ActionListener.class);
        action.execute(null, new PPLRequest("source=metrics | where status=500"), listener);

        verify(mockUnifiedQueryService).execute("source=metrics | where status=500");
        verifyNoMoreInteractions(mockUnifiedQueryService);
    }
}
