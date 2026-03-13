/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.http.HttpChannel;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for TerminalChannelReleasingListener to ensure proper REST channel lifecycle management
 * and exactly-once untracking semantics for streaming requests.
 */
public class TerminalChannelReleasingListenerTests extends OpenSearchTestCase {

    private RestCancellableNodeClient mockClient;
    private RestChannel mockChannel;
    private HttpChannel mockHttpChannel;
    private RestRequest mockRequest;
    private ActionListener<SearchResponse> mockDelegate;
    private SearchResponse mockResponse;
    private Exception testException;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockClient = mock(RestCancellableNodeClient.class);
        mockChannel = mock(RestChannel.class);
        mockHttpChannel = mock(HttpChannel.class);
        mockRequest = mock(RestRequest.class);
        mockDelegate = mock(ActionListener.class);
        mockResponse = mock(SearchResponse.class);
        testException = new RuntimeException("Test exception");

        // Set up mock relationships
        when(mockChannel.request()).thenReturn(mockRequest);
        when(mockRequest.getHttpChannel()).thenReturn(mockHttpChannel);
    }

    public void testOnResponseCallsUntrackExactlyOnce() {
        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(
            mockClient,
            mockChannel,
            mockDelegate
        );

        listener.onResponse(mockResponse);

        verify(mockDelegate, times(1)).onResponse(mockResponse);
        verify(mockClient, times(1)).finishTracking(mockHttpChannel);
        assertTrue(listener.isFinished());
    }

    public void testOnFailureCallsUntrackExactlyOnce() {
        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(
            mockClient,
            mockChannel,
            mockDelegate
        );

        listener.onFailure(testException);

        verify(mockDelegate, times(1)).onFailure(testException);
        verify(mockClient, times(1)).finishTracking(mockHttpChannel);
        assertTrue(listener.isFinished());
    }

    public void testDoubleOnResponseCallsUntrackOnlyOnce() {
        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(
            mockClient,
            mockChannel,
            mockDelegate
        );

        listener.onResponse(mockResponse);
        listener.onResponse(mockResponse); // Second call should be ignored

        verify(mockDelegate, times(1)).onResponse(mockResponse); // Only first call processed
        verify(mockClient, times(1)).finishTracking(mockHttpChannel); // Untrack called only once
        assertTrue(listener.isFinished());
    }

    public void testDoubleOnFailureCallsUntrackOnlyOnce() {
        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(
            mockClient,
            mockChannel,
            mockDelegate
        );

        listener.onFailure(testException);
        listener.onFailure(testException); // Second call should be ignored

        verify(mockDelegate, times(1)).onFailure(testException); // Only first call processed
        verify(mockClient, times(1)).finishTracking(mockHttpChannel); // Untrack called only once
        assertTrue(listener.isFinished());
    }

    public void testOnResponseThenOnFailureCallsUntrackOnlyOnce() {
        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(
            mockClient,
            mockChannel,
            mockDelegate
        );

        listener.onResponse(mockResponse);
        listener.onFailure(testException); // Second call should be ignored

        verify(mockDelegate, times(1)).onResponse(mockResponse); // Only first call processed
        verify(mockDelegate, never()).onFailure(testException); // Second call ignored
        verify(mockClient, times(1)).finishTracking(mockHttpChannel); // Untrack called only once
        assertTrue(listener.isFinished());
    }

    public void testChannelCloseBackstopTriggersUntrack() {
        AtomicBoolean closeListenerCalled = new AtomicBoolean(false);

        doAnswer(invocation -> {
            ActionListener<Void> closeListener = invocation.getArgument(0);
            closeListenerCalled.set(true);
            closeListener.onResponse(null); // Simulate channel close
            return null;
        }).when(mockHttpChannel).addCloseListener(org.mockito.ArgumentMatchers.any(ActionListener.class));

        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(
            mockClient,
            mockChannel,
            mockDelegate
        );

        listener.setupBackstops();

        assertTrue("Close listener should have been called", closeListenerCalled.get());
        verify(mockClient, times(1)).finishTracking(mockHttpChannel);
        assertTrue(listener.isFinished());
    }

    public void testChannelCloseAfterTerminalSignalDoesNotDoubleUntrack() {
        AtomicReference<ActionListener<Void>> capturedCloseListener = new AtomicReference<>();

        doAnswer(invocation -> {
            ActionListener<Void> closeListener = invocation.getArgument(0);
            capturedCloseListener.set(closeListener);
            return null;
        }).when(mockHttpChannel).addCloseListener(org.mockito.ArgumentMatchers.any(ActionListener.class));

        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(
            mockClient,
            mockChannel,
            mockDelegate
        );

        listener.setupBackstops();

        // First, handle terminal signal
        listener.onResponse(mockResponse);

        // Then simulate channel close (should not double untrack)
        if (capturedCloseListener.get() != null) {
            capturedCloseListener.get().onResponse(null);
        }

        verify(mockDelegate, times(1)).onResponse(mockResponse);
        verify(mockClient, times(1)).finishTracking(mockHttpChannel); // Only one untrack call
        assertTrue(listener.isFinished());
    }

    public void testUntrackWithExceptionDoesNotPropagateException() {
        RuntimeException untrackException = new RuntimeException("Untrack failed");
        doAnswer(invocation -> { throw untrackException; }).when(mockClient).finishTracking(mockHttpChannel);

        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(
            mockClient,
            mockChannel,
            mockDelegate
        );

        // Should not throw exception even if finishTracking fails
        try {
            listener.onResponse(mockResponse);
        } catch (Exception e) {
            fail("Should not throw exception even if finishTracking fails, but got: " + e.getMessage());
        }

        verify(mockDelegate, times(1)).onResponse(mockResponse);
        verify(mockClient, times(1)).finishTracking(mockHttpChannel);
        assertTrue(listener.isFinished());
    }

    public void testNullClientHandledGracefully() {
        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(null, mockChannel, mockDelegate);

        // Should not throw exception with null client
        try {
            listener.onResponse(mockResponse);
        } catch (Exception e) {
            fail("Should not throw exception with null client, but got: " + e.getMessage());
        }

        verify(mockDelegate, times(1)).onResponse(mockResponse);
        assertTrue(listener.isFinished());
    }

    public void testNullChannelHandledGracefully() {
        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(mockClient, null, mockDelegate);

        // Should not throw exception with null channel
        try {
            listener.onResponse(mockResponse);
        } catch (Exception e) {
            fail("Should not throw exception with null channel, but got: " + e.getMessage());
        }

        verify(mockDelegate, times(1)).onResponse(mockResponse);
        assertTrue(listener.isFinished());
    }

    public void testGetChannel() {
        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(
            mockClient,
            mockChannel,
            mockDelegate
        );

        assertEquals(mockChannel, listener.getChannel());
    }

    public void testIsFinishedInitiallyFalse() {
        TerminalChannelReleasingListener<SearchResponse> listener = new TerminalChannelReleasingListener<>(
            mockClient,
            mockChannel,
            mockDelegate
        );

        assertFalse(listener.isFinished());
    }
}
