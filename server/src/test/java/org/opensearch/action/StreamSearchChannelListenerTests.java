/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.junit.Before;

import java.io.IOException;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests for StreamChannelActionListener streaming functionality
 */
public class StreamSearchChannelListenerTests extends OpenSearchTestCase {

    @Mock
    private TransportChannel channel;

    @Mock
    private TransportRequest request;

    private StreamSearchChannelListener<TestResponse, TransportRequest> listener;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        listener = new StreamSearchChannelListener<>(channel, "test-action", request);
    }

    public void testStreamResponseCall() {
        TestResponse response = new TestResponse("batch1");
        listener.onStreamResponse(response, false);

        verify(channel).sendResponseBatch(response);
        verifyNoMoreInteractions(channel);
    }

    public void testCompleteResponseCall() {
        TestResponse response = new TestResponse("final");
        listener.onStreamResponse(response, true);

        verify(channel).sendResponseBatch(response);
        verify(channel).completeStream();
    }

    public void testOnResponseDelegatesToCompleteResponse() {
        TestResponse response = new TestResponse("final");
        listener.onResponse(response);

        verify(channel).sendResponseBatch(response);
        verify(channel).completeStream();
    }

    public void testFailureCall() throws Exception {
        RuntimeException exception = new RuntimeException("test failure");
        listener.onFailure(exception);

        verify(channel).sendResponse(exception);
    }

    /**
     * Simple test response for testing
     */
    public static class TestResponse extends TransportResponse {
        private final String data;

        public TestResponse(String data) {
            this.data = data;
        }

        public String getData() {
            return data;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(data);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            TestResponse that = (TestResponse) obj;
            return data != null ? data.equals(that.data) : that.data == null;
        }

        @Override
        public int hashCode() {
            return data != null ? data.hashCode() : 0;
        }
    }
}
