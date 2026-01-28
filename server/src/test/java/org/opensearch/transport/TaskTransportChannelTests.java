/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for TaskTransportChannel to verify single-close semantics
 * for both streaming and non-streaming operations.
 */
public class TaskTransportChannelTests extends OpenSearchTestCase {

    /**
     * Test implementation of TransportChannel for tracking method calls
     */
    static class TestTransportChannel implements TransportChannel {
        private final List<String> callHistory = new ArrayList<>();
        private final Version version = Version.CURRENT;

        @Override
        public String getProfileName() {
            return "test";
        }

        @Override
        public String getChannelType() {
            return "test";
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            callHistory.add("sendResponse");
        }

        @Override
        public void sendResponseBatch(TransportResponse response) {
            callHistory.add("sendResponseBatch");
        }

        @Override
        public void completeStream() {
            callHistory.add("completeStream");
        }

        @Override
        public void sendResponse(Exception exception) throws IOException {
            callHistory.add("sendResponse(Exception)");
        }

        @Override
        public Version getVersion() {
            return version;
        }

        @Override
        public <T> Optional<T> get(String name, Class<T> clazz) {
            return Optional.empty();
        }

        public List<String> getCallHistory() {
            return callHistory;
        }
    }

    /**
     * Test implementation of Releasable for tracking close calls
     */
    static class TestReleasable implements Releasable {
        private final AtomicInteger closeCount = new AtomicInteger(0);

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        public int getCloseCount() {
            return closeCount.get();
        }
    }

    /**
     * Dummy transport response for testing
     */
    static class TestTransportResponse extends TransportResponse {
        public TestTransportResponse() {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // No-op for testing
        }
    }

    /**
     * Test non-streaming success: sendResponse should close once
     */
    public void testNonStreamingSuccess() throws IOException {
        TestTransportChannel innerChannel = new TestTransportChannel();
        TestReleasable releasable = new TestReleasable();
        TaskTransportChannel taskChannel = new TaskTransportChannel(innerChannel, releasable);

        // Send a non-streaming response
        taskChannel.sendResponse(new TestTransportResponse());

        // Verify task was closed exactly once
        assertEquals(1, releasable.getCloseCount());
        assertEquals(1, innerChannel.getCallHistory().size());
        assertEquals("sendResponse", innerChannel.getCallHistory().get(0));

        // Calling completeStream after non-streaming should not close again
        taskChannel.completeStream();
        assertEquals(1, releasable.getCloseCount()); // Still 1
        assertEquals(2, innerChannel.getCallHistory().size());
        assertEquals("completeStream", innerChannel.getCallHistory().get(1));
    }

    /**
     * Test non-streaming failure: sendResponse(Exception) should close once
     */
    public void testNonStreamingFailure() throws IOException {
        TestTransportChannel innerChannel = new TestTransportChannel();
        TestReleasable releasable = new TestReleasable();
        TaskTransportChannel taskChannel = new TaskTransportChannel(innerChannel, releasable);

        // Send an error response
        taskChannel.sendResponse(new IOException("Test error"));

        // Verify task was closed exactly once
        assertEquals(1, releasable.getCloseCount());
        assertEquals(1, innerChannel.getCallHistory().size());
        assertEquals("sendResponse(Exception)", innerChannel.getCallHistory().get(0));

        // Repeated error should not close again
        taskChannel.sendResponse(new IOException("Another error"));
        assertEquals(1, releasable.getCloseCount()); // Still 1
        assertEquals(2, innerChannel.getCallHistory().size());
        assertEquals("sendResponse(Exception)", innerChannel.getCallHistory().get(1));
    }

    /**
     * Test streaming success: sendResponseBatch should not close, completeStream should close once
     */
    public void testStreamingSuccess() {
        TestTransportChannel innerChannel = new TestTransportChannel();
        TestReleasable releasable = new TestReleasable();
        TaskTransportChannel taskChannel = new TaskTransportChannel(innerChannel, releasable);

        // Send multiple streaming responses
        taskChannel.sendResponseBatch(new TestTransportResponse());
        assertEquals(0, releasable.getCloseCount()); // No close yet

        taskChannel.sendResponseBatch(new TestTransportResponse());
        assertEquals(0, releasable.getCloseCount()); // Still no close

        taskChannel.sendResponseBatch(new TestTransportResponse());
        assertEquals(0, releasable.getCloseCount()); // Still no close

        // Complete the stream
        taskChannel.completeStream();

        // Verify task was closed exactly once
        assertEquals(1, releasable.getCloseCount());
        assertEquals(4, innerChannel.getCallHistory().size());
        assertEquals("sendResponseBatch", innerChannel.getCallHistory().get(0));
        assertEquals("sendResponseBatch", innerChannel.getCallHistory().get(1));
        assertEquals("sendResponseBatch", innerChannel.getCallHistory().get(2));
        assertEquals("completeStream", innerChannel.getCallHistory().get(3));

        // Repeated completeStream should not close again
        taskChannel.completeStream();
        assertEquals(1, releasable.getCloseCount()); // Still 1
    }

    /**
     * Test streaming failure: sendResponseBatch then sendResponse(Exception) should close once
     */
    public void testStreamingFailure() throws IOException {
        TestTransportChannel innerChannel = new TestTransportChannel();
        TestReleasable releasable = new TestReleasable();
        TaskTransportChannel taskChannel = new TaskTransportChannel(innerChannel, releasable);

        // Send some streaming responses
        taskChannel.sendResponseBatch(new TestTransportResponse());
        assertEquals(0, releasable.getCloseCount()); // No close yet

        taskChannel.sendResponseBatch(new TestTransportResponse());
        assertEquals(0, releasable.getCloseCount()); // Still no close

        // Send an error
        taskChannel.sendResponse(new IOException("Stream error"));

        // Verify task was closed exactly once
        assertEquals(1, releasable.getCloseCount());
        assertEquals(3, innerChannel.getCallHistory().size());
        assertEquals("sendResponseBatch", innerChannel.getCallHistory().get(0));
        assertEquals("sendResponseBatch", innerChannel.getCallHistory().get(1));
        assertEquals("sendResponse(Exception)", innerChannel.getCallHistory().get(2));

        // completeStream after failure should not close again
        taskChannel.completeStream();
        assertEquals(1, releasable.getCloseCount()); // Still 1
    }

    /**
     * Test idempotence: repeated completeStream calls should only close once
     */
    public void testIdempotentCompleteStream() {
        TestTransportChannel innerChannel = new TestTransportChannel();
        TestReleasable releasable = new TestReleasable();
        TaskTransportChannel taskChannel = new TaskTransportChannel(innerChannel, releasable);

        // Mark as streaming
        taskChannel.sendResponseBatch(new TestTransportResponse());
        assertEquals(0, releasable.getCloseCount());

        // Complete multiple times
        taskChannel.completeStream();
        assertEquals(1, releasable.getCloseCount());

        taskChannel.completeStream();
        assertEquals(1, releasable.getCloseCount()); // Still 1

        taskChannel.completeStream();
        assertEquals(1, releasable.getCloseCount()); // Still 1
    }

    /**
     * Test idempotence: repeated sendResponse(Exception) calls should only close once
     */
    public void testIdempotentErrorResponse() throws IOException {
        TestTransportChannel innerChannel = new TestTransportChannel();
        TestReleasable releasable = new TestReleasable();
        TaskTransportChannel taskChannel = new TaskTransportChannel(innerChannel, releasable);

        // Send multiple errors
        taskChannel.sendResponse(new IOException("Error 1"));
        assertEquals(1, releasable.getCloseCount());

        taskChannel.sendResponse(new IOException("Error 2"));
        assertEquals(1, releasable.getCloseCount()); // Still 1

        taskChannel.sendResponse(new IOException("Error 3"));
        assertEquals(1, releasable.getCloseCount()); // Still 1
    }

    /**
     * Test that non-streaming sendResponse after streaming operations doesn't close again
     */
    public void testNonStreamingAfterStreaming() throws IOException {
        TestTransportChannel innerChannel = new TestTransportChannel();
        TestReleasable releasable = new TestReleasable();
        TaskTransportChannel taskChannel = new TaskTransportChannel(innerChannel, releasable);

        // Start streaming
        taskChannel.sendResponseBatch(new TestTransportResponse());
        assertEquals(0, releasable.getCloseCount());

        // Complete stream
        taskChannel.completeStream();
        assertEquals(1, releasable.getCloseCount());

        // Try non-streaming response (should not close again)
        taskChannel.sendResponse(new TestTransportResponse());
        assertEquals(1, releasable.getCloseCount()); // Still 1
    }

    /**
     * Test that completeStream alone (without sendResponseBatch) closes properly
     */
    public void testCompleteStreamOnly() {
        TestTransportChannel innerChannel = new TestTransportChannel();
        TestReleasable releasable = new TestReleasable();
        TaskTransportChannel taskChannel = new TaskTransportChannel(innerChannel, releasable);

        // Call completeStream directly (marks as streaming and closes)
        taskChannel.completeStream();
        assertEquals(1, releasable.getCloseCount());

        // Subsequent calls should not close again
        taskChannel.completeStream();
        assertEquals(1, releasable.getCloseCount());
    }
}
