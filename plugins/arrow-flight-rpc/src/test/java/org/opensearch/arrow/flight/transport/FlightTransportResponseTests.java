/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.arrow.transport.ArrowBatchResponse;
import org.opensearch.arrow.transport.ArrowBatchResponseHandler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Header;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlightTransportResponseTests extends OpenSearchTestCase {

    public void testArrowHandlerSkipsDeserialization() {
        assertTrue(new TestArrowHandler().skipsDeserialization());
    }

    public void testNonArrowHandlerDoesNotSkip() {
        assertFalse(new TestByteHandler().skipsDeserialization());
    }

    public void testWrapperForwardsTrueFromArrowHandler() {
        assertTrue(new ForwardingWrapper<>(new TestArrowHandler()).skipsDeserialization());
    }

    public void testWrapperForwardsFalseFromNonArrowHandler() {
        assertFalse(new ForwardingWrapper<>(new TestByteHandler()).skipsDeserialization());
    }

    public void testRealMetricsTrackingWrapperForwards() {
        // MetricsTrackingResponseHandler in production path; null tracker is fine for this check.
        MetricsTrackingResponseHandler<TestArrowResponse> wrapped = new MetricsTrackingResponseHandler<>(new TestArrowHandler(), null);
        assertTrue(wrapped.skipsDeserialization());
    }

    // ── close() / prefetch-race stream lifecycle ─────────────────────────────

    /**
     * Builds a response wired to the given client. Uses package-private collaborators
     * directly (the test lives in the same package).
     */
    private FlightTransportResponse<TestArrowResponse> newResponse(FlightClient client, HeaderContext headerContext) {
        return new FlightTransportResponse<>(
            new TestArrowHandler(),
            1L,
            client,
            headerContext,
            new Ticket(new byte[0]),
            new NamedWriteableRegistry(List.of()),
            new FlightTransportConfig()
        );
    }

    /** Normal path: once the stream is published, close() closes it. */
    public void testCloseClosesPublishedStream() throws Exception {
        FlightClient client = mock(FlightClient.class);
        FlightStream stream = mock(FlightStream.class);
        when(client.getStream(any(Ticket.class), any())).thenReturn(stream);
        when(stream.next()).thenReturn(true);

        FlightTransportResponse<TestArrowResponse> response = newResponse(client, new HeaderContext());
        CompletableFuture<Header> future = new CompletableFuture<>();
        response.openAndPrefetchAsync(future);
        future.get(5, TimeUnit.SECONDS); // prefetch finished → flightStream published

        response.close();
        // close() ran after publish; the prefetch had already passed its closed-check, so close() owns the close.
        verify(stream, timeout(5_000).atLeastOnce()).close();
    }

    /**
     * The race the fix targets: close() runs while the prefetch thread is still inside getStream(),
     * so close() sees flightStream==null and closes nothing. When the prefetch then publishes the
     * stream, its synchronous closed-check must self-close it (closed already true) so the first-batch
     * root is not stranded on the client allocator.
     */
    public void testCloseDuringPrefetchSelfClosesStream() throws Exception {
        FlightStream stream = mock(FlightStream.class);
        when(stream.next()).thenReturn(true);

        CountDownLatch enteredGetStream = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        FlightClient client = mock(FlightClient.class);
        when(client.getStream(any(Ticket.class), any())).thenAnswer(inv -> {
            enteredGetStream.countDown();
            assertTrue("test must release getStream", proceed.await(5, TimeUnit.SECONDS));
            return stream;
        });

        FlightTransportResponse<TestArrowResponse> response = newResponse(client, new HeaderContext());
        CompletableFuture<Header> future = new CompletableFuture<>();
        response.openAndPrefetchAsync(future);

        // Prefetch thread is parked inside getStream(); flightStream is still null.
        assertTrue(enteredGetStream.await(5, TimeUnit.SECONDS));
        response.close(); // sees flightStream==null → closes nothing itself
        verify(stream, never()).close();

        // Let the prefetch publish the stream; its closed-check must self-close it.
        proceed.countDown();
        verify(stream, timeout(5_000)).close();
    }

    /** close() before any prefetch has a null stream — must be a no-op, no NPE. */
    public void testCloseBeforePrefetchIsNoOp() {
        FlightClient client = mock(FlightClient.class);
        FlightTransportResponse<TestArrowResponse> response = newResponse(client, new HeaderContext());
        response.close(); // no stream yet
        // A second close is still safe.
        response.close();
    }

    /** close() is idempotent: a second call short-circuits and does not re-close the stream. */
    public void testCloseIsIdempotent() throws Exception {
        FlightClient client = mock(FlightClient.class);
        FlightStream stream = mock(FlightStream.class);
        when(client.getStream(any(Ticket.class), any())).thenReturn(stream);
        when(stream.next()).thenReturn(true);

        FlightTransportResponse<TestArrowResponse> response = newResponse(client, new HeaderContext());
        CompletableFuture<Header> future = new CompletableFuture<>();
        response.openAndPrefetchAsync(future);
        future.get(5, TimeUnit.SECONDS);

        response.close();
        response.close();
        response.close();
        // The prefetch passed its closed-check before publishing the future, so it never self-closes;
        // only the first close() closes the stream and the later calls short-circuit.
        verify(stream, times(1)).close();
    }

    /** A benign already-closed error from the stream is swallowed. */
    public void testCloseSwallowsAlreadyClosedError() throws Exception {
        FlightClient client = mock(FlightClient.class);
        FlightStream stream = mock(FlightStream.class);
        when(client.getStream(any(Ticket.class), any())).thenReturn(stream);
        when(stream.next()).thenReturn(true);
        doThrow(new IllegalStateException("already closed")).when(stream).close();

        FlightTransportResponse<TestArrowResponse> response = newResponse(client, new HeaderContext());
        CompletableFuture<Header> future = new CompletableFuture<>();
        response.openAndPrefetchAsync(future);
        future.get(5, TimeUnit.SECONDS);

        response.close(); // must not propagate the IllegalStateException
    }

    /** An unexpected error from the stream is wrapped as a StreamException. */
    public void testCloseRethrowsUnexpectedErrorAsStreamException() throws Exception {
        FlightClient client = mock(FlightClient.class);
        FlightStream stream = mock(FlightStream.class);
        when(client.getStream(any(Ticket.class), any())).thenReturn(stream);
        when(stream.next()).thenReturn(true);
        doThrow(new RuntimeException("boom")).when(stream).close();

        FlightTransportResponse<TestArrowResponse> response = newResponse(client, new HeaderContext());
        CompletableFuture<Header> future = new CompletableFuture<>();
        response.openAndPrefetchAsync(future);
        future.get(5, TimeUnit.SECONDS);

        expectThrows(StreamException.class, response::close);
    }

    public void testCopyMetadataNullBuffer() {
        assertNull(FlightTransportResponse.copyMetadata(null));
    }

    public void testCopyMetadataEmptyBuffer() {
        try (RootAllocator allocator = new RootAllocator(); ArrowBuf buf = allocator.buffer(0)) {
            assertNull(FlightTransportResponse.copyMetadata(buf));
        }
    }

    public void testCopyMetadataPopulatedBuffer() {
        byte[] payload = "{\"output_rows\":42}".getBytes(StandardCharsets.UTF_8);
        try (RootAllocator allocator = new RootAllocator(); ArrowBuf buf = allocator.buffer(payload.length)) {
            buf.writeBytes(payload);
            byte[] copy = FlightTransportResponse.copyMetadata(buf);
            assertNotNull(copy);
            assertArrayEquals(payload, copy);
            // The copy is independent of the wire buffer.
            assertNotSame(payload, copy);
        }
    }

    private static final class TestArrowHandler extends ArrowBatchResponseHandler<TestArrowResponse> {
        @Override
        public TestArrowResponse read(org.opensearch.core.common.io.stream.StreamInput in) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void handleResponse(TestArrowResponse response) {}

        @Override
        public void handleException(TransportException exp) {}

        @Override
        public String executor() {
            return "same";
        }
    }

    private static final class TestByteHandler implements StreamTransportResponseHandler<TransportResponse> {
        @Override
        public TransportResponse read(org.opensearch.core.common.io.stream.StreamInput in) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void handleResponse(TransportResponse response) {}

        @Override
        public void handleException(TransportException exp) {}

        @Override
        public String executor() {
            return "same";
        }
    }

    private static final class ForwardingWrapper<T extends TransportResponse> implements TransportResponseHandler<T> {
        private final TransportResponseHandler<T> delegate;

        ForwardingWrapper(TransportResponseHandler<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public T read(org.opensearch.core.common.io.stream.StreamInput in) throws IOException {
            return delegate.read(in);
        }

        @Override
        public void handleResponse(T response) {
            delegate.handleResponse(response);
        }

        @Override
        public void handleException(TransportException exp) {
            delegate.handleException(exp);
        }

        @Override
        public String executor() {
            return delegate.executor();
        }

        @Override
        public boolean skipsDeserialization() {
            return delegate.skipsDeserialization();
        }
    }

    private static final class TestArrowResponse extends ArrowBatchResponse {
        TestArrowResponse() {
            super((org.apache.arrow.vector.VectorSchemaRoot) null);
        }
    }
}
