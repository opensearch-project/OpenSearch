/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to\n * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.Version;
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FlightTransportChannelTests extends OpenSearchTestCase {

    private FlightOutboundHandler mockOutboundHandler;
    private TcpChannel mockTcpChannel;
    private FlightStatsCollector mockStatsCollector;
    private Releasable mockReleasable;
    private FlightTransportChannel channel;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockOutboundHandler = mock(FlightOutboundHandler.class);
        mockTcpChannel = mock(TcpChannel.class);
        mockStatsCollector = mock(FlightStatsCollector.class);
        mockReleasable = mock(Releasable.class);

        channel = new FlightTransportChannel(
            mockOutboundHandler,
            mockTcpChannel,
            "test-action",
            123L,
            Version.CURRENT,
            Collections.emptySet(),
            false,
            false,
            mockReleasable
        );
    }

    public void testSendResponseThrowsUnsupportedOperation() {
        TransportResponse response = mock(TransportResponse.class);

        assertThrows(UnsupportedOperationException.class, () -> channel.sendResponse(response));
        assertEquals(
            "Use sendResponseBatch instead",
            assertThrows(UnsupportedOperationException.class, () -> channel.sendResponse(response)).getMessage()
        );
    }

    public void testSendResponseWithException() throws IOException {
        Exception exception = new RuntimeException("test exception");

        channel.sendResponse(exception);

        verify(mockOutboundHandler).sendErrorResponse(any(), any(), any(), any(), eq(123L), eq("test-action"), eq(exception));
    }

    public void testSendResponseBatchSuccess() throws IOException, InterruptedException {
        TransportResponse response = mock(TransportResponse.class);
        CountDownLatch latch = new CountDownLatch(1);

        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(mockOutboundHandler).sendResponseBatch(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean(), anyBoolean());

        channel.sendResponseBatch(response);

        assertTrue("sendResponseBatch should be called", latch.await(1, TimeUnit.SECONDS));
        verify(mockOutboundHandler).sendResponseBatch(
            eq(Version.CURRENT),
            eq(Collections.emptySet()),
            eq(mockTcpChannel),
            eq(channel),
            eq(123L),
            eq("test-action"),
            eq(response),
            eq(false),
            eq(false)
        );
    }

    public void testSendResponseBatchAfterStreamClosed() {
        TransportResponse response = mock(TransportResponse.class);

        channel.completeStream();

        StreamException exception = assertThrows(StreamException.class, () -> channel.sendResponseBatch(response));
        assertEquals(StreamErrorCode.UNAVAILABLE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Stream is closed for requestId [123]"));
    }

    public void testSendResponseBatchWithCancellationException() throws IOException {
        TransportResponse response = mock(TransportResponse.class);
        StreamException cancellationException = new StreamException(StreamErrorCode.CANCELLED, "cancelled");

        doThrow(cancellationException).when(mockOutboundHandler)
            .sendResponseBatch(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean(), anyBoolean());

        StreamException thrown = assertThrows(StreamException.class, () -> channel.sendResponseBatch(response));
        assertEquals(StreamErrorCode.CANCELLED, thrown.getErrorCode());
        verify(mockTcpChannel).close();
        verify(mockReleasable).close();
    }

    public void testSendResponseBatchWithGenericException() throws IOException {
        TransportResponse response = mock(TransportResponse.class);
        RuntimeException genericException = new RuntimeException("generic error");

        doThrow(genericException).when(mockOutboundHandler)
            .sendResponseBatch(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean(), anyBoolean());

        StreamException thrown = assertThrows(StreamException.class, () -> channel.sendResponseBatch(response));
        assertEquals(StreamErrorCode.INTERNAL, thrown.getErrorCode());
        assertEquals("Error sending response batch", thrown.getMessage());
        assertEquals(genericException, thrown.getCause());
        verify(mockTcpChannel).close();
        verify(mockReleasable).close();
    }

    public void testCompleteStreamSuccess() {
        channel.completeStream();

        verify(mockOutboundHandler).completeStream(
            eq(Version.CURRENT),
            eq(Collections.emptySet()),
            eq(mockTcpChannel),
            eq(channel),
            eq(123L),
            eq("test-action")
        );

        // Simulate async completion by manually creating and closing a BatchTask
        FlightOutboundHandler.BatchTask completeTask = new FlightOutboundHandler.BatchTask(
            Version.CURRENT,
            Collections.emptySet(),
            mockTcpChannel,
            channel,
            123L,
            "test-action",
            TransportResponse.Empty.INSTANCE,
            false,
            false,
            true,
            false,
            null,
            null
        );
        completeTask.close();

        verify(mockTcpChannel).close();
        verify(mockReleasable).close();
    }

    public void testCompleteStreamTwice() {
        channel.completeStream();

        StreamException exception = assertThrows(StreamException.class, () -> channel.completeStream());
        assertEquals(StreamErrorCode.UNAVAILABLE, exception.getErrorCode());
        assertEquals("FlightTransportChannel stream already closed.", exception.getMessage());
        verify(mockTcpChannel, times(1)).close();
        verify(mockReleasable, times(1)).close();
    }

    public void testCompleteStreamWithException() {
        RuntimeException outboundException = new RuntimeException("outbound error");
        doThrow(outboundException).when(mockOutboundHandler).completeStream(any(), any(), any(), any(), anyLong(), any());

        StreamException thrown = assertThrows(StreamException.class, () -> channel.completeStream());
        assertEquals(StreamErrorCode.INTERNAL, thrown.getErrorCode());
        assertEquals("Error completing stream", thrown.getMessage());
        assertEquals(outboundException, thrown.getCause());
        verify(mockTcpChannel).close();
        verify(mockReleasable).close();
    }

    public void testMultipleSendResponseBatchAfterComplete() {
        TransportResponse response = mock(TransportResponse.class);

        channel.completeStream();

        StreamException exception1 = assertThrows(StreamException.class, () -> channel.sendResponseBatch(response));
        StreamException exception2 = assertThrows(StreamException.class, () -> channel.sendResponseBatch(response));
        assertEquals(StreamErrorCode.UNAVAILABLE, exception1.getErrorCode());
        assertEquals(StreamErrorCode.UNAVAILABLE, exception2.getErrorCode());
    }
}
