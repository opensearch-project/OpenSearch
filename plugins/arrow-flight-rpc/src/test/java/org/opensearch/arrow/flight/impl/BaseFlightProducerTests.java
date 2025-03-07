/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.opensearch.arrow.flight.bootstrap.FlightClientManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BaseFlightProducerTests extends OpenSearchTestCase {

    private BaseFlightProducer baseFlightProducer;
    private FlightStreamManager streamManager;
    private StreamProducer<VectorSchemaRoot, BufferAllocator> streamProducer;
    private StreamProducer.BatchedJob<VectorSchemaRoot> batchedJob;
    private static final String LOCAL_NODE_ID = "localNodeId";
    private static final FlightClientManager flightClientManager = mock(FlightClientManager.class);
    private final Ticket ticket = new Ticket((new FlightStreamTicket("test-ticket", LOCAL_NODE_ID)).toBytes());
    private BufferAllocator allocator;

    @Override
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        FeatureFlagSetter.set(FeatureFlags.ARROW_STREAMS_SETTING.getKey());
        streamManager = mock(FlightStreamManager.class);
        when(streamManager.getStreamTicketFactory()).thenReturn(new FlightStreamTicketFactory(() -> LOCAL_NODE_ID));
        when(flightClientManager.getLocalNodeId()).thenReturn(LOCAL_NODE_ID);
        allocator = mock(BufferAllocator.class);
        streamProducer = mock(StreamProducer.class);
        batchedJob = mock(StreamProducer.BatchedJob.class);
        baseFlightProducer = new BaseFlightProducer(flightClientManager, streamManager, allocator);
    }

    private static class TestServerStreamListener implements FlightProducer.ServerStreamListener {
        private final CountDownLatch completionLatch = new CountDownLatch(1);
        private final AtomicInteger putNextCount = new AtomicInteger(0);
        private final AtomicBoolean isCancelled = new AtomicBoolean(false);
        private Throwable error;
        private final AtomicBoolean dataConsumed = new AtomicBoolean(false);
        private final AtomicBoolean ready = new AtomicBoolean(false);
        private Runnable onReadyHandler;
        private Runnable onCancelHandler;

        @Override
        public void putNext() {
            assertFalse(dataConsumed.get());
            putNextCount.incrementAndGet();
            dataConsumed.set(true);
        }

        @Override
        public boolean isReady() {
            return ready.get();
        }

        public void setReady(boolean val) {
            ready.set(val);
            if (this.onReadyHandler != null) {
                this.onReadyHandler.run();
            }
        }

        @Override
        public void start(VectorSchemaRoot root) {
            // No-op for this test
        }

        @Override
        public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {}

        @Override
        public void putNext(ArrowBuf metadata) {
            putNext();
        }

        @Override
        public void putMetadata(ArrowBuf metadata) {

        }

        @Override
        public void completed() {
            completionLatch.countDown();
        }

        @Override
        public void error(Throwable t) {
            error = t;
            completionLatch.countDown();
        }

        @Override
        public boolean isCancelled() {
            return isCancelled.get();
        }

        @Override
        public void setOnReadyHandler(Runnable handler) {
            this.onReadyHandler = handler;
        }

        @Override
        public void setOnCancelHandler(Runnable handler) {
            this.onCancelHandler = handler;
        }

        public void resetConsumptionLatch() {
            dataConsumed.set(false);
        }

        public boolean getDataConsumed() {
            return dataConsumed.get();
        }

        public int getPutNextCount() {
            return putNextCount.get();
        }

        public Throwable getError() {
            return error;
        }

        public void cancel() {
            isCancelled.set(true);
            if (this.onCancelHandler != null) {
                this.onCancelHandler.run();
            }
        }
    }

    public void testGetStream_SuccessfulFlow() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        when(streamManager.removeStreamProducer(any(FlightStreamTicket.class))).thenReturn(
            Optional.of(FlightStreamManager.StreamProducerHolder.create(streamProducer, allocator))
        );
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 3; i++) {
                Thread clientThread = new Thread(() -> {
                    listener.setReady(false);
                    listener.setReady(true);
                });
                listener.setReady(false);
                clientThread.start();
                flushSignal.awaitConsumption(TimeValue.timeValueMillis(100));
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(VectorSchemaRoot.class), any(StreamProducer.FlushSignal.class));
        baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener);

        assertNull(listener.getError());
        assertEquals(3, listener.getPutNextCount());
        assertEquals(3, flushCount.get());

        verify(streamManager).removeStreamProducer(any(FlightStreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithSlowClient() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.removeStreamProducer(any(FlightStreamTicket.class))).thenReturn(
            Optional.of(FlightStreamManager.StreamProducerHolder.create(streamProducer, allocator))
        );
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();

        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                Thread clientThread = new Thread(() -> {
                    try {
                        listener.setReady(false);
                        Thread.sleep(100);
                        listener.setReady(true);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                listener.setReady(false);
                clientThread.start();
                flushSignal.awaitConsumption(TimeValue.timeValueMillis(300)); // waiting for consumption for more than client thread sleep
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(), any());

        baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener);

        assertNull(listener.getError());
        assertEquals(5, listener.getPutNextCount());
        assertEquals(5, flushCount.get());

        verify(streamManager).removeStreamProducer(any(FlightStreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithSlowClientTimeout() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.removeStreamProducer(any(FlightStreamTicket.class))).thenReturn(
            Optional.of(FlightStreamManager.StreamProducerHolder.create(streamProducer, allocator))
        );
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                Thread clientThread = new Thread(() -> {
                    try {
                        listener.setReady(false);
                        Thread.sleep(400);
                        listener.setReady(true);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                listener.setReady(false);
                clientThread.start();
                flushSignal.awaitConsumption(TimeValue.timeValueMillis(100)); // waiting for consumption for less than client thread sleep
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(), any());

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));

        assertNotNull(listener.getError());
        assertEquals("Stream deadline exceeded for consumption", listener.getError().getMessage());
        assertEquals(0, listener.getPutNextCount());
        assertEquals(0, flushCount.get());

        verify(streamManager).removeStreamProducer(any(FlightStreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithClientCancel() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.removeStreamProducer(any(FlightStreamTicket.class))).thenReturn(
            Optional.of(FlightStreamManager.StreamProducerHolder.create(streamProducer, allocator))
        );
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                int finalI = i;
                Thread clientThread = new Thread(() -> {
                    if (finalI == 4) {
                        listener.cancel();
                    } else {
                        listener.setReady(false);
                        listener.setReady(true);
                    }
                });
                listener.setReady(false);
                clientThread.start();
                flushSignal.awaitConsumption(TimeValue.timeValueMillis(100)); // waiting for consumption for less than client thread sleep
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(), any());

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));
        assertNotNull(listener.getError());
        assertEquals("Stream cancelled by client", listener.getError().getMessage());
        assertEquals(4, listener.getPutNextCount());
        assertEquals(4, flushCount.get());

        verify(streamManager).removeStreamProducer(any(FlightStreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithUnresponsiveClient() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.removeStreamProducer(any(FlightStreamTicket.class))).thenReturn(
            Optional.of(FlightStreamManager.StreamProducerHolder.create(streamProducer, allocator))
        );
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                Thread clientThread = new Thread(() -> {
                    listener.setReady(false);
                    // not setting ready to simulate unresponsive behaviour
                });
                listener.setReady(false);
                clientThread.start();
                flushSignal.awaitConsumption(TimeValue.timeValueMillis(100)); // waiting for consumption for less than client thread sleep
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(), any());

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));

        assertNotNull(listener.getError());
        assertEquals("Stream deadline exceeded for consumption", listener.getError().getMessage());
        assertEquals(0, listener.getPutNextCount());
        assertEquals(0, flushCount.get());

        verify(streamManager).removeStreamProducer(any(FlightStreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithServerBackpressure() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.removeStreamProducer(any(FlightStreamTicket.class))).thenReturn(
            Optional.of(FlightStreamManager.StreamProducerHolder.create(streamProducer, allocator))
        );
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        TestServerStreamListener listener = new TestServerStreamListener();
        AtomicInteger flushCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                Thread clientThread = new Thread(() -> {
                    listener.setReady(false);
                    listener.setReady(true);
                });
                listener.setReady(false);
                clientThread.start();
                Thread.sleep(100); // simulating writer backpressure
                flushSignal.awaitConsumption(TimeValue.timeValueMillis(100));
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(VectorSchemaRoot.class), any(StreamProducer.FlushSignal.class));

        baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener);

        assertNull(listener.getError());
        assertEquals(5, listener.getPutNextCount());
        assertEquals(5, flushCount.get());

        verify(streamManager).removeStreamProducer(any(FlightStreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithServerError() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.removeStreamProducer(any(FlightStreamTicket.class))).thenReturn(
            Optional.of(FlightStreamManager.StreamProducerHolder.create(streamProducer, allocator))
        );
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        TestServerStreamListener listener = new TestServerStreamListener();
        AtomicInteger flushCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                Thread clientThread = new Thread(() -> {
                    listener.setReady(false);
                    listener.setReady(true);
                });
                listener.setReady(false);
                clientThread.start();
                if (i == 4) {
                    throw new RuntimeException("Server error");
                }
                flushSignal.awaitConsumption(TimeValue.timeValueMillis(100));
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(VectorSchemaRoot.class), any(StreamProducer.FlushSignal.class));

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));

        assertNotNull(listener.getError());
        assertEquals("Server error", listener.getError().getMessage());
        assertEquals(4, listener.getPutNextCount());
        assertEquals(4, flushCount.get());

        verify(streamManager).removeStreamProducer(any(FlightStreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_StreamNotFound() throws Exception {

        when(streamManager.getStreamProducer(any(FlightStreamTicket.class))).thenReturn(null);

        TestServerStreamListener listener = new TestServerStreamListener();

        baseFlightProducer.getStream(null, ticket, listener);

        assertNotNull(listener.getError());
        assertTrue(listener.getError().getMessage().contains("Stream not found"));
        assertEquals(0, listener.getPutNextCount());

        verify(streamManager).removeStreamProducer(any(FlightStreamTicket.class));
    }

    public void testGetStreamRemoteNode() {
        final String remoteNodeId = "remote-node";
        FlightStreamTicket remoteTicket = new FlightStreamTicket("test-id", remoteNodeId);
        FlightClient remoteClient = mock(FlightClient.class);
        FlightStream mockFlightStream = mock(FlightStream.class);

        when(flightClientManager.getFlightClient(remoteNodeId)).thenReturn(Optional.of(remoteClient));
        when(remoteClient.getStream(any(Ticket.class))).thenReturn(mockFlightStream);
        TestServerStreamListener listener = new TestServerStreamListener();

        baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), new Ticket(remoteTicket.toBytes()), listener);
        verify(remoteClient).getStream(any(Ticket.class));
    }

    public void testGetStreamRemoteNodeWithNonExistentClient() {
        final String remoteNodeId = "remote-node-5";
        FlightStreamTicket remoteTicket = new FlightStreamTicket("test-id", remoteNodeId);
        TestServerStreamListener listener = new TestServerStreamListener();

        expectThrows(
            RuntimeException.class,
            () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), new Ticket(remoteTicket.toBytes()), listener)
        );
        assertNotNull(listener.getError());
        assertEquals("Either server is not up yet or node does not support Streams.", listener.getError().getMessage());
    }

    public void testGetFlightInfo() {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getStreamProducer(any(FlightStreamTicket.class))).thenReturn(
            Optional.of(FlightStreamManager.StreamProducerHolder.create(streamProducer, allocator))
        );
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        Location location = Location.forGrpcInsecure(LOCAL_NODE_ID, 8815);
        when(flightClientManager.getFlightClientLocation(LOCAL_NODE_ID)).thenReturn(location);
        when(streamProducer.estimatedRowCount()).thenReturn(100);
        FlightDescriptor descriptor = FlightDescriptor.command(ticket.getBytes());
        FlightInfo flightInfo = baseFlightProducer.getFlightInfo(null, descriptor);

        assertNotNull(flightInfo);
        assertEquals(100L, flightInfo.getRecords());
        assertEquals(1, flightInfo.getEndpoints().size());
        assertEquals(location, flightInfo.getEndpoints().getFirst().getLocations().getFirst());
    }

    public void testGetFlightInfo_NotFound() {
        when(flightClientManager.getFlightClientLocation(LOCAL_NODE_ID)).thenReturn(null);

        FlightDescriptor descriptor = FlightDescriptor.command(ticket.getBytes());
        FlightRuntimeException exception = expectThrows(
            FlightRuntimeException.class,
            () -> baseFlightProducer.getFlightInfo(null, descriptor)
        );

        assertEquals("FlightInfo not found",
            exception.getMessage());
    }

    public void testGetFlightInfo_LocationNotFound() {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getStreamProducer(any(FlightStreamTicket.class))).thenReturn(
            Optional.of(FlightStreamManager.StreamProducerHolder.create(streamProducer, allocator))
        );
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        when(flightClientManager.getFlightClientLocation(LOCAL_NODE_ID)).thenReturn(null);

        FlightDescriptor descriptor = FlightDescriptor.command(ticket.getBytes());
        FlightRuntimeException exception = expectThrows(
            FlightRuntimeException.class,
            () -> baseFlightProducer.getFlightInfo(null, descriptor)
        );

        assertEquals("Internal error while determining location information from ticket.", exception.getMessage());
    }

    public void testGetFlightInfo_SchemaError() {
        when(streamManager.getStreamProducer(any(FlightStreamTicket.class))).thenReturn(
            Optional.of(FlightStreamManager.StreamProducerHolder.create(streamProducer, allocator))
        );
        Location location = Location.forGrpcInsecure("localhost", 8815);
        when(flightClientManager.getFlightClientLocation(LOCAL_NODE_ID)).thenReturn(location);
        when(streamProducer.estimatedRowCount()).thenThrow(new RuntimeException("Schema error"));

        FlightDescriptor descriptor = FlightDescriptor.command(ticket.getBytes());
        FlightRuntimeException exception = expectThrows(
            FlightRuntimeException.class,
            () -> baseFlightProducer.getFlightInfo(null, descriptor)
        );

        assertEquals("Internal error while creating VectorSchemaRoot.",
            exception.getMessage());
    }

    public void testGetFlightInfo_NonLocalNode() {
        final String remoteNodeId = "remote-node";
        FlightStreamTicket remoteTicket = new FlightStreamTicket("test-id", remoteNodeId);
        FlightClient remoteClient = mock(FlightClient.class);
        FlightInfo mockFlightInfo = mock(FlightInfo.class);
        when(flightClientManager.getFlightClient(remoteNodeId)).thenReturn(Optional.of(remoteClient));
        when(remoteClient.getInfo(any(FlightDescriptor.class))).thenReturn(mockFlightInfo);
        when(streamProducer.estimatedRowCount()).thenReturn(100);

        FlightDescriptor descriptor = FlightDescriptor.command(remoteTicket.toBytes());
        FlightInfo flightInfo = baseFlightProducer.getFlightInfo(null, descriptor);
        assertEquals(mockFlightInfo, flightInfo);
    }

    public void testGetFlightInfo_NonLocalNode_LocationNotFound() {
        final String remoteNodeId = "remote-node-2";
        FlightStreamTicket remoteTicket = new FlightStreamTicket("test-id", remoteNodeId);
        FlightDescriptor descriptor = FlightDescriptor.command(remoteTicket.toBytes());
        FlightRuntimeException exception = expectThrows(
            FlightRuntimeException.class,
            () -> baseFlightProducer.getFlightInfo(null, descriptor)
        );
        assertEquals("Client doesn't support Stream", exception.getMessage());
    }
}
