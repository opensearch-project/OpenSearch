/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.arrow.flight.bootstrap.FlightClientManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlightStreamManagerTests extends OpenSearchTestCase {

    private FlightClient flightClient;
    private FlightStreamManager flightStreamManager;
    private static final String NODE_ID = "testNodeId";
    private static final String TICKET_ID = "testTicketId";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        flightClient = mock(FlightClient.class);
        FlightClientManager clientManager = mock(FlightClientManager.class);
        when(clientManager.getLocalNodeId()).thenReturn(NODE_ID);
        when(clientManager.getFlightClient(NODE_ID)).thenReturn(Optional.of(flightClient));
        BufferAllocator allocator = mock(BufferAllocator.class);
        flightStreamManager = new FlightStreamManager();
        flightStreamManager.setAllocatorSupplier(() -> allocator);
        flightStreamManager.setClientManager(clientManager);
    }

    public void testGetStreamReader() throws Exception {
        StreamTicket ticket = new FlightStreamTicket(TICKET_ID, NODE_ID);
        FlightStream mockFlightStream = mock(FlightStream.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        when(flightClient.getStream(new Ticket(ticket.toBytes()))).thenReturn(mockFlightStream);
        when(mockFlightStream.getRoot()).thenReturn(mockRoot);
        when(mockRoot.getSchema()).thenReturn(new Schema(Collections.emptyList()));

        StreamReader<VectorSchemaRoot> streamReader = flightStreamManager.getStreamReader(ticket);

        assertNotNull(streamReader);
        assertNotNull(streamReader.getRoot());
        assertEquals(new Schema(Collections.emptyList()), streamReader.getRoot().getSchema());
        verify(flightClient).getStream(new Ticket(ticket.toBytes()));
    }

    public void testGetVectorSchemaRootWithException() {
        StreamTicket ticket = new FlightStreamTicket(TICKET_ID, NODE_ID);
        when(flightClient.getStream(new Ticket(ticket.toBytes()))).thenThrow(new RuntimeException("Test exception"));

        expectThrows(RuntimeException.class, () -> flightStreamManager.getStreamReader(ticket));
    }

    public void testRegisterStream() throws IOException {
        try (TestStreamProducer producer = new TestStreamProducer()) {
            assertNotNull(flightStreamManager.getStreamTicketFactory());
            StreamTicket resultTicket = flightStreamManager.registerStream(producer, null);
            assertNotNull(resultTicket);
            assertTrue(resultTicket instanceof FlightStreamTicket);
            FlightStreamTicket flightTicket = (FlightStreamTicket) resultTicket;
            assertEquals(NODE_ID, flightTicket.getNodeId());
            assertNotNull(flightTicket.getTicketId());
            Optional<FlightStreamManager.StreamProducerHolder> retrievedProducer = flightStreamManager.getStreamProducer(resultTicket);
            assertTrue(retrievedProducer.isPresent());
            assertEquals(producer, retrievedProducer.get().producer());
            assertNotNull(retrievedProducer.get().getRoot());
        }
    }

    public void testGetStreamProducerNotFound() {
        StreamTicket ticket = new FlightStreamTicket("nonexistent", NODE_ID);
        assertFalse(flightStreamManager.getStreamProducer(ticket).isPresent());
        StreamTicket ticket2 = new FlightStreamTicket("nonexistent", "unknown");
        try {
            flightStreamManager.getStreamReader(ticket2);
            fail("RuntimeException expected");
        } catch (RuntimeException e) {
            assertEquals("Flight client not found for node [unknown].", e.getMessage());
        }
    }

    public void testRemoveStreamProducer() throws IOException {
        try (TestStreamProducer producer = new TestStreamProducer()) {
            StreamTicket resultTicket = flightStreamManager.registerStream(producer, null);
            assertNotNull(resultTicket);
            assertTrue(resultTicket instanceof FlightStreamTicket);
            FlightStreamTicket flightTicket = (FlightStreamTicket) resultTicket;
            assertEquals(NODE_ID, flightTicket.getNodeId());
            assertNotNull(flightTicket.getTicketId());

            Optional<FlightStreamManager.StreamProducerHolder> retrievedProducer = flightStreamManager.removeStreamProducer(resultTicket);
            assertTrue(retrievedProducer.isPresent());
            assertEquals(producer, retrievedProducer.get().producer());
            assertNotNull(retrievedProducer.get().getRoot());
            assertFalse(flightStreamManager.getStreamProducer(resultTicket).isPresent());
        }
    }

    public void testRemoveNonExistentStreamProducer() {
        StreamTicket ticket = new FlightStreamTicket("nonexistent", NODE_ID);
        Optional<FlightStreamManager.StreamProducerHolder> removedProducer = flightStreamManager.removeStreamProducer(ticket);
        assertFalse(removedProducer.isPresent());
    }

    public void testStreamProducerExpired() {
        TestStreamProducer producer = new TestStreamProducer() {
            @Override
            public TimeValue getJobDeadline() {
                return TimeValue.timeValueMillis(0);
            }
        };
        StreamTicket ticket = flightStreamManager.registerStream(producer, null);
        Optional<FlightStreamManager.StreamProducerHolder> expiredProducer = flightStreamManager.getStreamProducer(ticket);
        assertFalse(expiredProducer.isPresent());
    }

    public void testClose() throws Exception {
        TestStreamProducer producer = new TestStreamProducer();
        StreamTicket ticket = flightStreamManager.registerStream(producer, null);
        flightStreamManager.close();
        assertFalse(flightStreamManager.getStreamProducer(ticket).isPresent());
    }

    static class TestStreamProducer implements StreamProducer<VectorSchemaRoot, BufferAllocator> {
        @Override
        public VectorSchemaRoot createRoot(BufferAllocator bufferAllocator) {
            return mock(VectorSchemaRoot.class);
        }

        @Override
        public BatchedJob<VectorSchemaRoot> createJob(BufferAllocator bufferAllocator) {
            return null;
        }

        @Override
        public TimeValue getJobDeadline() {
            return TimeValue.timeValueMillis(1000);
        }

        @Override
        public int estimatedRowCount() {
            return 0;
        }

        @Override
        public String getAction() {
            return "";
        }

        @Override
        public void close() throws IOException {

        }
    }
}
