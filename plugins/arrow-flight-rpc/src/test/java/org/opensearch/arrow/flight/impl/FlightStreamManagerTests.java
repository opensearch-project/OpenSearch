/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.OSFlightClient;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.arrow.flight.bootstrap.FlightClientManager;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlightStreamManagerTests extends OpenSearchTestCase {

    private OSFlightClient flightClient;
    private FlightStreamManager flightStreamManager;
    private static final String NODE_ID = "testNodeId";
    private static final String TICKET_ID = "testTicketId";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        flightClient = mock(OSFlightClient.class);
        FlightClientManager clientManager = mock(FlightClientManager.class);
        when(clientManager.getFlightClient(NODE_ID)).thenReturn(Optional.of(flightClient));
        BufferAllocator allocator = mock(BufferAllocator.class);
        flightStreamManager = new FlightStreamManager(() -> allocator);
        flightStreamManager.setClientManager(clientManager);
    }

    public void testGetStreamReader() throws Exception {
        StreamTicket ticket = new FlightStreamTicket(TICKET_ID, NODE_ID);
        FlightStream mockFlightStream = mock(FlightStream.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        when(flightClient.getStream(new Ticket(ticket.toBytes()))).thenReturn(mockFlightStream);
        when(mockFlightStream.getRoot()).thenReturn(mockRoot);
        when(mockRoot.getSchema()).thenReturn(new Schema(Collections.emptyList()));

        StreamReader streamReader = flightStreamManager.getStreamReader(ticket);

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
}
