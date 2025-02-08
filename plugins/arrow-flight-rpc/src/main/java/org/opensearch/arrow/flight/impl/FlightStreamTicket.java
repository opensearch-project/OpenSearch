/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.opensearch.arrow.spi.StreamTicket;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

class FlightStreamTicket implements StreamTicket {
    private static final int MAX_TOTAL_SIZE = 4096;
    private static final int MAX_ID_LENGTH = 256;

    private final String ticketID;
    private final String nodeID;

    public FlightStreamTicket(String ticketID, String nodeID) {
        this.ticketID = ticketID;
        this.nodeID = nodeID;
    }

    @Override
    public String getTicketId() {
        return ticketID;
    }

    @Override
    public String getNodeId() {
        return nodeID;
    }

    @Override
    public byte[] toBytes() {
        byte[] ticketIDBytes = ticketID.getBytes(StandardCharsets.UTF_8);
        byte[] nodeIDBytes = nodeID.getBytes(StandardCharsets.UTF_8);

        if (ticketIDBytes.length > Short.MAX_VALUE || nodeIDBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Field lengths exceed the maximum allowed size.");
        }
        ByteBuffer buffer = ByteBuffer.allocate(2 + ticketIDBytes.length + 2 + nodeIDBytes.length);
        buffer.putShort((short) ticketIDBytes.length);
        buffer.putShort((short) nodeIDBytes.length);
        buffer.put(ticketIDBytes);
        buffer.put(nodeIDBytes);
        return Base64.getEncoder().encode(buffer.array());
    }

    static StreamTicket fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length < 4) {
            throw new IllegalArgumentException("Invalid byte array input.");
        }

        if (bytes.length > MAX_TOTAL_SIZE) {
            throw new IllegalArgumentException("Input exceeds maximum allowed size");
        }

        ByteBuffer buffer = ByteBuffer.wrap(Base64.getDecoder().decode(bytes));

        short ticketIDLength = buffer.getShort();
        if (ticketIDLength < 0 || ticketIDLength > MAX_ID_LENGTH) {
            throw new IllegalArgumentException("Invalid ticketID length: " + ticketIDLength);
        }

        short nodeIDLength = buffer.getShort();
        if (nodeIDLength < 0 || nodeIDLength > MAX_ID_LENGTH) {
            throw new IllegalArgumentException("Invalid nodeID length: " + nodeIDLength);
        }

        byte[] ticketIDBytes = new byte[ticketIDLength];
        if (buffer.remaining() < ticketIDLength) {
            throw new IllegalArgumentException("Malformed byte array. Not enough data for TicketId.");
        }
        buffer.get(ticketIDBytes);

        byte[] nodeIDBytes = new byte[nodeIDLength];
        if (buffer.remaining() < nodeIDLength) {
            throw new IllegalArgumentException("Malformed byte array. Not enough data for NodeId.");
        }
        buffer.get(nodeIDBytes);

        String ticketID = new String(ticketIDBytes, StandardCharsets.UTF_8);
        String nodeID = new String(nodeIDBytes, StandardCharsets.UTF_8);
        return new FlightStreamTicket(ticketID, nodeID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ticketID, nodeID);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        FlightStreamTicket that = (FlightStreamTicket) obj;
        return Objects.equals(ticketID, that.ticketID) && Objects.equals(nodeID, that.nodeID);
    }

    @Override
    public String toString() {
        return "FlightStreamTicket{ticketID='" + ticketID + "', nodeID='" + nodeID + "'}";
    }
}
