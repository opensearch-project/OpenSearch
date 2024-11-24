/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class StreamTicketTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        String ticketID = "ticket123";
        String nodeID = "node456";
        StreamTicket ticket = new StreamTicket(ticketID, nodeID);

        assertEquals(ticketID, ticket.getTicketID());
        assertEquals(nodeID, ticket.getNodeID());
    }

    public void testToBytes() {
        StreamTicket ticket = new StreamTicket("ticket123", "node456");
        byte[] bytes = ticket.toBytes();

        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        // Decode the Base64 and check the structure
        byte[] decoded = Base64.getDecoder().decode(bytes);
        assertEquals(2 + 9 + 2 + 7, decoded.length); // 2 shorts + "ticket123" + "node456"
    }

    public void testFromBytes() {
        StreamTicket original = new StreamTicket("ticket123", "node456");
        byte[] bytes = original.toBytes();

        StreamTicket reconstructed = StreamTicket.fromBytes(bytes);

        assertEquals(original.getTicketID(), reconstructed.getTicketID());
        assertEquals(original.getNodeID(), reconstructed.getNodeID());
    }

    public void testToBytesWithLongStrings() {
        String longString = randomAlphaOfLength(Short.MAX_VALUE + 1);
        StreamTicket ticket = new StreamTicket(longString, "node456");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, ticket::toBytes);
        assertEquals("Field lengths exceed the maximum allowed size.", exception.getMessage());
    }

    public void testNullInput() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> StreamTicket.fromBytes(null));
        assertEquals("Invalid byte array input.", e.getMessage());
    }

    public void testEmptyInput() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> StreamTicket.fromBytes(new byte[0]));
        assertEquals("Invalid byte array input.", e.getMessage());
    }

    public void testMalformedBase64() {
        byte[] invalidBase64 = "Invalid Base64!@#$".getBytes(StandardCharsets.UTF_8);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> StreamTicket.fromBytes(invalidBase64));
        assertEquals("Illegal base64 character 20", e.getMessage());
    }

    public void testModifiedLengthFields() {
        StreamTicket original = new StreamTicket("ticket123", "node456");
        byte[] bytes = original.toBytes();
        byte[] decoded = Base64.getDecoder().decode(bytes);

        // Modify the length field to be larger than actual data
        decoded[0] = (byte) 0xFF;
        decoded[1] = (byte) 0xFF;

        byte[] modified = Base64.getEncoder().encode(decoded);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> StreamTicket.fromBytes(modified));
        assertEquals("Invalid ticketID length: -1", e.getMessage());
    }

    public void testEquals() {
        StreamTicket ticket1 = new StreamTicket("ticket123", "node456");
        StreamTicket ticket2 = new StreamTicket("ticket123", "node456");
        StreamTicket ticket3 = new StreamTicket("ticket789", "node456");

        assertEquals(ticket1, ticket2);
        assertNotEquals(ticket1, ticket3);
        assertNotEquals(null, ticket1);
        assertNotEquals("Not a StreamTicket", ticket1);
    }

    public void testHashCode() {
        StreamTicket ticket1 = new StreamTicket("ticket123", "node456");
        StreamTicket ticket2 = new StreamTicket("ticket123", "node456");

        assertEquals(ticket1.hashCode(), ticket2.hashCode());
    }

    public void testToString() {
        StreamTicket ticket = new StreamTicket("ticket123", "node456");
        String expected = "StreamTicket{ticketID='ticket123', nodeID='node456'}";
        assertEquals(expected, ticket.toString());
    }
}
