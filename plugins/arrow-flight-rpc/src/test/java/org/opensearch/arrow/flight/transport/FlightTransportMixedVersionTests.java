/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.transport.ConnectTransportException;

import java.net.InetAddress;

/**
 * A node that predates the stream transport (or has it disabled) publishes no stream address. In a mixed-version
 * cluster the stream connection driver must not attempt a Flight connection to such a node. These tests pin both
 * guards: the {@link org.opensearch.transport.StreamTransportService} skip and the {@link FlightTransport} backstop.
 */
public class FlightTransportMixedVersionTests extends FlightTransportTestBase {

    private DiscoveryNode nodeWithoutStreamAddress() {
        TransportAddress transportAddress = new TransportAddress(InetAddress.getLoopbackAddress(), 9);
        // The (id, address, version) constructor leaves streamAddress null - the mixed-version case.
        return new DiscoveryNode("old-node-id", transportAddress, Version.CURRENT);
    }

    public void testConnectToNodeSkipsNodeWithoutStreamAddress() {
        DiscoveryNode oldNode = nodeWithoutStreamAddress();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        streamTransportService.connectToNode(oldNode, future);
        assertNull("connect to a node without a stream address must be skipped, not attempted", future.actionGet());
        assertFalse(streamTransportService.nodeConnected(oldNode));
    }

    public void testInitiateChannelThrowsConnectTransportExceptionInsteadOfNpe() {
        DiscoveryNode oldNode = nodeWithoutStreamAddress();
        ConnectTransportException e = expectThrows(ConnectTransportException.class, () -> flightTransport.initiateChannel(oldNode));
        assertTrue(e.getMessage().contains("stream"));
    }
}
