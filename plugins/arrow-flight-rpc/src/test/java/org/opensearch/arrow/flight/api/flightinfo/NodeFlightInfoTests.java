/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.api.flightinfo;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class NodeFlightInfoTests extends OpenSearchTestCase {

    public void testNodeFlightInfoSerialization() throws Exception {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            "test_node",
            "hostname",
            "localhost",
            "127.0.0.1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            new HashMap<>(),
            new HashSet<>(),
            Version.CURRENT
        );

        TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 47470);
        BoundTransportAddress boundAddress = new BoundTransportAddress(new TransportAddress[] { address }, address);

        NodeFlightInfo originalInfo = new NodeFlightInfo(node, boundAddress);

        BytesStreamOutput output = new BytesStreamOutput();
        originalInfo.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        NodeFlightInfo deserializedInfo = new NodeFlightInfo(input);

        assertEquals(originalInfo.getNode(), deserializedInfo.getNode());
        assertEquals(originalInfo.getBoundAddress().boundAddresses().length, deserializedInfo.getBoundAddress().boundAddresses().length);
        assertEquals(originalInfo.getBoundAddress().boundAddresses()[0], deserializedInfo.getBoundAddress().boundAddresses()[0]);
        assertEquals(originalInfo.getBoundAddress().publishAddress(), deserializedInfo.getBoundAddress().publishAddress());
    }

    public void testNodeFlightInfoEquality() throws Exception {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            "test_node",
            "hostname",
            "localhost",
            "127.0.0.1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            new HashMap<>(),
            new HashSet<>(),
            Version.CURRENT
        );

        TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 47470);
        BoundTransportAddress boundAddress = new BoundTransportAddress(new TransportAddress[] { address }, address);

        NodeFlightInfo info1 = new NodeFlightInfo(node, boundAddress);
        NodeFlightInfo info2 = new NodeFlightInfo(node, boundAddress);

        assertEquals(info1.getBoundAddress(), info2.getBoundAddress());
    }

    public void testGetters() throws Exception {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            "test_node",
            "hostname",
            "localhost",
            "127.0.0.1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            new HashMap<>(),
            new HashSet<>(),
            Version.CURRENT
        );

        TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 47470);
        BoundTransportAddress boundAddress = new BoundTransportAddress(new TransportAddress[] { address }, address);

        NodeFlightInfo info = new NodeFlightInfo(node, boundAddress);

        assertEquals(node, info.getNode());
        assertEquals(boundAddress, info.getBoundAddress());
    }

    public void testToXContent() throws Exception {
        TransportAddress boundAddress1 = new TransportAddress(InetAddress.getLoopbackAddress(), 47470);
        TransportAddress boundAddress2 = new TransportAddress(InetAddress.getLoopbackAddress(), 47471);
        TransportAddress publishAddress = new TransportAddress(InetAddress.getLoopbackAddress(), 47472);

        BoundTransportAddress boundAddress = new BoundTransportAddress(
            new TransportAddress[] { boundAddress1, boundAddress2 },
            publishAddress
        );

        NodeFlightInfo info = new NodeFlightInfo(
            new DiscoveryNode(
                "test_node",
                new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                Collections.emptyMap(),
                Collections.emptySet(),
                Version.CURRENT
            ),
            boundAddress
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field("node_info");
        info.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            Map<String, Object> responseMap = parser.map();

            Map<String, Object> nodeInfo = (Map<String, Object>) responseMap.get("node_info");
            assertNotNull("node_info object should exist", nodeInfo);

            Map<String, Object> flightServer = (Map<String, Object>) nodeInfo.get("flight_server");
            assertNotNull("flight_server object should exist", flightServer);

            List<Map<String, Object>> boundAddresses = (List<Map<String, Object>>) flightServer.get("bound_addresses");
            assertNotNull("bound_addresses array should exist", boundAddresses);
            assertEquals("Should have 2 bound addresses", 2, boundAddresses.size());

            assertEquals("localhost", boundAddresses.get(0).get("host"));
            assertEquals(47470, boundAddresses.get(0).get("port"));

            assertEquals("localhost", boundAddresses.get(1).get("host"));
            assertEquals(47471, boundAddresses.get(1).get("port"));

            Map<String, Object> publishAddressMap = (Map<String, Object>) flightServer.get("publish_address");
            assertNotNull("publish_address object should exist", publishAddressMap);
            assertEquals("localhost", publishAddressMap.get("host"));
            assertEquals(47472, publishAddressMap.get("port"));
        }
    }
}
