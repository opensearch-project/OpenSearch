package org.opensearch.arrow.flight.api;/*
                                        * SPDX-License-Identifier: Apache-2.0
                                        *
                                        * The OpenSearch Contributors require contributions made to
                                        * this file be licensed under the Apache-2.0 license or a
                                        * compatible open source license.
                                        */

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.arrow.flight.bootstrap.FlightService;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportNodesFlightInfoActionTests extends OpenSearchTestCase {

    private DiscoveryNode localNode;
    private TransportNodesFlightInfoAction action;
    private BoundTransportAddress boundAddress;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        localNode = new DiscoveryNode(
            "local_node",
            "local_node",
            "host",
            "localhost",
            "127.0.0.1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            new HashMap<>(),
            new HashSet<>(),
            Version.CURRENT
        );

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterName()).thenReturn(new ClusterName("test-cluster"));
        when(clusterService.localNode()).thenReturn(localNode);

        TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 47470);
        boundAddress = new BoundTransportAddress(new TransportAddress[] { address }, address);

        FlightService flightService = mock(FlightService.class);
        when(flightService.getBoundAddress()).thenReturn(boundAddress);

        action = new TransportNodesFlightInfoAction(
            Settings.EMPTY,
            mock(ThreadPool.class),
            clusterService,
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            flightService
        );
    }

    public void testNewResponse() {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest();
        List<NodeFlightInfo> nodeFlightInfos = Collections.singletonList(new NodeFlightInfo(localNode, boundAddress));
        List<FailedNodeException> failures = Collections.emptyList();

        NodesFlightInfoResponse response = action.newResponse(request, nodeFlightInfos, failures);

        assertNotNull(response);
        assertEquals("test-cluster", response.getClusterName().value());
        assertEquals(1, response.getNodes().size());
        assertEquals(0, response.failures().size());

        NodeFlightInfo nodeInfo = response.getNodes().get(0);
        assertEquals(localNode, nodeInfo.getNode());
        assertEquals(boundAddress, nodeInfo.getBoundAddress());
    }

    public void testNewResponseWithFailures() {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest();
        List<NodeFlightInfo> nodeFlightInfos = Collections.emptyList();
        List<FailedNodeException> failures = Collections.singletonList(new FailedNodeException("failed_node", "test failure", null));

        NodesFlightInfoResponse response = action.newResponse(request, nodeFlightInfos, failures);

        assertNotNull(response);
        assertEquals("test-cluster", response.getClusterName().value());
        assertEquals(0, response.getNodes().size());
        assertEquals(1, response.failures().size());
        assertEquals("failed_node", response.failures().get(0).nodeId());
        assertEquals("test failure", response.failures().get(0).getMessage());
    }

    public void testNewNodeRequest() {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest("node1", "node2");
        NodesFlightInfoRequest.NodeFlightInfoRequest nodeRequest = action.newNodeRequest(request);

        assertNotNull(nodeRequest);
        assertArrayEquals(new String[] { "node1", "node2" }, nodeRequest.request.nodesIds());
    }

    public void testNewNodeResponse() throws IOException {
        NodeFlightInfo nodeInfo = new NodeFlightInfo(localNode, boundAddress);
        BytesStreamOutput out = new BytesStreamOutput();
        nodeInfo.writeTo(out);
        StreamInput in = out.bytes().streamInput();

        NodeFlightInfo deserializedInfo = action.newNodeResponse(in);

        assertNotNull(deserializedInfo);
        assertEquals(nodeInfo.getNode(), deserializedInfo.getNode());
        assertEquals(nodeInfo.getBoundAddress().publishAddress(), deserializedInfo.getBoundAddress().publishAddress());
    }

    public void testNodeOperation() {
        NodesFlightInfoRequest.NodeFlightInfoRequest nodeRequest = new NodesFlightInfoRequest.NodeFlightInfoRequest(
            new NodesFlightInfoRequest()
        );

        NodeFlightInfo response = action.nodeOperation(nodeRequest);

        assertNotNull(response);
        assertEquals(localNode, response.getNode());
        assertEquals(boundAddress.publishAddress(), response.getBoundAddress().publishAddress());
    }

    public void testNodeOperationWithSpecificNodes() throws IOException {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest("local_node");
        NodesFlightInfoRequest.NodeFlightInfoRequest nodeRequest = new NodesFlightInfoRequest.NodeFlightInfoRequest(request);

        NodeFlightInfo response = action.nodeOperation(nodeRequest);

        assertNotNull(response);
        assertEquals(localNode, response.getNode());
        assertEquals(boundAddress, response.getBoundAddress());
    }

    public void testNodeOperationWithInvalidNode() throws IOException {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest("invalid_node");
        NodesFlightInfoRequest.NodeFlightInfoRequest nodeRequest = new NodesFlightInfoRequest.NodeFlightInfoRequest(request);

        NodeFlightInfo response = action.nodeOperation(nodeRequest);

        assertNotNull(response);
        assertEquals(localNode, response.getNode());
        assertEquals(boundAddress, response.getBoundAddress());
    }

    public void testSerialization() throws IOException {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest("node1", "node2");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        NodesFlightInfoRequest deserializedRequest = new NodesFlightInfoRequest(in);

        assertArrayEquals(request.nodesIds(), deserializedRequest.nodesIds());
    }
}
