/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.nodes;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.opensearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.NodeService;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportClusterStatsActionTests extends TransportNodesActionTests {

    /**
     * By default, we send discovery nodes list to each request that is sent across from the coordinator node. This
     * behavior is asserted in this test.
     */
    public void testClusterStatsActionWithRetentionOfDiscoveryNodesList() {
        ClusterStatsRequest request = new ClusterStatsRequest();
        request.retainDiscoveryNodes(true);
        Map<String, List<MockClusterStatsNodeRequest>> combinedSentRequest = performNodesInfoAction(request);

        assertNotNull(combinedSentRequest);
        combinedSentRequest.forEach((node, capturedRequestList) -> {
            assertNotNull(capturedRequestList);
            capturedRequestList.forEach(sentRequest -> {
                assertNotNull(sentRequest.getDiscoveryNodes());
                assertEquals(sentRequest.getDiscoveryNodes().length, clusterService.state().nodes().getSize());
            });
        });
    }

    public void testClusterStatsActionWithPreFilledConcreteNodesAndWithRetentionOfDiscoveryNodesList() {
        ClusterStatsRequest request = new ClusterStatsRequest();
        Collection<DiscoveryNode> discoveryNodes = clusterService.state().getNodes().getNodes().values();
        request.setConcreteNodes(discoveryNodes.toArray(DiscoveryNode[]::new));
        Map<String, List<MockClusterStatsNodeRequest>> combinedSentRequest = performNodesInfoAction(request);

        assertNotNull(combinedSentRequest);
        combinedSentRequest.forEach((node, capturedRequestList) -> {
            assertNotNull(capturedRequestList);
            capturedRequestList.forEach(sentRequest -> {
                assertNotNull(sentRequest.getDiscoveryNodes());
                assertEquals(sentRequest.getDiscoveryNodes().length, clusterService.state().nodes().getSize());
            });
        });
    }

    /**
     * In the optimized ClusterStats Request, we do not send the DiscoveryNodes List to each node. This behavior is
     * asserted in this test.
     */
    public void testClusterStatsActionWithoutRetentionOfDiscoveryNodesList() {
        ClusterStatsRequest request = new ClusterStatsRequest();
        request.retainDiscoveryNodes(false);
        Map<String, List<MockClusterStatsNodeRequest>> combinedSentRequest = performNodesInfoAction(request);

        assertNotNull(combinedSentRequest);
        combinedSentRequest.forEach((node, capturedRequestList) -> {
            assertNotNull(capturedRequestList);
            capturedRequestList.forEach(sentRequest -> { assertNull(sentRequest.getDiscoveryNodes()); });
        });
    }

    public void testClusterStatsActionWithPreFilledConcreteNodesAndWithoutRetentionOfDiscoveryNodesList() {
        ClusterStatsRequest request = new ClusterStatsRequest();
        Collection<DiscoveryNode> discoveryNodes = clusterService.state().getNodes().getNodes().values();
        request.setConcreteNodes(discoveryNodes.toArray(DiscoveryNode[]::new));
        request.retainDiscoveryNodes(false);
        Map<String, List<MockClusterStatsNodeRequest>> combinedSentRequest = performNodesInfoAction(request);

        assertNotNull(combinedSentRequest);
        combinedSentRequest.forEach((node, capturedRequestList) -> {
            assertNotNull(capturedRequestList);
            capturedRequestList.forEach(sentRequest -> { assertNull(sentRequest.getDiscoveryNodes()); });
        });
    }

    private Map<String, List<MockClusterStatsNodeRequest>> performNodesInfoAction(ClusterStatsRequest request) {
        TransportNodesAction action = getTestTransportClusterStatsAction();
        PlainActionFuture<NodesStatsRequest> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        Map<String, List<MockClusterStatsNodeRequest>> combinedSentRequest = new HashMap<>();

        capturedRequests.forEach((node, capturedRequestList) -> {
            List<MockClusterStatsNodeRequest> sentRequestList = new ArrayList<>();

            capturedRequestList.forEach(preSentRequest -> {
                BytesStreamOutput out = new BytesStreamOutput();
                try {
                    TransportClusterStatsAction.ClusterStatsNodeRequest clusterStatsNodeRequestFromCoordinator =
                        (TransportClusterStatsAction.ClusterStatsNodeRequest) preSentRequest.request;
                    clusterStatsNodeRequestFromCoordinator.writeTo(out);
                    StreamInput in = out.bytes().streamInput();
                    MockClusterStatsNodeRequest mockClusterStatsNodeRequest = new MockClusterStatsNodeRequest(in);
                    sentRequestList.add(mockClusterStatsNodeRequest);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            combinedSentRequest.put(node, sentRequestList);
        });

        return combinedSentRequest;
    }

    private TestTransportClusterStatsAction getTestTransportClusterStatsAction() {
        return new TestTransportClusterStatsAction(
            THREAD_POOL,
            clusterService,
            transportService,
            nodeService,
            indicesService,
            new ActionFilters(Collections.emptySet())
        );
    }

    private static class TestTransportClusterStatsAction extends TransportClusterStatsAction {
        public TestTransportClusterStatsAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            NodeService nodeService,
            IndicesService indicesService,
            ActionFilters actionFilters
        ) {
            super(threadPool, clusterService, transportService, nodeService, indicesService, actionFilters);
        }
    }

    private static class MockClusterStatsNodeRequest extends TransportClusterStatsAction.ClusterStatsNodeRequest {

        public MockClusterStatsNodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        public DiscoveryNode[] getDiscoveryNodes() {
            return this.request.concreteNodes();
        }
    }
}
