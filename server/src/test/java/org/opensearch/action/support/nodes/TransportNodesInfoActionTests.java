/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.nodes;

import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.node.NodeService;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportNodesInfoActionTests extends TransportNodesActionTests {

    /**
     * By default, we send discovery nodes list to each request that is sent across from the coordinator node. This
     * behavior is asserted in this test.
     */
    public void testNodesInfoActionWithRetentionOfDiscoveryNodesList() {
        NodesInfoRequest request = new NodesInfoRequest();
        request.setIncludeDiscoveryNodes(true);
        Map<String, List<MockNodesInfoRequest>> combinedSentRequest = performNodesInfoAction(request);

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
    public void testNodesInfoActionWithoutRetentionOfDiscoveryNodesList() {
        NodesInfoRequest request = new NodesInfoRequest();
        request.setIncludeDiscoveryNodes(false);
        Map<String, List<MockNodesInfoRequest>> combinedSentRequest = performNodesInfoAction(request);

        assertNotNull(combinedSentRequest);
        combinedSentRequest.forEach((node, capturedRequestList) -> {
            assertNotNull(capturedRequestList);
            capturedRequestList.forEach(sentRequest -> { assertNull(sentRequest.getDiscoveryNodes()); });
        });
    }

    private Map<String, List<MockNodesInfoRequest>> performNodesInfoAction(NodesInfoRequest request) {
        TransportNodesAction action = getTestTransportNodesInfoAction();
        PlainActionFuture<NodesStatsRequest> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        Map<String, List<MockNodesInfoRequest>> combinedSentRequest = new HashMap<>();

        capturedRequests.forEach((node, capturedRequestList) -> {
            List<MockNodesInfoRequest> sentRequestList = new ArrayList<>();

            capturedRequestList.forEach(preSentRequest -> {
                BytesStreamOutput out = new BytesStreamOutput();
                try {
                    TransportNodesInfoAction.NodeInfoRequest nodesInfoRequestFromCoordinator =
                        (TransportNodesInfoAction.NodeInfoRequest) preSentRequest.request;
                    nodesInfoRequestFromCoordinator.writeTo(out);
                    StreamInput in = out.bytes().streamInput();
                    MockNodesInfoRequest nodesStatsRequest = new MockNodesInfoRequest(in);
                    sentRequestList.add(nodesStatsRequest);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            combinedSentRequest.put(node, sentRequestList);
        });

        return combinedSentRequest;
    }

    private TestTransportNodesInfoAction getTestTransportNodesInfoAction() {
        return new TestTransportNodesInfoAction(
            THREAD_POOL,
            clusterService,
            transportService,
            nodeService,
            new ActionFilters(Collections.emptySet())
        );
    }

    private static class TestTransportNodesInfoAction extends TransportNodesInfoAction {
        public TestTransportNodesInfoAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            NodeService nodeService,
            ActionFilters actionFilters
        ) {
            super(threadPool, clusterService, transportService, nodeService, actionFilters);
        }
    }

    private static class MockNodesInfoRequest extends TransportNodesInfoAction.NodeInfoRequest {

        public MockNodesInfoRequest(StreamInput in) throws IOException {
            super(in);
        }

        public DiscoveryNode[] getDiscoveryNodes() {
            return this.request.concreteNodes();
        }
    }
}
