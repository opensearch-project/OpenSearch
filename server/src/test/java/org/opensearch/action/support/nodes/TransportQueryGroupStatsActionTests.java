/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.nodes;

import org.opensearch.action.admin.cluster.wlm.QueryGroupStatsRequest;
import org.opensearch.action.admin.cluster.wlm.TransportQueryGroupStatsAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.QueryGroupService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class TransportQueryGroupStatsActionTests extends TransportNodesActionTests {

    /**
     * We don't want to send discovery nodes list to each request that is sent across from the coordinator node.
     * This behavior is asserted in this test.
     */
    public void testQueryGroupStatsActionWithRetentionOfDiscoveryNodesList() {
        QueryGroupStatsRequest request = new QueryGroupStatsRequest();
        Map<String, List<MockNodeQueryGroupStatsRequest>> combinedSentRequest = performQueryGroupStatsAction(request);

        assertNotNull(combinedSentRequest);
        combinedSentRequest.forEach((node, capturedRequestList) -> {
            assertNotNull(capturedRequestList);
            capturedRequestList.forEach(sentRequest -> { assertNull(sentRequest.getDiscoveryNodes()); });
        });
    }

    private Map<String, List<MockNodeQueryGroupStatsRequest>> performQueryGroupStatsAction(QueryGroupStatsRequest request) {
        TransportNodesAction action = getTestTransportQueryGroupStatsAction();
        PlainActionFuture<QueryGroupStatsRequest> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        Map<String, List<MockNodeQueryGroupStatsRequest>> combinedSentRequest = new HashMap<>();

        capturedRequests.forEach((node, capturedRequestList) -> {
            List<MockNodeQueryGroupStatsRequest> sentRequestList = new ArrayList<>();

            capturedRequestList.forEach(preSentRequest -> {
                BytesStreamOutput out = new BytesStreamOutput();
                try {
                    TransportQueryGroupStatsAction.NodeQueryGroupStatsRequest QueryGroupStatsRequestFromCoordinator =
                        (TransportQueryGroupStatsAction.NodeQueryGroupStatsRequest) preSentRequest.request;
                    QueryGroupStatsRequestFromCoordinator.writeTo(out);
                    StreamInput in = out.bytes().streamInput();
                    MockNodeQueryGroupStatsRequest QueryGroupStatsRequest = new MockNodeQueryGroupStatsRequest(in);
                    sentRequestList.add(QueryGroupStatsRequest);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            combinedSentRequest.put(node, sentRequestList);
        });

        return combinedSentRequest;
    }

    private TestTransportQueryGroupStatsAction getTestTransportQueryGroupStatsAction() {
        return new TestTransportQueryGroupStatsAction(
            THREAD_POOL,
            clusterService,
            transportService,
            mock(QueryGroupService.class),
            new ActionFilters(Collections.emptySet())
        );
    }

    private static class TestTransportQueryGroupStatsAction extends TransportQueryGroupStatsAction {
        public TestTransportQueryGroupStatsAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            QueryGroupService queryGroupService,
            ActionFilters actionFilters
        ) {
            super(threadPool, clusterService, transportService, queryGroupService, actionFilters);
        }
    }

    private static class MockNodeQueryGroupStatsRequest extends TransportQueryGroupStatsAction.NodeQueryGroupStatsRequest {

        public MockNodeQueryGroupStatsRequest(StreamInput in) throws IOException {
            super(in);
        }

        public DiscoveryNode[] getDiscoveryNodes() {
            return this.request.concreteNodes();
        }
    }
}
