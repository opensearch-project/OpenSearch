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
<<<<<<< HEAD
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
=======
>>>>>>> ffe0d7fa2cd (address comments)
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.transport.CapturingTransport;
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
        Map<String, List<QueryGroupStatsRequest>> combinedSentRequest = performQueryGroupStatsAction(request);
=======
        Map<String, List<MockNodeQueryGroupStatsRequest>> combinedSentRequest = performQueryGroupStatsAction(request);
>>>>>>> b5cbfa4de9e (changelog)
=======
        Map<String, List<NodeQueryGroupStatsRequest>> combinedSentRequest = performQueryGroupStatsAction(request);
>>>>>>> ffe0d7fa2cd (address comments)
=======
        Map<String, List<QueryGroupStatsRequest>> combinedSentRequest = performQueryGroupStatsAction(request);
>>>>>>> 3a7ac33beb6 (modify based on comments)

        assertNotNull(combinedSentRequest);
        combinedSentRequest.forEach((node, capturedRequestList) -> {
            assertNotNull(capturedRequestList);
<<<<<<< HEAD
<<<<<<< HEAD
            capturedRequestList.forEach(sentRequest -> { assertNull(sentRequest.concreteNodes()); });
        });
    }

    private Map<String, List<QueryGroupStatsRequest>> performQueryGroupStatsAction(QueryGroupStatsRequest request) {
        TransportNodesAction action = new TransportQueryGroupStatsAction(
            THREAD_POOL,
            clusterService,
            transportService,
            mock(QueryGroupService.class),
            new ActionFilters(Collections.emptySet())
        );
        PlainActionFuture<QueryGroupStatsRequest> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        Map<String, List<QueryGroupStatsRequest>> combinedSentRequest = new HashMap<>();

        capturedRequests.forEach((node, capturedRequestList) -> {
            List<QueryGroupStatsRequest> sentRequestList = new ArrayList<>();
=======
            capturedRequestList.forEach(sentRequest -> { assertNull(sentRequest.getDiscoveryNodes()); });
=======
            capturedRequestList.forEach(sentRequest -> { assertNull(sentRequest.concreteNodes()); });
>>>>>>> 3a7ac33beb6 (modify based on comments)
        });
    }

    private Map<String, List<QueryGroupStatsRequest>> performQueryGroupStatsAction(QueryGroupStatsRequest request) {
        TransportNodesAction action = new TransportQueryGroupStatsAction(
            THREAD_POOL,
            clusterService,
            transportService,
            mock(QueryGroupService.class),
            new ActionFilters(Collections.emptySet())
        );
        PlainActionFuture<QueryGroupStatsRequest> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        Map<String, List<QueryGroupStatsRequest>> combinedSentRequest = new HashMap<>();

        capturedRequests.forEach((node, capturedRequestList) -> {
<<<<<<< HEAD
<<<<<<< HEAD
            List<MockNodeQueryGroupStatsRequest> sentRequestList = new ArrayList<>();
>>>>>>> b5cbfa4de9e (changelog)
=======
            List<NodeQueryGroupStatsRequest> sentRequestList = new ArrayList<>();
>>>>>>> ffe0d7fa2cd (address comments)
=======
            List<QueryGroupStatsRequest> sentRequestList = new ArrayList<>();
>>>>>>> 3a7ac33beb6 (modify based on comments)

            capturedRequestList.forEach(preSentRequest -> {
                BytesStreamOutput out = new BytesStreamOutput();
                try {
<<<<<<< HEAD
<<<<<<< HEAD
                    QueryGroupStatsRequest QueryGroupStatsRequestFromCoordinator = (QueryGroupStatsRequest) preSentRequest.request;
                    QueryGroupStatsRequestFromCoordinator.writeTo(out);
                    StreamInput in = out.bytes().streamInput();
                    QueryGroupStatsRequest QueryGroupStatsRequest = new QueryGroupStatsRequest(in);
=======
                    TransportQueryGroupStatsAction.NodeQueryGroupStatsRequest QueryGroupStatsRequestFromCoordinator =
                        (TransportQueryGroupStatsAction.NodeQueryGroupStatsRequest) preSentRequest.request;
                    QueryGroupStatsRequestFromCoordinator.writeTo(out);
                    StreamInput in = out.bytes().streamInput();
<<<<<<< HEAD
                    MockNodeQueryGroupStatsRequest QueryGroupStatsRequest = new MockNodeQueryGroupStatsRequest(in);
>>>>>>> b5cbfa4de9e (changelog)
=======
                    NodeQueryGroupStatsRequest QueryGroupStatsRequest = new NodeQueryGroupStatsRequest(in);
>>>>>>> ffe0d7fa2cd (address comments)
=======
                    QueryGroupStatsRequest QueryGroupStatsRequestFromCoordinator = (QueryGroupStatsRequest) preSentRequest.request;
                    QueryGroupStatsRequestFromCoordinator.writeTo(out);
                    StreamInput in = out.bytes().streamInput();
                    QueryGroupStatsRequest QueryGroupStatsRequest = new QueryGroupStatsRequest(in);
>>>>>>> 3a7ac33beb6 (modify based on comments)
                    sentRequestList.add(QueryGroupStatsRequest);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            combinedSentRequest.put(node, sentRequestList);
        });

        return combinedSentRequest;
    }
<<<<<<< HEAD
<<<<<<< HEAD
=======

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
>>>>>>> b5cbfa4de9e (changelog)
=======
>>>>>>> ffe0d7fa2cd (address comments)
}
