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
        Map<String, List<QueryGroupStatsRequest>> combinedSentRequest = performQueryGroupStatsAction(request);

        assertNotNull(combinedSentRequest);
        combinedSentRequest.forEach((node, capturedRequestList) -> {
            assertNotNull(capturedRequestList);
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

            capturedRequestList.forEach(preSentRequest -> {
                BytesStreamOutput out = new BytesStreamOutput();
                try {
                    QueryGroupStatsRequest QueryGroupStatsRequestFromCoordinator = (QueryGroupStatsRequest) preSentRequest.request;
                    QueryGroupStatsRequestFromCoordinator.writeTo(out);
                    StreamInput in = out.bytes().streamInput();
                    QueryGroupStatsRequest QueryGroupStatsRequest = new QueryGroupStatsRequest(in);
                    sentRequestList.add(QueryGroupStatsRequest);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            combinedSentRequest.put(node, sentRequestList);
        });

        return combinedSentRequest;
    }
}
