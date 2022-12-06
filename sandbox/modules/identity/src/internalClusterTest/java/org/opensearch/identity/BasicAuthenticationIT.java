/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.Operator;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.store.IndicesStore;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.admin.cluster.RestClusterHealthAction;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BasicAuthenticationIT extends HttpSmokeTestCaseWithIdentity {

    public void testBasicAuth() throws Exception {
        logger.info("--> cluster has [{}] nodes", internalCluster().size());
        if (internalCluster().size() < 2) {
            final int nodesToStart = 2;
            logger.info("--> growing to [{}] nodes", nodesToStart);
            internalCluster().startNodes(nodesToStart);
        }
        ensureGreen();

        System.out.println("Node names");
        List<TransportService> transportServices = new ArrayList<TransportService>();
        for (String nodeName : internalCluster().getNodeNames()) {
            System.out.println(nodeName);
            TransportService service = internalCluster().getInstance(TransportService.class, nodeName);
            transportServices.add(service);
        }

        for (TransportService service : transportServices) {
            service.addMessageListener(new TransportMessageListener() {
                @Override
                public void onRequestReceived(long requestId, String action) {
                    String prefix = "(nodeName=" + service.getLocalNode().getName() + ", requestId=" + requestId + ", action=" + action + " onRequestReceived)";

                    final ThreadPool threadPoolA = internalCluster().getInstance(ThreadPool.class, service.getLocalNode().getName());
                    System.out.println(prefix + " Headers: " + threadPoolA.getThreadContext().getHeaders());
                }

                @Override
                public void onRequestSent(
                    DiscoveryNode node,
                    long requestId,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions finalOptions
                ) {
                    String prefix = "(nodeName=" + service.getLocalNode().getName() + ", requestId=" + requestId + ", action=" + action + " onRequestSent)";

                    final ThreadPool threadPoolA = internalCluster().getInstance(ThreadPool.class, service.getLocalNode().getName());
                    System.out.println(prefix + " Headers: " + threadPoolA.getThreadContext().getHeaders());
                }
            });
        }

//        ClusterHealthRequest request = new ClusterHealthRequest();
//        System.out.println("Sending Cluster Health Request");
//        ClusterHealthResponse resp = client().admin().cluster().health(request).actionGet();

//        Map<String, String> params = new HashMap<>();
//        FakeRestRequest restRequest = buildRestRequest(params);
//        ClusterHealthRequest clusterHealthRequest = RestClusterHealthAction.fromRequest(request);
//        ClusterHealthResponse resp = client().admin().cluster().health(clusterHealthRequest).actionGet();


        System.out.println("Sending Cluster Health Request");
        Request request2 = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic YWRtaW46YWRtaW4=").build(); // admin:admin
        request2.setOptions(options);
        Response response = getRestClient().performRequest(request2);

        System.out.println("=== HERE ===");
        System.out.println("testBasicAuth");
//        System.out.println(resp);
        System.out.println(response);

        ensureStableCluster(2);
        assertThat(internalCluster().size(), is(2));
    }

    private FakeRestRequest buildRestRequest(Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_cluster/health")
            .withParams(params)
            .build();
    }
}

