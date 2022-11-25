/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.Operator;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.store.IndicesStore;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BasicAuthenticationIT extends OpenSearchIntegTestCase {

    public void testStartingAndStoppingNodes() throws IOException {
        logger.info("--> cluster has [{}] nodes", internalCluster().size());
        if (internalCluster().size() < 5) {
            final int nodesToStart = randomIntBetween(Math.max(2, internalCluster().size() + 1), 5);
            logger.info("--> growing to [{}] nodes", nodesToStart);
            internalCluster().startNodes(nodesToStart);
        }
        ensureGreen();

        while (internalCluster().size() > 1) {
            final int nodesToRemain = randomIntBetween(1, internalCluster().size() - 1);
            logger.info("--> reducing to [{}] nodes", nodesToRemain);
            internalCluster().ensureAtMostNumDataNodes(nodesToRemain);
            assertThat(internalCluster().size(), lessThanOrEqualTo(nodesToRemain));
        }

        ensureGreen();
    }

    public void testBasicAuth() {
        logger.info("--> cluster has [{}] nodes", internalCluster().size());
        if (internalCluster().size() < 2) {
            final int nodesToStart = 2;
            logger.info("--> growing to [{}] nodes", nodesToStart);
            internalCluster().startNodes(nodesToStart);
        }
        ensureGreen();

        System.out.println("Node names");
        for (String nodeName : internalCluster().getNodeNames()) {
            System.out.println(nodeName);
        }

        TransportService serviceA = internalCluster().getInstance(TransportService.class, "node_t0");
        TransportService serviceB = internalCluster().getInstance(TransportService.class, "node_t1");

        serviceA.addMessageListener(new TransportMessageListener() {
            @Override
            public void onRequestReceived(long requestId, String action) {
                System.out.println("serviceA onRequestReceived");
                System.out.println(requestId);
                System.out.println(action);

                final ThreadPool threadPoolA = internalCluster().getInstance(ThreadPool.class, "node_t0");
                System.out.println(threadPoolA.getThreadContext().getHeaders());
            }

            @Override
            public void onRequestSent(
                DiscoveryNode node,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions finalOptions
            ) {
                System.out.println("serviceA onRequestSent");
                System.out.println(request);
                System.out.println(finalOptions);

                final ThreadPool threadPoolA = internalCluster().getInstance(ThreadPool.class, "node_t0");
                System.out.println(threadPoolA.getThreadContext().getHeaders());
            }
        });

        serviceB.addMessageListener(new TransportMessageListener() {
            @Override
            public void onRequestReceived(long requestId, String action) {
                System.out.println("serviceB onRequestReceived");
                System.out.println(requestId);
                System.out.println(action);

                final ThreadPool threadPoolB = internalCluster().getInstance(ThreadPool.class, "node_t1");
                System.out.println(threadPoolB.getThreadContext().getHeaders());
            }

            @Override
            public void onRequestSent(
                DiscoveryNode node,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions finalOptions
            ) {
                System.out.println("serviceB onRequestSent");
                System.out.println(request);
                System.out.println(finalOptions);

                final ThreadPool threadPoolB = internalCluster().getInstance(ThreadPool.class, "node_t1");
                System.out.println(threadPoolB.getThreadContext().getHeaders());
            }
        });

        ClusterHealthRequest request = new ClusterHealthRequest();
        ClusterHealthResponse resp = client().admin().cluster().health(request).actionGet();


        System.out.println("testBasicAuth");
        System.out.println(resp);
    }
}

