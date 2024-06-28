/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.discovery;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.JoinHelper;
import org.opensearch.cluster.coordination.PublicationTransportHandler;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.disruption.ServiceDisruptionScheme;
import org.opensearch.test.disruption.SlowClusterStateProcessing;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;

/**
 * Tests for discovery during disruptions.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiscoveryDisruptionIT extends AbstractDisruptionTestCase {

    /**
     * Test cluster join with issues in cluster state publishing *
     */
    public void testClusterJoinDespiteOfPublishingIssues() throws Exception {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        String nonClusterManagerNode = internalCluster().startDataOnlyNode();

        DiscoveryNodes discoveryNodes = internalCluster().getInstance(ClusterService.class, nonClusterManagerNode).state().nodes();

        TransportService clusterManagerTranspotService = internalCluster().getInstance(
            TransportService.class,
            discoveryNodes.getClusterManagerNode().getName()
        );

        logger.info("blocking requests from non cluster-manager [{}] to cluster-manager [{}]", nonClusterManagerNode, clusterManagerNode);
        MockTransportService nonClusterManagerTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            nonClusterManagerNode
        );
        nonClusterManagerTransportService.addFailToSendNoConnectRule(clusterManagerTranspotService);

        assertNoClusterManager(nonClusterManagerNode);

        logger.info(
            "blocking cluster state publishing from cluster-manager [{}] to non cluster-manager [{}]",
            clusterManagerNode,
            nonClusterManagerNode
        );
        MockTransportService clusterManagerTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            clusterManagerNode
        );
        TransportService localTransportService = internalCluster().getInstance(
            TransportService.class,
            discoveryNodes.getLocalNode().getName()
        );
        if (randomBoolean()) {
            clusterManagerTransportService.addFailToSendNoConnectRule(
                localTransportService,
                PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME
            );
        } else {
            clusterManagerTransportService.addFailToSendNoConnectRule(
                localTransportService,
                PublicationTransportHandler.COMMIT_STATE_ACTION_NAME
            );
        }

        logger.info(
            "allowing requests from non cluster-manager [{}] to cluster-manager [{}], waiting for two join request",
            nonClusterManagerNode,
            clusterManagerNode
        );
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        nonClusterManagerTransportService.addSendBehavior(
            clusterManagerTransportService,
            (connection, requestId, action, request, options) -> {
                if (action.equals(JoinHelper.JOIN_ACTION_NAME)) {
                    countDownLatch.countDown();
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );

        nonClusterManagerTransportService.addConnectBehavior(clusterManagerTransportService, Transport::openConnection);

        countDownLatch.await();

        logger.info("waiting for cluster to reform");
        clusterManagerTransportService.clearOutboundRules(localTransportService);
        nonClusterManagerTransportService.clearOutboundRules(localTransportService);

        ensureStableCluster(2);

        // shutting down the nodes, to avoid the leakage check tripping
        // on the states associated with the commit requests we may have dropped
        internalCluster().stopRandomNodeNotCurrentClusterManager();
    }

    public void testClusterFormingWithASlowNode() {

        SlowClusterStateProcessing disruption = new SlowClusterStateProcessing(random(), 0, 0, 1000, 2000);

        // don't wait for initial state, we want to add the disruption while the cluster is forming
        internalCluster().startNodes(3);

        logger.info("applying disruption while cluster is forming ...");

        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        ensureStableCluster(3);
    }

    public void testElectClusterManagerWithLatestVersion() throws Exception {
        final Set<String> nodes = new HashSet<>(internalCluster().startNodes(3));
        ensureStableCluster(3);
        ServiceDisruptionScheme isolateAllNodes = new NetworkDisruption(
            new NetworkDisruption.IsolateAllNodes(nodes),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(isolateAllNodes);

        logger.info("--> forcing a complete election to make sure \"preferred\" cluster-manager is elected");
        isolateAllNodes.startDisrupting();
        for (String node : nodes) {
            assertNoClusterManager(node);
        }
        internalCluster().clearDisruptionScheme();
        ensureStableCluster(3);
        final String preferredClusterManagerName = internalCluster().getClusterManagerName();
        final DiscoveryNode preferredClusterManager = internalCluster().clusterService(preferredClusterManagerName).localNode();

        logger.info("--> preferred cluster-manager is {}", preferredClusterManager);
        final Set<String> nonPreferredNodes = new HashSet<>(nodes);
        nonPreferredNodes.remove(preferredClusterManagerName);
        final ServiceDisruptionScheme isolatePreferredClusterManager = isolateClusterManagerDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(isolatePreferredClusterManager);
        isolatePreferredClusterManager.startDisrupting();

        client(randomFrom(nonPreferredNodes)).admin()
            .indices()
            .prepareCreate("test")
            .setSettings(
                Settings.builder().put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1).put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            )
            .get();

        internalCluster().clearDisruptionScheme(false);
        internalCluster().setDisruptionScheme(isolateAllNodes);

        logger.info("--> forcing a complete election again");
        isolateAllNodes.startDisrupting();
        for (String node : nodes) {
            assertNoClusterManager(node);
        }

        isolateAllNodes.stopDisrupting();

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        if (state.metadata().hasIndex("test") == false) {
            fail("index 'test' was lost. current cluster state: " + state);
        }

    }

    /**
     * Adds an asymmetric break between a cluster-manager and one of the nodes and makes
     * sure that the node is removed form the cluster, that the node start pinging and that
     * the cluster reforms when healed.
     */
    public void testNodeNotReachableFromClusterManager() throws Exception {
        startCluster(3);

        String clusterManagerNode = internalCluster().getClusterManagerName();
        String nonClusterManagerNode = null;
        while (nonClusterManagerNode == null) {
            nonClusterManagerNode = randomFrom(internalCluster().getNodeNames());
            if (nonClusterManagerNode.equals(clusterManagerNode)) {
                nonClusterManagerNode = null;
            }
        }

        logger.info("blocking request from cluster-manager [{}] to [{}]", clusterManagerNode, nonClusterManagerNode);
        MockTransportService clusterManagerTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            clusterManagerNode
        );
        if (randomBoolean()) {
            clusterManagerTransportService.addUnresponsiveRule(
                internalCluster().getInstance(TransportService.class, nonClusterManagerNode)
            );
        } else {
            clusterManagerTransportService.addFailToSendNoConnectRule(
                internalCluster().getInstance(TransportService.class, nonClusterManagerNode)
            );
        }

        logger.info("waiting for [{}] to be removed from cluster", nonClusterManagerNode);
        ensureStableCluster(2, clusterManagerNode);

        logger.info("waiting for [{}] to have no cluster-manager", nonClusterManagerNode);
        assertNoClusterManager(nonClusterManagerNode);

        logger.info("healing partition and checking cluster reforms");
        clusterManagerTransportService.clearAllRules();

        ensureStableCluster(3);
    }

}
