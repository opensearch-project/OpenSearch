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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.coordination.Coordinator;
import org.opensearch.cluster.coordination.FollowersChecker;
import org.opensearch.cluster.coordination.LeaderChecker;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.Strings;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.LongGCDisruption;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.disruption.NetworkDisruption.NetworkLinkDisruptionType;
import org.opensearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.opensearch.test.disruption.SingleNodeDisruption;
import org.opensearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests relating to the loss of the cluster-manager, but which work with the default fault detection settings which are rather lenient and will
 * not detect a cluster-manager failure too quickly.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class StableClusterManagerDisruptionIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    /**
     * Test that no split brain occurs under partial network partition. See https://github.com/elastic/elasticsearch/issues/2488
     */
    public void testFailWithMinimumClusterManagerNodesConfigured() throws Exception {
        List<String> nodes = internalCluster().startNodes(3);
        ensureStableCluster(3);

        // Figure out what is the elected cluster-manager node
        final String clusterManagerNode = internalCluster().getClusterManagerName();
        logger.info("---> legit elected cluster-manager node={}", clusterManagerNode);

        // Pick a node that isn't the elected cluster-manager.
        Set<String> nonClusterManagers = new HashSet<>(nodes);
        nonClusterManagers.remove(clusterManagerNode);
        final String unluckyNode = randomFrom(nonClusterManagers.toArray(Strings.EMPTY_ARRAY));

        // Simulate a network issue between the unlucky node and elected cluster-manager node in both directions.

        NetworkDisruption networkDisconnect = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(clusterManagerNode, unluckyNode),
            NetworkDisruption.DISCONNECT
        );
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();

        // Wait until elected cluster-manager has removed that the unlucky node...
        ensureStableCluster(2, clusterManagerNode);

        // The unlucky node must report *no* cluster-manager node, since it can't connect to cluster-manager and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected cluster-manager
        ensureNoMaster(unluckyNode);

        networkDisconnect.stopDisrupting();

        // Wait until the cluster-manager node sees all 3 nodes again.
        ensureStableCluster(3);

        // The elected cluster-manager shouldn't have changed, since the unlucky node never could have elected itself as cluster-manager
        assertThat(internalCluster().getClusterManagerName(), equalTo(clusterManagerNode));
    }

    private void ensureNoMaster(String node) throws Exception {
        assertBusy(
            () -> assertNull(
                client(node).admin().cluster().state(new ClusterStateRequest().local(true)).get().getState().nodes().getClusterManagerNode()
            )
        );
    }

    /**
     * Verify that nodes fault detection detects a disconnected node after cluster-manager reelection
     */
    public void testFollowerCheckerDetectsDisconnectedNodeAfterClusterManagerReelection() throws Exception {
        testFollowerCheckerAfterClusterManagerReelection(NetworkDisruption.DISCONNECT, Settings.EMPTY);
    }

    /**
     * Verify that nodes fault detection detects an unresponsive node after cluster-manager reelection
     */
    public void testFollowerCheckerDetectsUnresponsiveNodeAfterClusterManagerReelection() throws Exception {
        testFollowerCheckerAfterClusterManagerReelection(
            NetworkDisruption.UNRESPONSIVE,
            Settings.builder()
                .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "4")
                .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
                .build()
        );
    }

    private void testFollowerCheckerAfterClusterManagerReelection(NetworkLinkDisruptionType networkLinkDisruptionType, Settings settings)
        throws Exception {
        internalCluster().startNodes(4, settings);
        ensureStableCluster(4);

        logger.info("--> stopping current cluster-manager");
        internalCluster().stopCurrentClusterManagerNode();

        ensureStableCluster(3);

        final String clusterManager = internalCluster().getClusterManagerName();
        final List<String> nonClusterManagers = Arrays.stream(internalCluster().getNodeNames())
            .filter(n -> clusterManager.equals(n) == false)
            .collect(Collectors.toList());
        final String isolatedNode = randomFrom(nonClusterManagers);
        final String otherNode = nonClusterManagers.get(nonClusterManagers.get(0).equals(isolatedNode) ? 1 : 0);

        logger.info("--> isolating [{}]", isolatedNode);

        final NetworkDisruption networkDisruption = new NetworkDisruption(
            new TwoPartitions(singleton(isolatedNode), Sets.newHashSet(clusterManager, otherNode)),
            networkLinkDisruptionType
        );
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        logger.info("--> waiting for cluster-manager to remove it");
        ensureStableCluster(2, clusterManager);
        ensureNoMaster(isolatedNode);

        networkDisruption.stopDisrupting();
        ensureStableCluster(3);
    }

    /**
     * Tests that emulates a frozen elected cluster-manager node that unfreezes and pushes its cluster state to other nodes that already are
     * following another elected cluster-manager node. These nodes should reject this cluster state and prevent them from following the stale cluster-manager.
     */
    public void testStaleClusterManagerNotHijackingMajority() throws Exception {
        final List<String> nodes = internalCluster().startNodes(
            3,
            Settings.builder()
                .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
                .build()
        );
        ensureStableCluster(3);

        // Save the current cluster-manager node as old cluster-manager node, because that node will get frozen
        final String oldClusterManagerNode = internalCluster().getClusterManagerName();

        // Simulating a painful gc by suspending all threads for a long time on the current elected cluster-manager node.
        SingleNodeDisruption clusterManagerNodeDisruption = new LongGCDisruption(random(), oldClusterManagerNode);

        // Save the majority side
        final List<String> majoritySide = new ArrayList<>(nodes);
        majoritySide.remove(oldClusterManagerNode);

        // Keeps track of the previous and current cluster-manager when a cluster-manager node transition took place on each node on the
        // majority side:
        final Map<String, List<Tuple<String, String>>> clusterManagers = Collections.synchronizedMap(new HashMap<>());
        for (final String node : majoritySide) {
            clusterManagers.put(node, new ArrayList<>());
            internalCluster().getInstance(ClusterService.class, node).addListener(event -> {
                DiscoveryNode previousClusterManager = event.previousState().nodes().getClusterManagerNode();
                DiscoveryNode currentClusterManager = event.state().nodes().getClusterManagerNode();
                if (!Objects.equals(previousClusterManager, currentClusterManager)) {
                    logger.info(
                        "--> node {} received new cluster state: {} \n and had previous cluster state: {}",
                        node,
                        event.state(),
                        event.previousState()
                    );
                    String previousClusterManagerNodeName = previousClusterManager != null ? previousClusterManager.getName() : null;
                    String currentClusterManagerNodeName = currentClusterManager != null ? currentClusterManager.getName() : null;
                    clusterManagers.get(node).add(new Tuple<>(previousClusterManagerNodeName, currentClusterManagerNodeName));
                }
            });
        }

        final CountDownLatch oldClusterManagerNodeSteppedDown = new CountDownLatch(1);
        internalCluster().getInstance(ClusterService.class, oldClusterManagerNode).addListener(event -> {
            if (event.state().nodes().getClusterManagerNodeId() == null) {
                oldClusterManagerNodeSteppedDown.countDown();
            }
        });

        internalCluster().setDisruptionScheme(clusterManagerNodeDisruption);
        logger.info("--> freezing node [{}]", oldClusterManagerNode);
        clusterManagerNodeDisruption.startDisrupting();

        // Wait for majority side to elect a new cluster-manager
        assertBusy(() -> {
            for (final Map.Entry<String, List<Tuple<String, String>>> entry : clusterManagers.entrySet()) {
                final List<Tuple<String, String>> transitions = entry.getValue();
                assertTrue(entry.getKey() + ": " + transitions, transitions.stream().anyMatch(transition -> transition.v2() != null));
            }
        });

        // The old cluster-manager node is frozen, but here we submit a cluster state update task that doesn't get executed, but will be
        // queued and
        // once the old cluster-manager node un-freezes it gets executed. The old cluster-manager node will send this update + the cluster
        // state where it is
        // flagged as cluster-manager to the other nodes that follow the new cluster-manager. These nodes should ignore this update.
        internalCluster().getInstance(ClusterService.class, oldClusterManagerNode)
            .submitStateUpdateTask("sneaky-update", new ClusterStateUpdateTask(Priority.IMMEDIATE) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(() -> new ParameterizedMessage("failure [{}]", source), e);
                }
            });

        // Save the new elected cluster-manager node
        final String newClusterManagerNode = internalCluster().getClusterManagerName(majoritySide.get(0));
        logger.info("--> new detected cluster-manager node [{}]", newClusterManagerNode);

        // Stop disruption
        logger.info("--> unfreezing node [{}]", oldClusterManagerNode);
        clusterManagerNodeDisruption.stopDisrupting();

        oldClusterManagerNodeSteppedDown.await(30, TimeUnit.SECONDS);
        logger.info("--> [{}] stepped down as cluster-manager", oldClusterManagerNode);
        ensureStableCluster(3);

        assertThat(clusterManagers.size(), equalTo(2));
        for (Map.Entry<String, List<Tuple<String, String>>> entry : clusterManagers.entrySet()) {
            String nodeName = entry.getKey();
            List<Tuple<String, String>> transitions = entry.getValue();
            assertTrue(
                "["
                    + nodeName
                    + "] should not apply state from old cluster-manager ["
                    + oldClusterManagerNode
                    + "] but it did: "
                    + transitions,
                transitions.stream().noneMatch(t -> oldClusterManagerNode.equals(t.v2()))
            );
        }
    }

}
