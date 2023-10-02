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
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.coordination.Coordinator;
import org.opensearch.cluster.coordination.FollowersChecker;
import org.opensearch.cluster.coordination.LeaderChecker;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.disruption.NetworkDisruption.Bridge;
import org.opensearch.test.disruption.NetworkDisruption.DisruptedLinks;
import org.opensearch.test.disruption.NetworkDisruption.NetworkLinkDisruptionType;
import org.opensearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.opensearch.test.disruption.ServiceDisruptionScheme;
import org.opensearch.test.disruption.SlowClusterStateProcessing;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportSettings;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public abstract class AbstractDisruptionTestCase extends OpenSearchIntegTestCase {

    static final TimeValue DISRUPTION_HEALING_OVERHEAD = TimeValue.timeValueSeconds(40); // we use 30s as timeout in many places.

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(DEFAULT_SETTINGS).build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            // sync global checkpoint quickly so we can verify seq_no_stats aligned between all copies after tests.
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .build();
    }

    @Override
    protected int numberOfShards() {
        return 3;
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    private boolean disableBeforeIndexDeletion;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        disableBeforeIndexDeletion = false;
    }

    @Override
    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        if (scheme instanceof NetworkDisruption
            && ((NetworkDisruption) scheme).getNetworkLinkDisruptionType() == NetworkDisruption.UNRESPONSIVE) {
            // the network unresponsive disruption may leave operations in flight
            // this is because this disruption scheme swallows requests by design
            // as such, these operations will never be marked as finished
            disableBeforeIndexDeletion = true;
        }
        super.setDisruptionScheme(scheme);
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        if (disableBeforeIndexDeletion == false) {
            super.beforeIndexDeletion();
            internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
            internalCluster().assertSeqNos();
            internalCluster().assertSameDocIdsOnShards();
        }
    }

    List<String> startCluster(int numberOfNodes) {
        InternalTestCluster internalCluster = internalCluster();
        List<String> nodes = internalCluster.startNodes(numberOfNodes);
        ensureStableCluster(numberOfNodes);
        return nodes;
    }

    public static final Settings DEFAULT_SETTINGS = Settings.builder()
        .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "5s") // for hitting simulated network failures quickly
        .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 1) // for hitting simulated network failures quickly
        .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "5s") // for hitting simulated network failures quickly
        .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1) // for hitting simulated network failures quickly
        .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "5s") // <-- for hitting simulated network failures quickly
        .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "10s") // Network delay disruption waits for the min between this
        // value and the time of disruption and does not recover immediately
        // when disruption is stop. We should make sure we recover faster
        // then the default of 30s, causing ensureGreen and friends to time out
        .build();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class);
    }

    ClusterState getNodeClusterState(String node) {
        return client(node).admin().cluster().prepareState().setLocal(true).get().getState();
    }

    void assertNoClusterManager(final String node) throws Exception {
        assertNoClusterManager(node, null, TimeValue.timeValueSeconds(30));
    }

    void assertNoClusterManager(final String node, TimeValue maxWaitTime) throws Exception {
        assertNoClusterManager(node, null, maxWaitTime);
    }

    void assertNoClusterManager(final String node, @Nullable final ClusterBlock expectedBlocks, TimeValue maxWaitTime) throws Exception {
        assertBusy(() -> {
            ClusterState state = getNodeClusterState(node);
            final DiscoveryNodes nodes = state.nodes();
            assertNull(
                "node [" + node + "] still has [" + nodes.getClusterManagerNode() + "] as cluster-manager",
                nodes.getClusterManagerNode()
            );
            if (expectedBlocks != null) {
                for (ClusterBlockLevel level : expectedBlocks.levels()) {
                    assertTrue(
                        "node [" + node + "] does have level [" + level + "] in it's blocks",
                        state.getBlocks().hasGlobalBlockWithLevel(level)
                    );
                }
            }
        }, maxWaitTime.getMillis(), TimeUnit.MILLISECONDS);
    }

    void assertDifferentClusterManager(final String node, final String oldClusterManagerNode) throws Exception {
        assertBusy(() -> {
            ClusterState state = getNodeClusterState(node);
            String clusterManagerNode = null;
            if (state.nodes().getClusterManagerNode() != null) {
                clusterManagerNode = state.nodes().getClusterManagerNode().getName();
            }
            logger.trace("[{}] cluster-manager is [{}]", node, state.nodes().getClusterManagerNode());
            assertThat(
                "node [" + node + "] still has [" + clusterManagerNode + "] as cluster-manager",
                oldClusterManagerNode,
                not(equalTo(clusterManagerNode))
            );
        }, 30, TimeUnit.SECONDS);
    }

    void assertClusterManager(String clusterManagerNode, List<String> nodes) throws Exception {
        assertBusy(() -> {
            for (String node : nodes) {
                ClusterState state = getNodeClusterState(node);
                String failMsgSuffix = "cluster_state:\n" + state;
                assertThat("wrong node count on [" + node + "]. " + failMsgSuffix, state.nodes().getSize(), equalTo(nodes.size()));
                String otherClusterManagerNodeName = state.nodes().getClusterManagerNode() != null
                    ? state.nodes().getClusterManagerNode().getName()
                    : null;
                assertThat(
                    "wrong cluster-manager on node [" + node + "]. " + failMsgSuffix,
                    otherClusterManagerNodeName,
                    equalTo(clusterManagerNode)
                );
            }
        });
    }

    public ServiceDisruptionScheme addRandomDisruptionScheme() {
        // TODO: add partial partitions
        final DisruptedLinks disruptedLinks;
        if (randomBoolean()) {
            disruptedLinks = TwoPartitions.random(random(), internalCluster().getNodeNames());
        } else {
            disruptedLinks = Bridge.random(random(), internalCluster().getNodeNames());
        }
        final NetworkLinkDisruptionType disruptionType;
        switch (randomInt(2)) {
            case 0:
                disruptionType = NetworkDisruption.UNRESPONSIVE;
                break;
            case 1:
                disruptionType = NetworkDisruption.DISCONNECT;
                break;
            case 2:
                disruptionType = NetworkDisruption.NetworkDelay.random(random());
                break;
            default:
                throw new IllegalArgumentException();
        }
        final ServiceDisruptionScheme scheme;
        if (rarely()) {
            scheme = new SlowClusterStateProcessing(random());
        } else {
            scheme = new NetworkDisruption(disruptedLinks, disruptionType);
        }
        setDisruptionScheme(scheme);
        return scheme;
    }

    NetworkDisruption addRandomDisruptionType(TwoPartitions partitions) {
        final NetworkLinkDisruptionType disruptionType;
        if (randomBoolean()) {
            disruptionType = NetworkDisruption.UNRESPONSIVE;
        } else {
            disruptionType = NetworkDisruption.DISCONNECT;
        }
        NetworkDisruption partition = new NetworkDisruption(partitions, disruptionType);

        setDisruptionScheme(partition);

        return partition;
    }

    TwoPartitions isolateNode(String isolatedNode) {
        Set<String> side1 = new HashSet<>();
        Set<String> side2 = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
        side1.add(isolatedNode);
        side2.remove(isolatedNode);

        return new TwoPartitions(side1, side2);
    }

}
