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

package org.opensearch.cluster.allocation;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.RerouteExplanation;
import org.opensearch.cluster.routing.allocation.RoutingExplanations;
import org.opensearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.opensearch.cluster.routing.allocation.command.AllocationCommand;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider.Allocation;
import org.opensearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

// testDelayWithALargeAmountOfShards does a lot of cluster state updates, and WindowsFS slows it down too much (#52000)
@LuceneTestCase.SuppressFileSystems(value = "WindowsFS")
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ClusterRerouteIT extends OpenSearchIntegTestCase {
    private final Logger logger = LogManager.getLogger(ClusterRerouteIT.class);

    public void testRerouteWithCommands_disableAllocationSettings() throws Exception {
        Settings commonSettings = Settings.builder()
            .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
            .build();
        rerouteWithCommands(commonSettings);
    }

    public void testRerouteWithCommands_enableAllocationSettings() throws Exception {
        Settings commonSettings = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.NONE.name())
            .build();
        rerouteWithCommands(commonSettings);
    }

    private void rerouteWithCommands(Settings commonSettings) throws Exception {
        List<String> nodesIds = internalCluster().startNodes(2, commonSettings);
        final String node_1 = nodesIds.get(0);
        final String node_2 = nodesIds.get(1);

        logger.info("--> create an index with 1 shard, 1 replica, nothing should allocate");
        client().admin()
            .indices()
            .prepareCreate("test")
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .execute()
            .actionGet();

        if (randomBoolean()) {
            client().admin().indices().prepareClose("test").get();
        }

        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(2));

        logger.info("--> explicitly allocate shard 1, *under dry_run*");
        state = client().admin()
            .cluster()
            .prepareReroute()
            .setExplain(randomBoolean())
            .add(new AllocateEmptyPrimaryAllocationCommand("test", 0, node_1, true))
            .setDryRun(true)
            .execute()
            .actionGet()
            .getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(1));
        assertThat(
            state.getRoutingNodes().node(state.nodes().resolveNode(node_1).getId()).iterator().next().state(),
            equalTo(ShardRoutingState.INITIALIZING)
        );

        logger.info("--> get the state, verify nothing changed because of the dry run");
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(2));

        logger.info("--> explicitly allocate shard 1, actually allocating, no dry run");
        state = client().admin()
            .cluster()
            .prepareReroute()
            .setExplain(randomBoolean())
            .add(new AllocateEmptyPrimaryAllocationCommand("test", 0, node_1, true))
            .execute()
            .actionGet()
            .getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(1));
        assertThat(
            state.getRoutingNodes().node(state.nodes().resolveNode(node_1).getId()).iterator().next().state(),
            equalTo(ShardRoutingState.INITIALIZING)
        );

        ClusterHealthResponse healthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForYellowStatus()
            .execute()
            .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("--> get the state, verify shard 1 primary allocated");
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(1));
        assertThat(
            state.getRoutingNodes().node(state.nodes().resolveNode(node_1).getId()).iterator().next().state(),
            equalTo(ShardRoutingState.STARTED)
        );

        logger.info("--> move shard 1 primary from node1 to node2");
        state = client().admin()
            .cluster()
            .prepareReroute()
            .setExplain(randomBoolean())
            .add(new MoveAllocationCommand("test", 0, node_1, node_2))
            .execute()
            .actionGet()
            .getState();

        assertThat(
            state.getRoutingNodes().node(state.nodes().resolveNode(node_1).getId()).iterator().next().state(),
            equalTo(ShardRoutingState.RELOCATING)
        );
        assertThat(
            state.getRoutingNodes().node(state.nodes().resolveNode(node_2).getId()).iterator().next().state(),
            equalTo(ShardRoutingState.INITIALIZING)
        );

        healthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForYellowStatus()
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("--> get the state, verify shard 1 primary moved from node1 to node2");
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(1));
        assertThat(
            state.getRoutingNodes().node(state.nodes().resolveNode(node_2).getId()).iterator().next().state(),
            equalTo(ShardRoutingState.STARTED)
        );
    }

    public void testRerouteWithAllocateLocalGateway_disableAllocationSettings() throws Exception {
        Settings commonSettings = Settings.builder()
            .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
            .build();
        rerouteWithAllocateLocalGateway(commonSettings);
    }

    public void testRerouteWithAllocateLocalGateway_enableAllocationSettings() throws Exception {
        Settings commonSettings = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.NONE.name())
            .build();
        rerouteWithAllocateLocalGateway(commonSettings);
    }

    public void testDelayWithALargeAmountOfShards() throws Exception {
        Settings commonSettings = Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1)
            .build();
        logger.info("--> starting 4 nodes");
        String node_1 = internalCluster().startNode(commonSettings);
        internalCluster().startNode(commonSettings);
        internalCluster().startNode(commonSettings);
        internalCluster().startNode(commonSettings);

        assertThat(cluster().size(), equalTo(4));
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("4").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("--> create indices");
        for (int i = 0; i < 25; i++) {
            final String indexName = "test" + i;
            createIndex(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), randomIntBetween(250, 1000) + "ms")
                    .build()
            );
            if (randomBoolean()) {
                assertAcked(client().admin().indices().prepareClose(indexName));
            }
        }

        ensureGreen(TimeValue.timeValueMinutes(1));

        logger.info("--> stopping node1");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node_1));

        // This might run slowly on older hardware
        // In some case, the shards will be rebalanced back and forth, it seems like a very low probability bug.
        ensureGreen(TimeValue.timeValueMinutes(2), false);
    }

    private void rerouteWithAllocateLocalGateway(Settings commonSettings) throws Exception {
        logger.info("--> starting 2 nodes");
        String node_1 = internalCluster().startNode(commonSettings);
        internalCluster().startNode(commonSettings);
        assertThat(cluster().size(), equalTo(2));
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("--> create an index with 1 shard, 1 replica, nothing should allocate");
        client().admin()
            .indices()
            .prepareCreate("test")
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .execute()
            .actionGet();

        final boolean closed = randomBoolean();
        if (closed) {
            client().admin().indices().prepareClose("test").get();
        }

        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(2));

        logger.info("--> explicitly allocate shard 1, actually allocating, no dry run");
        state = client().admin()
            .cluster()
            .prepareReroute()
            .setExplain(randomBoolean())
            .add(new AllocateEmptyPrimaryAllocationCommand("test", 0, node_1, true))
            .execute()
            .actionGet()
            .getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(1));
        assertThat(
            state.getRoutingNodes().node(state.nodes().resolveNode(node_1).getId()).iterator().next().state(),
            equalTo(ShardRoutingState.INITIALIZING)
        );

        healthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForYellowStatus()
            .execute()
            .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("--> get the state, verify shard 1 primary allocated");
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(1));
        assertThat(
            state.getRoutingNodes().node(state.nodes().resolveNode(node_1).getId()).iterator().next().state(),
            equalTo(ShardRoutingState.STARTED)
        );

        if (closed == false) {
            client().prepareIndex("test").setId("1").setSource("field", "value").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        }
        final Index index = resolveIndex("test");

        logger.info("--> closing all nodes");
        Path[] shardLocation = internalCluster().getInstance(NodeEnvironment.class, node_1).availableShardPaths(new ShardId(index, 0));
        assertThat(FileSystemUtils.exists(shardLocation), equalTo(true)); // make sure the data is there!
        internalCluster().closeNonSharedNodes(false); // don't wipe data directories the index needs to be there!

        logger.info("--> deleting the shard data [{}] ", Arrays.toString(shardLocation));
        assertThat(FileSystemUtils.exists(shardLocation), equalTo(true)); // verify again after cluster was shut down
        IOUtils.rm(shardLocation);

        logger.info("--> starting nodes back, will not allocate the shard since it has no data, but the index will be there");
        node_1 = internalCluster().startNode(commonSettings);
        internalCluster().startNode(commonSettings);
        // wait a bit for the cluster to realize that the shard is not there...
        // TODO can we get around this? the cluster is RED, so what do we wait for?
        client().admin().cluster().prepareReroute().get();
        assertThat(
            client().admin().cluster().prepareHealth().setIndices("test").setWaitForNodes("2").execute().actionGet().getStatus(),
            equalTo(ClusterHealthStatus.RED)
        );
        logger.info("--> explicitly allocate primary");
        state = client().admin()
            .cluster()
            .prepareReroute()
            .setExplain(randomBoolean())
            .add(new AllocateEmptyPrimaryAllocationCommand("test", 0, node_1, true))
            .execute()
            .actionGet()
            .getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(1));
        assertThat(
            state.getRoutingNodes().node(state.nodes().resolveNode(node_1).getId()).iterator().next().state(),
            equalTo(ShardRoutingState.INITIALIZING)
        );

        logger.info("--> get the state, verify shard 1 primary allocated");
        final String nodeToCheck = node_1;
        assertBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
            String nodeId = clusterState.nodes().resolveNode(nodeToCheck).getId();
            assertThat(clusterState.getRoutingNodes().node(nodeId).iterator().next().state(), equalTo(ShardRoutingState.STARTED));
        });
    }

    public void testRerouteExplain() {
        Settings commonSettings = Settings.builder().build();

        logger.info("--> starting a node");
        String node_1 = internalCluster().startNode(commonSettings);

        assertThat(cluster().size(), equalTo(1));
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("1").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("--> create an index with 1 shard");
        createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test"));
        }
        ensureGreen("test");

        logger.info("--> disable allocation");
        Settings newSettings = Settings.builder().put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.NONE.name()).build();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(newSettings).execute().actionGet();

        logger.info("--> starting a second node");
        String node_2 = internalCluster().startNode(commonSettings);
        assertThat(cluster().size(), equalTo(2));
        healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("--> try to move the shard from node1 to node2");
        MoveAllocationCommand cmd = new MoveAllocationCommand("test", 0, node_1, node_2);
        ClusterRerouteResponse resp = client().admin().cluster().prepareReroute().add(cmd).setExplain(true).execute().actionGet();
        RoutingExplanations e = resp.getExplanations();
        assertThat(e.explanations().size(), equalTo(1));
        RerouteExplanation explanation = e.explanations().get(0);
        assertThat(explanation.command().name(), equalTo(cmd.name()));
        assertThat(((MoveAllocationCommand) explanation.command()).shardId(), equalTo(cmd.shardId()));
        assertThat(((MoveAllocationCommand) explanation.command()).fromNode(), equalTo(cmd.fromNode()));
        assertThat(((MoveAllocationCommand) explanation.command()).toNode(), equalTo(cmd.toNode()));
        assertThat(explanation.decisions().type(), equalTo(Decision.Type.YES));
    }

    public void testMessageLogging() throws Exception {
        final Settings settings = Settings.builder()
            .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.NONE.name())
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE.name())
            .build();

        final String nodeName1 = internalCluster().startNode(settings);
        assertThat(cluster().size(), equalTo(1));
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("1").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        final String nodeName2 = internalCluster().startNode(settings);
        assertThat(cluster().size(), equalTo(2));
        healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        final String indexName = "test_index";
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 2)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            )
            .execute()
            .actionGet();

        Logger actionLogger = LogManager.getLogger(TransportClusterRerouteAction.class);

        try (MockLogAppender dryRunMockLog = MockLogAppender.createForLoggers(actionLogger)) {
            dryRunMockLog.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no completed message logged on dry run",
                    TransportClusterRerouteAction.class.getName(),
                    Level.INFO,
                    "allocated an empty primary*"
                )
            );

            AllocationCommand dryRunAllocation = new AllocateEmptyPrimaryAllocationCommand(indexName, 0, nodeName1, true);
            ClusterRerouteResponse dryRunResponse = client().admin()
                .cluster()
                .prepareReroute()
                .setExplain(randomBoolean())
                .setDryRun(true)
                .add(dryRunAllocation)
                .execute()
                .actionGet();

            // during a dry run, messages exist but are not logged or exposed
            assertThat(dryRunResponse.getExplanations().getYesDecisionMessages(), hasSize(1));
            assertThat(dryRunResponse.getExplanations().getYesDecisionMessages().get(0), containsString("allocated an empty primary"));

            dryRunMockLog.assertAllExpectationsMatched();
        }

        try (MockLogAppender allocateMockLog = MockLogAppender.createForLoggers(actionLogger)) {
            allocateMockLog.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "message for first allocate empty primary",
                    TransportClusterRerouteAction.class.getName(),
                    Level.INFO,
                    "allocated an empty primary*" + nodeName1 + "*"
                )
            );
            allocateMockLog.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no message for second allocate empty primary",
                    TransportClusterRerouteAction.class.getName(),
                    Level.INFO,
                    "allocated an empty primary*" + nodeName2 + "*"
                )
            );

            AllocationCommand yesDecisionAllocation = new AllocateEmptyPrimaryAllocationCommand(indexName, 0, nodeName1, true);
            AllocationCommand noDecisionAllocation = new AllocateEmptyPrimaryAllocationCommand("noexist", 1, nodeName2, true);
            ClusterRerouteResponse response = client().admin()
                .cluster()
                .prepareReroute()
                .setExplain(true) // so we get a NO decision back rather than an exception
                .add(yesDecisionAllocation)
                .add(noDecisionAllocation)
                .execute()
                .actionGet();

            assertThat(response.getExplanations().getYesDecisionMessages(), hasSize(1));
            assertThat(response.getExplanations().getYesDecisionMessages().get(0), containsString("allocated an empty primary"));
            assertThat(response.getExplanations().getYesDecisionMessages().get(0), containsString(nodeName1));

            allocateMockLog.assertAllExpectationsMatched();
        }
    }

    public void testClusterRerouteWithBlocks() {
        List<String> nodesIds = internalCluster().startNodes(2);

        logger.info("--> create an index with 1 shard and 0 replicas");
        createIndex(
            "test-blocks",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test-blocks"));
        }
        ensureGreen("test-blocks");

        logger.info("--> check that the index has 1 shard");
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        List<ShardRouting> shards = state.routingTable().allShards("test-blocks");
        assertThat(shards, hasSize(1));

        logger.info("--> check that the shard is allocated");
        ShardRouting shard = shards.get(0);
        assertThat(shard.assignedToNode(), equalTo(true));

        logger.info("--> retrieve the node where the shard is allocated");
        DiscoveryNode node = state.nodes().resolveNode(shard.currentNodeId());
        assertNotNull(node);

        // toggle is used to mve the shard from one node to another
        int toggle = nodesIds.indexOf(node.getName());

        // Rerouting shards is not blocked
        for (String blockSetting : Arrays.asList(
            SETTING_BLOCKS_READ,
            SETTING_BLOCKS_WRITE,
            SETTING_READ_ONLY,
            SETTING_BLOCKS_METADATA,
            SETTING_READ_ONLY_ALLOW_DELETE
        )) {
            try {
                enableIndexBlock("test-blocks", blockSetting);
                assertAcked(
                    client().admin()
                        .cluster()
                        .prepareReroute()
                        .add(new MoveAllocationCommand("test-blocks", 0, nodesIds.get(toggle % 2), nodesIds.get(++toggle % 2)))
                );

                ClusterHealthResponse healthResponse = client().admin()
                    .cluster()
                    .prepareHealth()
                    .setIndices("test-blocks")
                    .setWaitForYellowStatus()
                    .setWaitForNoRelocatingShards(true)
                    .execute()
                    .actionGet();
                assertThat(healthResponse.isTimedOut(), equalTo(false));
            } finally {
                disableIndexBlock("test-blocks", blockSetting);
            }
        }

        // Rerouting shards is blocked when the cluster is read only
        try {
            setClusterReadOnly(true);
            assertBlocked(
                client().admin()
                    .cluster()
                    .prepareReroute()
                    .add(new MoveAllocationCommand("test-blocks", 1, nodesIds.get(toggle % 2), nodesIds.get(++toggle % 2)))
            );
        } finally {
            setClusterReadOnly(false);
        }
    }
}
