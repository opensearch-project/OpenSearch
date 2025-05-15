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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexMetadata.State;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, minNumDataNodes = 2)
public class AwarenessAllocationIT extends OpenSearchIntegTestCase {

    private final Logger logger = LogManager.getLogger(AwarenessAllocationIT.class);

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    public void testSimpleAwareness() throws Exception {
        Settings commonSettings = Settings.builder().put("cluster.routing.allocation.awareness.attributes", "rack_id").build();

        logger.info("--> starting 2 nodes on the same rack");
        internalCluster().startNodes(2, Settings.builder().put(commonSettings).put("node.attr.rack_id", "rack_1").build());

        Settings settings = Settings.builder()
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), false)
            .build();
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(settings);

        createIndex("test1");
        createIndex("test2");

        NumShards test1 = getNumShards("test1");
        NumShards test2 = getNumShards("test2");
        // no replicas will be allocated as both indices end up on a single node
        final int totalPrimaries = test1.numPrimaries + test2.numPrimaries;

        ensureGreen();

        final List<String> indicesToClose = randomSubsetOf(Arrays.asList("test1", "test2"));
        indicesToClose.forEach(indexToClose -> assertAcked(client().admin().indices().prepareClose(indexToClose).get()));

        logger.info("--> starting 1 node on a different rack");
        final String node3 = internalCluster().startNode(Settings.builder().put(commonSettings).put("node.attr.rack_id", "rack_2").build());

        // On slow machines the initial relocation might be delayed
        assertBusy(() -> {
            logger.info("--> waiting for no relocation");
            ClusterHealthResponse clusterHealth = client().admin()
                .cluster()
                .prepareHealth()
                .setIndices("test1", "test2")
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForGreenStatus()
                .setWaitForNodes("3")
                .setWaitForNoRelocatingShards(true)
                .get();

            assertThat("Cluster health request timed out", clusterHealth.isTimedOut(), equalTo(false));

            logger.info("--> checking current state");
            ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

            // check that closed indices are effectively closed
            final List<String> notClosedIndices = indicesToClose.stream()
                .filter(index -> clusterState.metadata().index(index).getState() != State.CLOSE)
                .collect(Collectors.toList());
            assertThat("Some indices not closed", notClosedIndices, empty());

            // verify that we have all the primaries on node3
            final Map<String, Integer> counts = new HashMap<>();
            for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
                for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                    for (ShardRouting shardRouting : indexShardRoutingTable) {
                        counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                    }
                }
            }
            assertThat(counts.get(node3), equalTo(totalPrimaries));
        }, 10, TimeUnit.SECONDS);
    }

    public void testAwarenessZones() {
        Settings commonSettings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a,b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        logger.info("--> starting 4 nodes on different zones");
        List<String> nodes = internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build()
        );
        String A_0 = nodes.get(0);
        String B_0 = nodes.get(1);
        String B_1 = nodes.get(2);
        String A_1 = nodes.get(3);

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("4").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );

        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test"));
        }

        logger.info("--> waiting for shards to be allocated");
        health = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        final Map<String, Integer> counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }
        assertThat(counts.get(A_1), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(B_1), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(A_0), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(B_0), anyOf(equalTo(2), equalTo(3)));
    }

    public void testAwarenessZonesIncrementalNodes() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b")
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .build();

        logger.info("--> starting 2 nodes on zones 'a' & 'b'");
        List<String> nodes = internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build()
        );
        String A_0 = nodes.get(0);
        String B_0 = nodes.get(1);

        createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );

        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test"));
        }

        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("2")
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        Map<String, Integer> counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }
        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(5));
        logger.info("--> starting another node in zone 'b'");

        String B_1 = internalCluster().startNode(Settings.builder().put(commonSettings).put("node.attr.zone", "b").build());
        health = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("3")
            .execute()
            .actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        client().admin().cluster().prepareReroute().get();
        health = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("3")
            .setWaitForActiveShards(10)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }
        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));

        String noZoneNode = internalCluster().startNode();
        health = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("4")
            .execute()
            .actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        client().admin().cluster().prepareReroute().get();
        health = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("4")
            .setWaitForActiveShards(10)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }

        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));
        assertThat(counts.containsKey(noZoneNode), equalTo(false));
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.routing.allocation.awareness.attributes", "").build())
            .get();

        health = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("4")
            .setWaitForActiveShards(10)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }

        assertThat(counts.get(A_0), equalTo(3));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));
        assertThat(counts.get(noZoneNode), equalTo(2));
    }

    public void testThreeZoneOneReplicaWithForceZoneValueAndLoadAwareness() throws Exception {
        int nodeCountPerAZ = 5;
        int numOfShards = 30;
        int numOfReplica = 1;
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.allocation.load_awareness.skew_factor", "0.0")
            .put("cluster.routing.allocation.load_awareness.provisioned_capacity", Integer.toString(nodeCountPerAZ * 3))
            .put("cluster.routing.allocation.allow_rebalance", "indices_primaries_active")
            .build();

        logger.info("--> starting a dedicated cluster manager node");
        internalCluster().startClusterManagerOnlyNode();

        logger.info("--> starting 15 nodes on zones 'a' & 'b' & 'c'");
        List<String> nodes_in_zone_a = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build()
        );
        List<String> nodes_in_zone_b = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build()
        );
        List<String> nodes_in_zone_c = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        // Creating index with 30 primary and 1 replica
        createIndex(
            "test-1",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numOfReplica)
                .build()
        );

        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test-1")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(Integer.toString(nodeCountPerAZ * 3 + 1))
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        final Map<String, Integer> counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }

        assertThat(counts.size(), equalTo(nodeCountPerAZ * 3));
        // All shards should be started
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(numOfShards * (numOfReplica + 1)));

        // stopping half nodes in zone a
        int nodesToStop = nodeCountPerAZ / 2;
        List<Settings> nodeDataPathSettings = new ArrayList<>();
        for (int i = 0; i < nodesToStop; i++) {
            nodeDataPathSettings.add(internalCluster().dataPathSettings(nodes_in_zone_a.get(i)));
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodes_in_zone_a.get(i)));
        }

        client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        health = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test-1")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes(Integer.toString(nodeCountPerAZ * 3 - nodesToStop + 1))
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

        // Creating another index with 30 primary and 1 replica
        createIndex(
            "test-2",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numOfReplica)
                .build()
        );

        health = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test-1", "test-2")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes(Integer.toString(nodeCountPerAZ * 3 - nodesToStop + 1))
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

        // Restarting the nodes back
        for (int i = 0; i < nodesToStop; i++) {
            internalCluster().startNode(
                Settings.builder()
                    .put("node.name", nodes_in_zone_a.get(i))
                    .put(nodeDataPathSettings.get(i))
                    .put(commonSettings)
                    .put("node.attr.zone", "a")
                    .build()
            );
        }
        client().admin().cluster().prepareReroute().setRetryFailed(true).get();

        health = client().admin()
            .cluster()
            .prepareHealth()
            .setIndices("test-1", "test-2")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes(Integer.toString(nodeCountPerAZ * 3 + 1))
            .setWaitForGreenStatus()
            .setWaitForActiveShards(2 * numOfShards * (numOfReplica + 1))
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .execute()
            .actionGet();
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        // All shards should be started now and cluster health should be green
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2 * numOfShards * (numOfReplica + 1)));
        assertThat(health.isTimedOut(), equalTo(false));
    }

    public void testAwarenessBalanceWithForcedAwarenessCreateAndUpdateIndex() {
        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.balance", "true")
            .build();

        logger.info("--> starting 3 nodes on zones a,b,c");
        internalCluster().startNodes(
            Settings.builder().put(settings).put("node.attr.zone", "a").build(),
            Settings.builder().put(settings).put("node.attr.zone", "b").build(),
            Settings.builder().put(settings).put("node.attr.zone", "c").build()
        );

        // Create index with 2 replicas ie total 3 shards
        createIndex(
            "test-idx",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build()
        );

        // Update the number of replicas to 4
        final Settings newsettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 4).build();

        assertThrows(IllegalArgumentException.class, () -> {
            assertAcked(client().admin().indices().prepareUpdateSettings("test-idx").setSettings(newsettings));
        });
    }
}
