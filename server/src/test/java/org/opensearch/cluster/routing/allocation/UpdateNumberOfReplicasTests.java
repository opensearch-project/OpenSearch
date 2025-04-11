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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;

import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class UpdateNumberOfReplicasTests extends OpenSearchAllocationTestCase {
    private final Logger logger = LogManager.getLogger(UpdateNumberOfReplicasTests.class);

    public void testUpdateNumberOfReplicas() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        assertThat(initialRoutingTable.index("test").shards().size(), equalTo(1));
        assertThat(initialRoutingTable.index("test").shard(0).size(), equalTo(2));
        assertThat(initialRoutingTable.index("test").shard(0).shards().size(), equalTo(2));
        assertThat(initialRoutingTable.index("test").shard(0).shards().get(0).state(), equalTo(UNASSIGNED));
        assertThat(initialRoutingTable.index("test").shard(0).shards().get(1).state(), equalTo(UNASSIGNED));
        assertThat(initialRoutingTable.index("test").shard(0).shards().get(0).currentNodeId(), nullValue());
        assertThat(initialRoutingTable.index("test").shard(0).shards().get(1).currentNodeId(), nullValue());

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Start all the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("Start all the replica shards");
        ClusterState newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        final String nodeHoldingPrimary = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        final String nodeHoldingReplica = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();
        assertThat(nodeHoldingPrimary, not(equalTo(nodeHoldingReplica)));
        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(), equalTo(nodeHoldingReplica));

        logger.info("add another replica");
        final String[] indices = { "test" };
        RoutingTable updatedRoutingTable = RoutingTable.builder(clusterState.routingTable()).updateNumberOfReplicas(2, indices).build();
        metadata = Metadata.builder(clusterState.metadata()).updateNumberOfReplicas(2, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(updatedRoutingTable).metadata(metadata).build();

        assertThat(clusterState.metadata().index("test").getNumberOfReplicas(), equalTo(2));

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(3));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(), equalTo(nodeHoldingReplica));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(1).state(), equalTo(UNASSIGNED));

        logger.info("Add another node and start the added replica");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(3));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).size(), equalTo(1));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).get(0).currentNodeId(),
            equalTo(nodeHoldingReplica)
        );
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(INITIALIZING).get(0).currentNodeId(),
            equalTo("node3")
        );

        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(3));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).size(), equalTo(2));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).get(0).currentNodeId(),
            anyOf(equalTo(nodeHoldingReplica), equalTo("node3"))
        );
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).get(1).currentNodeId(),
            anyOf(equalTo(nodeHoldingReplica), equalTo("node3"))
        );

        logger.info("now remove a replica");
        updatedRoutingTable = RoutingTable.builder(clusterState.routingTable()).updateNumberOfReplicas(1, indices).build();
        metadata = Metadata.builder(clusterState.metadata()).updateNumberOfReplicas(1, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(updatedRoutingTable).metadata(metadata).build();

        assertThat(clusterState.metadata().index("test").getNumberOfReplicas(), equalTo(1));

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(STARTED));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(),
            anyOf(equalTo(nodeHoldingReplica), equalTo("node3"))
        );

        logger.info("do a reroute, should remain the same");
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));
    }

    public void testUpdateNumberOfReplicasDoesNotImpactSearchReplicas() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .numberOfSearchReplicas(1)
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        assertEquals(1, routingTable.index("test").shards().size());
        IndexShardRoutingTable shardRoutingTable = routingTable.index("test").shard(0);
        // 1 primary, 1 replica, 1 search replica
        assertEquals(3, shardRoutingTable.size());
        assertEquals(2, shardRoutingTable.replicaShards().size());
        assertEquals(1, shardRoutingTable.searchOnlyReplicas().size());
        assertEquals(UNASSIGNED, shardRoutingTable.shards().get(0).state());
        assertEquals(UNASSIGNED, shardRoutingTable.shards().get(1).state());
        assertEquals(UNASSIGNED, shardRoutingTable.shards().get(2).state());
        assertNull(shardRoutingTable.shards().get(0).currentNodeId());
        assertNull(shardRoutingTable.shards().get(1).currentNodeId());
        assertNull(shardRoutingTable.shards().get(2).currentNodeId());

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newSearchNode("node3")))
            .build();

        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Start all the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("Start all the replica and search shards");
        ClusterState newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertNotEquals(newState, clusterState);
        clusterState = newState;

        shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        final String nodeHoldingPrimary = shardRoutingTable.primaryShard().currentNodeId();
        final String nodeHoldingSearchReplica = shardRoutingTable.searchOnlyReplicas().get(0).currentNodeId();
        final String nodeHoldingReplica = shardRoutingTable.writerReplicas().get(0).currentNodeId();

        assertNotEquals(nodeHoldingPrimary, nodeHoldingReplica);
        assertNotEquals(nodeHoldingPrimary, nodeHoldingSearchReplica);
        assertNotEquals(nodeHoldingReplica, nodeHoldingSearchReplica);

        assertEquals(
            "There is a single routing shard routing table in the cluster",
            clusterState.routingTable().index("test").shards().size(),
            1
        );
        assertEquals("There are three shards as part of the shard routing table", 3, shardRoutingTable.size());
        assertEquals("There are two replicas one search and one write", 2, shardRoutingTable.replicaShards().size());
        assertEquals(1, shardRoutingTable.searchOnlyReplicas().size());
        assertEquals(STARTED, shardRoutingTable.shards().get(0).state());
        assertEquals(STARTED, shardRoutingTable.shards().get(1).state());
        assertEquals(STARTED, shardRoutingTable.shards().get(2).state());

        logger.info("add another replica");
        final String[] indices = { "test" };
        routingTable = RoutingTable.builder(clusterState.routingTable()).updateNumberOfReplicas(2, indices).build();
        metadata = Metadata.builder(clusterState.metadata()).updateNumberOfReplicas(2, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metadata(metadata).build();
        IndexMetadata indexMetadata = clusterState.metadata().index("test");
        assertEquals(2, indexMetadata.getNumberOfReplicas());
        assertEquals(1, indexMetadata.getNumberOfSearchOnlyReplicas());
        shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        assertEquals(4, shardRoutingTable.size());
        assertEquals(3, shardRoutingTable.replicaShards().size());
        assertEquals(2, shardRoutingTable.writerReplicas().size());
        assertEquals(1, shardRoutingTable.searchOnlyReplicas().size());
        assertEquals(shardRoutingTable.primaryShard().state(), STARTED);
        assertEquals(shardRoutingTable.searchOnlyReplicas().get(0).state(), STARTED);

        ShardRouting existingReplica = shardRoutingTable.writerReplicas().get(0);
        assertEquals(existingReplica.state(), STARTED);
        assertEquals(existingReplica.currentNodeId(), nodeHoldingReplica);
        ShardRouting newReplica = shardRoutingTable.writerReplicas().get(0);
        assertEquals(newReplica.state(), STARTED);

        logger.info("Add another node and start the added replica");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        newState = strategy.reroute(clusterState, "reroute");
        newState = startInitializingShardsAndReroute(strategy, newState);
        assertNotEquals(newState, clusterState);
        clusterState = newState;
        shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        for (ShardRouting replicaShard : shardRoutingTable.replicaShards()) {
            assertEquals(replicaShard.state(), STARTED);
        }
        assertTrue(shardRoutingTable.replicaShards().stream().allMatch(r -> r.state().equals(STARTED)));

        // remove both replicas and assert search replica is unchanged
        routingTable = RoutingTable.builder(clusterState.routingTable()).updateNumberOfReplicas(0, indices).build();
        metadata = Metadata.builder(clusterState.metadata()).updateNumberOfReplicas(0, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metadata(metadata).build();
        indexMetadata = clusterState.metadata().index("test");
        assertEquals(0, indexMetadata.getNumberOfReplicas());
        assertEquals(1, indexMetadata.getNumberOfSearchOnlyReplicas());
        shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        assertEquals(2, shardRoutingTable.size());
        assertEquals(1, shardRoutingTable.replicaShards().size());
        assertEquals(0, shardRoutingTable.writerReplicas().size());
        assertEquals(1, shardRoutingTable.searchOnlyReplicas().size());
        assertEquals(shardRoutingTable.primaryShard().state(), STARTED);
        assertEquals(shardRoutingTable.searchOnlyReplicas().get(0).state(), STARTED);
        assertEquals(shardRoutingTable.searchOnlyReplicas().get(0).currentNodeId(), nodeHoldingSearchReplica);
    }

    public void testUpdateSearchReplicasDoesNotImpactRegularReplicas() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .numberOfSearchReplicas(1)
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        assertEquals(1, routingTable.index("test").shards().size());
        IndexShardRoutingTable shardRoutingTable = routingTable.index("test").shard(0);
        // 1 primary, 1 replica, 1 search replica
        assertEquals(3, shardRoutingTable.size());
        assertEquals(2, shardRoutingTable.replicaShards().size());
        assertEquals(1, shardRoutingTable.searchOnlyReplicas().size());
        assertEquals(UNASSIGNED, shardRoutingTable.shards().get(0).state());
        assertEquals(UNASSIGNED, shardRoutingTable.shards().get(1).state());
        assertEquals(UNASSIGNED, shardRoutingTable.shards().get(2).state());
        assertNull(shardRoutingTable.shards().get(0).currentNodeId());
        assertNull(shardRoutingTable.shards().get(1).currentNodeId());
        assertNull(shardRoutingTable.shards().get(2).currentNodeId());

        logger.info("Adding three nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newSearchNode("node3")))
            .build();

        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Start all the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("Start all the replica and search shards");
        ClusterState newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertNotEquals(newState, clusterState);
        clusterState = newState;

        shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        final String nodeHoldingPrimary = shardRoutingTable.primaryShard().currentNodeId();
        final String nodeHoldingSearchReplica = shardRoutingTable.searchOnlyReplicas().get(0).currentNodeId();
        final String nodeHoldingReplica = shardRoutingTable.writerReplicas().get(0).currentNodeId();

        assertNotEquals(nodeHoldingPrimary, nodeHoldingReplica);
        assertNotEquals(nodeHoldingPrimary, nodeHoldingSearchReplica);
        assertNotEquals(nodeHoldingReplica, nodeHoldingSearchReplica);

        assertEquals(
            "There is a single routing shard routing table in the cluster",
            clusterState.routingTable().index("test").shards().size(),
            1
        );
        assertEquals("There are three shards as part of the shard routing table", 3, shardRoutingTable.size());
        assertEquals("There are two replicas one search and one write", 2, shardRoutingTable.replicaShards().size());
        assertEquals(1, shardRoutingTable.searchOnlyReplicas().size());
        assertEquals(STARTED, shardRoutingTable.shards().get(0).state());
        assertEquals(STARTED, shardRoutingTable.shards().get(1).state());
        assertEquals(STARTED, shardRoutingTable.shards().get(2).state());

        logger.info("add another replica");
        final String[] indices = { "test" };
        routingTable = RoutingTable.builder(clusterState.routingTable()).updateNumberOfSearchReplicas(2, indices).build();
        metadata = Metadata.builder(clusterState.metadata()).updateNumberOfSearchReplicas(2, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metadata(metadata).build();
        IndexMetadata indexMetadata = clusterState.metadata().index("test");
        assertEquals(1, indexMetadata.getNumberOfReplicas());
        assertEquals(2, indexMetadata.getNumberOfSearchOnlyReplicas());
        shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        assertEquals(4, shardRoutingTable.size());
        assertEquals(3, shardRoutingTable.replicaShards().size());
        assertEquals(1, shardRoutingTable.writerReplicas().size());
        assertEquals(2, shardRoutingTable.searchOnlyReplicas().size());
        assertEquals(shardRoutingTable.primaryShard().state(), STARTED);
        assertEquals(shardRoutingTable.writerReplicas().get(0).state(), STARTED);
        assertEquals(shardRoutingTable.searchOnlyReplicas().get(0).state(), STARTED);
        assertEquals(shardRoutingTable.searchOnlyReplicas().get(1).state(), UNASSIGNED);

        logger.info("Add another node and start the added replica");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newSearchNode("node4")))
            .build();
        newState = strategy.reroute(clusterState, "reroute");
        newState = startInitializingShardsAndReroute(strategy, newState);
        assertNotEquals(newState, clusterState);
        clusterState = newState;
        shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        for (ShardRouting replicaShard : shardRoutingTable.replicaShards()) {
            assertEquals(replicaShard.state(), STARTED);
        }
        assertTrue(shardRoutingTable.replicaShards().stream().allMatch(r -> r.state().equals(STARTED)));

        // remove both replicas and assert search replica is unchanged
        routingTable = RoutingTable.builder(clusterState.routingTable()).updateNumberOfSearchReplicas(0, indices).build();
        metadata = Metadata.builder(clusterState.metadata()).updateNumberOfSearchReplicas(0, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metadata(metadata).build();
        indexMetadata = clusterState.metadata().index("test");
        assertEquals(1, indexMetadata.getNumberOfReplicas());
        assertEquals(0, indexMetadata.getNumberOfSearchOnlyReplicas());
        shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        assertEquals(2, shardRoutingTable.size());
        assertEquals(1, shardRoutingTable.replicaShards().size());
        assertEquals(1, shardRoutingTable.writerReplicas().size());
        assertEquals(0, shardRoutingTable.searchOnlyReplicas().size());
        assertEquals(shardRoutingTable.primaryShard().state(), STARTED);
        assertEquals(shardRoutingTable.replicaShards().get(0).state(), STARTED);
        assertEquals(shardRoutingTable.replicaShards().get(0).currentNodeId(), nodeHoldingReplica);
    }
}
