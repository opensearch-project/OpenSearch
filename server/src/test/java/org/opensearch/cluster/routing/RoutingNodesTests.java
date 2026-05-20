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

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.junit.Before;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RoutingNodesTests extends OpenSearchAllocationTestCase {
    private static final String TEST_INDEX_1 = "test1";
    private static final String TEST_INDEX_2 = "test2";
    private RoutingTable emptyRoutingTable;
    private int numberOfShards;
    private int numberOfReplicas;
    private int shardsPerIndex;
    private int totalNumberOfShards;
    private static final Settings DEFAULT_SETTINGS = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
    private final AllocationService ALLOCATION_SERVICE = createAllocationService(
        Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", Integer.MAX_VALUE) // don't limit recoveries
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", Integer.MAX_VALUE)
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .build()
    );
    private ClusterState clusterState;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.numberOfShards = 5;
        this.numberOfReplicas = 2;
        this.shardsPerIndex = this.numberOfShards * (this.numberOfReplicas + 1);
        this.totalNumberOfShards = this.shardsPerIndex * 2;
        logger.info("Setup test with {} shards and {} replicas.", this.numberOfShards, this.numberOfReplicas);
        this.emptyRoutingTable = new RoutingTable.Builder().build();
        Metadata metadata = Metadata.builder().put(createIndexMetadata(TEST_INDEX_1)).put(createIndexMetadata(TEST_INDEX_2)).build();

        RoutingTable testRoutingTable = new RoutingTable.Builder().add(
            new IndexRoutingTable.Builder(metadata.index(TEST_INDEX_1).getIndex()).initializeAsNew(metadata.index(TEST_INDEX_1)).build()
        )
            .add(
                new IndexRoutingTable.Builder(metadata.index(TEST_INDEX_2).getIndex()).initializeAsNew(metadata.index(TEST_INDEX_2)).build()
            )
            .build();
        this.clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(testRoutingTable)
            .build();
    }

    /**
     * Puts primary shard index routings into initializing state
     */
    private void initPrimaries() {
        logger.info("adding {} nodes and performing rerouting", this.numberOfReplicas + 1);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < this.numberOfReplicas + 1; i++) {
            discoBuilder = discoBuilder.add(newNode("node" + i));
        }
        this.clusterState = ClusterState.builder(clusterState).nodes(discoBuilder).build();
        ClusterState rerouteResult = ALLOCATION_SERVICE.reroute(clusterState, "reroute");
        assertThat(rerouteResult, not(equalTo(this.clusterState)));
        this.clusterState = rerouteResult;
    }

    /**
     * Moves initializing shards into started state
     */
    private void startInitializingShards(String index) {
        clusterState = startInitializingShardsAndReroute(ALLOCATION_SERVICE, clusterState, index);
    }

    private IndexMetadata.Builder createIndexMetadata(String indexName) {
        return new IndexMetadata.Builder(indexName).settings(DEFAULT_SETTINGS)
            .numberOfReplicas(this.numberOfReplicas)
            .numberOfShards(this.numberOfShards);
    }

    public void testInterleavedShardIteratorPrimaryFirst() {
        // Initialize all the shards for test index 1 and 2
        initPrimaries();
        startInitializingShards(TEST_INDEX_1);
        startInitializingShards(TEST_INDEX_1);
        startInitializingShards(TEST_INDEX_2);
        startInitializingShards(TEST_INDEX_2);

        // Create primary shard count imbalance between two nodes
        final RoutingNode node0 = this.clusterState.getRoutingNodes().node("node0");
        final RoutingNode node1 = this.clusterState.getRoutingNodes().node("node1");
        final List<ShardRouting> shardRoutingList = node0.shardsWithState(TEST_INDEX_1, ShardRoutingState.STARTED);
        for (ShardRouting routing : shardRoutingList) {
            if (routing.primary()) {
                node0.remove(routing);
                ShardRouting swap = node1.getByShardId(routing.shardId());
                node0.add(swap);
                node1.remove(swap);
                node1.add(routing);
            }
        }

        // Get primary first shard iterator and assert primary shards are iterated over first
        final Iterator<ShardRouting> iterator = this.clusterState.getRoutingNodes()
            .nodeInterleavedShardIterator(ShardMovementStrategy.PRIMARY_FIRST);
        boolean iteratingPrimary = true;
        int shardCount = 0;
        while (iterator.hasNext()) {
            final ShardRouting shard = iterator.next();
            if (iteratingPrimary) {
                iteratingPrimary = shard.primary();
            } else {
                assertFalse(shard.primary());
            }
            shardCount++;
        }
        assertEquals(shardCount, this.totalNumberOfShards);
    }

    public void testInterleavedShardIteratorNoPreference() {
        // Initialize all the shards for test index 1 and 2
        initPrimaries();
        startInitializingShards(TEST_INDEX_1);
        startInitializingShards(TEST_INDEX_1);
        startInitializingShards(TEST_INDEX_2);
        startInitializingShards(TEST_INDEX_2);

        final Iterator<ShardRouting> iterator = this.clusterState.getRoutingNodes()
            .nodeInterleavedShardIterator(ShardMovementStrategy.NO_PREFERENCE);
        int shardCount = 0;
        while (iterator.hasNext()) {
            final ShardRouting shard = iterator.next();
            shardCount++;
        }
        assertEquals(shardCount, this.totalNumberOfShards);
    }

    public void testInterleavedShardIteratorReplicaFirst() {
        // Initialize all the shards for test index 1 and 2
        initPrimaries();
        startInitializingShards(TEST_INDEX_1);
        startInitializingShards(TEST_INDEX_1);
        startInitializingShards(TEST_INDEX_2);
        startInitializingShards(TEST_INDEX_2);

        // Get replica first shard iterator and assert replica shards are iterated over first
        final Iterator<ShardRouting> iterator = this.clusterState.getRoutingNodes()
            .nodeInterleavedShardIterator(ShardMovementStrategy.REPLICA_FIRST);
        boolean iteratingReplica = true;
        int shardCount = 0;
        while (iterator.hasNext()) {
            final ShardRouting shard = iterator.next();
            if (iteratingReplica) {
                iteratingReplica = shard.primary() == false;
            } else {
                assertTrue(shard.primary());
            }
            shardCount++;
        }
        assertEquals(shardCount, this.totalNumberOfShards);
    }
}
