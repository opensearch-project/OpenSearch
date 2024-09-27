/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class RoutingTableDiffTests extends OpenSearchAllocationTestCase {

    private static final String TEST_INDEX_1 = "test1";
    private static final String TEST_INDEX_2 = "test2";
    private static final String TEST_INDEX_3 = "test3";
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
        this.numberOfShards = randomIntBetween(1, 5);
        this.numberOfReplicas = randomIntBetween(1, 5);
        this.shardsPerIndex = this.numberOfShards * (this.numberOfReplicas + 1);
        this.totalNumberOfShards = this.shardsPerIndex * 2;
        logger.info("Setup test with {} shards and {} replicas.", this.numberOfShards, this.numberOfReplicas);
        RoutingTable emptyRoutingTable = new RoutingTable.Builder().build();
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
     * puts primary shard indexRoutings into initializing state
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

    private void startInitializingShards(String index) {
        logger.info("start primary shards for index {}", index);
        clusterState = startInitializingShardsAndReroute(ALLOCATION_SERVICE, clusterState, index);
    }

    private IndexMetadata.Builder createIndexMetadata(String indexName) {
        return new IndexMetadata.Builder(indexName).settings(DEFAULT_SETTINGS)
            .numberOfReplicas(this.numberOfReplicas)
            .numberOfShards(this.numberOfShards);
    }

    public void testRoutingTableUpserts() {
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards));
        initPrimaries();
        int expectedUnassignedShardCount = this.totalNumberOfShards - 2 * this.numberOfShards;
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(expectedUnassignedShardCount));
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(2 * this.numberOfShards));
        Metadata metadata = Metadata.builder().put(createIndexMetadata(TEST_INDEX_1)).put(createIndexMetadata(TEST_INDEX_2)).build();
        ClusterState oldClusterState = clusterState;

        // create index routing table for TEST_INDEX_3
        metadata = Metadata.builder(metadata).put(createIndexMetadata(TEST_INDEX_3)).build();
        RoutingTable testRoutingTable = new RoutingTable.Builder(clusterState.routingTable()).add(
            new IndexRoutingTable.Builder(metadata.index(TEST_INDEX_3).getIndex()).initializeAsNew(metadata.index(TEST_INDEX_3)).build()
        ).build();
        this.clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(testRoutingTable)
            .build();
        this.totalNumberOfShards = this.shardsPerIndex * 3;
        assertThat(
            clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(expectedUnassignedShardCount + this.shardsPerIndex)
        );
        Diff<RoutingTable> fullDiff = clusterState.routingTable().diff(oldClusterState.getRoutingTable());
        Diff<RoutingTable> incrementalDiff = clusterState.routingTable().incrementalDiff(oldClusterState.getRoutingTable());
        RoutingTable newRoutingTable = incrementalDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTable.version());
            assertEquals(indexRoutingTable, newRoutingTable.index(indexRoutingTable.getIndex()));
        }
        RoutingTable newRoutingTableWithFullDiff = fullDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTableWithFullDiff.version());
            assertEquals(indexRoutingTable, newRoutingTableWithFullDiff.index(indexRoutingTable.getIndex()));
        }
    }

    public void testRoutingTableDeletes() {
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards));
        initPrimaries();
        int expectedUnassignedShardCount = this.totalNumberOfShards - 2 * this.numberOfShards;
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(expectedUnassignedShardCount));
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(2 * this.numberOfShards));
        Metadata metadata = Metadata.builder().put(createIndexMetadata(TEST_INDEX_1)).put(createIndexMetadata(TEST_INDEX_2)).build();
        ClusterState oldClusterState = clusterState;

        // delete index routing table for TEST_INDEX_1
        metadata = Metadata.builder(metadata).put(createIndexMetadata(TEST_INDEX_3)).build();
        RoutingTable testRoutingTable = new RoutingTable.Builder(clusterState.routingTable()).remove(TEST_INDEX_1).build();
        this.clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(testRoutingTable)
            .build();
        this.totalNumberOfShards = this.shardsPerIndex;
        assertThat(
            clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(expectedUnassignedShardCount - this.numberOfShards * this.numberOfReplicas)
        );
        Diff<RoutingTable> fullDiff = clusterState.routingTable().diff(oldClusterState.getRoutingTable());
        Diff<RoutingTable> incrementalDiff = clusterState.routingTable().incrementalDiff(oldClusterState.getRoutingTable());
        RoutingTable newRoutingTable = incrementalDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTable.version());
            assertEquals(indexRoutingTable, newRoutingTable.index(indexRoutingTable.getIndex()));
        }
        RoutingTable newRoutingTableWithFullDiff = fullDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTableWithFullDiff.version());
            assertEquals(indexRoutingTable, newRoutingTableWithFullDiff.index(indexRoutingTable.getIndex()));
        }
    }

    public void testRoutingTableUpsertsWithDiff() {
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards));
        initPrimaries();
        int expectedUnassignedShardCount = this.totalNumberOfShards - 2 * this.numberOfShards;
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(expectedUnassignedShardCount));
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(2 * this.numberOfShards));
        Metadata metadata = Metadata.builder().put(createIndexMetadata(TEST_INDEX_1)).put(createIndexMetadata(TEST_INDEX_2)).build();
        ClusterState oldClusterState = clusterState;

        // create index routing table for TEST_INDEX_3
        metadata = Metadata.builder(metadata).put(createIndexMetadata(TEST_INDEX_3)).build();
        RoutingTable testRoutingTable = new RoutingTable.Builder(clusterState.routingTable()).add(
            new IndexRoutingTable.Builder(metadata.index(TEST_INDEX_3).getIndex()).initializeAsNew(metadata.index(TEST_INDEX_3)).build()
        ).build();
        this.clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(testRoutingTable)
            .build();
        this.totalNumberOfShards = this.shardsPerIndex * 3;
        assertThat(
            clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(expectedUnassignedShardCount + this.shardsPerIndex)
        );
        initPrimaries();
        clusterState = startRandomInitializingShard(clusterState, ALLOCATION_SERVICE, TEST_INDEX_2);
        // assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(2 * this.numberOfShards + 1));
        Diff<RoutingTable> fullDiff = clusterState.routingTable().diff(oldClusterState.getRoutingTable());
        Diff<RoutingTable> incrementalDiff = clusterState.routingTable().incrementalDiff(oldClusterState.getRoutingTable());
        RoutingTable newRoutingTable = incrementalDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTable.version());
            assertEquals(indexRoutingTable, newRoutingTable.index(indexRoutingTable.getIndex()));
        }
        RoutingTable newRoutingTableWithFullDiff = fullDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTableWithFullDiff.version());
            assertEquals(indexRoutingTable, newRoutingTableWithFullDiff.index(indexRoutingTable.getIndex()));
        }
    }

    public void testRoutingTableDiffWithReplicaAdded() {
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards));
        initPrimaries();
        int expectedUnassignedShardCount = this.totalNumberOfShards - 2 * this.numberOfShards;
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(expectedUnassignedShardCount));
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(2 * this.numberOfShards));
        ClusterState oldClusterState = clusterState;

        // update replica count for TEST_INDEX_1
        RoutingTable updatedRoutingTable = RoutingTable.builder(clusterState.routingTable())
            .updateNumberOfReplicas(this.numberOfReplicas + 1, new String[] { TEST_INDEX_1 })
            .build();
        Metadata metadata = Metadata.builder(clusterState.metadata())
            .updateNumberOfReplicas(this.numberOfReplicas + 1, new String[] { TEST_INDEX_1 })
            .build();
        clusterState = ClusterState.builder(clusterState).routingTable(updatedRoutingTable).metadata(metadata).build();
        assertThat(
            clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(expectedUnassignedShardCount + this.numberOfShards)
        );
        Diff<RoutingTable> fullDiff = clusterState.routingTable().diff(oldClusterState.getRoutingTable());
        Diff<RoutingTable> incrementalDiff = clusterState.routingTable().incrementalDiff(oldClusterState.getRoutingTable());
        RoutingTable newRoutingTable = incrementalDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTable.version());
            assertEquals(indexRoutingTable, newRoutingTable.index(indexRoutingTable.getIndex()));
        }
        RoutingTable newRoutingTableWithFullDiff = fullDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTableWithFullDiff.version());
            assertEquals(indexRoutingTable, newRoutingTableWithFullDiff.index(indexRoutingTable.getIndex()));
        }
    }

    public void testRoutingTableDiffWithReplicaRemoved() {
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards));
        initPrimaries();
        int expectedUnassignedShardCount = this.totalNumberOfShards - 2 * this.numberOfShards;
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(expectedUnassignedShardCount));
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(2 * this.numberOfShards));
        ClusterState oldClusterState = clusterState;

        // update replica count for TEST_INDEX_1
        RoutingTable updatedRoutingTable = RoutingTable.builder(clusterState.routingTable())
            .updateNumberOfReplicas(this.numberOfReplicas - 1, new String[] { TEST_INDEX_1 })
            .build();
        Metadata metadata = Metadata.builder(clusterState.metadata())
            .updateNumberOfReplicas(this.numberOfReplicas - 1, new String[] { TEST_INDEX_1 })
            .build();
        clusterState = ClusterState.builder(clusterState).routingTable(updatedRoutingTable).metadata(metadata).build();
        assertThat(
            clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(expectedUnassignedShardCount - this.numberOfShards)
        );
        Diff<RoutingTable> fullDiff = clusterState.routingTable().diff(oldClusterState.getRoutingTable());
        Diff<RoutingTable> incrementalDiff = clusterState.routingTable().incrementalDiff(oldClusterState.getRoutingTable());
        RoutingTable newRoutingTable = incrementalDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTable.version());
            assertEquals(indexRoutingTable, newRoutingTable.index(indexRoutingTable.getIndex()));
        }
        RoutingTable newRoutingTableWithFullDiff = fullDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTableWithFullDiff.version());
            assertEquals(indexRoutingTable, newRoutingTableWithFullDiff.index(indexRoutingTable.getIndex()));
        }
    }

    public void testRoutingTableDiffsWithStartedState() {
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards));
        initPrimaries();
        assertThat(
            clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(this.totalNumberOfShards - 2 * this.numberOfShards)
        );
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(2 * this.numberOfShards));

        startInitializingShards(TEST_INDEX_1);
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.STARTED).size(), is(this.numberOfShards));
        int initializingExpected = this.numberOfShards + this.numberOfShards * this.numberOfReplicas;
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(initializingExpected));
        assertThat(
            clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(this.totalNumberOfShards - initializingExpected - this.numberOfShards)
        );

        startInitializingShards(TEST_INDEX_2);
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.STARTED).size(), is(2 * this.numberOfShards));
        initializingExpected = 2 * this.numberOfShards * this.numberOfReplicas;
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(initializingExpected));
        assertThat(
            clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(this.totalNumberOfShards - initializingExpected - 2 * this.numberOfShards)
        );
        ClusterState oldClusterState = clusterState;
        // start a random replica to change a single shard routing
        clusterState = startRandomInitializingShard(clusterState, ALLOCATION_SERVICE);
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(initializingExpected - 1));
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.STARTED).size(), is(2 * this.numberOfShards + 1));
        Diff<RoutingTable> fullDiff = clusterState.routingTable().diff(oldClusterState.getRoutingTable());
        Diff<RoutingTable> incrementalDiff = clusterState.routingTable().incrementalDiff(oldClusterState.getRoutingTable());
        RoutingTable newRoutingTable = incrementalDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTable.version());
            assertEquals(indexRoutingTable, newRoutingTable.index(indexRoutingTable.getIndex()));
        }
        RoutingTable newRoutingTableWithFullDiff = fullDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTableWithFullDiff.version());
            assertEquals(indexRoutingTable, newRoutingTableWithFullDiff.index(indexRoutingTable.getIndex()));
        }
    }

}
